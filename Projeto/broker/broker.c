#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <zmq.h>

#define MAX_FRAMES 8
#define MAX_SERVERS 16

static char server_names[MAX_SERVERS][64];
static size_t server_name_sizes[MAX_SERVERS];
static zmq_msg_t server_routes[MAX_SERVERS];
static size_t server_count = 0;

static zmq_msg_t *find_server_route(const char *name, size_t name_size) {
    for (size_t i = 0; i < server_count; ++i) {
        if (name_size == server_name_sizes[i] &&
            memcmp(server_names[i], name, name_size) == 0) {
            return &server_routes[i];
        }
    }
    return NULL;
}

static int receive_frames(void *socket, zmq_msg_t frames[MAX_FRAMES],
                          size_t *frame_count) {
    *frame_count = 0;
    do {
        if (*frame_count == MAX_FRAMES) {
            fprintf(stderr, "Mensagem excede o limite de frames.\n");
            return -1;
        }

        zmq_msg_init(&frames[*frame_count]);
        if (zmq_msg_recv(&frames[*frame_count], socket, 0) < 0) {
            zmq_msg_close(&frames[*frame_count]);
            return -1;
        }
        ++(*frame_count);

        int more = 0;
        size_t more_size = sizeof(more);
        if (zmq_getsockopt(socket, ZMQ_RCVMORE, &more, &more_size) != 0) {
            return -1;
        }
        if (!more) {
            break;
        }
    } while (1);

    return 0;
}

static int send_frame(void *socket, zmq_msg_t *frame, int more) {
    int flags = more ? ZMQ_SNDMORE : 0;
    if (zmq_msg_send(frame, socket, flags) < 0) {
        fprintf(stderr, "Falha ao encaminhar frame: %s\n", zmq_strerror(errno));
        return -1;
    }
    return 0;
}

static void close_frames(zmq_msg_t frames[MAX_FRAMES], size_t frame_count) {
    for (size_t i = 0; i < frame_count; ++i) {
        zmq_msg_close(&frames[i]);
    }
}

int main(void) {
    void *context = zmq_ctx_new();
    if (context == NULL) {
        fprintf(stderr, "Não foi possível criar o contexto ZeroMQ.\n");
        return 1;
    }

    void *frontend = zmq_socket(context, ZMQ_ROUTER);
    void *backend = zmq_socket(context, ZMQ_ROUTER);
    if (frontend == NULL || backend == NULL) {
        fprintf(stderr, "Não foi possível inicializar o ZeroMQ.\n");
        if (frontend != NULL) {
            zmq_close(frontend);
        }
        if (backend != NULL) {
            zmq_close(backend);
        }
        zmq_ctx_term(context);
        return 1;
    }

    int linger = 0;
    int mandatory = 1;
    zmq_setsockopt(frontend, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_setsockopt(backend, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_setsockopt(backend, ZMQ_ROUTER_MANDATORY, &mandatory,
                   sizeof(mandatory));

    if (zmq_bind(frontend, "tcp://*:5555") != 0 ||
        zmq_bind(backend, "tcp://*:5556") != 0) {
        fprintf(stderr, "Falha ao abrir portas ZeroMQ: %s\n",
                zmq_strerror(errno));
        zmq_close(frontend);
        zmq_close(backend);
        zmq_ctx_term(context);
        return 1;
    }
    printf("Broker ativo: clientes em :5555, servidores em :5556\n");
    fflush(stdout);

    zmq_pollitem_t items[] = {
        {frontend, 0, ZMQ_POLLIN, 0},
        {backend, 0, ZMQ_POLLIN, 0},
    };

    while (1) {
        if (zmq_poll(items, 2, -1) < 0) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "Falha no poll: %s\n", zmq_strerror(errno));
            break;
        }

        if (items[0].revents & ZMQ_POLLIN) {
            zmq_msg_t frames[MAX_FRAMES];
            size_t count = 0;
            if (receive_frames(frontend, frames, &count) == 0) {
                if ((count == 4 && zmq_msg_size(&frames[1]) == 0) || count == 3) {
                    size_t server_frame = count == 4 ? 2 : 1;
                    size_t payload_frame = count == 4 ? 3 : 2;
                    const char *server_id = zmq_msg_data(&frames[server_frame]);
                    printf("REQ roteado para %.*s (%zu bytes)\n",
                           (int)zmq_msg_size(&frames[server_frame]), server_id,
                           zmq_msg_size(&frames[payload_frame]));
                    fflush(stdout);
                    zmq_msg_t *route = find_server_route(server_id,
                                                         zmq_msg_size(&frames[server_frame]));
                    if (route != NULL &&
                        zmq_send(backend, zmq_msg_data(route), zmq_msg_size(route), ZMQ_SNDMORE) >= 0 &&
                        send_frame(backend, &frames[0], 1) == 0) {
                        (void)send_frame(backend, &frames[payload_frame], 0);
                    } else {
                        fprintf(stderr, "Servidor %.*s não registrado.\n",
                                (int)zmq_msg_size(&frames[server_frame]), server_id);
                    }
                } else {
                    fprintf(stderr, "REQ inválido: esperado 3 ou 4 frames, recebido %zu.\n", count);
                }
                close_frames(frames, count);
            }
        }

        if (items[1].revents & ZMQ_POLLIN) {
            zmq_msg_t frames[MAX_FRAMES];
            size_t count = 0;
            if (receive_frames(backend, frames, &count) == 0) {
                fprintf(stderr, "Backend recebeu %zu frames", count);
                for (size_t i = 0; i < count; ++i) {
                    fprintf(stderr, " [%zu:%zu]", i, zmq_msg_size(&frames[i]));
                }
                fprintf(stderr, "\n");
                if (count == 3) {
                    if (send_frame(frontend, &frames[1], 1) == 0 &&
                        zmq_send(frontend, "", 0, ZMQ_SNDMORE) >= 0) {
                        (void)send_frame(frontend, &frames[2], 0);
                    }
                } else if (count == 3 && zmq_msg_size(&frames[1]) == 5 &&
                           memcmp(zmq_msg_data(&frames[1]), "READY", 5) == 0) {
                    size_t name_size = zmq_msg_size(&frames[2]);
                    if (name_size > 0 && name_size < sizeof(server_names[0]) &&
                        server_count < MAX_SERVERS) {
                        memcpy(server_names[server_count], zmq_msg_data(&frames[2]), name_size);
                        server_names[server_count][name_size] = '\0';
                        server_name_sizes[server_count] = name_size;
                        zmq_msg_init_size(&server_routes[server_count], zmq_msg_size(&frames[0]));
                        memcpy(zmq_msg_data(&server_routes[server_count]),
                               zmq_msg_data(&frames[0]), zmq_msg_size(&frames[0]));
                        ++server_count;
                    }
                    printf("Servidor registrado: %.*s\n",
                           (int)name_size, (char *)zmq_msg_data(&frames[2]));
                    fflush(stdout);
                } else {
                    fprintf(stderr, "REP inválido: esperado 3 frames, recebido %zu.\n", count);
                }
                close_frames(frames, count);
            }
        }
    }

    zmq_close(frontend);
    zmq_close(backend);
    zmq_ctx_term(context);
    return 0;
}
