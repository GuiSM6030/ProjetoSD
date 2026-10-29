#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <zmq.h>

#define MAX_FRAMES 8

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
                if (count == 4 && zmq_msg_size(&frames[1]) == 0) {
                    const char *server_id = zmq_msg_data(&frames[2]);
                    printf("REQ roteado para %.*s (%zu bytes)\n",
                           (int)zmq_msg_size(&frames[2]), server_id,
                           zmq_msg_size(&frames[3]));
                    fflush(stdout);
                    if (send_frame(backend, &frames[2], 1) == 0 &&
                        send_frame(backend, &frames[0], 1) == 0) {
                        (void)send_frame(backend, &frames[3], 0);
                    }
                } else {
                    fprintf(stderr, "REQ inválido: esperado 4 frames.\n");
                }
                close_frames(frames, count);
            }
        }

        if (items[1].revents & ZMQ_POLLIN) {
            zmq_msg_t frames[MAX_FRAMES];
            size_t count = 0;
            if (receive_frames(backend, frames, &count) == 0) {
                if (count == 3) {
                    if (send_frame(frontend, &frames[1], 1) == 0 &&
                        zmq_send(frontend, "", 0, ZMQ_SNDMORE) >= 0) {
                        (void)send_frame(frontend, &frames[2], 0);
                    }
                } else {
                    fprintf(stderr, "REP inválido: esperado 3 frames.\n");
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
