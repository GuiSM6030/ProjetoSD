import os
import re
import signal
import time

import zmq
from google.protobuf import text_format
from google.protobuf.message import DecodeError

import bbs_pb2
from clocks import LogicalClock, PhysicalClockOffset, unix_time_ms
from server.coordination import ServerCoordinator
from server.reference_client import ReferenceClient
from server.replication import ReplicationWorker
from server.control import (
    peer_request_from_proto,
    peer_response_to_proto,
)
from .store import Store


usuario_valido = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
canal_valido = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$")
executando = True


def criar_mensagem(relogio_logico=None, relogio_fisico=None):
    return bbs_pb2.Envelope(
        sent_at_unix_ms=(
            relogio_fisico.now_ms() if relogio_fisico else unix_time_ms()
        ),
        message_id=f"server-{time.time_ns()}",
        logical_clock=(
            relogio_logico.before_send() if relogio_logico else 0
        ),
    )


def processar_mensagem(
    cliente_id, mensagem, dados, usuarios, publisher=None,
    relogio_logico=None, relogio_fisico=None, origin="unknown",
):
    if relogio_logico is not None:
        relogio_logico.on_receive(mensagem.logical_clock)
    resposta = bbs_pb2.Envelope()
    tipo = mensagem.WhichOneof("payload") or "unknown"

    def finalizar_resposta():
        resposta.sent_at_unix_ms = (
            relogio_fisico.now_ms() if relogio_fisico else unix_time_ms()
        )
        resposta.message_id = f"server-{time.time_ns()}"
        resposta.logical_clock = (
            relogio_logico.before_send() if relogio_logico else 0
        )
        usuario = usuarios.get(cliente_id, "")
        canal = None
        conteudo = text_format.MessageToString(mensagem).strip()
        if tipo == "create_channel_request":
            canal = mensagem.create_channel_request.name
        elif tipo == "publish_request":
            canal = mensagem.publish_request.channel_name
        recebida_em = (
            relogio_fisico.now_ms() if relogio_fisico else unix_time_ms()
        )
        dados.record_exchange(
            "client_to_server",
            tipo if tipo.endswith("_request") else f"{tipo}_request",
            mensagem.message_id,
            usuario, canal, conteudo, mensagem.sent_at_unix_ms,
            recebida_em, origin=origin,
        )
        response_type = resposta.WhichOneof("payload") or "unknown"
        dados.record_exchange(
            "server_to_client",
            response_type if response_type.endswith("_response")
            else f"{response_type}_response",
            mensagem.message_id, usuario, canal,
            text_format.MessageToString(resposta).strip(),
            resposta.sent_at_unix_ms, recebida_em, origin=origin,
        )
        return resposta

    if mensagem.sent_at_unix_ms <= 0:
        resposta.error_response.error_code = "MISSING_TIMESTAMP"
        resposta.error_response.detail = (
            "Toda mensagem deve incluir um timestamp de envio válido."
        )
        return finalizar_resposta()

    if tipo == "login_request":
        nome = mensagem.login_request.username
        if not usuario_valido.fullmatch(nome):
            resposta.login_response.error_code = "INVALID_USERNAME"
            resposta.login_response.detail = (
                "Use de 1 a 32 letras, números, '_' ou '-'."
            )
        else:
            dados.record_login(
                nome, mensagem.sent_at_unix_ms,
                event_id=f"login:{mensagem.message_id}", origin=origin,
            )
            usuarios[cliente_id] = nome
            resposta.login_response.accepted = True
            resposta.login_response.detail = "Login realizado."
        return finalizar_resposta()

    if cliente_id not in usuarios:
        resposta.error_response.error_code = "LOGIN_REQUIRED"
        resposta.error_response.detail = "Faça login antes de acessar os canais."
        return finalizar_resposta()

    if tipo == "list_channels_request":
        resposta.list_channels_response.channel_names.extend(
            dados.list_channels()
        )
    elif tipo == "create_channel_request":
        nome = mensagem.create_channel_request.name
        if not canal_valido.fullmatch(nome):
            resposta.create_channel_response.error_code = "INVALID_CHANNEL_NAME"
            resposta.create_channel_response.detail = (
                "Use de 1 a 32 caracteres; comece com letra ou número e "
                "use letras, números, '_' ou '-'."
            )
        elif dados.create_channel(
            nome, usuarios[cliente_id], mensagem.sent_at_unix_ms,
            event_id=f"channel:{mensagem.message_id}", origin=origin,
        ):
            resposta.create_channel_response.created = True
            resposta.create_channel_response.detail = "Canal criado."
        else:
            resposta.create_channel_response.error_code = "CHANNEL_EXISTS"
            resposta.create_channel_response.detail = "Esse canal já existe."
    elif tipo == "publish_request":
        pedido = mensagem.publish_request
        if not canal_valido.fullmatch(pedido.channel_name):
            resposta.publish_response.error_code = "INVALID_CHANNEL_NAME"
            resposta.publish_response.detail = "Nome de canal inválido."
        elif not 1 <= len(pedido.content.strip()) <= 1024:
            resposta.publish_response.error_code = "INVALID_CONTENT"
            resposta.publish_response.detail = "O conteúdo deve ter entre 1 e 1024 caracteres."
        elif not dados.channel_exists(pedido.channel_name):
            resposta.publish_response.error_code = "CHANNEL_NOT_FOUND"
            resposta.publish_response.detail = "O canal não existe neste servidor."
        else:
            dados.record_publication(
                mensagem.message_id, pedido.channel_name, usuarios[cliente_id],
                pedido.content, mensagem.sent_at_unix_ms, origin=origin,
            )
            if publisher is not None:
                publicacao = criar_mensagem(relogio_logico, relogio_fisico)
                publicacao.publication.channel_name = pedido.channel_name
                publicacao.publication.content = pedido.content
                publicacao.publication.username = usuarios[cliente_id]
                publicacao.publication.sent_at_unix_ms = mensagem.sent_at_unix_ms
                publicacao.message_id = mensagem.message_id
                publisher.send_multipart([
                    pedido.channel_name.encode("utf-8"),
                    publicacao.SerializeToString(),
                ])
                print(
                    f"PUBLICADO no tópico {pedido.channel_name}:\n"
                    f"{text_format.MessageToString(publicacao).strip()}",
                    flush=True,
                )
            resposta.publish_response.published = True
            resposta.publish_response.detail = "Publicação aceita pelo servidor."
    elif tipo == "history_request":
        canal = mensagem.history_request.channel_name
        if canal and not canal_valido.fullmatch(canal):
            resposta.error_response.error_code = "INVALID_CHANNEL_NAME"
            resposta.error_response.detail = "Nome de canal inválido."
        elif canal and not dados.channel_exists(canal):
            resposta.error_response.error_code = "CHANNEL_NOT_FOUND"
            resposta.error_response.detail = "O canal não existe neste servidor."
        else:
            for item in dados.publication_history(canal):
                resposta.history_response.publications.add(
                    channel_name=item["channel_name"],
                    username=item["username"],
                    content=item["content"],
                    sent_at_unix_ms=item["sent_at_unix_ms"],
                )
    else:
        resposta.error_response.error_code = "UNKNOWN_REQUEST"
        resposta.error_response.detail = "Tipo de mensagem não reconhecido."

    return finalizar_resposta()


def parar(_sinal, _quadro):
    global executando
    executando = False


def executar():
    global executando

    servidor_id = os.environ.get("SERVER_ID", "server-1")
    endereco = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
    arquivo = os.environ.get("DATABASE_PATH", "/data/server.sqlite3")
    referencia_endpoint = os.environ.get(
        "REFERENCE_ENDPOINT", "tcp://reference:5559"
    )
    dados = Store(arquivo)
    usuarios = {}
    relogio_logico = LogicalClock()
    relogio_fisico = PhysicalClockOffset()

    context = zmq.Context()
    referencia = ReferenceClient(
        context,
        referencia_endpoint,
        relogio_logico,
        relogio_fisico,
        timeout_ms=int(os.environ.get("REFERENCE_TIMEOUT_MS", "1000")),
    )
    rank = referencia.solicitar_rank(servidor_id)
    servidores_disponiveis = referencia.solicitar_lista()

    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(endereco)
    socket.send_multipart([b"READY", servidor_id.encode("utf-8")])

    publisher = context.socket(zmq.PUB)
    publisher.setsockopt(zmq.LINGER, 0)
    publisher.connect(os.environ.get("PUBSUB_XSUB", "tcp://pubsub-proxy:5557"))

    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"servers")
    subscriber.connect(os.environ.get("PUBSUB_XPUB", "tcp://pubsub-proxy:5558"))

    control_socket = context.socket(zmq.REP)
    control_socket.setsockopt(zmq.LINGER, 0)
    control_socket.bind(os.environ.get(
        "SERVER_CONTROL_ENDPOINT", "tcp://*:5561"
    ))
    replication_socket = context.socket(zmq.REP)
    replication_socket.setsockopt(zmq.LINGER, 0)
    replication_socket.bind("tcp://*:5560")

    coordenacao = ServerCoordinator(
        context,
        servidor_id,
        rank,
        relogio_logico,
        relogio_fisico,
        publisher,
        timeout_ms=int(os.environ.get("SERVER_CONTROL_TIMEOUT_MS", "700")),
        replication_store=dados,
    )
    if servidores_disponiveis is None:
        servidores_disponiveis = [
            {"name": nome, "rank": indice}
            for indice, nome in enumerate(
                [
                    item.strip()
                    for item in os.environ.get(
                        "SERVER_IDS", "server-1,server-2"
                    ).split(",")
                    if item.strip()
                ],
                start=1,
            )
        ]
    coordenacao.update_servers(servidores_disponiveis)
    replication_worker = ReplicationWorker(
        arquivo,
        servidor_id,
        [item.strip() for item in os.environ.get(
            "SERVER_IDS", "server-1,server-2"
        ).split(",") if item.strip()],
        timeout_ms=int(os.environ.get("SERVER_CONTROL_TIMEOUT_MS", "3000")),
    )
    if os.environ.get("REPLICATION_WORKER_ENABLED", "1") == "1":
        replication_worker.start()

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(control_socket, zmq.POLLIN)
    poller.register(replication_socket, zmq.POLLIN)
    poller.register(subscriber, zmq.POLLIN)

    signal.signal(signal.SIGTERM, parar)
    signal.signal(signal.SIGINT, parar)
    print(f"Servidor {servidor_id} conectado ao broker.", flush=True)
    mensagens_recebidas = 0

    def sincronizar_referencia():
        nonlocal rank
        if rank is None:
            rank = referencia.solicitar_rank(servidor_id)
            coordenacao.rank = rank or 0
        referencia.enviar_heartbeat(servidor_id, rank)
        servidores_atualizados = referencia.solicitar_lista()
        if servidores_atualizados is None:
            servidores_atualizados = servidores_disponiveis
        coordenacao.update_servers(servidores_atualizados)
        replication_worker.update_peers(
            item["name"] for item in servidores_atualizados or []
        )

    def sincronizar_relogio_se_necessario():
        if coordenacao.synchronization_due():
            coordenacao.synchronize_clock()

    try:
        while executando:
            socket.send_multipart([b"READY", servidor_id.encode("utf-8")])
            eventos = dict(poller.poll(500))

            if control_socket in eventos:
                try:
                    pedido_controle = bbs_pb2.ControlEnvelope()
                    pedido_controle.ParseFromString(control_socket.recv())
                    request_data = peer_request_from_proto(
                        pedido_controle.peer_request
                    )
                    request_data["logical_clock"] = (
                        pedido_controle.logical_clock
                    )
                except DecodeError:
                    request_data = {"action": "invalid"}
                coordenacao.note_message()
                response_data = coordenacao.handle_peer_request(request_data)
                resposta_controle = bbs_pb2.ControlEnvelope(
                    sent_at_unix_ms=response_data.get(
                        "sent_at_unix_ms", relogio_fisico.now_ms()
                    ),
                    message_id=response_data.get(
                        "message_id", f"server-{time.time_ns()}"
                    ),
                    logical_clock=response_data.get(
                        "logical_clock", relogio_logico.before_send()
                    ),
                )
                resposta_controle.peer_response.CopyFrom(
                    peer_response_to_proto(response_data)
                )
                control_socket.send(resposta_controle.SerializeToString())
                coordenacao.note_message()

            if replication_socket in eventos:
                pedido_replicacao = bbs_pb2.ControlEnvelope()
                try:
                    pedido_replicacao.ParseFromString(
                        replication_socket.recv()
                    )
                    replication_data = peer_request_from_proto(
                        pedido_replicacao.peer_request
                    )
                    replication_data["logical_clock"] = (
                        pedido_replicacao.logical_clock
                    )
                except DecodeError:
                    replication_data = {"action": "invalid"}
                if replication_data.get("action") == "replicate_pull":
                    response_data = {
                        "ok": True,
                        "events": dados.replication_events(),
                    }
                elif replication_data.get("action") == "replicate_apply":
                    applied = []
                    for event in replication_data.get("events", []):
                        if dados.apply_replication_event(event):
                            applied.append(event.get("event_id"))
                    response_data = {
                        "ok": True,
                        "applied": [item for item in applied if item],
                    }
                else:
                    response_data = {
                        "ok": False,
                        "detail": "Ação desconhecida.",
                    }
                resposta_replicacao = bbs_pb2.ControlEnvelope(
                    sent_at_unix_ms=relogio_fisico.now_ms(),
                    message_id=f"server-{time.time_ns()}",
                    logical_clock=relogio_logico.before_send(),
                )
                resposta_replicacao.peer_response.CopyFrom(
                    peer_response_to_proto(response_data)
                )
                replication_socket.send(
                    resposta_replicacao.SerializeToString()
                )

            if subscriber in eventos:
                frames_pubsub = subscriber.recv_multipart()
                if len(frames_pubsub) == 2 and frames_pubsub[0] == b"servers":
                    coordenacao.note_message()
                    try:
                        anuncio = bbs_pb2.ControlEnvelope()
                        anuncio.ParseFromString(frames_pubsub[1])
                    except DecodeError:
                        anuncio = None
                    if anuncio and anuncio.HasField(
                        "coordinator_announcement"
                    ):
                        coordenacao.receive_announcement({
                            "coordinator": (
                                anuncio.coordinator_announcement.coordinator
                            ),
                            "logical_clock": anuncio.logical_clock,
                        })

            if socket not in eventos:
                sincronizar_relogio_se_necessario()
                continue

            frames = socket.recv_multipart()
            if len(frames) != 2:
                print(f"Mensagem inválida: {len(frames)} frames.", flush=True)
                continue

            cliente_id, conteudo = frames
            mensagens_recebidas += 1
            coordenacao.note_message()
            mensagem = bbs_pb2.Envelope()
            try:
                mensagem.ParseFromString(conteudo)
            except DecodeError as erro:
                print(f"Protobuf inválido: {erro}", flush=True)
                resposta = criar_mensagem(relogio_logico, relogio_fisico)
                resposta.error_response.error_code = "INVALID_PROTOBUF"
                resposta.error_response.detail = (
                    "A mensagem não é um Envelope Protobuf válido."
                )
                socket.send_multipart(
                    [cliente_id, resposta.SerializeToString()]
                )
                coordenacao.note_message()
                if mensagens_recebidas % 10 == 0:
                    sincronizar_referencia()
                sincronizar_relogio_se_necessario()
                continue

            print(
                f"RECEBIDO do cliente "
                f"{cliente_id.decode('utf-8', errors='replace')}:\n"
                f"{text_format.MessageToString(mensagem).strip()}",
                flush=True,
            )
            resposta = processar_mensagem(
                cliente_id, mensagem, dados, usuarios, publisher,
                relogio_logico, relogio_fisico, servidor_id,
            )
            print(
                f"ENVIADO ao cliente "
                f"{cliente_id.decode('utf-8', errors='replace')}:\n"
                f"{text_format.MessageToString(resposta).strip()}",
                flush=True,
            )
            socket.send_multipart([cliente_id, resposta.SerializeToString()])
            coordenacao.note_message()
            if mensagens_recebidas % 10 == 0:
                sincronizar_referencia()
            sincronizar_relogio_se_necessario()
    finally:
        if os.environ.get("REPLICATION_WORKER_ENABLED", "1") == "1":
            replication_worker.stop()
        socket.close(0)
        publisher.close(0)
        subscriber.close(0)
        control_socket.close(0)
        replication_socket.close(0)
        context.term()
        dados.close()


if __name__ == "__main__":
    executar()
