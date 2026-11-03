import os
import re
import signal
import time

import zmq
from google.protobuf import text_format
from google.protobuf.message import DecodeError

import bbs_pb2
from clocks import LogicalClock, PhysicalClockOffset, unix_time_ms
from server.reference_client import ReferenceClient
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
    relogio_logico=None, relogio_fisico=None,
):
    if relogio_logico is not None:
        relogio_logico.on_receive(mensagem.logical_clock)
    resposta = bbs_pb2.Envelope()

    def finalizar_resposta():
        resposta.sent_at_unix_ms = (
            relogio_fisico.now_ms() if relogio_fisico else unix_time_ms()
        )
        resposta.message_id = f"server-{time.time_ns()}"
        resposta.logical_clock = (
            relogio_logico.before_send() if relogio_logico else 0
        )
        return resposta

    if mensagem.sent_at_unix_ms <= 0:
        resposta.error_response.error_code = "MISSING_TIMESTAMP"
        resposta.error_response.detail = (
            "Toda mensagem deve incluir um timestamp de envio válido."
        )
        return finalizar_resposta()

    tipo = mensagem.WhichOneof("payload")

    if tipo == "login_request":
        nome = mensagem.login_request.username
        if not usuario_valido.fullmatch(nome):
            resposta.login_response.error_code = "INVALID_USERNAME"
            resposta.login_response.detail = (
                "Use de 1 a 32 letras, números, '_' ou '-'."
            )
        else:
            dados.record_login(nome, mensagem.sent_at_unix_ms)
            usuarios[cliente_id] = nome
            resposta.login_response.accepted = True
            resposta.login_response.detail = "Login realizado."
        return finalizar_resposta()

    if cliente_id not in usuarios:
        resposta.error_response.error_code = "LOGIN_REQUIRED"
        resposta.error_response.detail = "Faça login antes de acessar os canais."
        return resposta

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
            nome, usuarios[cliente_id], mensagem.sent_at_unix_ms
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
            dados.record_exchange(
                "client_to_server", "publish_request", mensagem.message_id,
                usuarios[cliente_id], pedido.channel_name, pedido.content,
                mensagem.sent_at_unix_ms,
                relogio_fisico.now_ms() if relogio_fisico else unix_time_ms(),
            )
            dados.record_publication(
                mensagem.message_id, pedido.channel_name, usuarios[cliente_id],
                pedido.content, mensagem.sent_at_unix_ms,
            )
            if publisher is not None:
                publicacao = criar_mensagem(relogio_logico, relogio_fisico)
                publicacao.publication.channel_name = pedido.channel_name
                publicacao.publication.content = pedido.content
                publicacao.message_id = mensagem.message_id
                publisher.send_multipart([
                    pedido.channel_name.encode("utf-8"),
                    publicacao.SerializeToString(),
                ])
            resposta.publish_response.published = True
            resposta.publish_response.detail = "Publicação aceita pelo servidor."
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
    referencia.solicitar_lista()

    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(endereco)
    socket.send_multipart([b"READY", servidor_id.encode("utf-8")])

    publisher = context.socket(zmq.PUB)
    publisher.setsockopt(zmq.LINGER, 0)
    publisher.connect(os.environ.get("PUBSUB_XSUB", "tcp://pubsub-proxy:5557"))

    signal.signal(signal.SIGTERM, parar)
    signal.signal(signal.SIGINT, parar)
    print(f"Servidor {servidor_id} conectado ao broker.", flush=True)
    mensagens_recebidas = 0

    def sincronizar_referencia():
        nonlocal rank
        if rank is None:
            rank = referencia.solicitar_rank(servidor_id)
        referencia.enviar_heartbeat(servidor_id, rank)
        referencia.solicitar_lista()

    try:
        while executando:
            socket.send_multipart([b"READY", servidor_id.encode("utf-8")])
            print(f"Pronto: heartbeat {servidor_id} enviado ao broker.", flush=True)
            if not socket.poll(500, zmq.POLLIN):
                continue

            frames = socket.recv_multipart()
            if len(frames) != 2:
                print(f"Mensagem inválida: {len(frames)} frames.", flush=True)
                continue

            cliente_id, conteudo = frames
            mensagens_recebidas += 1
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
                if mensagens_recebidas % 10 == 0:
                    sincronizar_referencia()
                continue

            print(
                f"RECEBIDO do cliente "
                f"{cliente_id.decode('utf-8', errors='replace')}:\n"
                f"{text_format.MessageToString(mensagem).strip()}",
                flush=True,
            )
            resposta = processar_mensagem(
                cliente_id, mensagem, dados, usuarios, publisher,
                relogio_logico, relogio_fisico,
            )
            print(
                f"ENVIADO ao cliente "
                f"{cliente_id.decode('utf-8', errors='replace')}:\n"
                f"{text_format.MessageToString(resposta).strip()}",
                flush=True,
            )
            socket.send_multipart([cliente_id, resposta.SerializeToString()])
            if mensagens_recebidas % 10 == 0:
                sincronizar_referencia()
    finally:
        socket.close(0)
        publisher.close(0)
        context.term()
        dados.close()


if __name__ == "__main__":
    executar()
