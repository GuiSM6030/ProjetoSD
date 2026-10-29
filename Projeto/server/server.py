import os
import re
import signal
import time

import zmq
from google.protobuf import text_format
from google.protobuf.message import DecodeError

import bbs_pb2
from .store import Store


usuario_valido = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
canal_valido = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$")
executando = True


def criar_mensagem():
    return bbs_pb2.Envelope(
        sent_at_unix_ms=time.time_ns() // 1_000_000,
        message_id=f"server-{time.time_ns()}",
    )


def processar_mensagem(cliente_id, mensagem, dados, usuarios):
    resposta = criar_mensagem()
    if mensagem.sent_at_unix_ms <= 0:
        resposta.error_response.error_code = "MISSING_TIMESTAMP"
        resposta.error_response.detail = (
            "Toda mensagem deve incluir um timestamp de envio válido."
        )
        return resposta

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
        return resposta

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
    else:
        resposta.error_response.error_code = "UNKNOWN_REQUEST"
        resposta.error_response.detail = "Tipo de mensagem não reconhecido."

    return resposta


def parar(_sinal, _quadro):
    global executando
    executando = False


def executar():
    global executando

    servidor_id = os.environ.get("SERVER_ID", "server-1")
    endereco = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
    arquivo = os.environ.get("DATABASE_PATH", "/data/server.sqlite3")
    dados = Store(arquivo)
    usuarios = {}

    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, servidor_id.encode("utf-8"))
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(endereco)

    signal.signal(signal.SIGTERM, parar)
    signal.signal(signal.SIGINT, parar)
    print(f"Servidor {servidor_id} conectado ao broker.", flush=True)

    try:
        while executando:
            if not socket.poll(500, zmq.POLLIN):
                continue

            frames = socket.recv_multipart()
            if len(frames) != 2:
                print(f"Mensagem inválida: {len(frames)} frames.", flush=True)
                continue

            cliente_id, conteudo = frames
            mensagem = bbs_pb2.Envelope()
            try:
                mensagem.ParseFromString(conteudo)
            except DecodeError as erro:
                print(f"Protobuf inválido: {erro}", flush=True)
                resposta = criar_mensagem()
                resposta.error_response.error_code = "INVALID_PROTOBUF"
                resposta.error_response.detail = (
                    "A mensagem não é um Envelope Protobuf válido."
                )
                socket.send_multipart(
                    [cliente_id, resposta.SerializeToString()]
                )
                continue

            print(
                f"RECEBIDO do cliente "
                f"{cliente_id.decode('utf-8', errors='replace')}:\n"
                f"{text_format.MessageToString(mensagem).strip()}",
                flush=True,
            )
            resposta = processar_mensagem(
                cliente_id, mensagem, dados, usuarios
            )
            print(
                f"ENVIADO ao cliente "
                f"{cliente_id.decode('utf-8', errors='replace')}:\n"
                f"{text_format.MessageToString(resposta).strip()}",
                flush=True,
            )
            socket.send_multipart([cliente_id, resposta.SerializeToString()])
    finally:
        socket.close(0)
        context.term()
        dados.close()


if __name__ == "__main__":
    executar()
