import os
import time
import uuid

import zmq
from google.protobuf import text_format

import bbs_pb2


usuario = os.environ.get("BOT_NAME", "bot-alice")
canal = os.environ.get("CHANNEL_NAME", "geral")
broker = os.environ.get("BROKER_FRONTEND", "tcp://broker:5555")
servidores = [
    item.strip()
    for item in os.environ.get("SERVER_IDS", "server-1,server-2").split(",")
    if item.strip()
]
timeout = int(os.environ.get("REQUEST_TIMEOUT_MS", "1500"))
cliente_id = f"{usuario}-{uuid.uuid4().hex[:8]}"
context = zmq.Context()
servidor_atual = None


def criar_mensagem(tipo, dados):
    mensagem = bbs_pb2.Envelope(
        sent_at_unix_ms=time.time_ns() // 1_000_000,
        message_id=str(uuid.uuid4()),
    )
    getattr(mensagem, tipo).CopyFrom(dados)
    return mensagem


def enviar_mensagem(servidor_id, mensagem):
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.IDENTITY, cliente_id.encode("utf-8"))
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.SNDTIMEO, timeout)
    socket.setsockopt(zmq.RCVTIMEO, timeout)
    socket.connect(broker)

    print(
        f"ENVIADO para {servidor_id}:\n"
        f"{text_format.MessageToString(mensagem).strip()}",
        flush=True,
    )
    try:
        socket.send_multipart(
            [servidor_id.encode("utf-8"), mensagem.SerializeToString()]
        )
        resposta = bbs_pb2.Envelope()
        resposta.ParseFromString(socket.recv())
        print(
            f"RECEBIDO de {servidor_id}:\n"
            f"{text_format.MessageToString(resposta).strip()}",
            flush=True,
        )
        return resposta
    finally:
        socket.close(0)


def fazer_login(servidor_id):
    global servidor_atual

    mensagem = criar_mensagem(
        "login_request", bbs_pb2.LoginRequest(username=usuario)
    )
    try:
        resposta = enviar_mensagem(servidor_id, mensagem)
    except zmq.error.Again:
        print(f"Servidor {servidor_id} indisponível durante o login.", flush=True)
        return False

    if resposta.WhichOneof("payload") != "login_response":
        print(f"Resposta de login inválida de {servidor_id}.", flush=True)
        return False
    if not resposta.login_response.accepted:
        print(
            f"Login recusado: {resposta.login_response.error_code} - "
            f"{resposta.login_response.detail}",
            flush=True,
        )
        return False

    servidor_atual = servidor_id
    print(f"Login de {usuario} realizado em {servidor_id}.", flush=True)
    return True


def conectar():
    while True:
        for servidor_id in servidores:
            if fazer_login(servidor_id):
                return
        print("Nenhum servidor disponível. Nova tentativa em 2 s.", flush=True)
        time.sleep(2)


def enviar_com_tentativa(tipo, dados):
    global servidor_atual

    while True:
        if servidor_atual in servidores:
            inicio = servidores.index(servidor_atual)
        else:
            inicio = 0
        lista_servidores = servidores[inicio:] + servidores[:inicio]

        for servidor_id in lista_servidores:
            if servidor_id != servidor_atual and not fazer_login(servidor_id):
                continue

            mensagem = criar_mensagem(tipo, dados)
            try:
                resposta = enviar_mensagem(servidor_id, mensagem)
                servidor_atual = servidor_id
                return resposta
            except zmq.error.Again:
                print(f"Servidor {servidor_id} indisponível.", flush=True)
                servidor_atual = None

        print("Nenhum servidor disponível. Nova tentativa em 2 s.", flush=True)
        time.sleep(2)


def listar_canais():
    resposta = enviar_com_tentativa(
        "list_channels_request", bbs_pb2.ListChannelsRequest()
    )
    if resposta.WhichOneof("payload") != "list_channels_response":
        print("Resposta inesperada ao listar canais.", flush=True)
        return None

    canais = list(resposta.list_channels_response.channel_names)
    print(f"Canais disponíveis: {canais}", flush=True)
    return canais


def executar():
    if not servidores:
        raise ValueError("SERVER_IDS deve conter ao menos um servidor.")

    conectar()
    while True:
        canais = listar_canais()
        if canais is not None and canal not in canais:
            resposta = enviar_com_tentativa(
                "create_channel_request",
                bbs_pb2.CreateChannelRequest(name=canal),
            )
            if resposta.WhichOneof("payload") == "create_channel_response":
                resultado = resposta.create_channel_response
                if not resultado.created and resultado.error_code != "CHANNEL_EXISTS":
                    print(
                        f"Falha ao criar canal: {resultado.error_code} - "
                        f"{resultado.detail}",
                        flush=True,
                    )
            listar_canais()
        time.sleep(10)


if __name__ == "__main__":
    executar()
