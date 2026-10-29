import os
import random
import time
import uuid

import zmq
from google.protobuf import text_format

import bbs_pb2


usuario = os.environ.get("BOT_NAME", "bot-alice")
canal_inicial = os.environ.get("CHANNEL_NAME", "geral")
broker = os.environ.get("BROKER_FRONTEND", "tcp://broker:5555")
pubsub = os.environ.get("PUBSUB_XPUB", "tcp://pubsub-proxy:5558")
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
        socket.send_multipart([servidor_id.encode("utf-8"), mensagem.SerializeToString()])
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
    try:
        resposta = enviar_mensagem(
            servidor_id,
            criar_mensagem("login_request", bbs_pb2.LoginRequest(username=usuario)),
        )
    except zmq.error.Again:
        print(f"Servidor {servidor_id} indisponível durante o login.", flush=True)
        return False

    if (resposta.WhichOneof("payload") != "login_response"
            or not resposta.login_response.accepted):
        print(f"Login recusado em {servidor_id}.", flush=True)
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
        inicio = servidores.index(servidor_atual) if servidor_atual in servidores else 0
        lista = servidores[inicio:] + servidores[:inicio]
        for servidor_id in lista:
            if servidor_id != servidor_atual and not fazer_login(servidor_id):
                continue
            try:
                resposta = enviar_mensagem(
                    servidor_id, criar_mensagem(tipo, dados)
                )
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
        return None
    canais = list(resposta.list_channels_response.channel_names)
    print(f"Canais disponíveis: {canais}", flush=True)
    return canais


def criar_canal(nome):
    resposta = enviar_com_tentativa(
        "create_channel_request", bbs_pb2.CreateChannelRequest(name=nome)
    )
    if resposta.WhichOneof("payload") == "create_channel_response":
        resultado = resposta.create_channel_response
        if not resultado.created and resultado.error_code != "CHANNEL_EXISTS":
            print(f"Falha ao criar canal: {resultado.detail}", flush=True)


def preparar_canais(subscriber):
    canais = listar_canais() or []
    if canal_inicial not in canais:
        criar_canal(canal_inicial)

    for numero in range(1, 6):
        candidato = f"canal-{numero}"
        if numero <= 4 or candidato == canal_inicial:
            if candidato not in canais:
                criar_canal(candidato)

    canais = listar_canais() or []
    if len(canais) < 5:
        for numero in range(5, 12):
            candidato = f"canal-{numero}"
            if candidato not in canais and len(canais) < 5:
                criar_canal(candidato)
                canais.append(candidato)

    inscritos = []
    while len(inscritos) < min(3, len(canais)):
        escolha = random.choice(canais)
        if escolha not in inscritos:
            inscritos.append(escolha)
            subscriber.setsockopt(zmq.SUBSCRIBE, escolha.encode("utf-8"))
    print(f"Assinado nos canais: {inscritos}", flush=True)
    return canais


def receber(subscriber):
    while subscriber.poll(0, zmq.POLLIN):
        frames = subscriber.recv_multipart()
        if len(frames) != 2:
            continue
        canal = frames[0].decode("utf-8", errors="replace")
        publicacao = bbs_pb2.Envelope()
        publicacao.ParseFromString(frames[1])
        recebido_em = time.time_ns() // 1_000_000
        print(
            f"RECEBIDO canal={canal} conteúdo={publicacao.publication.content} "
            f"enviado_em={publicacao.sent_at_unix_ms} recebido_em={recebido_em}",
            flush=True,
        )


def executar():
    if not servidores:
        raise ValueError("SERVER_IDS deve conter ao menos um servidor.")
    conectar()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.connect(pubsub)
    canais = preparar_canais(subscriber)
    textos = ["mensagem distribuida", "novo evento", "teste pubsub", "olá bbs"]

    while True:
        canal = random.choice(canais)
        for numero in range(1, 11):
            recebida_em = time.time_ns() // 1_000_000
            texto = f"{usuario}: {random.choice(textos)} {numero}"
            resposta = enviar_com_tentativa(
                "publish_request",
                bbs_pb2.PublishRequest(channel_name=canal, content=texto),
            )
            receber(subscriber)
            if subscriber.poll(1000, zmq.POLLIN):
                receber(subscriber)
            print(
                f"PUBLICAÇÃO pedido={recebida_em} canal={canal} resposta={resposta.publish_response.detail}",
                flush=True,
            )


if __name__ == "__main__":
    executar()
