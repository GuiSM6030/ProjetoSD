import os
import signal
import time
import uuid

import zmq
from google.protobuf.message import DecodeError

import bbs_pb2
from clocks import LogicalClock, unix_time_ms


class ReferenceRegistry:
    def __init__(self, server_names=(), timeout_seconds=60):
        nomes = list(dict.fromkeys(nome for nome in server_names if nome))
        self.ranks = {nome: indice for indice, nome in enumerate(nomes, 1)}
        self.next_rank = len(self.ranks) + 1
        self.servers = {}
        self.timeout_seconds = timeout_seconds

    def register(self, name, now=None):
        if not name:
            raise ValueError("O nome do servidor não pode ser vazio.")
        if name not in self.ranks:
            self.ranks[name] = self.next_rank
            self.next_rank += 1
        self.servers[name] = {
            "rank": self.ranks[name],
            "last_heartbeat": time.monotonic() if now is None else now,
        }
        return self.ranks[name]

    def heartbeat(self, name, rank, now=None):
        if name not in self.ranks or self.ranks[name] != rank:
            return False
        self.servers[name] = {
            "rank": rank,
            "last_heartbeat": time.monotonic() if now is None else now,
        }
        return True

    def expire(self, now=None):
        current_time = time.monotonic() if now is None else now
        expired = [
            name for name, server in self.servers.items()
            if current_time - server["last_heartbeat"] > self.timeout_seconds
        ]
        for name in expired:
            del self.servers[name]
        return expired

    def list_available(self, now=None):
        self.expire(now)
        return [
            {"name": name, "rank": server["rank"]}
            for name, server in sorted(
                self.servers.items(), key=lambda item: item[1]["rank"]
            )
        ]


class ReferenceService:
    def __init__(self, registry):
        self.registry = registry
        self.logical_clock = LogicalClock()

    def handle(self, request):
        self.logical_clock.on_receive(request.logical_clock)
        action = request.reference_request.action
        details = bbs_pb2.ReferenceResponse(ok=True, detail="OK")
        if action == "rank":
            try:
                details.rank = self.registry.register(
                    request.reference_request.name
                )
            except ValueError as error:
                details.ok = False
                details.detail = str(error)
        elif action == "list":
            for server in self.registry.list_available():
                details.servers.add(
                    name=server["name"],
                    rank=server["rank"],
                )
        elif action == "heartbeat":
            if not self.registry.heartbeat(
                request.reference_request.name,
                request.reference_request.rank,
            ):
                details.ok = False
                details.detail = "Servidor ou rank não cadastrado."
        else:
            details.ok = False
            details.detail = "Ação desconhecida."

        response = bbs_pb2.ControlEnvelope(
            sent_at_unix_ms=unix_time_ms(),
            message_id=str(uuid.uuid4()),
            logical_clock=self.logical_clock.before_send(),
        )
        response.reference_response.CopyFrom(details)
        return response


executando = True


def parar(_sinal, _quadro):
    global executando
    executando = False


def executar():
    global executando
    endpoint = os.environ.get("REFERENCE_ENDPOINT", "tcp://*:5559")
    nomes = os.environ.get("SERVER_IDS", "server-1,server-2").split(",")
    timeout = float(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "60"))
    registry = ReferenceRegistry(nomes, timeout)
    service = ReferenceService(registry)

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.setsockopt(zmq.LINGER, 0)
    socket.bind(endpoint)
    signal.signal(signal.SIGTERM, parar)
    signal.signal(signal.SIGINT, parar)
    print(
        f"Referência escutando em {endpoint}; timeout de heartbeat={timeout}s.",
        flush=True,
    )

    try:
        while executando:
            if not socket.poll(1000, zmq.POLLIN):
                registry.expire()
                continue
            try:
                pedido = bbs_pb2.ControlEnvelope()
                pedido.ParseFromString(socket.recv())
            except DecodeError:
                pedido = bbs_pb2.ControlEnvelope()
                pedido.reference_request.action = "invalid"
            resposta = service.handle(pedido)
            print(
                f"REQ {pedido.reference_request} | "
                f"REP {resposta.reference_response}",
                flush=True,
            )
            socket.send(resposta.SerializeToString())
            registry.expire()
    finally:
        socket.close(0)
        context.term()


if __name__ == "__main__":
    executar()