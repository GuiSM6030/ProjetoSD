import uuid

import zmq
from google.protobuf.message import DecodeError

import bbs_pb2


class ReferenceClient:
    def __init__(
        self, context, endpoint, logical_clock, physical_clock, timeout_ms=1000
    ):
        self.context = context
        self.endpoint = endpoint
        self.logical_clock = logical_clock
        self.physical_clock = physical_clock
        self.timeout_ms = timeout_ms

    def _request(self, action, name=None, rank=None):
        request = bbs_pb2.ControlEnvelope(
            sent_at_unix_ms=self.physical_clock.now_ms(),
            message_id=str(uuid.uuid4()),
            logical_clock=self.logical_clock.before_send(),
        )
        request.reference_request.action = action
        if name is not None:
            request.reference_request.name = name
        if rank is not None:
            request.reference_request.rank = rank

        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        socket.connect(self.endpoint)
        try:
            print(f"REQ referência: {request.reference_request}", flush=True)
            socket.send(request.SerializeToString())
            response = bbs_pb2.ControlEnvelope()
            response.ParseFromString(socket.recv())
        except (zmq.error.Again, zmq.error.ZMQError, DecodeError) as erro:
            print(f"Referência indisponível ({action}): {erro}", flush=True)
            return None
        finally:
            socket.close(0)

        self.logical_clock.on_receive(response.logical_clock)
        if not response.HasField("reference_response"):
            print("A referência retornou uma resposta inválida.", flush=True)
            return None
        print(
            f"REP referência: {response.reference_response}",
            flush=True,
        )
        return response.reference_response

    def solicitar_rank(self, name):
        print(f"Rank solicitado para {name}.", flush=True)
        response = self._request("rank", name=name)
        if response and response.ok:
            print(f"Rank recebido para {name}: {response.rank}.", flush=True)
            return response.rank
        return None

    def solicitar_lista(self):
        response = self._request("list")
        if response and response.ok:
            servers = [
                {"name": server.name, "rank": server.rank}
                for server in response.servers
            ]
            print(f"Servidores disponíveis: {servers}.", flush=True)
            return servers
        return None

    def enviar_heartbeat(self, name, rank):
        print(f"Heartbeat enviado: servidor={name}, rank={rank}.", flush=True)
        return self._request("heartbeat", name=name, rank=rank)