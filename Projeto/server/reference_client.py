import uuid

import zmq

from clocks import unix_time_ms


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
        request = {
            "action": action,
            "sent_at_unix_ms": self.physical_clock.now_ms(),
            "message_id": str(uuid.uuid4()),
            "logical_clock": self.logical_clock.before_send(),
        }
        if name is not None:
            request["name"] = name
        if rank is not None:
            request["rank"] = rank

        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        socket.connect(self.endpoint)
        try:
            print(f"REQ referência: {request}", flush=True)
            socket.send_json(request)
            response = socket.recv_json()
        except (zmq.error.Again, ValueError) as erro:
            print(f"Referência indisponível ({action}): {erro}", flush=True)
            return None
        finally:
            socket.close(0)

        self.logical_clock.on_receive(response.get("logical_clock", 0))
        print(f"REP referência: {response}", flush=True)
        return response

    def solicitar_rank(self, name):
        print(f"Rank solicitado para {name}.", flush=True)
        response = self._request("rank", name=name)
        if response and response.get("ok"):
            print(f"Rank recebido para {name}: {response['rank']}.", flush=True)
            return response["rank"]
        return None

    def solicitar_lista(self):
        response = self._request("list")
        if response and response.get("ok"):
            print(f"Servidores disponíveis: {response['servers']}.", flush=True)
            return response["servers"]
        return None

    def enviar_heartbeat(self, name, rank):
        print(f"Heartbeat enviado: servidor={name}, rank={rank}.", flush=True)
        return self._request("heartbeat", name=name, rank=rank)