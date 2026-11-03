import json
import threading
import time

import zmq

from .store import Store


class ReplicationResponder:
    def __init__(self, database_path, endpoint="tcp://*:5560"):
        self.database_path = database_path
        self.endpoint = endpoint
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopped.set()
        self.thread.join(timeout=2)

    def run(self):
        context = zmq.Context()
        store = Store(self.database_path)
        socket = context.socket(zmq.REP)
        socket.setsockopt(zmq.LINGER, 0)
        socket.bind(self.endpoint)
        print(f"Responder de replicação ativo em {self.endpoint}.", flush=True)
        try:
            while not self.stopped.is_set():
                if not socket.poll(500, zmq.POLLIN):
                    continue
                try:
                    request = socket.recv_json()
                    print(f"Replicação recebida: {request.get('action')}", flush=True)
                except (ValueError, zmq.error.ZMQError):
                    continue
                action = request.get("action") if isinstance(request, dict) else None
                if action == "replicate_pull":
                    socket.send_json({
                        "ok": True,
                        "events": store.replication_events(),
                    })
                elif action == "replicate_apply":
                    applied = []
                    for event in request.get("events", []):
                        if store.apply_replication_event(event):
                            applied.append(event.get("event_id"))
                    socket.send_json({
                        "ok": True,
                        "applied": [item for item in applied if item],
                    })
                else:
                    socket.send_json({"ok": False, "detail": "Ação desconhecida."})
        finally:
            socket.close(0)
            store.close()
            context.term()


class ReplicationWorker:
    def __init__(
        self, database_path, server_id, peer_names, timeout_ms=3000,
    ):
        self.database_path = database_path
        self.server_id = server_id
        self.peer_names = [
            name for name in peer_names if name != server_id
        ]
        self.timeout_ms = timeout_ms
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopped.set()
        self.thread.join(timeout=2)

    def request(self, context, peer, payload):
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDHWM, 10)
        socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        socket.connect(f"tcp://{peer}:5560")
        try:
            socket.send_json(payload)
            response = socket.recv_json()
            return response if isinstance(response, dict) else None
        except (zmq.error.Again, zmq.error.ZMQError, ValueError) as error:
            print(f"Replicação indisponível em {peer}: {error}", flush=True)
            return None
        finally:
            socket.close(0)

    def run(self):
        context = zmq.Context()
        store = Store(self.database_path)
        delay = 1.0
        if self.server_id != "server-1":
            delay += 4.0
        time.sleep(delay)
        try:
            while not self.stopped.wait(5):
                for peer in self.peer_names:
                    remote = self.request(context, peer, {"action": "replicate_pull"})
                    if remote and remote.get("ok"):
                        for event in remote.get("events", []):
                            store.apply_replication_event(event)
                    pending = store.pending_replication_events_for(peer)
                    if pending:
                        response = self.request(context, peer, {
                            "action": "replicate_apply",
                            "events": pending,
                        })
                        if response and response.get("ok"):
                            delivered = set(response.get("applied", []))
                            for event in pending:
                                if event["event_id"] in delivered:
                                    store.mark_delivered(
                                        event["event_id"], peer,
                                        int(time.time() * 1000),
                                    )
        finally:
            store.close()
            context.term()
