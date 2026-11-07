import threading
import time
import uuid

import zmq
from google.protobuf import text_format

import bbs_pb2
from clocks import LogicalClock, unix_time_ms
from .control import (
    peer_request_to_proto,
    peer_response_from_proto,
)
from .store import Store


class ReplicationWorker:
    def __init__(
        self, database_path, server_id, peer_names, timeout_ms=3000,
    ):
        self.database_path = database_path
        self.server_id = server_id
        self.peer_names = [
            name for name in peer_names if name != server_id
        ]
        self.peer_names_lock = threading.Lock()
        self.timeout_ms = timeout_ms
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.logical_clock = LogicalClock()

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopped.set()
        self.thread.join(timeout=2)

    def update_peers(self, peer_names):
        with self.peer_names_lock:
            self.peer_names = list(dict.fromkeys(
                name for name in peer_names if name != self.server_id
            ))

    def request(self, context, peer, payload):
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDHWM, 10)
        socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        socket.connect(f"tcp://{peer}:5560")
        request = bbs_pb2.ControlEnvelope(
            sent_at_unix_ms=unix_time_ms(),
            message_id=str(uuid.uuid4()),
            logical_clock=self.logical_clock.before_send(),
        )
        request.peer_request.CopyFrom(peer_request_to_proto(payload))
        print(
            f"ENVIADO ao servidor {peer}:\n"
            f"{text_format.MessageToString(request).strip()}",
            flush=True,
        )
        try:
            socket.send(request.SerializeToString())
            response = bbs_pb2.ControlEnvelope()
            response.ParseFromString(socket.recv())
            self.logical_clock.on_receive(response.logical_clock)
            print(
                f"RECEBIDO do servidor {peer}:\n"
                f"{text_format.MessageToString(response).strip()}",
                flush=True,
            )
            if not response.HasField("peer_response"):
                return None
            return peer_response_from_proto(response.peer_response)
        except (zmq.error.Again, zmq.error.ZMQError) as error:
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
        if self.stopped.wait(delay):
            store.close()
            context.term()
            return
        try:
            while not self.stopped.wait(5):
                with self.peer_names_lock:
                    peers = list(self.peer_names)
                for peer in peers:
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
