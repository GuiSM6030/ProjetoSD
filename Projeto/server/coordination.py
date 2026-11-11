import uuid

import zmq
from google.protobuf.message import DecodeError

import bbs_pb2
from clocks import unix_time_ms
from .control import peer_request_to_proto, peer_response_from_proto


def choose_coordinator(servers):
    if not servers:
        return None
    return max(
        servers,
        key=lambda server: (int(server["rank"]), server["name"]),
    )["name"]


def berkeley_average(times_unix_ms):
    if not times_unix_ms:
        raise ValueError("É necessário ao menos um horário para calcular a média.")
    return sum(int(value) for value in times_unix_ms) // len(times_unix_ms)


class ServerCoordinator:
    def __init__(
        self,
        context,
        server_id,
        rank,
        logical_clock,
        physical_clock,
        publisher,
        timeout_ms=700,
        replication_store=None,
    ):
        self.context = context
        self.server_id = server_id
        self.rank = rank or 0
        self.logical_clock = logical_clock
        self.physical_clock = physical_clock
        self.publisher = publisher
        self.timeout_ms = timeout_ms
        self.available_servers = []
        self.failed_servers = set()
        self.coordinator_name = None
        self.messages_since_sync = 0
        self.replication_store = replication_store

    def _message(self, payload):
        message = bbs_pb2.ControlEnvelope(
            sent_at_unix_ms=self.physical_clock.now_ms(),
            message_id=str(uuid.uuid4()),
            logical_clock=self.logical_clock.before_send(),
        )
        message.peer_request.CopyFrom(peer_request_to_proto(payload))
        return message

    def _receive_clock(self, message):
        logical_clock = (
            message.get("logical_clock", 0)
            if isinstance(message, dict)
            else message.logical_clock
        )
        try:
            self.logical_clock.on_receive(logical_clock)
        except (TypeError, ValueError):
            self.logical_clock.on_receive(0)

    def request_peer(self, server_name, payload):
        request = self._message(payload)
        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
        socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        socket.connect(f"tcp://{server_name}:5561")
        try:
            print(
                f"REQ servidor {server_name}: {request.peer_request}",
                flush=True,
            )
            socket.send(request.SerializeToString())
            self.note_message()
            response = bbs_pb2.ControlEnvelope()
            response.ParseFromString(socket.recv())
        except (
            zmq.error.Again,
            zmq.error.ZMQError,
            DecodeError,
        ) as error:
            self.failed_servers.add(server_name)
            print(f"Servidor {server_name} indisponível: {error}", flush=True)
            return None
        finally:
            socket.close(0)

        if not response.HasField("peer_response"):
            self.failed_servers.add(server_name)
            return None
        self.note_message()
        self._receive_clock(response)
        response_data = peer_response_from_proto(response.peer_response)
        if response_data.get("ok"):
            self.failed_servers.discard(server_name)
        print(
            f"REP servidor {server_name}: {response.peer_response}",
            flush=True,
        )
        return response_data

    def _eligible_servers(self):
        return [
            server for server in self.available_servers
            if server["name"] not in self.failed_servers
        ]

    def update_servers(self, servers):
        unique = {}
        for server in servers or []:
            name = server.get("name")
            try:
                rank = int(server.get("rank"))
            except (TypeError, ValueError):
                continue
            if name:
                unique[name] = {"name": name, "rank": rank}
        unique[self.server_id] = {
            "name": self.server_id,
            "rank": int(self.rank),
        }
        for name in self.failed_servers.intersection(unique):
            response = self.request_peer(name, {"action": "election"})
            if response and response.get("ok"):
                try:
                    unique[name]["rank"] = int(
                        response.get("rank", unique[name]["rank"])
                    )
                except (TypeError, ValueError):
                    pass
                self.failed_servers.discard(name)
        self.available_servers = list(unique.values())
        preferred = choose_coordinator(self._eligible_servers())
        if preferred is None:
            return None

        if self.coordinator_name != preferred:
            self.coordinator_name = preferred
            print(f"Coordenador atualizado: {preferred}.", flush=True)
            if preferred == self.server_id:
                self.announce_coordinator()
        return self.coordinator_name

    def announce_coordinator(self):
        self.coordinator_name = self.server_id
        announcement = bbs_pb2.ControlEnvelope(
            sent_at_unix_ms=self.physical_clock.now_ms(),
            message_id=str(uuid.uuid4()),
            logical_clock=self.logical_clock.before_send(),
        )
        announcement.coordinator_announcement.coordinator = self.server_id
        try:
            self.publisher.send_multipart([
                b"servers",
                announcement.SerializeToString(),
            ])
            self.messages_since_sync += 1
            print(f"Coordenador anunciado no tópico servers: {self.server_id}.", flush=True)
            return True
        except zmq.error.ZMQError as error:
            print(f"Falha ao anunciar coordenador: {error}", flush=True)
            return False

    def receive_announcement(self, payload):
        if not isinstance(payload, dict):
            return False
        self._receive_clock(payload)
        name = payload.get("coordinator")
        available_names = {
            server["name"] for server in self.available_servers
        }
        if name not in available_names:
            return False
        self.failed_servers.discard(name)
        preferred = choose_coordinator(self._eligible_servers())
        if name != preferred:
            return False
        self.coordinator_name = name
        print(f"Coordenador recebido pelo tópico servers: {name}.", flush=True)
        return True

    def handle_peer_request(self, request):
        if not isinstance(request, dict):
            request = {}
        self._receive_clock(request)
        action = request.get("action")
        response = {"ok": True, "detail": "OK"}

        if action == "election":
            response.update(server_name=self.server_id, rank=self.rank)
        elif action == "announce_coordinator":
            if request.get("coordinator") != self.server_id:
                response.update(ok=False, detail="Anúncio direcionado a outro servidor.")
            else:
                self.announce_coordinator()
        elif action == "replicate_apply":
            events = request.get("events", [])
            applied = []
            if self.replication_store is not None:
                for event in events if isinstance(events, list) else []:
                    if self.replication_store.apply_replication_event(event):
                        applied.append(event.get("event_id"))
            response.update(applied=[item for item in applied if item])
        elif action == "replicate_pull":
            if self.replication_store is not None:
                events, next_sequence = self.replication_store.replication_events_after(
                    request.get("after_sequence", 0)
                )
                response["events"] = events
                response["next_sequence"] = next_sequence
        elif action == "clock_report":
            response["server_time_unix_ms"] = self.physical_clock.now_ms()
        elif action == "clock":
            if self.coordinator_name != self.server_id:
                response.update(
                    ok=False,
                    detail="Este servidor não é o coordenador atual.",
                )
            else:
                times = []
                try:
                    times.append(int(request["server_time_unix_ms"]))
                except (KeyError, TypeError, ValueError):
                    response.update(ok=False, detail="Horário de origem inválido.")
                if response["ok"]:
                    times.append(self.physical_clock.now_ms())
                    requester = request.get("server_name")
                    for server in self._eligible_servers():
                        name = server["name"]
                        if name in {self.server_id, requester}:
                            continue
                        report = self.request_peer(name, {"action": "clock_report"})
                        if report and report.get("ok"):
                            try:
                                times.append(int(report["server_time_unix_ms"]))
                            except (KeyError, TypeError, ValueError):
                                continue
                    response["correct_time_unix_ms"] = berkeley_average(times)
        else:
            response.update(ok=False, detail="Ação desconhecida.")

        response.update(
            sent_at_unix_ms=self.physical_clock.now_ms(),
            message_id=str(uuid.uuid4()),
            logical_clock=self.logical_clock.before_send(),
        )
        return response

    def start_election(self):
        while True:
            candidates = [{"name": self.server_id, "rank": int(self.rank)}]
            for server in self._eligible_servers():
                name = server["name"]
                if name == self.server_id:
                    continue
                response = self.request_peer(name, {"action": "election"})
                if response and response.get("ok"):
                    candidates.append({
                        "name": response.get("server_name", name),
                        "rank": int(response.get("rank", server["rank"])),
                    })

            winner = choose_coordinator(candidates)
            if winner == self.server_id:
                self.announce_coordinator()
                print(f"Eleição concluída: coordenador={winner}.", flush=True)
                return winner

            response = self.request_peer(winner, {
                "action": "announce_coordinator",
                "coordinator": winner,
            })
            if response and response.get("ok"):
                self.coordinator_name = winner
                print(f"Eleição concluída: coordenador={winner}.", flush=True)
                return winner
            self.failed_servers.add(winner)

    def note_message(self, count=1):
        self.messages_since_sync += count

    def synchronization_due(self):
        if self.messages_since_sync < 15:
            return False
        self.messages_since_sync -= 15
        return True

    def synchronize_replication(self):
        if self.replication_store is None:
            return
        for server in self._eligible_servers():
            name = server["name"]
            if name == self.server_id:
                continue
            remote = self.request_peer(name, {
                "action": "replicate_pull",
                "after_sequence": self.replication_store.replication_cursor(name),
            })
            if remote and remote.get("ok"):
                applied_all = True
                for event in remote.get("events", []):
                    if not self.replication_store.apply_replication_event(event):
                        applied_all = False
                if applied_all:
                    self.replication_store.set_replication_cursor(
                        name, remote.get("next_sequence", 0)
                    )
            pending = self.replication_store.pending_replication_events_for(name)
            if pending:
                response = self.request_peer(name, {
                    "action": "replicate_apply",
                    "events": pending,
                })
                if response and response.get("ok"):
                    delivered = set(response.get("applied", []))
                    for event in pending:
                        if event["event_id"] in delivered:
                            self.replication_store.mark_delivered(
                                event["event_id"], name, unix_time_ms()
                            )

    def synchronize_clock(self):
        if not self.coordinator_name:
            self.start_election()

        if self.coordinator_name == self.server_id:
            times = [self.physical_clock.now_ms()]
            for server in self._eligible_servers():
                name = server["name"]
                if name == self.server_id:
                    continue
                report = self.request_peer(name, {"action": "clock_report"})
                if report and report.get("ok"):
                    try:
                        times.append(int(report["server_time_unix_ms"]))
                    except (KeyError, TypeError, ValueError):
                        continue
            correct_time = berkeley_average(times)
        else:
            response = self.request_peer(self.coordinator_name, {
                "action": "clock",
                "server_name": self.server_id,
                "server_time_unix_ms": self.physical_clock.now_ms(),
            })
            if not response or not response.get("ok"):
                print(
                    f"Coordenador {self.coordinator_name} indisponível; iniciando eleição.",
                    flush=True,
                )
                self.failed_servers.add(self.coordinator_name)
                self.start_election()
                if self.coordinator_name == self.server_id:
                    return self.synchronize_clock()
                response = self.request_peer(self.coordinator_name, {
                    "action": "clock",
                    "server_name": self.server_id,
                    "server_time_unix_ms": self.physical_clock.now_ms(),
                })
            if not response or not response.get("ok"):
                return None
            correct_time = response.get("correct_time_unix_ms")

        if correct_time is None:
            return None
        offset, step = self.physical_clock.adjust(correct_time)
        print(
            f"Hora Berkeley recebida={correct_time}; ajuste aplicado={step}ms; "
            f"offset atual={offset}ms; coordenador={self.coordinator_name}.",
            flush=True,
        )
        return correct_time