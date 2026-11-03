import json
import sqlite3
import uuid
from pathlib import Path


class Store:
    def __init__(self, database_path: str) -> None:
        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database_path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS logins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                logged_in_at_unix_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS channels (
                name TEXT PRIMARY KEY,
                created_by TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS publications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL,
                channel_name TEXT NOT NULL,
                username TEXT NOT NULL,
                content TEXT NOT NULL,
                sent_at_unix_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS exchanged_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direction TEXT NOT NULL,
                message_type TEXT NOT NULL,
                message_id TEXT NOT NULL,
                username TEXT NOT NULL,
                channel_name TEXT,
                content TEXT,
                sent_at_unix_ms INTEGER NOT NULL,
                received_at_unix_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS replication_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                origin TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                applied_at_unix_ms INTEGER
            );
            CREATE TABLE IF NOT EXISTS replication_outbox (
                event_id TEXT PRIMARY KEY,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS replication_delivered (
                event_id TEXT NOT NULL,
                target TEXT NOT NULL,
                delivered_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY (event_id, target)
            );
            CREATE TABLE IF NOT EXISTS replication_applied (
                event_id TEXT PRIMARY KEY,
                applied_at_unix_ms INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        self.connection.commit()

    def _enqueue_event(
        self, event_type: str, payload: dict, origin: str, event_id: str | None = None
    ) -> str:
        event_id = event_id or f"{event_type}:{uuid.uuid4()}"
        payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        self.connection.execute(
            """
            INSERT OR IGNORE INTO replication_events
                (event_id, event_type, payload, origin, created_at_unix_ms)
            VALUES (?, ?, ?, ?, ?)
            """,
            (event_id, event_type, payload_json, origin, payload.get("sent_at_unix_ms", 0)),
        )
        if self.connection.execute(
            "SELECT changes()"
        ).fetchone()[0]:
            self.connection.execute(
                "INSERT OR IGNORE INTO replication_outbox (event_id, created_at_unix_ms) VALUES (?, ?)",
                (event_id, payload.get("sent_at_unix_ms", 0)),
            )
        return event_id

    def record_login(
        self, username: str, timestamp_ms: int, event_id: str | None = None,
        origin: str = "unknown",
    ) -> None:
        payload = {"username": username, "sent_at_unix_ms": timestamp_ms}
        event_id = event_id or f"login:{username}:{timestamp_ms}:{uuid.uuid4()}"
        with self.connection:
            self.connection.execute(
                "INSERT INTO replication_applied (event_id) VALUES (?) ON CONFLICT(event_id) DO NOTHING",
                (event_id,),
            )
            if self.connection.execute(
                "SELECT changes()"
            ).fetchone()[0]:
                self.connection.execute(
                    "INSERT INTO logins (username, logged_in_at_unix_ms) VALUES (?, ?)",
                    (username, timestamp_ms),
                )
                self._enqueue_event("login", payload, origin, event_id)

    def list_channels(self) -> list[str]:
        rows = self.connection.execute(
            "SELECT name FROM channels ORDER BY name"
        ).fetchall()
        return [row[0] for row in rows]

    def create_channel(
        self, name: str, username: str, timestamp_ms: int,
        event_id: str | None = None, origin: str = "unknown",
    ) -> bool:
        if self.channel_exists(name):
            return False
        event_id = event_id or f"channel:{name}:{origin}:{timestamp_ms}:{uuid.uuid4()}"
        payload = {
            "name": name,
            "created_by": username,
            "created_by_origin": origin,
            "created_at_unix_ms": timestamp_ms,
        }
        with self.connection:
            self.connection.execute(
                "INSERT INTO replication_applied (event_id) VALUES (?) ON CONFLICT(event_id) DO NOTHING",
                (event_id,),
            )
            if self.connection.execute("SELECT changes()").fetchone()[0]:
                self.connection.execute(
                    "INSERT INTO channels (name, created_by, created_at_unix_ms) VALUES (?, ?, ?)",
                    (name, username, timestamp_ms),
                )
                self._enqueue_event("channel", payload, origin, event_id)
        return True

    def channel_exists(self, name: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM channels WHERE name = ?", (name,)
        ).fetchone()
        return row is not None

    def record_publication(
        self, message_id: str, channel_name: str, username: str, content: str,
        timestamp_ms: int, origin: str = "unknown",
    ) -> None:
        event_id = f"publication:{message_id}"
        payload = {
            "message_id": message_id,
            "channel_name": channel_name,
            "username": username,
            "content": content,
            "sent_at_unix_ms": timestamp_ms,
        }
        with self.connection:
            self.connection.execute(
                "INSERT INTO replication_applied (event_id) VALUES (?) ON CONFLICT(event_id) DO NOTHING",
                (event_id,),
            )
            if self.connection.execute("SELECT changes()").fetchone()[0]:
                self.connection.execute(
                    """
                    INSERT INTO publications
                        (message_id, channel_name, username, content, sent_at_unix_ms)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (message_id, channel_name, username, content, timestamp_ms),
                )
                self._enqueue_event("publication", payload, origin, event_id)

    def record_exchange(
        self, direction: str, message_type: str, message_id: str, username: str,
        channel_name: str | None, content: str | None, sent_at_unix_ms: int,
        received_at_unix_ms: int, origin: str = "unknown",
    ) -> None:
        event_id = f"exchange:{direction}:{message_type}:{message_id}"
        payload = {
            "direction": direction,
            "message_type": message_type,
            "message_id": message_id,
            "username": username,
            "channel_name": channel_name,
            "content": content,
            "sent_at_unix_ms": sent_at_unix_ms,
            "received_at_unix_ms": received_at_unix_ms,
        }
        with self.connection:
            self.connection.execute(
                "INSERT INTO replication_applied (event_id) VALUES (?) ON CONFLICT(event_id) DO NOTHING",
                (event_id,),
            )
            if self.connection.execute("SELECT changes()").fetchone()[0]:
                self.connection.execute(
                    """
                    INSERT INTO exchanged_messages
                        (direction, message_type, message_id, username, channel_name,
                         content, sent_at_unix_ms, received_at_unix_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    tuple(payload[key] for key in (
                        "direction", "message_type", "message_id", "username",
                        "channel_name", "content", "sent_at_unix_ms",
                        "received_at_unix_ms",
                    )),
                )
                self._enqueue_event("exchange", payload, origin, event_id)

    def pending_replication_events(self) -> list[dict]:
        rows = self.connection.execute(
            """
            SELECT e.event_id, e.event_type, e.payload, e.origin
            FROM replication_events e
            JOIN replication_outbox o ON o.event_id = e.event_id
            ORDER BY e.created_at_unix_ms, e.event_id
            """
        ).fetchall()
        return [
            {"event_id": row[0], "event_type": row[1], "payload": json.loads(row[2]), "origin": row[3]}
            for row in rows
        ]

    def replication_events(self) -> list[dict]:
        rows = self.connection.execute(
            """
            SELECT event_id, event_type, payload, origin
            FROM replication_events ORDER BY created_at_unix_ms, event_id
            """
        ).fetchall()
        return [
            {"event_id": row[0], "event_type": row[1], "payload": json.loads(row[2]), "origin": row[3]}
            for row in rows
        ]

    def mark_delivered(self, event_id: str, target: str, timestamp_ms: int) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO replication_delivered (event_id, target, delivered_at_unix_ms) VALUES (?, ?, ?)",
                (event_id, target, timestamp_ms),
            )

    def pending_replication_events_for(self, target: str) -> list[dict]:
        rows = self.connection.execute(
            """
            SELECT e.event_id, e.event_type, e.payload, e.origin
            FROM replication_events e
            JOIN replication_outbox o ON o.event_id = e.event_id
            WHERE NOT EXISTS (
                SELECT 1 FROM replication_delivered d
                WHERE d.event_id = e.event_id AND d.target = ?
            )
            ORDER BY e.created_at_unix_ms, e.event_id
            """,
            (target,),
        ).fetchall()
        return [
            {"event_id": row[0], "event_type": row[1], "payload": json.loads(row[2]), "origin": row[3]}
            for row in rows
        ]

    def apply_replication_event(self, event: dict) -> bool:
        event_id = str(event.get("event_id", ""))
        event_type = str(event.get("event_type", ""))
        payload = event.get("payload", {})
        if not event_id or not isinstance(payload, dict):
            return False
        if self.connection.execute(
            "SELECT 1 FROM replication_applied WHERE event_id = ?", (event_id,)
        ).fetchone():
            return True

        origin = str(event.get("origin", "unknown"))
        with self.connection:
            if event_type == "login":
                self.connection.execute(
                    "INSERT INTO logins (username, logged_in_at_unix_ms) VALUES (?, ?)",
                    (payload["username"], payload["sent_at_unix_ms"]),
                )
            elif event_type == "channel":
                name = payload["name"]
                candidates = [{
                    "payload": payload,
                    "key": (
                        int(payload["created_at_unix_ms"]),
                        payload.get("created_by_origin", origin),
                        payload["created_by"],
                    ),
                }]
                for event_row in self.connection.execute(
                    "SELECT payload, origin FROM replication_events WHERE event_type = 'channel'"
                ):
                    stored_payload = json.loads(event_row[0])
                    if stored_payload.get("name") == name:
                        candidates.append({
                            "payload": stored_payload,
                            "key": (
                                int(stored_payload["created_at_unix_ms"]),
                                stored_payload.get("created_by_origin", event_row[1]),
                                stored_payload["created_by"],
                            ),
                        })
                winner_payload = min(candidates, key=lambda candidate: candidate["key"])["payload"]
                self.connection.execute(
                    """
                    INSERT INTO channels (name, created_by, created_at_unix_ms) VALUES (?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        created_by = excluded.created_by,
                        created_at_unix_ms = excluded.created_at_unix_ms
                    """,
                    (
                        name,
                        winner_payload["created_by"],
                        winner_payload["created_at_unix_ms"],
                    ),
                )
            elif event_type == "publication":
                self.connection.execute(
                    "INSERT OR IGNORE INTO publications (message_id, channel_name, username, content, sent_at_unix_ms) VALUES (?, ?, ?, ?, ?)",
                    (payload["message_id"], payload["channel_name"], payload["username"], payload["content"], payload["sent_at_unix_ms"]),
                )
            elif event_type == "exchange":
                self.connection.execute(
                    "INSERT INTO exchanged_messages (direction, message_type, message_id, username, channel_name, content, sent_at_unix_ms, received_at_unix_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (payload["direction"], payload["message_type"], payload["message_id"], payload["username"], payload["channel_name"], payload["content"], payload["sent_at_unix_ms"], payload["received_at_unix_ms"]),
                )
            else:
                return False
            self.connection.execute(
                "INSERT OR IGNORE INTO replication_events (event_id, event_type, payload, origin, created_at_unix_ms) VALUES (?, ?, ?, ?, ?)",
                (event_id, event_type, json.dumps(payload, sort_keys=True, ensure_ascii=False), origin, payload.get("sent_at_unix_ms", payload.get("created_at_unix_ms", 0))),
            )
            self.connection.execute(
                "INSERT OR REPLACE INTO replication_applied (event_id, applied_at_unix_ms) VALUES (?, ?)",
                (event_id, payload.get("sent_at_unix_ms", payload.get("created_at_unix_ms", 0))),
            )
        return True

    def close(self) -> None:
        self.connection.close()
