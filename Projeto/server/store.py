import sqlite3
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
            """
        )
        self.connection.commit()

    def record_login(self, username: str, timestamp_ms: int) -> None:
        self.connection.execute(
            "INSERT INTO logins (username, logged_in_at_unix_ms) VALUES (?, ?)",
            (username, timestamp_ms),
        )
        self.connection.commit()

    def list_channels(self) -> list[str]:
        rows = self.connection.execute(
            "SELECT name FROM channels ORDER BY name"
        ).fetchall()
        return [row[0] for row in rows]

    def create_channel(
        self, name: str, username: str, timestamp_ms: int
    ) -> bool:
        try:
            self.connection.execute(
                """
                INSERT INTO channels (name, created_by, created_at_unix_ms)
                VALUES (?, ?, ?)
                """,
                (name, username, timestamp_ms),
            )
        except sqlite3.IntegrityError:
            return False
        self.connection.commit()
        return True

    def channel_exists(self, name: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM channels WHERE name = ?", (name,)
        ).fetchone()
        return row is not None

    def record_publication(
        self,
        message_id: str,
        channel_name: str,
        username: str,
        content: str,
        timestamp_ms: int,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO publications
                (message_id, channel_name, username, content, sent_at_unix_ms)
            VALUES (?, ?, ?, ?, ?)
            """,
            (message_id, channel_name, username, content, timestamp_ms),
        )
        self.connection.commit()

    def record_exchange(
        self,
        direction: str,
        message_type: str,
        message_id: str,
        username: str,
        channel_name: str | None,
        content: str | None,
        sent_at_unix_ms: int,
        received_at_unix_ms: int,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO exchanged_messages
                (direction, message_type, message_id, username, channel_name,
                 content, sent_at_unix_ms, received_at_unix_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                direction,
                message_type,
                message_id,
                username,
                channel_name,
                content,
                sent_at_unix_ms,
                received_at_unix_ms,
            ),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()
