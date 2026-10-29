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

    def close(self) -> None:
        self.connection.close()
