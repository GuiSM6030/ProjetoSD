import sqlite3
import tempfile
import unittest
from pathlib import Path

import bbs_pb2
from server.server import processar_mensagem
from server.store import Store


class BbsServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = str(
            Path(self.temporary_directory.name) / "server.sqlite3"
        )
        self.dados = Store(self.database_path)
        self.usuarios = {}
        self.client_id = b"bot-test"

    def tearDown(self) -> None:
        self.dados.close()
        self.temporary_directory.cleanup()

    def test_login_create_list_duplicate_and_persistence(self) -> None:
        login = bbs_pb2.Envelope(
            sent_at_unix_ms=1,
            login_request=bbs_pb2.LoginRequest(username="bot_test"),
        )
        login_response = processar_mensagem(
            self.client_id, login, self.dados, self.usuarios
        )
        self.assertTrue(login_response.login_response.accepted)

        create = bbs_pb2.Envelope(
            sent_at_unix_ms=2,
            create_channel_request=bbs_pb2.CreateChannelRequest(name="geral"),
        )
        create_response = processar_mensagem(
            self.client_id, create, self.dados, self.usuarios
        )
        self.assertTrue(create_response.create_channel_response.created)

        duplicate_response = processar_mensagem(
            self.client_id, create, self.dados, self.usuarios
        )
        self.assertEqual(
            duplicate_response.create_channel_response.error_code,
            "CHANNEL_EXISTS",
        )

        listing = processar_mensagem(
            self.client_id,
            bbs_pb2.Envelope(
                sent_at_unix_ms=3,
                list_channels_request=bbs_pb2.ListChannelsRequest(),
            ),
            self.dados,
            self.usuarios,
        )
        self.assertEqual(list(listing.list_channels_response.channel_names), ["geral"])

        self.dados.close()
        connection = sqlite3.connect(self.database_path)
        self.assertEqual(
            connection.execute(
                "SELECT username, logged_in_at_unix_ms FROM logins"
            ).fetchall(),
            [("bot_test", 1)],
        )
        self.assertEqual(
            connection.execute("SELECT name FROM channels").fetchall(),
            [("geral",)],
        )
        connection.close()

    def test_channel_access_requires_login_and_invalid_name_is_rejected(self) -> None:
        unauthenticated = processar_mensagem(
            self.client_id,
            bbs_pb2.Envelope(
                sent_at_unix_ms=1,
                list_channels_request=bbs_pb2.ListChannelsRequest(),
            ),
            self.dados,
            self.usuarios,
        )
        self.assertEqual(
            unauthenticated.error_response.error_code, "LOGIN_REQUIRED"
        )

        login = bbs_pb2.Envelope(
            sent_at_unix_ms=1,
            login_request=bbs_pb2.LoginRequest(username="bot_test"),
        )
        processar_mensagem(self.client_id, login, self.dados, self.usuarios)
        create = bbs_pb2.Envelope(
            sent_at_unix_ms=2,
            create_channel_request=bbs_pb2.CreateChannelRequest(name="bad name"),
        )
        response = processar_mensagem(
            self.client_id, create, self.dados, self.usuarios
        )
        self.assertEqual(
            response.create_channel_response.error_code, "INVALID_CHANNEL_NAME"
        )

    def test_requests_without_timestamp_are_rejected(self) -> None:
        response = processar_mensagem(
            self.client_id,
            bbs_pb2.Envelope(
                login_request=bbs_pb2.LoginRequest(username="bot_test")
            ),
            self.dados,
            self.usuarios,
        )
        self.assertEqual(response.error_response.error_code, "MISSING_TIMESTAMP")
        self.assertGreater(response.sent_at_unix_ms, 0)


if __name__ == "__main__":
    unittest.main()
