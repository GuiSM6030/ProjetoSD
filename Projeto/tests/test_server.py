import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bbs_pb2
from clocks import LogicalClock
from client import bot
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

    def test_server_receives_then_advances_logical_clock_before_reply(self) -> None:
        clock = LogicalClock()
        response = processar_mensagem(
            self.client_id,
            bbs_pb2.Envelope(
                sent_at_unix_ms=1,
                logical_clock=12,
                login_request=bbs_pb2.LoginRequest(username="bot_test"),
            ),
            self.dados,
            self.usuarios,
            relogio_logico=clock,
        )

        self.assertEqual(clock.value, 14)
        self.assertEqual(response.logical_clock, 14)

    def test_bot_increments_clock_when_creating_outgoing_envelope(self) -> None:
        bot.relogio_logico = LogicalClock()

        message = bot.criar_mensagem(
            "list_channels_request", bbs_pb2.ListChannelsRequest()
        )

        self.assertEqual(message.logical_clock, 1)

    def test_bot_creates_only_one_channel_when_fewer_than_five_exist(self) -> None:
        class Subscriber:
            def __init__(self) -> None:
                self.subscriptions = []

            def setsockopt(self, option, value) -> None:
                self.subscriptions.append(value)

        subscriber = Subscriber()
        existing_channels = ["um", "dois", "tres", "quatro"]
        with (
            patch.object(bot, "canal_inicial", "novo"),
            patch.object(
                bot,
                "listar_canais",
                side_effect=[existing_channels, existing_channels + ["novo"]],
            ),
            patch.object(bot, "criar_canal") as create_channel,
        ):
            channels = bot.preparar_canais(subscriber)

        create_channel.assert_called_once_with("novo")
        self.assertEqual(len(channels), 5)
        self.assertEqual(len(subscriber.subscriptions), 3)

    def test_bot_does_not_create_channel_when_five_already_exist(self) -> None:
        class Subscriber:
            def setsockopt(self, option, value) -> None:
                pass

        with (
            patch.object(bot, "listar_canais", return_value=[
                "um", "dois", "tres", "quatro", "cinco",
            ]),
            patch.object(bot, "criar_canal") as create_channel,
        ):
            bot.preparar_canais(Subscriber())

        create_channel.assert_not_called()

    def test_publish_requires_login_existing_channel_and_valid_content(self) -> None:
        request = bbs_pb2.Envelope(
            sent_at_unix_ms=10,
            publish_request=bbs_pb2.PublishRequest(
                channel_name="geral", content="olá"
            ),
        )
        response = processar_mensagem(
            self.client_id, request, self.dados, self.usuarios
        )
        self.assertEqual(response.error_response.error_code, "LOGIN_REQUIRED")

        processar_mensagem(
            self.client_id,
            bbs_pb2.Envelope(
                sent_at_unix_ms=11,
                login_request=bbs_pb2.LoginRequest(username="bot_test"),
            ),
            self.dados,
            self.usuarios,
        )
        response = processar_mensagem(
            self.client_id, request, self.dados, self.usuarios
        )
        self.assertEqual(response.publish_response.error_code, "CHANNEL_NOT_FOUND")

        invalid_content = bbs_pb2.Envelope(
            sent_at_unix_ms=12,
            publish_request=bbs_pb2.PublishRequest(
                channel_name="geral", content=" "
            ),
        )
        self.dados.create_channel("geral", "bot_test", 9)
        response = processar_mensagem(
            self.client_id, invalid_content, self.dados, self.usuarios
        )
        self.assertEqual(response.publish_response.error_code, "INVALID_CONTENT")

    def test_publish_is_timestamped_persisted_and_sent_to_pub(self) -> None:
        processar_mensagem(
            self.client_id,
            bbs_pb2.Envelope(
                sent_at_unix_ms=1,
                login_request=bbs_pb2.LoginRequest(username="bot_test"),
            ),
            self.dados,
            self.usuarios,
        )
        self.dados.create_channel("geral", "bot_test", 2)

        class Publisher:
            def __init__(self) -> None:
                self.frames = None

            def send_multipart(self, frames) -> None:
                self.frames = frames

        publisher = Publisher()
        logical_clock = LogicalClock()
        response = processar_mensagem(
            self.client_id,
            bbs_pb2.Envelope(
                sent_at_unix_ms=123,
                message_id="request-123",
                logical_clock=7,
                publish_request=bbs_pb2.PublishRequest(
                    channel_name="geral", content="evento persistido"
                ),
            ),
            self.dados,
            self.usuarios,
            publisher,
            logical_clock,
        )
        self.assertTrue(response.publish_response.published)
        self.assertGreater(response.sent_at_unix_ms, 0)
        self.assertEqual(publisher.frames[0], b"geral")

        publication = bbs_pb2.Envelope()
        publication.ParseFromString(publisher.frames[1])
        self.assertEqual(publication.message_id, "request-123")
        self.assertGreater(publication.sent_at_unix_ms, 0)
        self.assertEqual(publication.logical_clock, 9)
        self.assertEqual(response.logical_clock, 10)
        self.assertEqual(publication.publication.channel_name, "geral")
        self.assertEqual(publication.publication.content, "evento persistido")
        self.assertEqual(publication.publication.username, "bot_test")
        self.assertEqual(publication.publication.sent_at_unix_ms, 123)

        row = self.dados.connection.execute(
            "SELECT message_id, channel_name, content, sent_at_unix_ms "
            "FROM publications"
        ).fetchone()
        self.assertEqual(row, ("request-123", "geral", "evento persistido", 123))
        exchange = self.dados.connection.execute(
            "SELECT direction, message_type, message_id FROM exchanged_messages "
            "WHERE message_id = 'request-123' ORDER BY direction"
        ).fetchall()
        self.assertEqual(exchange, [
            ("client_to_server", "publish_request", "request-123"),
            ("server_to_client", "publish_response", "request-123"),
        ])

    def test_history_returns_persisted_publications_with_metadata(self) -> None:
        self.dados.create_channel("geral", "alice", 1)
        self.dados.record_publication(
            "message-1", "geral", "alice", "mensagem anterior", 10
        )
        self.usuarios[self.client_id] = "alice"

        response = processar_mensagem(
            self.client_id,
            bbs_pb2.Envelope(
                sent_at_unix_ms=20,
                history_request=bbs_pb2.HistoryRequest(channel_name="geral"),
            ),
            self.dados,
            self.usuarios,
        )

        self.assertEqual(response.WhichOneof("payload"), "history_response")
        self.assertEqual(len(response.history_response.publications), 1)
        publication = response.history_response.publications[0]
        self.assertEqual(publication.channel_name, "geral")
        self.assertEqual(publication.username, "alice")
        self.assertEqual(publication.content, "mensagem anterior")
        self.assertEqual(publication.sent_at_unix_ms, 10)

    def test_history_requires_login_and_rejects_unknown_channel(self) -> None:
        request = bbs_pb2.Envelope(
            sent_at_unix_ms=20,
            history_request=bbs_pb2.HistoryRequest(channel_name="geral"),
        )
        response = processar_mensagem(
            self.client_id, request, self.dados, self.usuarios
        )
        self.assertEqual(response.error_response.error_code, "LOGIN_REQUIRED")

        self.usuarios[self.client_id] = "alice"
        response = processar_mensagem(
            self.client_id, request, self.dados, self.usuarios
        )
        self.assertEqual(response.error_response.error_code, "CHANNEL_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
