import os
import tempfile
import unittest

from server.store import Store


class ReplicationStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.directory.cleanup()

    def store(self, name):
        return Store(os.path.join(self.directory.name, name + ".sqlite3"))

    def test_all_replicable_data_is_applied_without_duplicates(self):
        origin = self.store("origin")
        peer = self.store("peer")

        origin.record_login("alice", 1, "login-1", "server-1")
        origin.create_channel("geral", "alice", 2, "channel-1", "server-1")
        origin.record_publication("publication-1", "geral", "alice", "oi", 3, "server-1")
        origin.record_exchange(
            "client_to_server", "publish_request", "publication-1",
            "alice", "geral", "oi", 3, 4, "server-1",
        )

        events = origin.replication_events()
        for event in events:
            peer.apply_replication_event(event)
        for event in events:
            peer.apply_replication_event(event)

        self.assertEqual(peer.connection.execute("SELECT username FROM logins").fetchall(), [("alice",)])
        self.assertEqual(peer.connection.execute("SELECT name FROM channels").fetchall(), [("geral",)])
        self.assertEqual(peer.connection.execute("SELECT message_id FROM publications").fetchall(), [("publication-1",)])
        self.assertEqual(
            peer.publication_history("geral"),
            [{
                "channel_name": "geral",
                "username": "alice",
                "content": "oi",
                "sent_at_unix_ms": 3,
            }],
        )
        self.assertEqual(peer.connection.execute("SELECT message_id FROM exchanged_messages").fetchall(), [("publication-1",)])
        origin.close()
        peer.close()

    def test_concurrent_channel_conflict_is_resolved_by_stable_key(self):
        first = self.store("first")
        second = self.store("second")
        first.create_channel("geral", "alice", 20, "channel-first", "server-1")
        second.create_channel("geral", "bob", 20, "channel-second", "server-2")

        for event in second.replication_events():
            first.apply_replication_event(event)
        for event in first.replication_events():
            second.apply_replication_event(event)

        expected = ("geral", "alice", 20)
        self.assertEqual(first.connection.execute("SELECT name, created_by, created_at_unix_ms FROM channels WHERE name = 'geral'").fetchone(), expected)
        self.assertEqual(second.connection.execute("SELECT name, created_by, created_at_unix_ms FROM channels WHERE name = 'geral'").fetchone(), expected)
        first.close()
        second.close()

    def test_pending_event_survives_store_restart(self):
        path = os.path.join(self.directory.name, "persistent.sqlite3")
        store = Store(path)
        store.record_login("alice", 10, "login-persistent", "server-1")
        store.close()

        reopened = Store(path)
        pending = reopened.pending_replication_events_for("server-2")
        self.assertEqual([event["event_id"] for event in pending], ["login-persistent"])
        reopened.mark_delivered("login-persistent", "server-2", 11)
        self.assertEqual(reopened.pending_replication_events_for("server-2"), [])
        reopened.close()

    def test_delivery_is_tracked_independently_for_each_peer(self):
        store = self.store("delivery")
        store.record_login("alice", 1, "login-delivery", "server-1")
        store.mark_delivered("login-delivery", "server-1", 2)

        self.assertEqual(
            [event["event_id"] for event in store.pending_replication_events_for("server-2")],
            ["login-delivery"],
        )
        store.mark_delivered("login-delivery", "server-2", 3)
        self.assertEqual(store.pending_replication_events_for("server-2"), [])
        store.close()


if __name__ == "__main__":
    unittest.main()
