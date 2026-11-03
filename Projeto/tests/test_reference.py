import unittest

from reference import ReferenceRegistry, ReferenceService


class ReferenceTests(unittest.TestCase):
    def setUp(self):
        self.registry = ReferenceRegistry(
            ["server-1", "server-2"], timeout_seconds=30
        )

    def test_rank_is_stable_and_names_are_not_duplicated(self):
        first_rank = self.registry.register("server-1", now=0)
        repeated_rank = self.registry.register("server-1", now=1)

        self.assertEqual(first_rank, 1)
        self.assertEqual(repeated_rank, first_rank)
        self.assertEqual(len(self.registry.servers), 1)
        self.assertEqual(self.registry.list_available(now=2), [
            {"name": "server-1", "rank": 1}
        ])

    def test_registration_and_rank_lookup_are_available(self):
        self.assertEqual(self.registry.register("server-2", now=10), 2)
        self.assertEqual(self.registry.ranks["server-2"], 2)
        self.assertEqual(
            self.registry.list_available(now=20),
            [{"name": "server-2", "rank": 2}],
        )

    def test_heartbeat_refreshes_server_and_timeout_removes_availability(self):
        rank = self.registry.register("server-1", now=0)
        self.assertTrue(self.registry.heartbeat("server-1", rank, now=25))
        self.assertEqual(
            self.registry.list_available(now=50),
            [{"name": "server-1", "rank": 1}],
        )
        self.assertEqual(self.registry.list_available(now=56), [])
        self.assertEqual(self.registry.register("server-1", now=57), rank)

    def test_reference_rank_and_heartbeat_replies_have_message_metadata(self):
        service = ReferenceService(self.registry)
        rank_reply = service.handle({
            "action": "rank",
            "name": "server-1",
            "logical_clock": 4,
        })
        heartbeat_reply = service.handle({
            "action": "heartbeat",
            "name": "server-1",
            "rank": 1,
            "logical_clock": 6,
        })

        self.assertEqual(rank_reply["rank"], 1)
        self.assertTrue(heartbeat_reply["ok"])
        self.assertIn("server_time_unix_ms", heartbeat_reply)
        self.assertGreater(heartbeat_reply["logical_clock"], rank_reply["logical_clock"])
        self.assertGreater(heartbeat_reply["sent_at_unix_ms"], 0)
        self.assertTrue(heartbeat_reply["message_id"])


if __name__ == "__main__":
    unittest.main()