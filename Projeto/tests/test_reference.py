import unittest

import bbs_pb2
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
        rank_request = bbs_pb2.ControlEnvelope(
            sent_at_unix_ms=1,
            logical_clock=4,
        )
        rank_request.reference_request.action = "rank"
        rank_request.reference_request.name = "server-1"
        rank_reply = service.handle(rank_request)
        heartbeat_request = bbs_pb2.ControlEnvelope(
            sent_at_unix_ms=2,
            logical_clock=6,
        )
        heartbeat_request.reference_request.action = "heartbeat"
        heartbeat_request.reference_request.name = "server-1"
        heartbeat_request.reference_request.rank = 1
        heartbeat_reply = service.handle(heartbeat_request)

        self.assertEqual(rank_reply.reference_response.rank, 1)
        self.assertTrue(heartbeat_reply.reference_response.ok)
        self.assertEqual(heartbeat_reply.reference_response.ListFields(), [
            (bbs_pb2.ReferenceResponse.DESCRIPTOR.fields_by_name["ok"], True),
            (
                bbs_pb2.ReferenceResponse.DESCRIPTOR.fields_by_name["detail"],
                "OK",
            ),
        ])
        self.assertGreater(
            heartbeat_reply.logical_clock, rank_reply.logical_clock
        )
        self.assertGreater(heartbeat_reply.sent_at_unix_ms, 0)
        self.assertTrue(heartbeat_reply.message_id)


if __name__ == "__main__":
    unittest.main()