import unittest
from unittest.mock import Mock

import bbs_pb2
from clocks import LogicalClock
from server.coordination import berkeley_average, choose_coordinator
from server.coordination import ServerCoordinator


class FixedClock:
    def now_ms(self):
        return 100


class CoordinationProtocolTests(unittest.TestCase):
    def setUp(self):
        self.publisher = Mock()
        self.coordinator = ServerCoordinator(
            None,
            "server-1",
            1,
            LogicalClock(),
            FixedClock(),
            self.publisher,
        )

    def test_election_request_returns_ok_with_server_rank(self):
        response = self.coordinator.handle_peer_request({
            "action": "election",
            "logical_clock": 5,
        })

        self.assertTrue(response["ok"])
        self.assertEqual(response["server_name"], "server-1")
        self.assertEqual(response["rank"], 1)
        self.assertEqual(response["logical_clock"], 7)
        self.assertTrue(response["message_id"])

    def test_coordinator_returns_berkeley_average(self):
        self.coordinator.coordinator_name = "server-1"
        self.coordinator.available_servers = [
            {"name": "server-1", "rank": 1},
            {"name": "server-2", "rank": 2},
            {"name": "server-3", "rank": 3},
        ]
        self.coordinator.request_peer = Mock(return_value={
            "ok": True,
            "server_time_unix_ms": 300,
        })

        response = self.coordinator.handle_peer_request({
            "action": "clock",
            "server_name": "server-2",
            "server_time_unix_ms": 200,
            "logical_clock": 2,
        })

        self.assertEqual(response["correct_time_unix_ms"], 200)
        self.coordinator.request_peer.assert_called_once_with(
            "server-3", {"action": "clock_report"}
        )

    def test_coordinator_announcement_is_published_on_servers_topic(self):
        self.coordinator.announce_coordinator()

        frames = self.publisher.send_multipart.call_args.args[0]
        self.assertEqual(frames[0], b"servers")
        announcement = bbs_pb2.ControlEnvelope()
        announcement.ParseFromString(frames[1])
        self.assertEqual(
            announcement.coordinator_announcement.coordinator,
            "server-1",
        )

    def test_subscriber_accepts_available_coordinator_announcement(self):
        self.coordinator.available_servers = [
            {"name": "server-1", "rank": 1},
            {"name": "server-2", "rank": 2},
        ]
        self.coordinator.failed_servers.add("server-2")

        accepted = self.coordinator.receive_announcement({
            "coordinator": "server-2",
            "logical_clock": 3,
        })

        self.assertTrue(accepted)
        self.assertEqual(self.coordinator.coordinator_name, "server-2")
        self.assertNotIn("server-2", self.coordinator.failed_servers)

    def test_subscriber_rejects_announcement_from_lower_rank_server(self):
        self.coordinator.available_servers = [
            {"name": "server-1", "rank": 1},
            {"name": "server-2", "rank": 2},
        ]

        accepted = self.coordinator.receive_announcement({
            "coordinator": "server-1",
            "logical_clock": 3,
        })

        self.assertFalse(accepted)
        self.assertIsNone(self.coordinator.coordinator_name)

    def test_server_list_probes_failed_server_before_restoring_it(self):
        self.coordinator.failed_servers.add("server-2")
        self.coordinator.request_peer = Mock(return_value={
            "ok": True,
            "rank": 2,
        })

        coordinator = self.coordinator.update_servers([
            {"name": "server-1", "rank": 1},
            {"name": "server-2", "rank": 2},
        ])

        self.assertEqual(coordinator, "server-2")
        self.assertNotIn("server-2", self.coordinator.failed_servers)
        self.coordinator.request_peer.assert_called_once_with(
            "server-2", {"action": "election"}
        )

    def test_sync_threshold_is_fifteen_messages(self):
        self.coordinator.note_message(14)
        self.assertFalse(self.coordinator.synchronization_due())
        self.coordinator.note_message()
        self.assertTrue(self.coordinator.synchronization_due())
        self.assertFalse(self.coordinator.synchronization_due())


class CoordinationTests(unittest.TestCase):
    def test_coordinator_uses_highest_rank_with_stable_tie_break(self):
        servers = [
            {"name": "server-1", "rank": 1},
            {"name": "server-2", "rank": 2},
            {"name": "server-3", "rank": 2},
        ]

        self.assertEqual(choose_coordinator(servers), "server-3")
        self.assertIsNone(choose_coordinator([]))

    def test_berkeley_time_is_average_of_reported_clocks(self):
        self.assertEqual(berkeley_average([100, 103, 107]), 103)

    def test_berkeley_time_requires_at_least_one_clock(self):
        with self.assertRaises(ValueError):
            berkeley_average([])


if __name__ == "__main__":
    unittest.main()