import unittest
from unittest.mock import patch

from clocks import LogicalClock, PhysicalClockOffset


class ClockTests(unittest.TestCase):
    def test_logical_clock_increments_before_sending(self):
        clock = LogicalClock()

        self.assertEqual(clock.before_send(), 1)
        self.assertEqual(clock.before_send(), 2)

    def test_logical_clock_uses_maximum_received_then_increments(self):
        clock = LogicalClock()
        clock.value = 8

        self.assertEqual(clock.on_receive(3), 9)
        self.assertEqual(clock.on_receive(14), 15)

    def test_physical_adjustment_is_gradual_and_does_not_set_system_clock(self):
        clock = PhysicalClockOffset(maximum_step_ms=25)

        with patch("clocks.unix_time_ms", return_value=1_000):
            offset, step = clock.adjust(1_200)
            self.assertEqual((offset, step), (25, 25))
            self.assertEqual(clock.now_ms(), 1_025)

        self.assertEqual(clock.offset_ms, 25)


if __name__ == "__main__":
    unittest.main()