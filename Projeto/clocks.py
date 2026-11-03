import time


def unix_time_ms():
    return time.time_ns() // 1_000_000


class LogicalClock:
    def __init__(self):
        self.value = 0

    def before_send(self):
        self.value += 1
        return self.value

    def on_receive(self, received_value):
        self.value = max(self.value, int(received_value)) + 1
        return self.value


class PhysicalClockOffset:
    def __init__(self, maximum_step_ms=100):
        self.offset_ms = 0
        self.maximum_step_ms = maximum_step_ms

    def now_ms(self):
        return unix_time_ms() + self.offset_ms

    def adjust(self, reference_time_ms, local_time_ms=None):
        if local_time_ms is None:
            local_time_ms = unix_time_ms()
        difference = int(reference_time_ms) - (int(local_time_ms) + self.offset_ms)
        step = max(-self.maximum_step_ms, min(self.maximum_step_ms, difference))
        self.offset_ms += step
        return self.offset_ms, step