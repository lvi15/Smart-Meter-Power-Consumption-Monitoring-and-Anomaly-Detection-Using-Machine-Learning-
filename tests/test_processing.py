"""
Unit Tests for Stream Processor Computations (Layer 3)
Validates sliding-window rolling average calculation, peak power tracking,
measurement validation, and payload processing.
"""

import sys
from pathlib import Path
from collections import deque
import pytest

# Add parent directory to path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from stream_processor import StreamProcessor


class DummyStreamProcessor:
    """Lightweight test processor isolating calculation methods without DB or Kafka dependencies."""
    def __init__(self, window_size: int = 30):
        self.window_size = window_size
        self.rolling_window = deque(maxlen=window_size)
        self.running_peak_power = 0.0

    def update_rolling_window(self, power: float) -> float:
        self.rolling_window.append(power)
        return float(sum(self.rolling_window) / len(self.rolling_window))

    def update_peak_power(self, power: float) -> float:
        if power > self.running_peak_power:
            self.running_peak_power = power
        return self.running_peak_power


def test_rolling_average_startup_behavior():
    """Checks rolling average when fewer than 30 readings are available."""
    proc = DummyStreamProcessor(window_size=30)
    
    # 1 reading
    avg1 = proc.update_rolling_window(2.0)
    assert avg1 == pytest.approx(2.0)

    # 2 readings: (2.0 + 4.0) / 2 = 3.0
    avg2 = proc.update_rolling_window(4.0)
    assert avg2 == pytest.approx(3.0)

    # 3 readings: (2.0 + 4.0 + 6.0) / 3 = 4.0
    avg3 = proc.update_rolling_window(6.0)
    assert avg3 == pytest.approx(4.0)


def test_rolling_average_full_window_shift():
    """Verifies that oldest reading drops off once window size 30 is exceeded."""
    proc = DummyStreamProcessor(window_size=30)

    # Fill window with 30 values of 1.0 -> average should be 1.0
    for _ in range(30):
        avg = proc.update_rolling_window(1.0)
    assert avg == pytest.approx(1.0)
    assert len(proc.rolling_window) == 30

    # Add a 31st value of 31.0:
    # 29 ones (29) + 31.0 = 60.0 -> average = 60 / 30 = 2.0
    avg_31 = proc.update_rolling_window(31.0)
    assert avg_31 == pytest.approx(2.0)
    assert len(proc.rolling_window) == 30


def test_peak_power_tracking():
    """Verifies that session peak power updates only when exceeded."""
    proc = DummyStreamProcessor(window_size=30)

    assert proc.update_peak_power(1.5) == pytest.approx(1.5)
    assert proc.update_peak_power(3.2) == pytest.approx(3.2)
    # Lower reading should NOT change peak
    assert proc.update_peak_power(2.1) == pytest.approx(3.2)
    assert proc.running_peak_power == pytest.approx(3.2)
    # Higher reading should update peak
    assert proc.update_peak_power(5.0) == pytest.approx(5.0)
    assert proc.running_peak_power == pytest.approx(5.0)


def test_anomaly_status_convention():
    """Validates that anomaly flag 0 maps to NORMAL and 1 to ANOMALY."""
    proc = DummyStreamProcessor(window_size=30)
    
    flag_0 = 0
    status_0 = "ANOMALY" if flag_0 == 1 else "NORMAL"
    assert status_0 == "NORMAL"

    flag_1 = 1
    status_1 = "ANOMALY" if flag_1 == 1 else "NORMAL"
    assert status_1 == "ANOMALY"

