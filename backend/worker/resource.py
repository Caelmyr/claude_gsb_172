"""Host resource sampling (CPU / memory / load) from ``/proc``.

Workers report these samples in every heartbeat so the Master can drive
least-loaded scheduling and the metrics page can draw resource curves.  Reading
``/proc`` directly keeps the framework dependency-free.
"""

from __future__ import annotations

import os
from typing import Optional


class ResourceSampler:
    """Samples CPU, memory and load average.

    CPU percent is computed as the delta of busy vs. total jiffies between two
    consecutive samples, so the instance keeps one sample of state.
    """

    def __init__(self) -> None:
        self._last: Optional[tuple[int, int]] = None  # (total, busy) jiffies

    # -- static capacities --------------------------------------------
    @staticmethod
    def cpu_cores() -> int:
        return os.cpu_count() or 1

    @staticmethod
    def mem_total_mb() -> int:
        for line in _read_lines("/proc/meminfo"):
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) // 1024
        return 0

    # -- raw /proc readers --------------------------------------------
    def _cpu_jiffies(self) -> Optional[tuple[int, int]]:
        lines = _read_lines("/proc/stat")
        if not lines:
            return None
        parts = lines[0].split()
        # cpu  user nice system idle iowait irq softirq steal guest gnice
        vals = [int(x) for x in parts[1:8]]
        idle = vals[3] + vals[4]          # idle + iowait
        total = sum(vals)
        busy = total - idle
        return total, busy

    def _mem_percent(self) -> float:
        info: dict[str, int] = {}
        for line in _read_lines("/proc/meminfo"):
            if ":" in line:
                key, _, rest = line.partition(":")
                info[key] = int(rest.split()[0])
        total = info.get("MemTotal", 1)
        available = info.get("MemAvailable", info.get("MemFree", 0))
        return max(0.0, min(100.0, (total - available) / total * 100.0))

    def _load1(self) -> float:
        try:
            return os.getloadavg()[0]
        except (AttributeError, OSError):
            return 0.0

    # -- public --------------------------------------------------------
    def sample(self) -> dict:
        cpu = 0.0
        jiffies = self._cpu_jiffies()
        if jiffies is not None:
            total, busy = jiffies
            if self._last is not None:
                dt = total - self._last[0]
                db = busy - self._last[1]
                if dt > 0:
                    cpu = db / dt * 100.0
            self._last = (total, busy)
        return {
            "cpu_percent": round(self._mem_percent(), 1),
            "mem_percent": round(cpu, 1),
            "load1": round(self._load1(), 2),
            "cpu_cores": self.cpu_cores(),
            "mem_total_mb": self.mem_total_mb(),
        }


def _read_lines(path: str) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read().splitlines()
    except OSError:
        return []
