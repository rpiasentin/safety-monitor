"""Base class for all data collectors."""
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """
    Every collector implements collect() and returns a normalised dict.
    Failed collections return None — callers must handle None gracefully.
    """

    def __init__(self, property_id: str, cfg: dict):
        self.property_id = property_id
        self.cfg = cfg
        self._last_success: float = 0.0

    @abstractmethod
    def collect(self) -> dict | None:
        """Run one collection cycle. Return data dict or None on failure."""

    def seconds_since_success(self) -> float:
        if self._last_success == 0:
            return float("inf")
        return time.time() - self._last_success

    def _ok(self, data: dict) -> dict:
        self._last_success = time.time()
        return data

    def _fail(self, exc: Exception | str) -> None:
        logger.error("[%s/%s] collection failed: %s",
                     self.property_id, self.__class__.__name__, exc)
        return None
