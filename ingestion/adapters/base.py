"""Abstract base class for all source adapters."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator

from ingestion.models import RawJobPosting


class BaseAdapter(ABC):
    """
    Every source adapter must implement fetch().
    This enforces a common contract so the ingestion runner
    can swap sources without changing pipeline logic.
    """

    source_platform: str = "other"

    @abstractmethod
    def fetch(self) -> Iterator[RawJobPosting]:
        """Yield RawJobPosting objects from the source."""
        ...

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} platform={self.source_platform}>"
