from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Optional


class Executor:
    @abstractmethod
    def run(
        self,
        pipeline: Any,
        arguments: Optional[Dict] = None,
        **kwargs,
    ):
        pass
