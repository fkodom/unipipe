from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Optional

from flo.dsl import Pipeline


class Backend:
    @abstractmethod
    def build(self, pipeline: Pipeline):
        pass

    @abstractmethod
    def run(
        self,
        pipeline: Any,
        arguments: Optional[Dict] = None,
        **kwargs,
    ):
        pass

    def build_and_run(
        self,
        pipeline: Pipeline,
        arguments: Optional[Dict] = None,
        **kwargs,
    ):
        built = self.build(pipeline)
        self.run(built, arguments=arguments, **kwargs)
