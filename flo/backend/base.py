from __future__ import annotations

from abc import abstractmethod
from typing import Dict, Optional

from flo.dsl import Pipeline


class RunnablePipeline:
    pass


class Backend:
    @abstractmethod
    def build(self, pipeline: Pipeline) -> RunnablePipeline:
        pass

    @abstractmethod
    def run(
        self,
        pipeline: RunnablePipeline,
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
