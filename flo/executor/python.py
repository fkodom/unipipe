from __future__ import annotations

from typing import Any, Dict, Optional

from flo.dsl import Component, LazyAttribute, Pipeline
from flo.executor.base import Executor


class PythonExecutor(Executor):
    def run(
        self,
        pipeline: Pipeline,
        arguments: Optional[Dict] = None,
        **kwargs,
    ):
        if arguments is None:
            arguments = {}

        def resolve_value(v: Any) -> Any:
            if isinstance(v, LazyAttribute):
                return getattr(resolve_value(v.parent), v.key)
            elif isinstance(v, Component):
                assert isinstance(arguments, dict)
                return arguments[v.name]
            else:
                return v

        for _component in pipeline.components:
            # TODO: Analyze signature for Input/Output types???
            _kwargs = {k: resolve_value(v) for k, v in _component.inputs.items()}
            result = _component.func(**_kwargs)
            arguments[_component.name] = result
