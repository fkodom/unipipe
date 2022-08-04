from __future__ import annotations

from typing import Any, Dict, Optional

from unipipe.dsl import Component, LazyAttribute, Pipeline
from unipipe.executor.base import Executor
from unipipe.utils.annotations import get_annotations


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
            _kwargs = {k: resolve_value(v) for k, v in _component.inputs.items()}
            result = _component.func(**_kwargs)
            return_type = get_annotations(_component.func, eval_str=True)["return"]
            if issubclass(return_type, tuple):
                result = return_type(*result)
            arguments[_component.name] = result
