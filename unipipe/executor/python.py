from __future__ import annotations

from copy import deepcopy
from inspect import isclass
from typing import Any, Dict, Optional

from unipipe.dsl import Component, LazyAttribute, Pipeline
from unipipe.executor.base import Executor
from unipipe.utils.annotations import get_annotations


def resolve_value(arguments: Dict, value: Any) -> Any:
    if isinstance(value, LazyAttribute):
        return getattr(resolve_value(arguments, value.parent), value.key)
    elif isinstance(value, Pipeline):
        if value.name in arguments:
            return arguments[value.name]
        return resolve_value(arguments, value.return_value)
    elif isinstance(value, (tuple, list)):
        return tuple(resolve_value(arguments, x) for x in value)
    elif isinstance(value, Component):
        assert isinstance(arguments, dict)
        return arguments[value.name]
    else:
        return value


class PythonExecutor(Executor):
    def run(self, pipeline: Pipeline, arguments: Optional[Dict] = None, **kwargs):
        if arguments is None:
            arguments = deepcopy(pipeline.inputs)

        for comp in pipeline.components:
            _kwargs = {k: resolve_value(arguments, v) for k, v in comp.inputs.items()}
            if isinstance(comp, Pipeline):
                result = self.run(comp, arguments=_kwargs)
            else:
                result = comp.func(**_kwargs)
                return_type = get_annotations(comp.func, eval_str=True).get("return")
                if isclass(return_type) and issubclass(return_type, tuple):
                    result = return_type(*result)

            arguments[comp.name] = result

        return resolve_value(arguments, pipeline.return_value)
