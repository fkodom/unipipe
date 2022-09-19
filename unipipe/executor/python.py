from __future__ import annotations

from inspect import isclass
from typing import Any, Dict

from unipipe.dsl import Component, ConditionalPipeline, LazyAttribute, Pipeline
from unipipe.executor.base import LocalExecutor
from unipipe.utils.compat import get_annotations


class PythonExecutor(LocalExecutor):
    def resolve_local_value(self, _locals: Dict, value: Any) -> Any:
        if isinstance(value, LazyAttribute):
            return getattr(self.resolve_local_value(_locals, value.parent), value.key)
        elif isinstance(value, Pipeline):
            if value.name in _locals:
                return _locals[value.name]
            return self.resolve_local_value(_locals, value.return_value)
        elif isinstance(value, (tuple, list)):
            return tuple(self.resolve_local_value(_locals, x) for x in value)
        elif isinstance(value, Component):
            assert isinstance(_locals, dict)
            return _locals[value.name]
        else:
            return value

    def run_component(self, component: Component, **kwargs):
        result = component.func(**kwargs)
        return_type = get_annotations(component.func, eval_str=True).get("return")

        if isclass(return_type):
            if issubclass(return_type, tuple):
                result = return_type(*result)
            if not isinstance(result, return_type):
                raise TypeError(
                    f"Component function {component.func.__name__}() expected a return "
                    f"value of type '{return_type}', but found '{type(result)}'."
                )

        return result

    def run_conditional_pipeline_with_locals(
        self, pipeline: ConditionalPipeline, _locals: Dict[str, Any]
    ):
        operand1 = self.resolve_local_value(_locals, pipeline.condition.operand1)
        operand2 = self.resolve_local_value(_locals, pipeline.condition.operand2)
        comparator = pipeline.condition.comparator

        if comparator(operand1, operand2):
            return self.run_pipeline_with_locals(pipeline, _locals=_locals)
        else:
            return None, _locals
