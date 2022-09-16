from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Tuple

from unipipe.dsl import Component, ConditionalPipeline, Pipeline


class Executor:
    @abstractmethod
    def run(self, pipeline: Pipeline):
        pass


class LocalExecutor(Executor):
    @abstractmethod
    def resolve_local_value(self, _locals: Dict, value: Any) -> Any:
        pass

    @abstractmethod
    def run_component(self, component: Component, **kwargs):
        pass

    @abstractmethod
    def run_conditional_pipeline_with_locals(
        self, pipeline: ConditionalPipeline, _locals: Dict[str, Any]
    ):
        pass

    def run_pipeline_with_locals(
        self, pipeline: Pipeline, _locals: Dict[str, Any]
    ) -> Tuple[Any, Dict[str, Any]]:
        for component in pipeline.components:
            kwargs = {
                k: self.resolve_local_value(_locals, v)
                for k, v in component.inputs.items()
            }
            __locals = {**_locals, **kwargs}
            if isinstance(component, ConditionalPipeline):
                result, _ = self.run_conditional_pipeline_with_locals(
                    component, _locals=__locals
                )
            elif isinstance(component, Pipeline):
                result, _ = self.run_pipeline_with_locals(component, _locals=__locals)
            elif isinstance(component, Component):
                result = self.run_component(component, **kwargs)
            else:
                raise TypeError(
                    f"Found pipeline component {component} with unexpected type: "
                    f"{type(component)}. Valid component types are "
                    "[Component, ConditionalPipeline, Pipeline]."
                )

            _locals[component.name] = result

        return_value = self.resolve_local_value(_locals, pipeline.return_value)
        return return_value, _locals

    def run(self, pipeline: Pipeline):
        return_value, _ = self.run_pipeline_with_locals(
            pipeline, _locals=pipeline.inputs
        )
        return return_value
