from __future__ import annotations

from typing import Dict, Optional

from flo.dsl import Component, Pipeline
from flo.executor.base import Executor
from flo.utils.sort import topological_sort


class PythonExecutor(Executor):
    def run(
        self,
        pipeline: Pipeline,
        arguments: Optional[Dict] = None,
        **kwargs,
    ):
        pipeline.components = topological_sort(pipeline.components)
        if arguments is None:
            arguments = {}

        for _component in pipeline.components:
            # TODO: Analyze signature for Input/Output types
            #
            # from inspect import signature
            # sign = signature(_component.func)
            # for name, param in sign.parameters.items():
            #     param_type = type(eval(param.annotation))
            #     if param_type is type(Output):
            #         arguments[_component.name]

            _kwargs = {
                k: arguments[v.name] if isinstance(v, Component) else v
                for k, v in _component.inputs.items()
            }
            result = _component.func(**_kwargs)
            arguments[_component.name] = result


if __name__ == "__main__":
    from flo.dsl import component, pipeline

    # TODO: Turn these examples into unit tests!
    # Example using function decorators
    @component
    def echo(phrase: str):
        print(phrase)
        return phrase

    @pipeline
    def example_pipeline():
        echo1 = echo(phrase="Hello, world!")
        _ = echo(phrase=echo1)

    example = example_pipeline()
    PythonExecutor().run(example)
