from __future__ import annotations

from inspect import signature
from typing import Dict, List, Optional, Sequence

import networkx as nx

from flo.dsl import Component, Pipeline, Input, Output
from flo.backend.base import Backend


class PythonBackend(Backend):
    def build(self, pipeline: Pipeline) -> Pipeline:
        pipeline.components = topological_sort(pipeline.components)
        return pipeline

    def run(
        self,
        pipeline: Pipeline,
        arguments: Optional[Dict] = None,
        **kwargs,
    ):
        if arguments is None:
            arguments = {}

        for _component in pipeline.components:
            # TODO: Analyze signature for Input/Output types
            #
            # sign = signature(_component.func)
            # for name, param in sign.parameters.items():
            #     param_type = type(eval(param.annotation))
            #     if param_type is type(Output):
            #         arguments[_component.name]

            kwargs = {
                k: arguments[v.name] if isinstance(v, Component) else v
                for k, v in _component.inputs.items()
            }
            result = _component.func(**kwargs)
            arguments[_component.name] = result


def topological_sort(components: Sequence[Component]) -> List[Component]:
    digraph = nx.DiGraph(
        [
            (_input, component.name)
            for component in components
            for _input in component.inputs.values()
        ]
    )
    ordered = nx.topological_sort(digraph)
    components_by_name = {component.name: component for component in components}
    result = [components_by_name.get(name, None) for name in ordered]
    return [x for x in result if x is not None]


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
    PythonBackend().build_and_run(example)
