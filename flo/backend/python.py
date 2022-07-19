from __future__ import annotations

from typing import Dict, List, Optional, Sequence

import networkx as nx

from flo.dsl import Component, Pipeline
from flo.backend.base import Backend, RunnablePipeline


class PythonBackend(Backend):
    def build(self, pipeline: Pipeline) -> Pipeline:
        pipeline.components = topological_sort(pipeline.components)
        return pipeline

    def run(
        self,
        pipeline: RunnablePipeline,
        arguments: Optional[Dict] = None,
        **kwargs,
    ):
        return super().run(pipeline, arguments, **kwargs)


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
    built = PythonBackend().build(example)
    print([(c, list(c.inputs.items())) for c in built.components])
