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
        # pipeline.components = topological_sort(pipeline.components)
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
            # TODO: Analyze signature for Input/Output types
            #
            # from inspect import signature
            # sign = signature(_component.func)
            # for name, param in sign.parameters.items():
            #     param_type = type(eval(param.annotation))
            #     if param_type is type(Output):
            #         arguments[_component.name]

            _kwargs = {k: resolve_value(v) for k, v in _component.inputs.items()}
            result = _component.func(**_kwargs)
            arguments[_component.name] = result


if __name__ == "__main__":
    from typing import NamedTuple

    from flo.dsl import component, pipeline

    # TODO: Turn these examples into unit tests!

    class EchoOutputs(NamedTuple):
        phrase1: str
        phrase2: str

    @component
    def echo(phrase: str) -> EchoOutputs:
        print(phrase)
        return EchoOutputs(phrase1=f"{phrase}_1", phrase2=f"{phrase}_2")

    @pipeline
    def example_pipeline():
        phrase = echo(phrase="Hello, world!")
        _ = echo(phrase=phrase.phrase1)
        _ = echo(phrase=phrase.phrase2)
        # _ = echo_to_file(phrase=phrase)

    example = example_pipeline()
    PythonExecutor().run(example)
