from __future__ import annotations

from contextlib import ExitStack
from functools import partial
from types import TracebackType
from typing import Annotated, Dict, Optional, Callable, List, TypeVar
from uuid import uuid1

from kfp.v2.dsl import Artifact


class InputAnnotation:
    """Marker type for input artifacts."""


class OutputAnnotation:
    """Marker type for output artifacts."""


T = TypeVar("T")
# Input represents an Input artifact of type T.
Input = Annotated[T, InputAnnotation]
# Output represents an Output artifact of type T.
Output = Annotated[T, OutputAnnotation]


class Component:
    def __init__(
        self,
        func: Callable,
        name: Optional[str] = None,
        inputs: Optional[Dict] = None,
        # Not used by the local executor
        base_image: Optional[str] = None,
        packages_to_install: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        """
        Args:
            func: (Callable) Function that defines this component.
            name: (str) The name for this component.
            inputs: Optional(Dict) A dictionary containing the input values assigned to their parameter names.
        """
        if name is None:
            uuid = str(uuid1())[:8]
            name = f"{func.__name__}-{uuid}"

        self.func = func
        self.name = name.replace("_", "-")
        self.inputs = inputs or {}
        self.base_image = base_image
        self.packages_to_install = packages_to_install
        self.kwargs = kwargs

        pipeline = PipelineContext().current
        if pipeline is not None:
            pipeline.components.append(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"


def component(
    func: Optional[Callable] = None,
    name: Optional[str] = None,
    base_image: Optional[str] = None,
    packages_to_install: Optional[List[str]] = None,
    **kwargs,
):
    new_component = partial(
        Component,
        name=name,
        base_image=base_image,
        packages_to_install=packages_to_install,
        **kwargs,
    )

    def wrapped_component(**inputs):
        return new_component(func=func, inputs=inputs)

    def wrapper(func: Callable) -> Callable:
        def wrapped_component(**inputs):
            return new_component(func=func, inputs=inputs)

        return wrapped_component

    if func is None:
        return wrapper
    else:
        return wrapped_component


class Pipeline(ExitStack):
    def __init__(
        self,
        name: Optional[str] = None,
        components: Optional[List[Component]] = None,
    ) -> None:
        """
        Args:
            name: Name of the pipeline.
            components: List of components to register to the pipeline.
        """
        super().__init__()
        if name is None:
            uuid = str(uuid1())[:8]
            name = f"{self.__class__.__name__.lower()}-{uuid}"

        self.name = name.replace("_", "-")  # Cannot use _ in pipeline names
        self.components = components or []
        self.parent: Optional[Pipeline] = None

    def __enter__(self):
        context = PipelineContext()
        self.parent = context.current
        context.current = self
        return self

    def __exit__(
        self,
        type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        context = PipelineContext()
        context.current = self.parent
        return super().__exit__(type, exc, traceback)

    def __len__(self):
        return len(self.components)


class PipelineContext:
    current: Optional[Pipeline] = None

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance


def pipeline(
    func: Optional[Callable] = None,
    name: Optional[str] = None,
    **kwargs,
):
    def wrapped_pipeline(**inputs):
        with Pipeline(name=name, **kwargs) as pipe:
            func(**inputs)
        return pipe

    def wrapper(func: Callable) -> Callable:
        def wrapped_pipeline(**inputs):
            with Pipeline(name=name, **kwargs) as pipe:
                func(**inputs)
            return pipe

        return wrapped_pipeline

    if func is None:
        return wrapper
    else:
        return wrapped_pipeline


if __name__ == "__main__":
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
    print(len(example))
    print(example.components[1].inputs)

    # Example using context managers, with an additional nested pipeline.
    # Not sure yet if/when child pipelines would be useful, but it's possible anyway.
    with Pipeline() as parent:
        echo1 = echo(phrase="Hello, world!")
        echo2 = echo(phrase=echo1)

        with Pipeline() as child:
            echo3 = echo(phrase="Goodbye, world!")
            print(len(child))

    print(len(parent))
    print(parent.components[1].inputs)
