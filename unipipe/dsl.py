from __future__ import annotations

import logging
from contextlib import ExitStack
from enum import Enum
from functools import partial, wraps
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Type, Union
from uuid import uuid1

from pydantic import BaseModel, parse_obj_as

from unipipe.utils import ops
from unipipe.utils.annotations import get_annotations

__all__ = [
    "AcceleratorType",
    # "Artifact",
    "Component",
    "Hardware",
    # "Input",
    # "Output",
    "Pipeline",
    "component",
    "pipeline",
]


class AcceleratorType(str, Enum):
    T4 = "nvidia-tesla-t4"
    V100 = "nvidia-tesla-v100"
    P4 = "nvidia-tesla-p4"
    P100 = "nvidia-tesla-p100"
    K80 = "nvidia-tesla-k80"


class Accelerator(BaseModel):
    count: Optional[str] = None
    type: Optional[AcceleratorType] = None

    class Config:
        allow_population_by_field_name: bool = True


class Hardware(BaseModel):
    """
    Hardware resources for a pipeline component.
    """

    cpus: Optional[str] = None
    memory: Optional[str] = None
    accelerator: Optional[Accelerator] = None

    class Config:
        allow_population_by_field_name: bool = True


MINIMAL_HARDWARE = Hardware(cpus=1, memory="512M")


class _Operable:
    def __len__(self, other: Any) -> Component:
        return dispatch_to_component(ops.len_, a=self, b=other)

    def __str__(self, other: Any) -> Component:  # type: ignore
        return dispatch_to_component(ops.str_, a=self, b=other)

    def __int__(self, other: Any) -> Component:  # type: ignore
        return dispatch_to_component(ops.int_, a=self, b=other)

    def __float__(self, other: Any) -> Component:  # type: ignore
        return dispatch_to_component(ops.float_, a=self, b=other)

    def __add__(self, other: Any) -> Component:
        return dispatch_to_component(ops.add, a=self, b=other)

    def __sub__(self, other: Any) -> Component:
        return dispatch_to_component(ops.sub, a=self, b=other)

    def __mul__(self, other: Any) -> Component:
        return dispatch_to_component(ops.mul, a=self, b=other)

    def __div__(self, other: Any) -> Component:
        return dispatch_to_component(ops.div, a=self, b=other)

    def __eq__(self, other: Any) -> Component:  # type: ignore
        return dispatch_to_component(ops.equal, a=self, b=other)


class LazyAttribute(BaseModel, _Operable):  # type: ignore
    parent: Any
    key: str


class LazyItem(BaseModel, _Operable):  # type: ignore
    parent: Any
    key: int


def wrap_logging_info(
    func: Callable, component_name: str, logging_level: int
) -> Callable:
    @wraps(func)
    def wrapped(*args, **kwargs):
        import logging

        logging.getLogger().setLevel(logging_level)
        result = func(*args, **kwargs)
        logging.info(f"[{component_name}] - {result}")
        return result

    return wrapped


class Component(_Operable):
    def __init__(
        self,
        func: Callable,
        name: Optional[str] = None,
        inputs: Optional[Dict] = None,
        logging_level: Optional[int] = None,
        # Not used by the local executor
        base_image: Optional[str] = None,
        packages_to_install: Optional[List[str]] = None,
        hardware: Optional[Hardware] = None,
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
        inputs = inputs or {}

        self.name = name.replace("_", "-")
        logging_level = logging_level or logging.INFO
        self.func = wrap_logging_info(
            func, component_name=name, logging_level=logging_level
        )
        self.inputs = inputs
        self.logging_level = logging_level
        self.base_image = base_image
        self.packages_to_install = packages_to_install
        self.hardware = parse_obj_as(Hardware, hardware) if hardware else Hardware()
        self.kwargs = kwargs

        pipeline = PipelineContext().current
        if pipeline is not None:
            pipeline.components.append(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"

    @property
    def return_type(self):
        return get_annotations(self.func, eval_str=True)["return"]

    def _len(self) -> int:
        if issubclass(self.return_type, tuple):
            return len(self.return_type._fields)

        raise TypeError(
            "Only components with 'NamedTuple' return type have a defined length. "
            f"Found return type {self.return_type}."
        )

    def __iter__(self):
        return (self[i] for i in range(self._len()))

    def __getattr__(self, key: str) -> LazyAttribute:
        return LazyAttribute(parent=self, key=key)

    def __getitem__(self, idx: int) -> LazyAttribute:
        if issubclass(self.return_type, tuple):
            key = self.return_type._fields[idx]
            return LazyAttribute(parent=self, key=key)

        raise TypeError(
            "Only components with 'NamedTuple' return type have '__getitem__' method. "
            f"Found return type {self.return_type}."
        )


def component(
    func: Optional[Callable] = None,
    name: Optional[str] = None,
    logging_level: Optional[int] = None,
    base_image: Optional[str] = None,
    packages_to_install: Optional[List[str]] = None,
    hardware: Optional[Hardware] = None,
    **kwargs,
) -> Callable[..., Component]:
    new_component = partial(
        Component,
        name=name,
        logging_level=logging_level,
        base_image=base_image,
        packages_to_install=packages_to_install,
        hardware=hardware,
        **kwargs,
    )

    if func is None:

        def wrapper(func: Callable) -> Callable:
            @wraps(func)
            def wrapped_component(**inputs):
                return new_component(func=func, inputs=inputs)

            return wrapped_component

        return wrapper
    else:

        @wraps(func)
        def wrapped_component(**inputs):
            return new_component(func=func, inputs=inputs)

        return wrapped_component


def dispatch_to_component(dispatch: ops.MultipleDispatch, **kwargs) -> Component:
    func = dispatch[kwargs]
    component_func = component(func=func, hardware=MINIMAL_HARDWARE)
    return component_func(**kwargs)


# class Condition(ExitStack):
#     pass


class Pipeline(ExitStack, _Operable):
    def __init__(
        self,
        name: Optional[str] = None,
        components: Optional[List[Union[Component, Pipeline]]] = None,
        inputs: Optional[Dict] = None,
        return_value: Optional[Any] = None,
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
        self.inputs = inputs or {}
        self.return_value = return_value
        self.parent: Optional[Pipeline] = None

        context = PipelineContext()
        if context.current is not None:
            context.current.components.append(self)

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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"

    @property
    def return_type(self) -> Type:
        return type(self.return_value)

    def _len(self) -> int:
        if issubclass(self.return_type, (tuple, list, dict)):
            assert self.return_value is not None
            return len(self.return_value)

        raise TypeError(
            f"Length of {self.return_value=} is not defined at runtime (not determined "
            "until execution time). Pipelines with return types (tuple, list, dict) "
            "are defined at runtime, though, and can be indexed/iterated like normal "
            "Python container objects."
        )

    def __iter__(self):
        return (self[i] for i in range(self._len()))

    def __getitem__(self, idx: int) -> LazyItem:
        return LazyItem(parent=self.return_value, idx=idx)

    def __getattr__(self, key: str) -> LazyAttribute:
        return LazyAttribute(parent=self, key=key)


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
    if func is None:

        def wrapper(func: Callable) -> Callable:
            def wrapped_pipeline(**inputs):
                with Pipeline(name=name, **kwargs) as pipe:
                    return_value = func(**inputs)
                    pipe.return_value = return_value
                return pipe

            return wrapped_pipeline

        return wrapper
    else:

        def wrapped_pipeline(**inputs):
            with Pipeline(name=name, **kwargs) as pipe:
                return_value = func(**inputs)
                pipe.return_value = return_value
            return pipe

        return wrapped_pipeline
