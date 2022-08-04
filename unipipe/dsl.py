from __future__ import annotations

import logging
from contextlib import ExitStack
from enum import Enum
from functools import partial, wraps
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid1

from pydantic import BaseModel, parse_obj_as

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


ALLOWED_OUTPUT_TYPES = (str, int, float, bool)


class Outputs(BaseModel):
    def __init__(self, **data: Any) -> None:
        for key, value in data.items():
            if not isinstance(value, ALLOWED_OUTPUT_TYPES):
                raise TypeError(
                    f"Invalid {value=} for {key=}. Allowed types are "
                    f"{ALLOWED_OUTPUT_TYPES}, but found {type(value)=}."
                )
        super().__init__(**data)


class LazyAttribute(BaseModel):
    parent: Any
    key: str


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


class Component:
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

        self.name = name.replace("_", "-")
        logging_level = logging_level or logging.INFO
        self.func = wrap_logging_info(
            func, component_name=name, logging_level=logging_level
        )
        self.inputs = inputs or {}
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
    def _return_type(self):
        return get_annotations(self.func, eval_str=True)["return"]

    def __len__(self) -> int:
        if issubclass(self._return_type, tuple):
            return len(self._return_type._fields)

        raise TypeError(
            "Only components with 'NamedTuple' return type have a defined length. "
            f"Found return type {self._return_type}."
        )

    def __iter__(self):
        return (self[i] for i in range(len(self)))

    def __getattr__(self, key: str) -> LazyAttribute:
        return LazyAttribute(parent=self, key=key)

    def __getitem__(self, idx: int) -> LazyAttribute:
        if issubclass(self._return_type, tuple):
            key = self._return_type._fields[idx]
            return LazyAttribute(parent=self, key=key)

        raise TypeError(
            "Only components with 'NamedTuple' return type have '__getitem__' method. "
            f"Found return type {self._return_type}."
        )


def component(
    func: Optional[Callable] = None,
    name: Optional[str] = None,
    logging_level: Optional[int] = None,
    base_image: Optional[str] = None,
    packages_to_install: Optional[List[str]] = None,
    hardware: Optional[Hardware] = None,
    **kwargs,
):
    new_component = partial(
        Component,
        name=name,
        logging_level=logging_level,
        base_image=base_image,
        packages_to_install=packages_to_install,
        hardware=hardware,
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
