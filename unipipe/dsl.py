from __future__ import annotations

import logging
from contextlib import ExitStack, contextmanager
from enum import Enum
from functools import partial, wraps
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
)
from uuid import uuid1

from pydantic import BaseModel, parse_obj_as

from unipipe.utils import ops
from unipipe.utils.annotations import get_annotations, wrap_cast_output_type

T_co = TypeVar("T_co", covariant=True)


class AcceleratorType(str, Enum):
    T4 = "nvidia-tesla-t4"
    V100 = "nvidia-tesla-v100"
    P4 = "nvidia-tesla-p4"
    P100 = "nvidia-tesla-p100"
    K80 = "nvidia-tesla-k80"


class Accelerator(BaseModel):
    count: Optional[int] = None
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


# TODO: Open an issue with google about the minimum allowable hardware. Even though
# custom jobs allow the 'e2-standard-1' machine type, apparently Vertex Pipelines
# will only allocate 'e2-highmem-2' as the smallest machine type.
#
# Not a huge deal, but will cost you a few cents here and there for small jobs.
MINIMAL_HARDWARE = Hardware(cpus=1, memory="512M")


class _Operable:
    def __len__(self) -> Component:
        return dispatch_to_component(ops.len_, a=self)

    def __str__(self) -> Component:  # type: ignore
        return dispatch_to_component(ops.str_, a=self)

    def __int__(self) -> Component:  # type: ignore
        return dispatch_to_component(ops.int_, a=self)

    def __float__(self) -> Component:  # type: ignore
        return dispatch_to_component(ops.float_, a=self)

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
    idx: int


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


def _base_image_for_hardware(hardware: Hardware) -> str:
    accelerator = hardware.accelerator
    if accelerator is not None and accelerator.count:
        return "fkodom/unipipe:latest-cuda"
    else:
        return "fkodom/unipipe:latest"


def get_pip_index_urls(urls: Optional[Sequence[str]]) -> Optional[List[str]]:
    if urls is None:
        return None

    PYPI_URL = "https://pypi.org/simple"
    result = [url for url in urls]
    if PYPI_URL not in result:
        result.append(PYPI_URL)

    return result


class Component(_Operable, Generic[T_co]):
    def __init__(
        self,
        func: Callable,
        name: Optional[str] = None,
        inputs: Optional[Dict] = None,
        logging_level: Optional[int] = None,
        # Not used by the local executor
        base_image: Optional[str] = None,
        packages_to_install: Optional[List[str]] = None,
        pip_index_urls: Optional[List[str]] = None,
        hardware: Optional[Union[Dict, Hardware]] = None,
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
        self.return_type = get_annotations(func, eval_str=True)["return"]
        self.func = wrap_logging_info(
            wrap_cast_output_type(func, _type=self.return_type),
            component_name=name,
            logging_level=logging_level,
        )
        self.inputs = inputs
        self.logging_level = logging_level
        self.packages_to_install = packages_to_install
        self.pip_index_urls = get_pip_index_urls(pip_index_urls)
        self.hardware = parse_obj_as(Hardware, hardware) if hardware else Hardware()
        self.base_image = base_image or _base_image_for_hardware(self.hardware)

        pipeline = PipelineContext().current
        if pipeline is not None:
            pipeline.components.append(self)

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
    pip_index_urls: Optional[List[str]] = None,
    hardware: Optional[Union[Dict, Hardware]] = None,
) -> Callable:
    new_component = partial(
        Component,
        name=name,
        logging_level=logging_level,
        base_image=base_image,
        packages_to_install=packages_to_install,
        pip_index_urls=pip_index_urls,
        hardware=hardware,
    )

    if func is None:

        def wrapper(func: Callable) -> Callable[..., Component]:
            @wraps(func)
            def wrapped_component(**inputs) -> Component:
                return new_component(func=func, inputs=inputs)

            return wrapped_component

        return wrapper
    else:

        @wraps(func)
        def wrapped_component(**inputs) -> Component:
            return new_component(func=func, inputs=inputs)

        return wrapped_component


def dispatch_to_component(dispatch: ops.MultipleDispatch, **kwargs) -> Component:
    func = dispatch[kwargs]
    component_func = component(func=func, hardware=MINIMAL_HARDWARE)
    return component_func(**kwargs)


class Pipeline(ExitStack, _Operable, Generic[T_co]):
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

    @property
    def return_type(self) -> Type:
        return type(self.return_value)

    def _len(self) -> int:
        if issubclass(self.return_type, (tuple, list, dict)):
            assert self.return_value is not None
            return len(self.return_value)

        raise TypeError(
            f"Length of 'return_value={self.return_value}' is not defined at runtime "
            "(not determined until execution time). Pipelines with return types "
            "(tuple, list, dict) are defined at runtime, though, and can be "
            "indexed/iterated like normal Python container objects."
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
            def wrapped_pipeline(**inputs) -> Pipeline:
                with Pipeline(name=name, **kwargs) as pipe:
                    return_value = func(**inputs)
                    pipe.return_value = return_value
                return pipe

            return wrapped_pipeline

        return wrapper
    else:

        def wrapped_pipeline(**inputs) -> Pipeline:
            assert func is not None
            with Pipeline(name=name, **kwargs) as pipe:
                return_value = func(**inputs)
                pipe.return_value = return_value
            return pipe

        return wrapped_pipeline


class Condition(BaseModel):
    operand1: Any
    operand2: Any
    comparator: Callable


class ConditionalPipeline(Pipeline):
    def __init__(
        self,
        condition: Condition,
        name: Optional[str] = None,
        components: Optional[List[Union[Component, Pipeline]]] = None,
        inputs: Optional[Dict] = None,
        return_value: Optional[Any] = None,
    ) -> None:
        super().__init__(name, components, inputs, return_value)
        self.condition = condition


@contextmanager
def condition(operand1: Any, operand2: Any, comparator: Callable[[Any, Any], bool]):
    _condition = Condition(operand1=operand1, operand2=operand2, comparator=comparator)
    pipeline = ConditionalPipeline(condition=_condition)
    try:
        with pipeline:
            yield pipeline
    finally:
        pass


equal = partial(condition, comparator=lambda o1, o2: o1 == o2)
not_equal = partial(condition, comparator=lambda o1, o2: o1 != o2)
