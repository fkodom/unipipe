from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Type

from unipipe.utils.annotations import get_annotations


class MultipleDispatch:
    def __init__(self):
        self.funcs: List[Callable] = []
        self.signatures: List[Dict] = []

    def add(self, func: Callable, signature: Optional[Dict] = None):
        if not signature:
            signature = get_annotations(func, eval_str=True)
        assert signature is not None
        if "return" in signature:
            signature.pop("return")
        self.funcs.append(func)
        self.signatures.append(signature)

    def __getitem__(self, inputs):
        signature = _infer_signature(inputs)
        for func, _signature in zip(self.funcs, self.signatures):
            if not len(signature) == len(_signature):
                continue
            elif all(issubclass(v, _signature[k]) for k, v in signature.items()):
                return func
        print(self.signatures)
        print(signature)
        return None

    def __call__(self, **inputs):
        func = self[inputs]
        return func(**inputs)


class DispatchRegistry:
    d: Dict[str, MultipleDispatch] = defaultdict(MultipleDispatch)

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __call__(self, func: Callable, signature: Optional[Dict] = None):
        name = func.__qualname__
        self.d[name].add(func, signature=signature)
        return self.d[name]


dispatch = DispatchRegistry()


def component_dispatch(**kwargs):
    from unipipe.dsl import Hardware, component

    if "hardware" not in kwargs:
        kwargs["hardware"] = Hardware(cpus=1, memory="512M")

    def wrapper(func: Callable):
        comp = component(func, **kwargs)
        signature = get_annotations(func, eval_str=True)
        dispatch(comp, signature=signature)
        return comp

    return wrapper


def _infer_type(obj: Any) -> Type:
    from unipipe.dsl import Component, LazyAttribute

    if isinstance(obj, Component):
        return get_annotations(obj.func, eval_str=True)["return"]
    elif isinstance(obj, LazyAttribute):
        parent_type = _infer_type(obj.parent)
        return get_annotations(parent_type, eval_str=True).get(obj.key)
    else:
        return type(obj)


def _infer_signature(inputs: Dict) -> Dict:
    return {k: _infer_type(v) for k, v in inputs.items() if k != "return"}


# fmt: off

# @dispatch
# def _bool(a: str): return bool(str)
# @dispatch
# def _bool(a: int): return bool(int)
# @dispatch
# def _bool(a: float): return bool(float)

@dispatch
def equal(a: str, b: str): return a == b  # noqa: E704, F811
@dispatch  # type: ignore
def equal(a: int, b: str): return a == b  # noqa: E704, F811
@dispatch  # type: ignore
def equal(a: float, b: str): return a == b  # noqa: E704, F811


@dispatch
def add(a: str, b: str) -> str: return a + b  # noqa: E704, F811
@dispatch  # type: ignore
def add(a: int, b: int) -> int: return a + b  # noqa: E704, F811
@dispatch  # type: ignore
def add(a: int, b: bool) -> int: return a + b  # noqa: E704, F811
@dispatch  # type: ignore
def add(a: int, b: float) -> float: return a + b  # noqa: E704, F811
@dispatch  # type: ignore
def add(a: float, b: int) -> float: return a + b  # noqa: E704, F811
@dispatch  # type: ignore
def add(a: float, b: bool) -> float: return a + b  # noqa: E704, F811
@dispatch  # type: ignore
def add(a: float, b: float) -> float: return a + b  # noqa: E704, F811
@dispatch  # type: ignore
def add(a: bool, b: bool) -> bool: return a + b  # noqa: E704, F811
@dispatch  # type: ignore
def add(a: bool, b: int) -> int: return a + b  # noqa: E704, F811
@dispatch  # type: ignore
def add(a: bool, b: float) -> float: return a + b  # noqa: E704, F811


@dispatch
def sub(a: int, b: int) -> int: return a - b  # noqa: E704, F811
@dispatch  # type: ignore
def sub(a: int, b: bool) -> int: return a - b  # noqa: E704, F811
@dispatch  # type: ignore
def sub(a: int, b: float) -> float: return a - b  # noqa: E704, F811
@dispatch  # type: ignore
def sub(a: float, b: int) -> float: return a - b  # noqa: E704, F811
@dispatch  # type: ignore
def sub(a: float, b: bool) -> float: return a - b  # noqa: E704, F811
@dispatch  # type: ignore
def sub(a: float, b: float) -> float: return a - b  # noqa: E704, F811
@dispatch  # type: ignore
def sub(a: bool, b: bool) -> bool: return a - b  # noqa: E704, F811
@dispatch  # type: ignore
def sub(a: bool, b: int) -> int: return a - b  # noqa: E704, F811
@dispatch  # type: ignore
def sub(a: bool, b: float) -> float: return a - b  # noqa: E704, F811

# fmt: on
