from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

from unipipe.utils.annotations import get_annotations, infer_input_types


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

    def __getitem__(self, inputs: Dict[str, Any]):
        signature = infer_input_types(inputs)
        for func, _signature in zip(self.funcs, self.signatures):
            if not len(signature) == len(_signature):
                continue
            elif all(issubclass(v, _signature[k]) for k, v in signature.items()):
                return func

        raise TypeError(
            f"Could not find a function with 'signature={signature}'. Available "
            f"signatures include: [{self.signatures}]."
        )

    def __call__(self, **inputs):
        func = self[inputs]
        return func(**inputs)


class DispatchRegistry:
    d: Dict[str, MultipleDispatch] = defaultdict(MultipleDispatch)

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __getitem__(self, name: str) -> MultipleDispatch:
        return self.d[name]

    def __call__(self, func: Callable, signature: Optional[Dict] = None):
        name = func.__qualname__
        self[name].add(func, signature=signature)
        return self.d[name]


dispatch = DispatchRegistry()


# fmt: off

@dispatch
def len_(a: str) -> int: return len(a)  # noqa: E704, F811
@dispatch  # type: ignore
def len_(a: tuple) -> int: return len(a)  # noqa: E704, F811


@dispatch
def int_(a: str) -> int: return int(a)  # noqa: E704, F811
@dispatch  # type: ignore
def int_(a: int) -> int: return int(a)  # noqa: E704, F811
@dispatch  # type: ignore
def int_(a: float) -> int: return int(a)  # noqa: E704, F811
@dispatch  # type: ignore
def int_(a: bool) -> int: return int(a)  # noqa: E704, F811


@dispatch
def float_(a: str) -> float: return float(a)  # noqa: E704, F811
@dispatch  # type: ignore
def float_(a: int) -> float: return float(a)  # noqa: E704, F811
@dispatch  # type: ignore
def float_(a: float) -> float: return float(a)  # noqa: E704, F811
@dispatch  # type: ignore
def float_(a: bool) -> float: return float(a)  # noqa: E704, F811


@dispatch
def str_(a: str) -> str: return str(a)  # noqa: E704, F811
@dispatch  # type: ignore
def str_(a: int) -> str: return str(a)  # noqa: E704, F811
@dispatch  # type: ignore
def str_(a: float) -> str: return str(a)  # noqa: E704, F811
@dispatch  # type: ignore
def str_(a: bool) -> str: return str(a)  # noqa: E704, F811


# TODO:
#   - abs
#   - round
#   - ceil
#   - floor
#   - ge
#   - gt
#   - le
#   - lt
#   - ne
#   - contains


@dispatch
def equal(a: str, b: str): return a == b  # noqa: E704, F811
@dispatch  # type: ignore
def equal(a: int, b: int): return a == b  # noqa: E704, F811
@dispatch  # type: ignore
def equal(a: int, b: float): return a == b  # noqa: E704, F811
@dispatch  # type: ignore
def equal(a: float, b: int): return a == b  # noqa: E704, F811
@dispatch  # type: ignore
def equal(a: float, b: float): return a == b  # noqa: E704, F811
@dispatch  # type: ignore
def equal(a: tuple, b: tuple): return a == b  # noqa: E704, F811


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


@dispatch
def mul(a: str, b: int) -> str: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: int, b: str) -> str: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: int, b: int) -> int: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: int, b: bool) -> int: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: int, b: float) -> float: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: float, b: int) -> float: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: float, b: bool) -> float: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: float, b: float) -> float: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: bool, b: bool) -> int: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: bool, b: int) -> int: return a * b  # noqa: E704, F811
@dispatch  # type: ignore
def mul(a: bool, b: float) -> float: return a * b  # noqa: E704, F811


@dispatch
def div(a: int, b: int) -> int: return a / b  # noqa: E704, F811
@dispatch  # type: ignore
def div(a: int, b: float) -> float: return a / b  # noqa: E704, F811
@dispatch  # type: ignore
def div(a: float, b: int) -> float: return a / b  # noqa: E704, F811
@dispatch  # type: ignore
def div(a: float, b: float) -> float: return a / b  # noqa: E704, F811


@dispatch
def floordiv(a: int, b: int) -> int: return a // b  # noqa: E704, F811
@dispatch  # type: ignore
def floordiv(a: int, b: float) -> float: return a // b  # noqa: E704, F811
@dispatch  # type: ignore
def floordiv(a: float, b: int) -> float: return a // b  # noqa: E704, F811
@dispatch  # type: ignore
def floordiv(a: float, b: float) -> float: return a // b  # noqa: E704, F811


@dispatch
def contains(a: str, b: str) -> bool: return a.__contains__(b)  # noqa: E704, F811

# fmt: on
