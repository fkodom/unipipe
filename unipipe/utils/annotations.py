from __future__ import annotations

import functools
from inspect import isclass
from typing import Any, Callable, Dict, Type, TypeVar

from unipipe.utils.compat import get_annotations


def resolve_annotations(obj: Callable) -> Callable:
    """Returns an identical Callable object, but with all stringified type annotations
    resolved to Type objects.

    NOTE: KFP requires us to de-stringify type annotations before compiling
    components/pipelines.  It's also helpful for other backends/executors, since
    we no longer have to worry about de-stringify annotations each type we
    perform a type check.
    """
    setattr(obj, "__annotations__", get_annotations(obj, eval_str=True))
    return obj


def infer_type(obj: Any) -> Type:
    from unipipe.dsl import Component, LazyAttribute, LazyItem, Pipeline

    if isinstance(obj, Component):
        return get_annotations(obj.func, eval_str=True)["return"]
    elif isinstance(obj, Pipeline):
        return obj.return_type or infer_type(obj.return_value)
    elif isinstance(obj, LazyAttribute):
        parent_type = infer_type(obj.parent)
        return get_annotations(parent_type, eval_str=True).get(obj.key)
    elif isinstance(obj, LazyItem):
        return infer_type(obj.parent[obj.idx])
    else:
        return type(obj)


def infer_input_types(inputs: Dict) -> Dict:
    return {k: infer_type(v) for k, v in inputs.items() if k != "return"}


_T = TypeVar("_T", str, float, int, bool, tuple)


def cast_output_type(output: Any, _type: Type[_T]) -> _T:
    if _type is None:
        assert output is None
        return output
    elif isclass(_type) and issubclass(_type, tuple):
        annotations = get_annotations(_type, eval_str=True)
        _kwargs = {
            k: cast_output_type(v, annotations[k])
            for k, v in zip(_type._fields, output)  # type: ignore
        }
        return _type(**_kwargs)  # type: ignore
    else:
        return _type(output)


def wrap_cast_output_type(func: Callable, _type: Type[_T]) -> Callable[..., _T]:
    @functools.wraps(func)
    def wrapped(*args, **kwargs) -> _T:
        result = func(*args, **kwargs)
        return cast_output_type(result, _type)

    return wrapped
