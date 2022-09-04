from __future__ import annotations

import functools
import sys
import types
from inspect import isclass
from typing import Any, Callable, Dict, Type, TypeVar


def get_annotations(obj, *, globals=None, locals=None, eval_str=False):  # noqa: C901
    """Copy-pasta from the 'inspect' module in Python>=3.10.

    The '__future__.annotations' module works by "stringify-ing" type annotations,
    so they can be lazily evaluated at runtime, rather than at the time the
    function is defined.  With 'eval_str=True', this function will evaluate the
    type annotations for Callable objects, even when those annotations have been
    stringified.  (That's the intended usage within 'flo', but I avoided changing
    the default value 'eval_str=True' to be consistent with Python>=3.10.)

    ----------------

    Compute the annotations dict for an object.
    obj may be a callable, class, or module.
    Passing in an object of any other type raises TypeError.
    Returns a dict.  get_annotations() returns a new dict every time
    it's called; calling it twice on the same object will return two
    different but equivalent dicts.
    This function handles several details for you:
      * If eval_str is true, values of type str will
        be un-stringized using eval().  This is intended
        for use with stringized annotations
        ("from __future__ import annotations").
      * If obj doesn't have an annotations dict, returns an
        empty dict.  (Functions and methods always have an
        annotations dict; classes, modules, and other types of
        callables may not.)
      * Ignores inherited annotations on classes.  If a class
        doesn't have its own annotations dict, returns an empty dict.
      * All accesses to object members and dict values are done
        using getattr() and dict.get() for safety.
      * Always, always, always returns a freshly-created dict.
    eval_str controls whether or not values of type str are replaced
    with the result of calling eval() on those values:
      * If eval_str is true, eval() is called on values of type str.
      * If eval_str is false (the default), values of type str are unchanged.
    globals and locals are passed in to eval(); see the documentation
    for eval() for more information.  If either globals or locals is
    None, this function may replace that value with a context-specific
    default, contingent on type(obj):
      * If obj is a module, globals defaults to obj.__dict__.
      * If obj is a class, globals defaults to
        sys.modules[obj.__module__].__dict__ and locals
        defaults to the obj class namespace.
      * If obj is a callable, globals defaults to obj.__globals__,
        although if obj is a wrapped function (using
        functools.update_wrapper()) it is first unwrapped.
    """
    if isinstance(obj, type):
        # class
        obj_dict = getattr(obj, "__dict__", None)
        if obj_dict and hasattr(obj_dict, "get"):
            ann = obj_dict.get("__annotations__", None)
            if isinstance(ann, types.GetSetDescriptorType):
                ann = None
        else:
            ann = None

        obj_globals = None
        module_name = getattr(obj, "__module__", None)
        if module_name:
            module = sys.modules.get(module_name, None)
            if module:
                obj_globals = getattr(module, "__dict__", None)
        obj_locals = dict(vars(obj))
        unwrap = obj
    elif isinstance(obj, types.ModuleType):
        # module
        ann = getattr(obj, "__annotations__", None)
        obj_globals = getattr(obj, "__dict__")
        obj_locals = None
        unwrap = None
    elif callable(obj):
        # this includes types.Function, types.BuiltinFunctionType,
        # types.BuiltinMethodType, functools.partial, functools.singledispatch,
        # "class funclike" from Lib/test/test_inspect... on and on it goes.
        ann = getattr(obj, "__annotations__", None)
        obj_globals = getattr(obj, "__globals__", None)
        obj_locals = None
        unwrap = obj
    else:
        raise TypeError(f"{obj!r} is not a module, class, or callable.")

    if ann is None:
        return {}

    if not isinstance(ann, dict):
        raise ValueError(f"{obj!r}.__annotations__ is neither a dict nor None")

    if not ann:
        return {}

    if not eval_str:
        return dict(ann)

    if unwrap is not None:
        while True:
            if hasattr(unwrap, "__wrapped__"):
                unwrap = unwrap.__wrapped__
                continue
            if isinstance(unwrap, functools.partial):
                unwrap = unwrap.func
                continue
            break
        if hasattr(unwrap, "__globals__"):
            obj_globals = unwrap.__globals__

    if globals is None:
        globals = obj_globals
    if locals is None:
        locals = obj_locals

    return_value = {
        key: value if not isinstance(value, str) else eval(value, globals, locals)
        for key, value in ann.items()
    }
    return return_value


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
    from unipipe.dsl import Component, LazyAttribute, Pipeline

    if isinstance(obj, Component):
        return get_annotations(obj.func, eval_str=True)["return"]
    elif isinstance(obj, LazyAttribute):
        parent_type = infer_type(obj.parent)
        return get_annotations(parent_type, eval_str=True).get(obj.key)
    elif isinstance(obj, Pipeline):
        return infer_type(obj.return_value)
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
