import pytest

from unipipe.utils import ops


def _nonsensical_function(a: int) -> str:
    return a * "nonsense"


def test_dispatch_registry():
    registry = ops.dispatch

    # Test adding a new function to the dispatch registry
    assert "_nonsensical_function" not in registry.d
    ops.dispatch(_nonsensical_function)
    assert "_nonsensical_function" in registry.d

    # Test that several of the expected functions exist in the registry
    for key in ["equal", "add", "int_", "str_"]:
        assert key in registry.d


def test_multiple_dispatch_getitem():
    multiple_dispatch = ops.dispatch["equal"]

    # Test getting a few valid function signatures:
    #   - (str, str)
    inputs = {"a": "hello", "b": "hello"}
    assert multiple_dispatch[inputs](**inputs) is True
    inputs = {"a": "hello", "b": "world"}
    multiple_dispatch[inputs](**inputs) is False
    #   - (int, float)
    inputs = {"a": 1, "b": 1.0}
    multiple_dispatch[inputs](**inputs) is True
    inputs = {"a": 1, "b": 2.0}
    multiple_dispatch[inputs](**inputs) is False
    #   - (tuple, tuple)
    inputs = {"a": (1, 2, 3), "b": (1, 2, 3)}
    multiple_dispatch[inputs](**inputs) is True
    inputs = {"a": (1, 2, 3), "b": (1, 2)}
    multiple_dispatch[inputs](**inputs) is False

    # Test that invalid signatures raise a TypeError:
    #   - (str, int)
    with pytest.raises(TypeError):
        multiple_dispatch[{"a": "hello", "b": 1}]
    #   - (int, tuple)
    with pytest.raises(TypeError):
        multiple_dispatch[{"a": 1, "b": (1, 2, 3)}]
    #   - (int, tuple)
    with pytest.raises(TypeError):
        multiple_dispatch[{"a": 1, "b": (1, 2, 3), "c": "world"}]


def test_multiple_dispatch_call():
    multiple_dispatch = ops.dispatch["equal"]

    # Test calling a few valid function signatures:
    #   - (str, str)
    assert multiple_dispatch(a="hello", b="hello") is True
    assert multiple_dispatch(a="hello", b="world") is False
    #   - (int, float)
    assert multiple_dispatch(a=1, b=1.0) is True
    assert multiple_dispatch(a=1, b=2.0) is False
    #   - (tuple, tuple)
    assert multiple_dispatch(a=(1, 2, 3), b=(1, 2, 3)) is True
    assert multiple_dispatch(a=(1, 2, 3), b=(1, 2)) is False

    # Test that invalid signatures raise a TypeError:
    #   - (str, int)
    with pytest.raises(TypeError):
        multiple_dispatch(a="hello", b=1)
    #   - (int, tuple)
    with pytest.raises(TypeError):
        multiple_dispatch(a=1, b=(1, 2, 3))
    #   - (int, tuple, str)
    with pytest.raises(TypeError):
        multiple_dispatch(a=1, b=(1, 2, 3), c="world")
