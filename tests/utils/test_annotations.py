from typing import NamedTuple

from unipipe import dsl
from unipipe.utils.annotations import (
    get_annotations,
    infer_input_types,
    infer_type,
    resolve_annotations,
)


def _stringified_annotations(a: "int") -> "str":
    return a * "nonsense"


def test_get_annotations():
    func = _stringified_annotations

    str_annotations = get_annotations(func)
    annotations = get_annotations(func, eval_str=True)
    assert all(isinstance(v, str) for v in str_annotations.values())
    assert all(eval(v) == annotations[k] for k, v in str_annotations.items())


def test_resolve_annotations():
    # NOTE: The 'copy.deepcopy' method does not create a deep copy of methods/classes,
    # so 'resolve_annotations' is forced to modify the existing function in-place.
    # Collect the stringified annotations before resolving.
    str_annotations = get_annotations(_stringified_annotations)
    assert all(isinstance(v, str) for v in str_annotations.values())

    func = resolve_annotations(_stringified_annotations)
    annotations = get_annotations(func)

    assert all(isinstance(v, type) for v in annotations.values())
    assert all(eval(v) == annotations[k] for k, v in str_annotations.items())


@dsl.component
def _split_name(name: str) -> NamedTuple("Output", first=str, last=str):  # type: ignore
    names = name.split(" ")
    return names[0], names[-1]


@dsl.component
def _hello(first_name: str, last_name: str) -> str:
    return f"Seven blessings, {first_name} of house {last_name}!"


def test_infer_type():
    name = "Tyrion Lannister"
    assert infer_type(name) == str

    split_name = _split_name(name=name)
    assert issubclass(infer_type(split_name), tuple)

    assert infer_type(split_name[0]) == str
    assert infer_type(split_name[1]) == str
    first, last = split_name
    assert infer_type(first) == str
    assert infer_type(last) == str

    greeting = _hello(first_name=first, last_name=last)
    assert infer_type(greeting) == str


def test_infer_input_types():
    split_name = _split_name(name="Tyrion Lannister")
    assert infer_input_types(split_name.inputs) == {"name": str}

    first, last = split_name
    greeting = _hello(first_name=first, last_name=last)
    assert infer_input_types(greeting.inputs) == {"first_name": str, "last_name": str}
