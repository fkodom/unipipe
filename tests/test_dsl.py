from typing import NamedTuple

from unipipe import dsl


@dsl.component(
    name="split-name",
    hardware={"accelerator": {"count": 1, "type": "nvidia-tesla-t4"}},
)
def split_name(name: str) -> NamedTuple("Output", first=str, last=str):  # type: ignore
    names = name.split(" ")
    return names[0], names[-1]


@dsl.component
def hello(first_name: str, last_name: str) -> str:
    return f"Seven blessings, {first_name} of house {last_name}!"


@dsl.component(packages_to_install=["requests"])
def lannister_house_motto() -> str:
    return "A Lannister always pays their debts..."


@dsl.pipeline(name="main")
def pipeline():
    split = nested_pipeline(name="Tyrion Lannister")
    first, last = split
    hello(first_name=first, last_name=last)
    hello(first_name=split.first, last_name=split.last)


@dsl.pipeline
def nested_pipeline(name: str) -> NamedTuple("Output", first=str, last=str):  # type: ignore
    split = split_name(name=name)
    first, last = split.first, split[1]
    hello(first_name=first, last_name=last)

    name_again = first + " " + last
    _, last = split_name(name=name_again)
    with dsl.equal(last, "Lannister"):
        lannister_house_motto()

    return first, last


def test_component_init():
    motto = lannister_house_motto()
    assert isinstance(motto, dsl.Component)
    assert not motto.inputs
    assert motto.name.startswith("lannister-house-motto")
    assert motto.base_image == "fkodom/unipipe:latest"
    assert motto.packages_to_install == ["requests"]
    assert motto.hardware == dsl.Hardware()

    split = split_name(name="Ned Stark")
    assert isinstance(split, dsl.Component)
    assert split.inputs == {"name": "Ned Stark"}
    assert split.name == "split-name"
    assert split.base_image == "fkodom/unipipe:latest-cuda"
    assert not split.packages_to_install
    assert split.hardware.accelerator.count == 1
    assert split.hardware.accelerator.type == dsl.AcceleratorType.T4

    split = split_name(name=motto)
    assert isinstance(split, dsl.Component)
    assert split.inputs == {"name": motto}


def test_pipeline_init():
    _ = pipeline()
