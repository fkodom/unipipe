import argparse
from typing import NamedTuple, Union

import unipipe
from unipipe import dsl


@dsl.component
def split_name(name: str) -> NamedTuple("Output", first=str, last=str):  # type: ignore
    names = name.split(" ")
    return names[0], names[-1]


@dsl.component
def hello(first_name: str, last_name: str) -> str:
    return f"Seven blessings, {first_name} of house {last_name}!"


@dsl.component
def house_motto(last_name: str) -> str:
    if last_name == "Lannister":
        return f"A {last_name} always pays their debts..."
    else:
        return "Winter is coming..."


@dsl.component
def echo(phrase: str) -> str:
    return phrase


@dsl.pipeline
def pipeline():
    tyrion, lannister = split_name(name="Tyrion Lannister")
    hello(first_name=tyrion, last_name=lannister)
    house_motto(last_name=lannister)

    # Pipelines can have non-null return values, which we can use again within
    # the current pipeline context. In this case, echo the Stark motto again.
    # You should see "Winter is coming..." twice in the pipeline logs.
    other_motto = other_pipeline(name="Ned Stark")
    echo(phrase=other_motto)


@dsl.pipeline
def other_pipeline(name: Union[str, dsl.Component[str]]) -> dsl.Component[str]:
    # Use the type annotation 'dsl.Component[<type>]' where appropriate to keep
    # 'mypy' from complaining at us.
    #
    # In this case, 'other_pipeline' can accept either a raw string, or a Component
    # that will evaluate to a string value. The pipeline returns a Component object,
    # which will eventually evaluate to a string.
    #
    # NOTE: Unlike with Components, these type annotations are not checked or enforced
    # by 'unipipe'.  They're just for better book keeping and readability.
    lord_stark = split_name(name=name)
    hello(first_name=lord_stark.first, last_name=lord_stark.last)
    motto = house_motto(last_name=lord_stark.last)

    return motto


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--executor", default="python")
    args = parser.parse_args()

    unipipe.run(
        executor=args.executor,
        pipeline=pipeline(),
    )

    # Expected output:
    #
    # INFO:root:[split_name-48b0285e] - ('Tyrion', 'Lannister')
    # INFO:root:[hello-48b02d2c] - Seven blessings, Tyrion of house Lannister!
    # INFO:root:[house_motto-48b02e8a] - A Lannister always pays their debts...
    # INFO:root:[split_name-48b02fac] - ('Ned', 'Stark')
    # INFO:root:[hello-48b03150] - Seven blessings, Ned of house Stark!
    # INFO:root:[house_motto-48b032cc] - Winter is coming...
    # INFO:root:[echo-b61427b0] - Winter is coming...
