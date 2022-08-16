import argparse
from typing import NamedTuple

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
def other_pipeline(name: str) -> str:
    lord_stark = split_name(name=name)
    hello(first_name=lord_stark.first, last_name=lord_stark.last)
    motto = house_motto(last_name=lord_stark.last)

    # Mypy doesn't like this return value -- it's annotated as 'str', but if you
    # inspect the 'motto' variable, it's actually a 'Component' object. It needs to
    # be a 'Component', since 'motto' is part of a 'Pipeline' that won't be executed
    # until later. There may be a future workaround for these linting annoyances, but
    # for now just ignore them.  ¯\_(ツ)_/¯
    return motto  # type: ignore


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--executor", default="python")
    args = parser.parse_args()

    unipipe.run(
        executor=args.executor,
        pipeline=pipeline(),
        project="frank-odom",
        pipeline_root="gs://frank-odom/experiments/",
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
