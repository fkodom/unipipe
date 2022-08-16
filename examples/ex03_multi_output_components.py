import argparse
from typing import NamedTuple

import unipipe
from unipipe import dsl


# NOTE: Multi-output components should use the 'NamedTuple' return type!
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


@dsl.pipeline
def pipeline():
    # Multi-output results can be used as if they were 'NamedTuple' instances.
    # The calculation of each value is delayed until 'unipipe.run' below, so we can
    # easily transfer it to each backend/executor type.

    # Example destructuring like a normal 'tuple' object:
    tyrion, lannister = split_name(name="Tyrion Lannister")
    hello(first_name=tyrion, last_name=lannister)
    house_motto(last_name=lannister)

    # Example accessing each result by its field name:
    lord_stark = split_name(name="Ned Stark")
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
