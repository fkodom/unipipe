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
def lannister_house_motto() -> str:
    return "A Lannister always pays their debts..."


@dsl.component
def stark_house_motto() -> str:
    return "Winter is coming..."


@dsl.pipeline
def pipeline():
    tyrion, lannister = split_name(name="Tyrion Lannister")
    hello(first_name=tyrion, last_name=lannister)

    # This should execute the Lannister house motto.
    with dsl.equal(lannister, "Lannister"):
        lannister_house_motto()

    ned, stark = split_name(name="Ned Stark")
    hello(first_name=ned, last_name=stark)

    # This won't do anything, because "Stark" != "Lannister".
    with dsl.equal(stark, "Lannister"):
        lannister_house_motto()

    # This should execute the Stark house motto.
    with dsl.not_equal(stark, "Stark"):
        stark_house_motto()

    # NOTE: You can also create a custom condition by providing the function used to
    # compare the two arguments.
    #
    # Be warned -- KFP isn't very flexible in what functions are allowed here.
    # It's safeest to stick with built-in operators like [==, !=, >, <, >=, <=, ...].
    # The condition below evaluates to True, so the Stark motto is executed again.
    with dsl.condition(stark, "S", lambda x, y: x >= y):
        stark_house_motto()


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
