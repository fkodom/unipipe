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


# Pipeline inputs can be dynamically defined, just like with components!
@dsl.pipeline
def pipeline(name: str):
    tyrion, lannister = split_name(name=name)
    hello(first_name=tyrion, last_name=lannister)
    house_motto(last_name=lannister)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--executor", default="python")
    args = parser.parse_args()

    unipipe.run(
        executor=args.executor,
        # Pass arguments into the decorated pipeline, as with any Python function.
        pipeline=pipeline(name=args.name),
    )

    # Tested using '--name="Tyrion Lannister"'
    #
    # # Expected output:
    #
    # INFO:root:[split_name-48b0285e] - ('Tyrion', 'Lannister')
    # INFO:root:[hello-48b02d2c] - Seven blessings, Tyrion of house Lannister!
    # INFO:root:[house_motto-48b02e8a] - A Lannister always pays their debts...
