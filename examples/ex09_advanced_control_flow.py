"""
Example of good (and bad) design patterns for pipelines with conditional
control flow.

Most challenges here come from:
    - 'unipipe' is a delayed-execution framework
    - Python is a dynamic, interpreted language

Essentially, 'unipipe' traces each possible execution branch in your script, and
delays the *actual* execution until you choose a backend (docker, vertex, etc.).
Every "if" and "for" statement in your script will be traced by 'unipipe', even
though those components may not ultimately get executed in the backend.

NOTE: See the 'bad_pipeline' example below.
"""

import argparse
from typing import NamedTuple

import unipipe
from unipipe import dsl


@dsl.component
def split_name(name: str) -> NamedTuple("Output", first=str, last=str):  # type: ignore
    names = name.split(" ")
    return names[0], names[-1]


@dsl.component
def lannister_house_motto() -> str:
    return "A Lannister always pays their debts..."


@dsl.component
def echo(phrase: str) -> str:
    return phrase


@dsl.pipeline
def bad_pipeline(name: str):
    motto = "Winter is coming..."
    _, last = split_name(name=name)

    with dsl.equal(last, "Lannister"):
        # This is a bug!!
        #
        # Ordinarily, you might think that 'motto' is only over-written when the
        # above condition is True.  But since 'unipipe' traces every execution
        # branch, and delays execution until a backend is chosen, this line below
        # will always be executed in the local process.
        motto = lannister_house_motto()

    # What happens when the 'dsl.equal(...)' clause above is False?
    #
    # The local variable 'motto' refers to the result of 'lannister_house_motto()'.
    # But if that component was never executed, that reference is not valid.
    # The following line will raise an error, unless the user provided "Lannister"
    # as the last name.
    echo(phrase=motto)


@dsl.pipeline
def good_pipeline(name: str):
    motto = "Winter is coming..."
    _, last = split_name(name=name)

    # This is valid for all input names.  Avoid over-writing the 'motto' variable.
    #
    # If control flow logic gets too complicated, consider breaking this pipeline
    # into multiple (nested) sub-pipelines, each with their own control flow logic.
    with dsl.equal(last, "Lannister"):
        lannister_motto = lannister_house_motto()
        echo(phrase=lannister_motto)
    with dsl.not_equal(last, "Lannister"):
        echo(phrase=motto)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--executor", default="python")
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    try:
        unipipe.run(executor=args.executor, pipeline=bad_pipeline(name=args.name))
    except KeyError:
        print("Bad pipeline crashed with KeyError, since last name is not Lannister :(")

    unipipe.run(executor=args.executor, pipeline=good_pipeline(name=args.name))

    # Tested using '--name="Ned Stark"'
    #
    # Expected output:
    #
    # INFO:root:[split_name-9701d30e] - ('Ned', 'Stark')
    # Bad pipeline crashed with KeyError, since last name is not Lannister :(
    #
    # INFO:root:[split_name-48b02fac] - ('Ned', 'Stark')
    # INFO:root:[house_motto-48b032cc] - Winter is coming...
