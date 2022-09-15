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
def hello(first_name: str, last_name: str) -> str:
    return f"Seven blessings, {first_name} of house {last_name}!"


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
        motto = lannister_house_motto()

    # This will cause an exception (KeyError)!
    #
    # Variables are not accessible after exiting the conditional scope above.
    # We can't guarantee that 'motto' exists, because execution of the
    # 'lannister_house_motto' component is delayed to the chosen backend (e.g.
    # Docker, Vertex, etc). In the local pipeline scope, there's not way of knowing
    # that 'lannister_house_motto' was ever executed in the first place.
    echo(phrase=motto)


@dsl.pipeline
def good_pipeline(name: str):
    motto = "Winter is coming..."
    first, last = split_name(name=name)

    # This is valid for all input names.  Avoid over-writing the 'motto' variable.
    #
    # If control flow logic gets too complicated, consider breaking this pipeline
    # into multiple (nested) sub-pipelines, each with their own control flow logic.
    with dsl.equal(last, "Lannister"):
        lannister_motto = lannister_house_motto()
        echo(phrase=lannister_motto)
    with dsl.not_equal(last, "Lannister"):
        stark_motto = echo(phrase=motto)

        # When components have side effects, such as modifying remote data, you can
        # mark other pipeline components as dependent. This ensures that the 'hello'
        # component below never executes before 'stark_motto' is returned.  Otherwise,
        # KubeFlow would try to run them in parallel, since 'hello' is not explicitly
        # dependent on 'stark_motto'.
        #
        # 'dsl.depends_on' is treated as a conditional clause, since the execution of
        # everything in this 'with' clause is dependent on 'stark_motto'. As a result,
        # none of the variables created in this context would be accessible outside
        # of the 'with' clause.
        with dsl.depends_on(stark_motto):
            hello(first_name=first, last_name=last)

    # You can also include multiple components in the 'depends_on' arguments:
    mottos = [lannister_house_motto() for _ in range(3)]
    with dsl.depends_on(*mottos):
        echo(phrase="That's too many Lannisters...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--executor", default="python")
    parser.add_argument("--name", required=True)
    args = parser.parse_args()

    try:
        unipipe.run(executor="python", pipeline=bad_pipeline(name=args.name))
    except KeyError:
        print("Bad pipeline crashed with KeyError.\n")

    unipipe.run(executor=args.executor, pipeline=good_pipeline(name=args.name))

    # Tested using '--name="Ned Stark"'
    #
    # Expected output:
    #
    # INFO:root:[split_name-9701d30e] - ('Ned', 'Stark')
    # Bad pipeline crashed with KeyError. :(
    #
    # INFO:root:[split_name-48b02fac] - ('Ned', 'Stark')
    # INFO:root:[house_motto-48b032cc] - Winter is coming...
    # INFO:root:[hello-48b03150] - Seven blessings, Ned of house Stark!
    # INFO:root:[hello-48b03150] - A Lannister always pays their debts...
    # INFO:root:[hello-48b03150] - A Lannister always pays their debts...
    # INFO:root:[hello-48b03150] - A Lannister always pays their debts...
    # INFO:root:[hello-48b03150] - That's too many Lannisters...
