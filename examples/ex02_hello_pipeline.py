import argparse

import unipipe
from unipipe import dsl


@dsl.component
def get_first_name(name: str) -> str:
    return name.split(" ")[0]


@dsl.component
def hello(name: str) -> str:
    return f"Hello, {name}!"


@dsl.pipeline
def pipeline():
    first_name = get_first_name(name="Tyrion Lannister")
    hello(name=first_name)


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
    # INFO:root:[hello-6b91e1c8] - Hello, Tyrion!
