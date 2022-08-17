import argparse

import unipipe
from unipipe import dsl


@dsl.component
def hello(name: str) -> str:
    return f"Hello, {name}!"


@dsl.pipeline
def pipeline():
    hello(name="world")


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
    # INFO:root:[hello-6028507e] - Hello, world!
