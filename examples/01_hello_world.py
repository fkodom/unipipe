import argparse

import flo
from flo import dsl


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

    flo.run(
        executor=args.executor,
        pipeline=pipeline(),
        project="frank-odom",
        pipeline_root="gs://frank-odom/experiments/",
    )