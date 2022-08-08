import argparse

import unipipe
from unipipe import dsl


@dsl.component
def hello(name: str) -> str:
    return f"Hello, {name}!"


@dsl.pipeline
def pipeline():
    message = hello(name="world")
    repeated = hello(name=message)
    hello(name=message)
    hello(name=repeated)


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
    # INFO:root:[hello-6b91e1c8] - Hello, world!
    # INFO:root:[hello-6b91e3e4] - Hello, Hello, world!!
    # INFO:root:[hello-6b91e506] - Hello, Hello, world!!
    # INFO:root:[hello-6b91e60a] - Hello, Hello, Hello, world!!!
