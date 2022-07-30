from typing import NamedTuple

from flo import dsl, executor


@dsl.component
def echo(phrase: str) -> NamedTuple("EchoOutputs", phrase1=str, phrase2=str):
    from collections import namedtuple

    print(phrase)
    echo_outputs = namedtuple("echo_outputs", ["phrase1", "phrase2"])
    return echo_outputs(phrase1=phrase, phrase2=phrase)


@dsl.pipeline
def pipeline():
    x = echo(phrase="Hello, world!")
    y = echo(phrase=x.phrase1)
    _ = echo(phrase=y.phrase1)
    _ = echo(phrase=x.phrase2)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", default="python")
    args = parser.parse_args()

    executor.run(
        executor=args.engine,
        pipeline=pipeline(),
        project="frank-odom",
        pipeline_root="gs://frank-odom/experiments/",
    )
