from typing import NamedTuple

from flo import dsl, executor


@dsl.component
def echo(phrase: str) -> NamedTuple("Outputs", phrase1=str, phrase2=str):
    print(phrase)
    return (phrase, f"{phrase} again")


@dsl.pipeline
def pipeline():
    x1, x2 = echo(phrase="Hello world")
    y = echo(phrase=x1)
    _ = echo(phrase=y.phrase1)
    _ = echo(phrase=x2)


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
