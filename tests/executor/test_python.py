import unipipe
from examples.ex01_hello_world import pipeline as pipeline_01


def test_example_01():
    unipipe.run(
        pipeline=pipeline_01(),
        executor="python",
    )
