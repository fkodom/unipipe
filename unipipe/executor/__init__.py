from typing import Dict, Optional, Type, Union

from unipipe.dsl import Pipeline
from unipipe.executor.base import Executor
from unipipe.executor.docker import DockerExecutor
from unipipe.executor.python import PythonExecutor
from unipipe.executor.vertex import VertexExecutor

EXECUTORS_BY_NAME: Dict[str, Type[Executor]] = {
    "docker": DockerExecutor,
    "python": PythonExecutor,
    "vertex": VertexExecutor,
}


def run(
    executor: Union[str, Executor],
    pipeline: Pipeline,
    arguments: Optional[Dict] = None,
    **kwargs,
):
    if isinstance(executor, str):
        executor = EXECUTORS_BY_NAME[executor]()
    return executor.run(pipeline, arguments=arguments, **kwargs)


if __name__ == "__main__":
    from unipipe.dsl import Hardware, component, pipeline

    # TODO: Turn these examples into unit tests!

    @component(name="echo-1", hardware=Hardware(cpus=1))
    def echo(phrase: str) -> str:
        print(phrase)
        return phrase

    @pipeline
    def example_pipeline():
        _ = echo(phrase="Hello, world!")

    run(
        executor="vertex",
        pipeline=example_pipeline(),
        project="frank-odom",
        pipeline_root="gs://frank-odom/experiments/",
    )
