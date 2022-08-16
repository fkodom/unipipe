from importlib import import_module
from typing import Dict, Optional, Union

from pydantic import BaseModel

from unipipe.dsl import Pipeline
from unipipe.executor.base import Executor


class ExecutorImport(BaseModel):
    module: str
    name: str


EXECUTOR_IMPORTS: Dict[str, ExecutorImport] = {
    "docker": ExecutorImport(module="unipipe.executor.docker", name="DockerExecutor"),
    "python": ExecutorImport(module="unipipe.executor.python", name="PythonExecutor"),
    "vertex": ExecutorImport(module="unipipe.executor.vertex", name="VertexExecutor"),
}


def run(
    executor: Union[str, Executor],
    pipeline: Pipeline,
    **kwargs,
):
    if isinstance(executor, str):
        _import = EXECUTOR_IMPORTS[executor]
        executor = getattr(import_module(_import.module), _import.name)()
        assert isinstance(executor, Executor)
    return executor.run(pipeline, **kwargs)
