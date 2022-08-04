import inspect
import json
import logging
import os
import tempfile
import textwrap
from typing import Any, Dict, Iterable, Optional, TypedDict

from docker.errors import BuildError

import docker
from unipipe.dsl import Component, LazyAttribute, Pipeline
from unipipe.executor.base import Executor
from unipipe.utils.annotations import get_annotations

IMPORTS = """
import unipipe
from unipipe import dsl
from unipipe.dsl import *
from typing import *
"""

LOGGING = """
import logging
logging.getLogger().setLevel({logging_level})
"""

COMMAND = """
import argparse
import json

parser = argparse.ArgumentParser()
{arguments}
args = parser.parse_args()

func = {function_name}(**vars(args)).func
output = func(**vars(args))
with open('/app/output.json', "w") as f:
    json.dump(dict(output=output), f)
"""


def build_script(component: Component) -> str:
    _logging = LOGGING.format(logging_level=component.logging_level)
    function = textwrap.dedent(inspect.getsource(component.func))
    annotations = get_annotations(component.func, eval_str=True)
    argument_lines = [
        f"parser.add_argument('--{k}', type={v.__name__})"
        for k, v in annotations.items()
        if k != "return"
    ]
    command = COMMAND.format(
        arguments="\n".join(argument_lines),
        function_name=component.func.__name__,
    )
    return "\n".join([IMPORTS, _logging, function, command])


DOCKERFILE = """
FROM {base_image}

WORKDIR /app
COPY ./ ./unipipe
RUN pip install ./unipipe
RUN pip install {packages}
"""


def _record_build_logs(logs: Iterable[Dict[str, str]], level: int):
    for line in logs:
        if "stream" in line:
            logging.log(level=level, msg=line["stream"])


def build_docker_image(component: Component, tag: str):
    client = docker.from_env()
    base_image = component.base_image or "python:3.8"
    logging.info(f"Building Docker image: ({tag=}, {base_image=})")
    dockerfile = DOCKERFILE.format(
        base_image=base_image,
        packages=" ".join(component.packages_to_install or ["pip"]),
    )
    with tempfile.TemporaryDirectory() as tempdir:
        dockerfile_path = os.path.join(tempdir, "Dockerfile")
        with open(dockerfile_path, "w") as f:
            f.write(dockerfile)
        try:
            _, logs = client.images.build(
                path="./", dockerfile=dockerfile_path, tag=tag, rm=True
            )
            level = logging.DEBUG
        except BuildError as e:
            logs = e.build_log
            level = logging.ERROR

    _record_build_logs(logs, level=level)
    return tag


class Volume(TypedDict):
    bind: str
    mode: str


def build_and_run(
    component: Component,
    arguments: Optional[Dict[str, Any]] = None,
    volumes: Optional[Dict[str, Volume]] = None,
    remove: bool = True,
):
    client = docker.from_env()
    tag = build_docker_image(component, tag=component.name)
    if arguments is None:
        arguments = {}
    if volumes is None:
        volumes = {}

    with tempfile.TemporaryDirectory() as tempdir:
        script_path = os.path.join(tempdir, "main.py")
        output_json = os.path.join(tempdir, "output.json")
        script = build_script(component)
        with open(script_path, "w") as f:
            f.write(script)

        volumes[tempdir] = {"bind": "/app/", "mode": "rw"}
        args = " ".join([f"--{k}='{v}'" for k, v in arguments.items()])
        container = client.containers.run(
            image=component.name,
            command=f"python3 /app/main.py {args}",
            volumes=volumes,
            remove=remove,
            detach=True,
            # TODO: Add logic for enabling GPUs when requested.
            # Example code from the Docker for Python SDK pasted as a placeholder.
            #
            # device_requests=[
            #     docker.types.DeviceRequest(
            #         device_ids=["0,2"],
            #         capabilities=[["gpu"]],
            #     )
            # ],
        )
        for line in container.logs(stream=True):
            line = line.strip()
            if line:
                print(line.decode("utf-8"))

        container.wait()
        client.images.remove(tag, noprune=False)

        with open(output_json, "r") as f:
            result = json.load(f)["output"]

    return result


class DockerExecutor(Executor):
    def run(
        self,
        pipeline: Pipeline,
        arguments: Optional[Dict] = None,
        **kwargs,
    ):
        if arguments is None:
            arguments = {}

        def resolve_value(v: Any) -> Any:
            if isinstance(v, LazyAttribute):
                return getattr(resolve_value(v.parent), v.key)
            elif isinstance(v, Component):
                assert isinstance(arguments, dict)
                return arguments[v.name]
            else:
                return v

        for _component in pipeline.components:
            _arguments = {k: resolve_value(v) for k, v in _component.inputs.items()}
            result = build_and_run(_component, arguments=_arguments)
            return_type = get_annotations(_component.func, eval_str=True)["return"]
            if issubclass(return_type, tuple):
                result = return_type(*result)
            arguments[_component.name] = result
