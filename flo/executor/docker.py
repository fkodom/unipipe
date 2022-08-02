import inspect
import logging
import os
import tempfile
import textwrap
from typing import Dict, Iterable

import docker
from docker.errors import BuildError

from flo.dsl import Component
from flo.utils.annotations import get_annotations

IMPORTS = """
import flo
from flo import dsl
from flo.dsl import *
from typing import *
"""

LOGGING = """
import logging
logging.getLogger().setLevel({logging_level})
"""

COMMAND = """
import argparse
parser = argparse.ArgumentParser()
{arguments}
args = parser.parse_args()

@dsl.pipeline
def _pipeline():
    {function_name}(**vars(args))

result = {function_name}(**vars(args))
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
COPY ./ ./flo
RUN pip install ./flo
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


def build_and_run(component: Component, rm: bool = True):
    client = docker.from_env()
    script = build_script(component)
    tag = build_docker_image(component, tag=_component.name)

    with tempfile.TemporaryDirectory() as tempdir:
        script_path = os.path.join(tempdir, "main.py")
        with open(script_path, "w") as f:
            f.write(script)

        container = client.containers.run(
            image=_component.name,
            command="python3 /app/main.py --name=world",
            volumes={tempdir: {"bind": "/app/", "mode": "rw"}},
            remove=True,
            detach=True,
        )
        for line in container.logs(stream=True):
            print(line.decode("utf-8"))

        container.wait()
        client.images.remove(tag, noprune=False)


if __name__ == "__main__":
    from flo import dsl

    @dsl.component
    def hello(name: str) -> str:
        return f"Hello, {name}!"

    _component = hello(name="world")
    build_and_run(_component)
