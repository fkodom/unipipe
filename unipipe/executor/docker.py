import json
import logging
import os
import sys
import tempfile
import textwrap
from inspect import getsource, isclass
from itertools import dropwhile
from typing import Any, Callable, Dict, Iterable, Optional, Union

from docker.errors import BuildError
from docker.types import DeviceRequest

import docker
from unipipe.dsl import Component, ConditionalPipeline, LazyAttribute, Pipeline
from unipipe.executor.base import LocalExecutor
from unipipe.utils.annotations import get_annotations

if sys.version_info >= (3, 8):
    from typing import TypedDict  # pylint: disable=no-name-in-module
else:
    from typing_extensions import TypedDict


IMPORTS = """
import unipipe
from unipipe import dsl
from unipipe.dsl import *
from unipipe.utils.ops import dispatch
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

output = {function_name}(**vars(args))
if isinstance(output, dsl.Component):
    output = output.func(**vars(args))

with open('/app/output.json', "w") as f:
    json.dump(dict(output=output), f)
"""


def _get_component_func_source(func: Callable) -> str:
    """Largely copy-pasta from 'kfp.v2'.  De-indents the function source code, and
    removes any decorators or other code preceding the 'def' statement.  Then,
    decorate the function with just '@dsl.component', so we can utilize type
    checking/casting, logging, etc. from the Component class.
    """
    # Function may be defined in another function/class. Dedent the source code.
    lines = textwrap.dedent(getsource(func)).split("\n")
    lines = list(dropwhile(lambda x: not x.startswith("def"), lines))

    if not lines:
        raise ValueError(
            'Failed to dedent and clean up the source of function "{}". '
            "It is probably not properly indented.".format(func.__name__)
        )

    return "\n".join(["@dsl.component", *lines])


def build_script(component: Component) -> str:
    _logging = LOGGING.format(logging_level=component.logging_level)
    function = _get_component_func_source(component.func)
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


# TODO: Once stable release is available in PyPI:
#   +++ pip install unipipe
#   --- pip install ./unipipe

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
    base_image = component.base_image
    logging.info(f"Building Docker image: ('tag={tag}', 'base_image={base_image}')")
    client = docker.from_env()

    # If no packages listed, just include 'pip' as a placeholder
    install_options = component.packages_to_install or ["pip"]
    if component.pip_index_urls:
        index_url, *extra_index_urls = component.pip_index_urls
        install_options.append(f"--index-url {index_url} --trusted-host {index_url}")
        install_options.extend(
            [f"--extra-index-url {i} --trusted-host {i}" for i in extra_index_urls]
        )

    packages = " ".join(install_options)
    dockerfile = DOCKERFILE.format(base_image=base_image, packages=packages)

    with tempfile.TemporaryDirectory() as tempdir:
        dockerfile_path = os.path.join(tempdir, "Dockerfile")
        with open(dockerfile_path, "w") as f:
            f.write(dockerfile)
        try:
            _, logs = client.images.build(
                path="./", dockerfile=dockerfile_path, tag=tag, pull=True, rm=True
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
    volumes: Optional[Dict[str, Union[Dict, Volume]]] = None,
    remove: bool = True,
):
    client = docker.from_env()
    tag = build_docker_image(component, tag=component.name)
    if arguments is None:
        arguments = {}

    if volumes is None:
        volumes = {}

    config_path = os.path.expanduser("~/.config/")
    if os.path.exists(config_path) and config_path not in volumes:
        volumes[config_path] = {"bind": "/root/.config/", "mode": "ro"}

    with tempfile.TemporaryDirectory() as tempdir:
        script_path = os.path.join(tempdir, "main.py")
        output_json = os.path.join(tempdir, "output.json")
        script = build_script(component)
        with open(script_path, "w") as f:
            f.write(script)

        volumes[tempdir] = {"bind": "/app/", "mode": "rw"}
        args = " ".join([f"--{k}='{v}'" for k, v in arguments.items()])

        device_requests = []
        accelerator = component.hardware.accelerator
        if accelerator is not None and accelerator.count:
            # TODO:
            #   - Make this logic work for TPUs as well
            #   - Allow users to pick specific device IDs?
            device_ids = list(range(int(accelerator.count)))
            device_requests.append(
                DeviceRequest(
                    device_ids=[",".join([str(i) for i in device_ids])],
                    capabilities=[["gpu"]],
                )
            )

        container = client.containers.run(
            image=component.name,
            command=f"python /app/main.py {args}",
            volumes=volumes,
            remove=remove,
            detach=True,
            device_requests=device_requests,
        )
        for line in container.logs(stream=True):
            line = line.strip()
            if line:
                print(line.decode("utf-8"))

        container.wait()
        client.images.remove(tag, force=True, noprune=False)

        with open(output_json, "r") as f:
            result = json.load(f)["output"]

    return result


def resolve_value(arguments: Dict, value: Any) -> Any:
    if isinstance(value, LazyAttribute):
        return getattr(resolve_value(arguments, value.parent), value.key)
    elif isinstance(value, Pipeline):
        if value.name in arguments:
            return arguments[value.name]
        return resolve_value(arguments, value.return_value)
    elif isinstance(value, (tuple, list)):
        return tuple(resolve_value(arguments, x) for x in value)
    elif isinstance(value, Component):
        assert isinstance(arguments, dict)
        return arguments[value.name]
    else:
        return value


class DockerExecutor(LocalExecutor):
    def resolve_local_value(self, _locals: Dict, value: Any) -> Any:
        if isinstance(value, LazyAttribute):
            return getattr(resolve_value(_locals, value.parent), value.key)
        elif isinstance(value, Pipeline):
            if value.name in _locals:
                return _locals[value.name]
            return resolve_value(_locals, value.return_value)
        elif isinstance(value, (tuple, list)):
            return tuple(resolve_value(_locals, x) for x in value)
        elif isinstance(value, Component):
            assert isinstance(_locals, dict)
            return _locals[value.name]
        else:
            return value

    def run_component(self, component: Component, **kwargs):
        result = build_and_run(component, kwargs)
        return_type = get_annotations(component.func, eval_str=True).get("return")
        if isclass(return_type) and issubclass(return_type, tuple):
            result = return_type(*result)

        return result

    def run_conditional_pipeline_with_locals(
        self, pipeline: ConditionalPipeline, _locals: Dict[str, Any]
    ):
        operand1 = self.resolve_local_value(_locals, pipeline.condition.operand1)
        operand2 = self.resolve_local_value(_locals, pipeline.condition.operand2)
        comparator = pipeline.condition.comparator

        if comparator(operand1, operand2):
            return self.run_pipeline_with_locals(pipeline, _locals=_locals)
        else:
            return None, _locals
