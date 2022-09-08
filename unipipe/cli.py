from typing import List, Optional, Sequence

import click
from pydantic import parse_raw_as

from unipipe import dsl
from unipipe.executor import EXECUTOR_IMPORTS
from unipipe.utils.scripts import run_script as unipipe_run_script

DEFAULT_SEQUENCE = ("None",)


@click.group()
def unipipe():
    pass


def validate_hardware(ctx, param, value) -> Optional[dsl.Hardware]:
    if value is None:
        return None
    else:
        return parse_raw_as(dsl.Hardware, value)


@unipipe.command(context_settings={"ignore_unknown_options": True})
@click.argument("path", nargs=1)
@click.argument("args", nargs=-1)
@click.option(
    "-e",
    "--executor",
    "executor",
    default="python",
    type=click.Choice(list(EXECUTOR_IMPORTS.keys())),
    help="Executor to use for launching the script. Default: 'python'",
)
@click.option(
    "-n",
    "--name",
    "name",
    default=None,
    type=str,
    help="Name of the resulting pipeline. Default: None",
)
@click.option(
    "-b",
    "--base-image",
    "base_image",
    default=None,
    type=str,
    help="Base Docker image for the script. Defaults to 'fkodom/unipipe:latest'.",
)
@click.option(
    "-p",
    "--package-to-install",
    "packages_to_install",
    multiple=True,
    type=str,
    help="Add a Python package to install for this job. Ex: 'torch==1.10.0'",
    default=DEFAULT_SEQUENCE,
)
@click.option(
    "-i",
    "--pip-index-url",
    "pip_index_urls",
    multiple=True,
    type=str,
    help="Add a custom PyPI URL for installing packages. Ex: 'https://pypi.org/simple'",
    default=DEFAULT_SEQUENCE,
)
@click.option(
    "-h",
    "--hardware",
    "hardware",
    default=None,
    type=click.UNPROCESSED,
    callback=validate_hardware,
    help=(
        "Override hardware to use for this job. Must be provided as a raw JSON string. "
        'Ex: \'{"cpus": 4, "memory": "4G"}\''
    ),
)
def run_script(
    path: str,
    args: Sequence[str],
    executor: str,
    name: Optional[str] = None,
    # Component args
    base_image: Optional[str] = None,
    packages_to_install: Optional[Sequence[str]] = None,
    pip_index_urls: Optional[Sequence[str]] = None,
    hardware: Optional[str] = None,
):
    if pip_index_urls == DEFAULT_SEQUENCE:
        pip_index_urls = None
    if packages_to_install == DEFAULT_SEQUENCE:
        packages_to_install = None

    unipipe_run_script(
        path=path,
        args=args,
        executor=executor,
        name=name,
        # Component args
        base_image=base_image,
        packages_to_install=packages_to_install,
        pip_index_urls=pip_index_urls,
        hardware=hardware,
    )
