from typing import List, Optional

import click
from pydantic import parse_raw_as

from unipipe import dsl
from unipipe.executor import EXECUTOR_IMPORTS
from unipipe.utils.scripts import run_script as unipipe_run_script


@click.group()
def cli():
    pass


def validate_hardware(ctx, param, value) -> dsl.Hardware:
    return parse_raw_as(dsl.Hardware, value)


@cli.command(context_settings={"ignore_unknown_options": True})
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
)
@click.option(
    "-i",
    "--pip-index-url",
    "pip_index_urls",
    multiple=True,
    type=str,
    help="Add a custom PyPI URL for installing packages. Ex: 'https://pypi.org/simple'",
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
    args: List[str],
    executor: str,
    name: Optional[str] = None,
    # Component args
    base_image: Optional[str] = None,
    packages_to_install: Optional[List[str]] = None,
    pip_index_urls: Optional[List[str]] = None,
    hardware: Optional[str] = None,
):
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
