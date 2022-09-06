from typing import List

import click

from unipipe.executor import EXECUTOR_IMPORTS
from unipipe.utils.scripts import run_script as unipipe_run_script


@click.group()
def cli():
    pass


@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("path", nargs=1)
@click.argument("args", nargs=-1)
@click.option(
    "-e",
    "--executor",
    "executor",
    default="python",
    type=click.Choice(list(EXECUTOR_IMPORTS.keys())),
)
def run_script(path: str, args: List[str], executor: str):
    unipipe_run_script(path=path, args=args, executor=executor)
