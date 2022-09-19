from __future__ import annotations

import ast
import importlib
import os
import random
import sys
import tempfile
from contextlib import ExitStack
from itertools import dropwhile
from typing import Any, Callable, Dict, Optional, Sequence

import unipipe
from unipipe import dsl
from unipipe.utils.compat import removeprefix, removesuffix

EXIT_STACK = ExitStack()


def get_docstring_from_script(path: str) -> Optional[str]:
    with open(path, "r") as f:
        tree = ast.parse(f.read())
    return ast.get_docstring(tree, clean=True)


def parse_component_kwargs_from_string(decorator: str):
    decorator = removeprefix(decorator, "@")
    decorator = removeprefix(decorator, "dsl.")
    decorator = removeprefix(decorator, "component")
    decorator = decorator.format(**os.environ)
    return eval(f"dict{decorator}")


def get_component_kwargs_from_docstring(docstring: str) -> Optional[Dict[str, Any]]:
    lines = docstring.split("\n")
    lines = list(dropwhile(lambda x: not x.strip().startswith("@dsl.component"), lines))
    lines = [line for line in lines if not line.strip().startswith("#")]
    if not lines:
        return None

    exc: Optional[SyntaxError] = None
    # TODO: Make this cleaner and more robust.
    # I'm relatively new to Python's 'ast' module, but it seems like it would
    # be possible to extract the args more elegantly.  This is very brute-force.
    #
    # Iteratively add more lines to the string we're trying to parse.  If it
    # throws a SyntaxError, then add another line and try again.  Otherwise,
    # we must ahve successfully parsed the component kwargs.
    for num_lines in range(len(lines)):
        try:
            return parse_component_kwargs_from_string("".join(lines[: num_lines + 1]))
        except SyntaxError as e:
            exc = e

    assert exc is not None
    raise exc


def get_component_kwargs_from_script(script_path: str) -> Optional[Dict[str, Any]]:
    docstring = get_docstring_from_script(script_path)
    if docstring is None:
        return None

    return get_component_kwargs_from_docstring(docstring)


FUNCTION_NAME = "script_component"
FUNCTION_CODE = """
from typing import *

import unipipe
from unipipe import dsl
from unipipe.dsl import *


def {function_name}(args: List[str]) -> str:
    import argparse
    import sys
    from uuid import uuid1

    sys.argv = ["{function_name}.py", *args]
    __name__ = "__main__"

    {script_code}

    return str(uuid1())
"""


def _random_python_file_name() -> str:
    characters = "abcdefghijklmnopqrstuvwxyz_"
    chosen = random.choices(characters, k=16)
    return "".join(chosen) + ".py"


def function_from_script(script_path: str) -> Callable:
    name = removesuffix(os.path.basename(script_path), ".py")
    with open(script_path, "r") as f:
        script_lines = f.readlines()
    indent = " " * 4
    code = FUNCTION_CODE.format(
        function_name=name, script_code=indent.join(script_lines)
    )

    tempdir = EXIT_STACK.enter_context(tempfile.TemporaryDirectory())
    file_name = _random_python_file_name()
    path = os.path.join(tempdir, file_name)
    with open(path, "w") as f:
        f.write(code)
        f.flush()

    if tempdir not in sys.path:
        sys.path.append(tempdir)
    module = importlib.import_module(removesuffix(file_name, ".py"))

    return getattr(module, name)


def component_from_script(path: str, **manual_kwargs) -> Callable[..., dsl.Component]:
    # NOTE: Get the default kwargs first!  If there is a syntax error in the docstring,
    # we prefer this function to throw a 'SyntaxError'.
    default_kwargs = get_component_kwargs_from_script(path)
    if default_kwargs is None:
        default_kwargs = {}

    func = function_from_script(path)

    # Remove any 'None' or empty values that may have been included by default
    # or forwarded from the CLI tool.
    default_kwargs = {k: v for k, v in default_kwargs.items() if v is not None}
    manual_kwargs = {k: v for k, v in manual_kwargs.items() if v is not None}
    kwargs = {**default_kwargs, **manual_kwargs}

    return dsl.component(func=func, **kwargs)


def run_script(
    path: str,
    args: Sequence[str] = (),
    executor: str = "python",
    name: Optional[str] = None,
    **kwargs,
):
    component_fn = component_from_script(path=path, **kwargs)
    # Component functions expect a 'List' object. It's easier to type-check a strict
    # 'List' annotation than 'Sequence', but we want the 'run_script' method to be as
    # user-friendly as possible.
    args = list(args)

    @dsl.pipeline(name=name)
    def pipeline():
        component_fn(args)

    unipipe.run(executor=executor, pipeline=pipeline())
