from __future__ import annotations

import ast
import importlib
import os
import sys
import tempfile
from itertools import dropwhile
from typing import Any, Callable, Dict, Optional, Sequence

import unipipe
from unipipe import dsl


def _removeprefix(string: str, prefix: str) -> str:
    if string.startswith(prefix):
        string = string[len(prefix) :]
    return string


def _removesuffix(string: str, suffix: str) -> str:
    if string.endswith(suffix):
        string = string[: -len(suffix)]
    return string


def get_docstring_from_script(path: str) -> Optional[str]:
    with open(path, "r") as f:
        tree = ast.parse(f.read())
    return ast.get_docstring(tree, clean=True)


def parse_component_kwargs_from_string(decorator: str):
    decorator = _removeprefix(decorator, "@")
    decorator = _removeprefix(decorator, "dsl.")
    decorator = _removeprefix(decorator, "component")
    decorator = decorator.format(**os.environ)
    return eval(f"dict{decorator}")


def get_component_kwargs_from_docstring(docstring: str) -> Optional[Dict[str, Any]]:
    lines = docstring.split("\n")
    lines = list(dropwhile(lambda x: not x.startswith("@dsl.component"), lines))
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


def get_component_kwargs_from_script(script_path: str) -> Dict[str, Any]:
    docstring = get_docstring_from_script(script_path)
    if docstring is None:
        return {}

    kwargs = get_component_kwargs_from_docstring(docstring)
    if kwargs is None:
        kwargs = {}

    return kwargs


FUNCTION_NAME = "script_component"
FUNCTION_CODE = """
from typing import *

import unipipe
from unipipe import dsl
from unipipe.dsl import *


def {function_name}(args: Tuple[str]) -> None:
    import argparse
    import sys

    sys.argv = ["{function_name}.py", *args]

    {script_code}
"""


def function_from_script(script_path: str) -> Callable:
    name = _removesuffix(os.path.basename(script_path), ".py")
    with open(script_path, "r") as f:
        script_lines = f.readlines()
    indent = " " * 4
    code = FUNCTION_CODE.format(
        function_name=name, script_code=indent.join(script_lines)
    )

    fp = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False)
    fp.write(code)
    fp.flush()

    module_path, file_name = os.path.split(fp.name)
    sys.path.append(module_path)
    module = importlib.import_module(_removesuffix(file_name, ".py"))

    return getattr(module, name)


def component_from_script(path: str, **kwargs) -> Callable[..., dsl.Component]:
    _kwargs = get_component_kwargs_from_script(path)
    func = function_from_script(path)
    merged_kwargs = {**_kwargs, **kwargs}
    return dsl.component(func=func, **merged_kwargs)


def run_script(path: str, args: Sequence[str] = (), executor: str = "python"):
    component_fn = component_from_script(path=path)
    # Component functions expect a 'List' object. For technical reasons, it's much
    # easier to type-check a strict 'List' annotation than 'Sequence', but we want
    # the 'run_script' method to be as user-friendly as possible.
    args = tuple(args)

    @dsl.pipeline
    def pipeline():
        component_fn(args)

    unipipe.run(executor=executor, pipeline=pipeline())
