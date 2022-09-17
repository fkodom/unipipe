import os
import tempfile
from typing import Any, Dict, Generator, Optional

import pytest

from unipipe import dsl
from unipipe.utils.scripts import (
    component_from_script,
    function_from_script,
    get_component_kwargs_from_docstring,
    get_component_kwargs_from_script,
    get_docstring_from_script,
)

TEMPDIR = tempfile.TemporaryDirectory()
ORDINARY_DOCSTRING = """
This is a docstring...
"""
COMPONENT_DOCSTRING = """
@dsl.component(
    hardware=dsl.Hardware(cpus=2, memory='1G'),
    packages_to_install=['numpy'],
    pip_index_urls=['https://pypi.org/simple'],
)
"""
INVALID_DOCSTRING = """
@dsl.component(
    hardware=dsl.Hardware(cpus=2, memory='1G'),
"""
SCRIPT_NAME = "main"
SCRIPT_TEMPLATE = """
{docstring}

print('Hello, world!')
"""


@pytest.fixture(
    scope="session",
    params=[
        COMPONENT_DOCSTRING,
        INVALID_DOCSTRING,
        ORDINARY_DOCSTRING,
        COMPONENT_DOCSTRING + ORDINARY_DOCSTRING,
        None,
    ],
)
def docstring(request) -> Generator[Optional[str], None, None]:
    yield request.param


@pytest.fixture(scope="session")
def script(docstring: Optional[str]) -> Generator[str, None, None]:
    path = os.path.join(TEMPDIR.name, f"{SCRIPT_NAME}.py")
    code = SCRIPT_TEMPLATE.format(docstring=f"'''{docstring}'''" if docstring else "")
    with open(path, "w") as f:
        f.write(code)

    yield path


@pytest.fixture(scope="session")
def component_kwargs(
    docstring: Optional[str],
) -> Generator[Optional[Dict[str, Any]], None, None]:
    if docstring is not None and COMPONENT_DOCSTRING in docstring:
        yield dict(
            hardware=dsl.Hardware(cpus=2, memory="1G"),
            packages_to_install=["numpy"],
            pip_index_urls=["https://pypi.org/simple"],
        )
    else:
        yield None


def test_get_docstring_from_script(script: str, docstring: Optional[str]):
    result = get_docstring_from_script(script)
    if result is None:
        assert docstring is None
    else:
        assert isinstance(docstring, str)
        assert isinstance(result, str)
        assert docstring.strip() == result.strip()


def test_get_component_kwargs_from_docstring(
    docstring: Optional[str], component_kwargs: Optional[Dict[str, Any]]
):
    if docstring is None:
        pass
    elif docstring == INVALID_DOCSTRING:
        with pytest.raises(SyntaxError):
            _ = get_component_kwargs_from_docstring(docstring)
    else:
        assert component_kwargs == get_component_kwargs_from_docstring(docstring)


def test_get_component_kwargs_from_script(
    script: str, docstring: Optional[str], component_kwargs: Optional[Dict[str, Any]]
):
    if docstring == INVALID_DOCSTRING:
        pass
        with pytest.raises(SyntaxError):
            _ = get_component_kwargs_from_script(script)
    else:
        assert component_kwargs == get_component_kwargs_from_script(script)


def test_function_from_script(script: str):
    func = function_from_script(script)
    assert func.__name__ == SCRIPT_NAME


def test_component_from_script(script: str, docstring: Optional[str]):
    if docstring == INVALID_DOCSTRING:
        with pytest.raises(SyntaxError):
            component_from_script(script)
    else:
        component_fn = component_from_script(script)
        component = component_fn(args=[])
        assert component.func.__name__ == SCRIPT_NAME
