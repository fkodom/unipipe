"""
Example of automatically translating scripts to Docker/Vertex with unipipe.

---------

Document things about your script, then include the default 'dsl.component'
arguments somewhere in your docstring.  (See below.)

The block below will be parsed as Python code.
* The 'unipipe' and 'dsl' modules are imported for you.
* Access ENV variables as: {VARIABLE_NAME}.
    - We can't 'import os' before defining the docstring, so it's not possible to
    access environment variables via 'os.environ["VARIABLE_NAME"]' in the docstring.


@dsl.component(
    hardware=dsl.Hardware(cpus=2, memory='1G'),
    packages_to_install=['my-secret-package'],
    pip_index_urls=[
        'https://{PYPI_USERNAME}:{PYPI_PASSWORD}@my-private-pypi.org/simple',
    ],
)
"""

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--hello", type=str, required=True)
args = parser.parse_args()

print(f"Hello, {args.hello}!")
