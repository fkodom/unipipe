"""
Example of automatically translating scripts to Docker/Vertex with unipipe.

To run this script, run the following CLI command:
    unipipe run-script [--executor <EXECUTOR> | <component-options>] examples/ex11_using_scripts.py [ARGS]
Examples:
    unipipe run-script examples/ex11_using_scripts.py --hello world
    unipipe run-script --executor vertex examples/ex11_using_scripts.py --hello Vertex
    unipipe run-script --executor vertex \
        --packages_to_install sklearn numpy \
         examples/ex11_using_scripts.py --hello Vertex

---------

Include the default 'dsl.component' args somewhere in your docstring.  (See below.)

The block below will be parsed as Python code.
* The 'unipipe' and 'dsl' modules are imported for you.
* Access ENV variables as: {VARIABLE_NAME}.
    - We can't 'import os' before defining the docstring, so it's not possible to
    access environment variables via 'os.environ["VARIABLE_NAME"]' in the docstring.


@dsl.component(
    hardware=dsl.Hardware(cpus=2, memory='1G'),
    packages_to_install=[
        # Commented out, so tests will pass for this dummy example :)
        # 'my-secret-package'
    ],
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
