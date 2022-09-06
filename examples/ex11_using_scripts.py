"""
Example of automatically translating scripts to Docker/Vertex with unipipe.

To run a script with unipipe, use the following CLI command:
    unipipe run-script [--executor <EXECUTOR> | <COMPONENT_OPTIONS>] examples/ex11_using_scripts.py [ARGS]

Examples:
    export PYPI_USERNAME="dummy-username"
    export PYPI_PASSWORD="dummy-password"

    unipipe run-script examples/ex11_using_scripts.py --hello world
    unipipe run-script \
        --executor vertex \
        examples/ex11_using_scripts.py --hello Vertex
    unipipe run-script \
        --packages_to_install sklearn numpy \
         examples/ex11_using_scripts.py --hello world
    * Override hardware with a raw JSON string, which is parsed by unipipe.
    unipipe run-script \
        --executor vertex \
        --hardware '{"cpus": 4, "memory": "4G"}' \
        examples/ex11_using_scripts.py --hello Vertex

For help, please see:
    unipipe run-script --help

---------

Include the default 'dsl.component' args somewhere in your docstring. This block
(below) will be parsed as Python code.  Note that:
* The 'unipipe' and 'dsl' modules are imported for you.
* You can access ENV variables as: {VARIABLE_NAME}.
    - We can't 'import os' before defining the docstring, so it's not possible to
    access environment variables via 'os.environ["VARIABLE_NAME"]' in the docstring.
    - Execution of the script is delayed, so you couldn't define the '__doc__'
    property in your script as a workaround.


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
