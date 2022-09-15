"""
@dsl.component(
    hardware=dsl.Hardware(cpus=2, memory='1G'),
    packages_to_install=[
        # Commented out, so tests will pass for this dummy example :)
        # 'my-secret-package'
    ],
    pip_index_urls=[
        # Example of using ENV variables to set private PyPI credentials.
        'https://{PYPI_USERNAME}:{PYPI_PASSWORD}@my-private-pypi.org/simple',
    ],
)
"""

# Example of running arbitrary Python scripts in Docker/Vertex with unipipe.
# Use the '@dsl.component' decorator in your docstring to declare the *default*
# hardware, packages, etc. for this script component.  You can override any of
# these defaults with the CLI tool.
#
# NOTE: Your docstring can include more text, too. But for clarity, this example
# only contains the '@dsl.component' decorator and arguments.
# NOTE: This script requires (dummy) env variables PYPI_USERNAME and PYPI_PASSWORD.
#     export PYPI_USERNAME="user"
#     export PYPI_PASSWORD="pass"
#
# CLI command:
#     unipipe run-script [--executor <EXECUTOR> | <COMPONENT_OPTIONS>] examples/ex11_using_scripts.py [ARGS]
#
# Examples:
#     unipipe run-script examples/ex11_using_scripts.py --hello world
#     unipipe run-script \
#         --executor vertex \
#         examples/ex11_using_scripts.py --hello Vertex
#
# For more details on usage and CLI options, please see:
#     unipipe run-script --help


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--hello", type=str, required=True)
    args = parser.parse_args()

    print(f"Hello, {args.hello}!")
