"""
Example of installing dependencies from *private* PyPI repositories.

In most organizations, some part of the codebase is not visible to the public.
In order to use private Python packages, though, we have to provide the URL for
a PyPI repository where those packages are hosted.

~~~ IMPORTANT ~~~
Be careful when using private information in any version-controlled repository.
It is strongly recommended to use environment variables for usernames and passwords,
where possible.  Do not commit your user credentials to version control!

The 'docker' and 'kfp' backends use ephemeral Docker images, which are immediately
dicarded after they are used in a pipeline.  Your information is not accessible
during/after runtime (unless you log to the console, or save to file for some reason).
Just don't expose sensitive information directly in the source code.
"""

import argparse
import os

import unipipe
from unipipe import dsl

# Example of building private PyPI URL from environment variables.
USERNAME = os.environ["PRIVATE_PYPI_USERNAME"]
PASSWORD = os.environ["PRIVATE_PYPI_PASSWORD"]
PRIVATE_PYPI_URL = f"https://{USERNAME}:{PASSWORD}@my-private-pypi.org/simple"


@dsl.component(
    packages_to_install=["my-private-package"],
    # When installing packages, index URLs are searched in the order that they are
    # provided below.  It is recommended to place private URLs first, to avoid
    # confusion with any public packages of the same name.
    #
    # The usual, public PyPI URL will be appended to the end of the sequence,
    # if it is not already given.  So below is equivalent to:
    #     pip_index_urls = [PRIVATE_PYPI_URL, "https://pypi.org/simple"]
    pip_index_urls=[PRIVATE_PYPI_URL],
)
def component_with_private_dependencies() -> None:
    from my_private_package import private_stuff

    private_stuff()


@dsl.pipeline
def pipeline():
    component_with_private_dependencies()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--executor", default="python")
    args = parser.parse_args()

    # NOTE: This pipeline won't run, because the private package above does not exist.
    # But in general, you would run it the usual way:
    #   unipipe.run(
    #       executor=args.executor,
    #       pipeline=pipeline(),
    #   )
