import os
from distutils.core import setup
from subprocess import getoutput

import setuptools


def get_version_tag() -> str:
    try:
        env_key = "UNIPIPE_VERSION".upper()
        version = os.environ[env_key]
    except KeyError:
        version = getoutput("git describe --tags --abbrev=0")

    if version.lower().startswith("fatal"):
        version = "0.0.0"

    return version


extras_require = {
    "docker": ["docker"],
    "vertex": ["kfp", "google-cloud-aiplatform", "google-cloud-storage"],
    "test": [
        "arrow",
        "black",
        "flake8",
        "isort",
        "mypy",
        "pytest",
        "pytest-cov",
        "torch",
        "torchvision",
    ],
}
extras_require["dev"] = ["pre-commit", "ipywidgets", *extras_require["test"]]
all_require = [r for reqs in extras_require.values() for r in reqs]
extras_require["all"] = all_require


setup(
    name="unipipe",
    version=get_version_tag(),
    author="Frank Odom",
    author_email="frank.odom.iii@gmail.com",
    url="https://github.com/fkodom/unipipe",
    packages=setuptools.find_packages(exclude=["tests"]),
    description="project_description",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    install_requires=["click", "pydantic", "typing_extensions"],
    extras_require=extras_require,
    entry_points={
        "console_scripts": [
            "unipipe = unipipe.cli:unipipe",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
