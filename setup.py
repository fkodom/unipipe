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
    "docker": ["docker>=5.0"],
    "vertex": ["kfp>=1.8", "google-cloud-aiplatform>=1.10"],
    "test": [
        "arrow>=1.2",
        "black>=22.6",
        "flake8>=5.0",
        "isort>=5.10",
        "mypy>=0.971",
        "pytest>=7.1",
        "pytest-cov>=3.0",
        "torch>=1.8",
        "torchvision>=0.9",
    ],
}
extras_require["dev"] = ["pre-commit>=2.20", *extras_require["test"]]
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
