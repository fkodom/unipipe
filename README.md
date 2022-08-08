# unipipe

**Uni**fied **pipe**line library. 

Build batch pipelines in Python that run anywhere -- on your laptop, on the server, and in the cloud.  

:warning: :warning: :warning: Experimental  (pre-alpha)

<p align="center">
    <img src="./doc/img/pipe.png" height=256 width=256/>
</p>


## About

`unipipe` makes it easy to build batch pipelines in Python, then run them either locally or in the cloud. It was originally created for machine learning workflows, but it works for any batch data processing pipeline.


## Install

From PyPI:
```bash
# Minimal install
pip install unipipe

# With additional executors (e.g. 'docker', 'vertex')
pip install unipipe[vertex]
```

From source:
```bash
# Minimal install
pip install "unipipe @ git+ssh://git@github.com/fkodom/unipipe.git"

# With additional executors (e.g. 'docker', 'vertex')
pip install[vertex] "unipipe @ git+ssh://git@github.com/fkodom/unipipe.git"
```

If you'd like to contribute, install all dependencies and pre-commit hooks:
```bash
# Install all dependencies
pip install "unipipe[all] @ git+ssh://git@github.com/fkodom/unipipe.git"
# Setup pre-commit hooks
pre-commit install
```


## Getting Started

Build a pipeline once using the `unipipe` DSL:

```python
from unipipe import dsl

@dsl.component
def say_hello(name: str) -> str:
    return f"Hello, {name}!"

@dsl.pipeline
def pipeline():
    say_hello(name="world")
```

Then, run the pipeline using any of the supported backends:
```python
from unipipe import run

run(
    # Supported executors include:
    #   'python' --> runs in the current Python process
    #   'docker' --> runs each component in a separate Docker container
    #   'vertex' --> runs in GCP through Vertex, which in turn uses KFP
    executor="python",
    pipeline=pipeline(),
)
```

Expected output:
```bash
INFO:root:[say_hello-1603ae3e] - Hello, world!
```

More examples and tutorials available in [examples/](./examples/).


## Why `unipipe`?

1. **`unipipe` was designed to mitigate issues with Kubeflow Pipelines (KFP).**
    * Kubeflow and KFP are often used by machine learning engineers to orchestrate training jobs, data preprocessing, and other computationally intensive tasks.
2. **KFP pipelines only run on Kubeflow.**
    * Kubeflow requires specialized knowledge and additional compute resources. It can be expensive and/or impractical for individuals and small teams.
    * Managed, serverless platforms like Vertex (Google Cloud) exist, which automate all of that. But still, pipelines only run on KFP/Vertex -- not on your laptop.
3. **Why write the same pipeline twice?**
    * KFP developers often write multiple pipeline scripts. One for their laptop, and another for the cloud. 
    * TODO: Finish this section...
