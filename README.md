# unipipe

**Uni**fied **pipe**line library. 

> Build batch pipelines in Python that run anywhere -- on your laptop, on the server, and in the cloud.  

<p align="center">
    <img src="./doc/img/pipe.png" height=256 width=256/>
</p>

## About

`unipipe` makes it easy to build batch pipelines in Python, then run them either locally or in the cloud. It was originally created for machine learning workflows, but it works the same for *any* batch data processing jobs.


## Install

From PyPI:
```bash
pip install unipipe
```

From source:
```bash
pip install "unipipe @ git+ssh://git@github.com/fkodom/unipipe.git"
```

If you'd like to contribute, install all dev dependencies and pre-commit hooks:
```bash
# Install all dev dependencies (tests etc.)
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

## More Examples

Please see the [examples/](./examples/) folder for examples and tutorials on building pipelines with `unipipe`.


## Why `unipipe`?

1. **`unipipe` was designed to mitigate issues with Kubeflow Pipelines (KFP).**
    * Kubeflow and KFP are often used by machine learning engineers to orchestrate training jobs, data preprocessing, and other computationally intensive tasks.
2. **KFP pipelines only run on Kubeflow.**
    * Creating a full-blown Kubeflow deployment requires specialized knowledge, which is unfamiliar to many practitioners. It also requires additional computational resources, which make it expensive for individuals and small teams to use.
    * Managed, serverless platforms like Vertex (Google Cloud) have emerged, which automated all of that specialized knowledge and infrastructure. But still, pipelines only run on KFP/Vertex -- the same pipeline won't run on your laptop without a ton of extra effort.
3. **Why write the same pipeline twice?**
    * To account for the issues above, developers write multiple pipeline scripts. One runs on your laptop, and another is designed to run in the cloud. 
    * TODO: Finish this section...
