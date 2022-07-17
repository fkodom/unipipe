# {{REPO_NAME}}

## Install

```bash
pip install "{{REPO_NAME}} @ git+ssh://git@github.com/{{REPO_OWNER}}/{{REPO_NAME}}.git"

# Install all dev dependencies (tests etc.)
pip install "{{REPO_NAME}}[all] @ git+ssh://git@github.com/{{REPO_OWNER}}/{{REPO_NAME}}.git"

# Setup pre-commit hooks
pre-commit install
```