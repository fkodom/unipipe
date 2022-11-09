from __future__ import annotations

import os
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional

from google.auth.credentials import Credentials
from google.cloud.aiplatform import PipelineJob

from unipipe.backend.kfp import KubeflowPipelinesBackend
from unipipe.executor.base import Executor


class VertexExecutor(Executor):
    def run(
        self,
        pipeline: Any,
        pipeline_root: Optional[str] = None,
        arguments: Optional[Dict] = None,
        enable_caching: bool = False,
        credentials: Optional[Credentials] = None,
        project: Optional[str] = None,
        location: str = "us-central1",
    ):
        if pipeline_root is None:
            raise ValueError(
                f"Must provide 'pipeline_root' argument for '{self.__class__.__name__}' "
                f"backend. Expected non-empty string, but found: '{pipeline_root}'"
            )

        with TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, "pipeline.json")
            KubeflowPipelinesBackend().compile(pipeline=pipeline, path=path)
            PipelineJob(
                display_name="example-vertex-pipeline",
                template_path=path,
                parameter_values=arguments,
                credentials=credentials,
                project=project,
                location=location,
                pipeline_root=pipeline_root,
                enable_caching=enable_caching,
            ).submit()
