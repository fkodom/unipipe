from __future__ import annotations

import os
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional

from google.auth.credentials import Credentials
from google.cloud.aiplatform import PipelineJob
from kfp.v2.compiler import Compiler

from unipipe.backend.kfp import KubeflowPipelinesBackend
from unipipe.dsl import Pipeline
from unipipe.executor.base import Executor

# from google.auth import default
# from google.cloud.storage import Client as StorageClient

# class PipelineState(str, Enum):
#     """
#     Enum for possible pipeline states.
#     """

#     running: str = "PIPELINE_STATE_RUNNING"
#     succeeded: str = "PIPELINE_STATE_SUCCEEDED"
#     failed: str = "PIPELINE_STATE_FAILED"
#     cancelled: str = "PIPELINE_STATE_CANCELLED"
#     cancelling: str = "PIPELINE_STATE_CANCELLING"
#     paused: str = "PIPELINE_STATE_PAUSED"
#     pending: str = "PIPELINE_STATE_PENDING"


# class TaskDetail(BaseModel):
#     taskName: Optional[str]
#     state: Optional[str]
#     execution: Optional[Dict]
#     inputs: Optional[Dict]
#     outputs: Optional[Dict]

#     @property
#     def metadata(self) -> str:
#         if self.execution:
#             return self.execution["metadata"]

#         return None

#     @property
#     def execution_state(self) -> str:
#         if self.execution:
#             return self.execution["state"]

#         return None


# class JobDetail(BaseModel):
#     taskDetails: Optional[List[TaskDetail]]


# class PipelineStatus(BaseModel):
#     """
#     Dataclass for pipeline status.
#     """

#     name: str
#     displayName: str
#     createTime: datetime.datetime
#     updateTime: datetime.datetime
#     pipelineSpec: Dict
#     state: PipelineState
#     jobDetail: Optional[JobDetail]

#     @property
#     def task_details(self) -> Optional[List[TaskDetail]]:
#         return self.jobDetail.taskDetails


class VertexExecutor(Executor):
    def run(
        self,
        pipeline: Any,
        arguments: Optional[Dict] = None,
        pipeline_root: Optional[str] = None,
        enable_caching: bool = False,
        credentials: Optional[Credentials] = None,
        project: str = None,
        location: str = "us-central1",
    ):
        if isinstance(pipeline, Pipeline):
            pipeline = KubeflowPipelinesBackend().build(pipeline)

        # StorageClient()
        # default_credentials, default_project = default()
        # if not credentials:
        #     credentials = default_credentials
        # if not project:
        #     project = default_project

        with TemporaryDirectory() as tempdir:
            template_path = os.path.join(tempdir, "pipeline.json")
            Compiler().compile(pipeline_func=pipeline, package_path=template_path)
            PipelineJob(
                display_name="example-vertex-pipeline",
                template_path=template_path,
                parameter_values=arguments,
                credentials=credentials,
                project=project,
                location=location,
                pipeline_root=pipeline_root,
                enable_caching=enable_caching,
            ).submit()
