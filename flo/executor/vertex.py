from __future__ import annotations

import os
from tempfile import TemporaryDirectory
from typing import Any, Dict, Optional

from kfp.v2.compiler import Compiler
from kfp.v2.google.client import AIPlatformClient

from flo.backend.kfp import KubeflowPipelinesBackend
from flo.dsl import Pipeline
from flo.executor.base import Executor


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
        enable_cache: bool = True,
        project_id: Optional[str] = "sense-staging",
        region: Optional[str] = "us-central1",
        **kwargs,
    ):
        client = AIPlatformClient(project_id=project_id, region=region)
        if isinstance(pipeline, Pipeline):
            pipeline = KubeflowPipelinesBackend().build(pipeline)

        with TemporaryDirectory() as tempdir:
            job_spec_path = os.path.join(tempdir)
            Compiler().compile(pipeline_func=pipeline, package_path=job_spec_path)

            client.create_run_from_job_spec(
                job_spec_path=job_spec_path,
                parameter_values=arguments,
                # job_id=self.job_id,
                pipeline_root=pipeline_root,
                enable_caching=enable_cache,
                **kwargs,
            )
