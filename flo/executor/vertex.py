from __future__ import annotations

import datetime
from typing import Dict, Optional, List
from enum import Enum

from pydantic import BaseModel, Field

from flo.dsl import Component


class Accelerator(str, Enum):
    """
    Enum of supported Accelerator ids.
    """

    T4: Accelerator = "nvidia-tesla-t4"
    V100: Accelerator = "nvidia-tesla-v100"
    P4: Accelerator = "nvidia-tesla-p4"
    P100: Accelerator = "nvidia-tesla-p100"
    K80: Accelerator = "nvidia-tesla-k80"


class HardwareResources(BaseModel):
    """
    Hardware resources for a pipeline component.
    """

    cpu_count: Optional[str] = Field(None, alias="cpuCount")
    memory: Optional[str] = None
    accelerator_count: Optional[str] = Field(None, alias="acceleratorCount")
    accelerator_type: Optional[Accelerator] = Field(None, alias="acceleratorType")


class PipelineState(str, Enum):
    """
    Enum for possible pipeline states.
    """

    running: str = "PIPELINE_STATE_RUNNING"
    succeeded: str = "PIPELINE_STATE_SUCCEEDED"
    failed: str = "PIPELINE_STATE_FAILED"
    cancelled: str = "PIPELINE_STATE_CANCELLED"
    cancelling: str = "PIPELINE_STATE_CANCELLING"
    paused: str = "PIPELINE_STATE_PAUSED"
    pending: str = "PIPELINE_STATE_PENDING"


class TaskDetail(BaseModel):
    taskName: Optional[str]
    state: Optional[str]
    execution: Optional[Dict]
    inputs: Optional[Dict]
    outputs: Optional[Dict]

    @property
    def metadata(self) -> str:
        if self.execution:
            return self.execution["metadata"]

        return None

    @property
    def execution_state(self) -> str:
        if self.execution:
            return self.execution["state"]

        return None


class JobDetail(BaseModel):
    taskDetails: Optional[List[TaskDetail]]


class PipelineStatus(BaseModel):
    """
    Dataclass for pipeline status.
    """

    name: str
    displayName: str
    createTime: datetime.datetime
    updateTime: datetime.datetime
    pipelineSpec: Dict
    state: PipelineState
    jobDetail: Optional[JobDetail]

    @property
    def task_details(self) -> Optional[List[TaskDetail]]:
        return self.jobDetail.taskDetails
