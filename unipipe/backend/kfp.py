from __future__ import annotations

from typing import Any, Dict

import kfp.v2.dsl as kfp_dsl
from kfp.v2.components.component_factory import create_component_from_func

from unipipe.dsl import Component, LazyAttribute, Pipeline
from unipipe.utils.annotations import resolve_annotations


def build_kubeflow_component(component: Component):
    comp = create_component_from_func(
        func=resolve_annotations(component.func),
        base_image=component.base_image,
        packages_to_install=component.packages_to_install,
    )
    comp.name = component.name
    return comp


def set_hardware_attributes(container_op: Any, component: Component):
    hardware = component.hardware
    if hardware.cpus:
        container_op.set_cpu_limit(hardware.cpus)
    if hardware.memory:
        container_op.set_memory_limit(hardware.memory)
    if hardware.accelerator:
        accelerator = hardware.accelerator
        if accelerator.count:
            container_op.set_gpu_limit(accelerator.count)
        if accelerator.type:
            container_op.add_node_selector_constraint(
                "cloud.google.com/gke-accelerator",
                accelerator.type.value,
            )

    return container_op


class KubeflowPipelinesBackend:
    def build(self, pipeline: Pipeline):
        if len(pipeline.components) == 0:
            raise RuntimeError(
                f"Cannot build a empty pipeline. Found {pipeline.components=}."
            )

        @kfp_dsl.pipeline(name=pipeline.name)
        def kfp_pipeline():
            outputs: Dict[str, Any] = {}

            def resolve_value(v: Any) -> Any:
                if isinstance(v, LazyAttribute):
                    return outputs[v.parent.name].outputs[v.key]
                elif isinstance(v, Component):
                    return outputs[v.name].output
                else:
                    return v

            for _component in pipeline.components:
                kfp_component = build_kubeflow_component(_component)
                kwargs = {k: resolve_value(v) for k, v in _component.inputs.items()}
                container_op = kfp_component(**kwargs)
                set_hardware_attributes(container_op, _component)

                try:
                    outputs[_component.name] = container_op
                except RuntimeError:
                    outputs[_component.name] = container_op

                # TODO: New class 'ComponentOutputs' that delays the output logic
                # until the backend/executor class.  So we can delineate the
                # behavior for Python/Docker/KFP backends.

        return kfp_pipeline
