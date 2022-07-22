from __future__ import annotations

from typing import Any, Dict

import kfp.v2.dsl as kfp_dsl
from kfp.v2.components.component_factory import create_component_from_func

from flo.dsl import Component, Pipeline


def build_kubeflow_component(component: Component):
    comp = create_component_from_func(
        func=component.func,
        base_image=component.base_image,
        packages_to_install=component.packages_to_install,
    )
    comp.name = component.name
    if component.hardware.cpu_count:
        comp.container.set_cpu_limit(component.hardware.cpu_count)

    if component.hardware.memory:
        comp.container.set_memory_limit(component.hardware.memory)

    if component.hardware.accelerator_type:
        if not component.hardware.accelerator_count:
            comp.set_accelerator_count(1)

        comp.container.set_gpu_limit(component.hardware.accelerator_count)
        comp.add_node_selector_constraint(
            "cloud.google.com/gke-accelerator",
            component.hardware.accelerator_type.value,
        )

    return comp


class KubeflowPipelinesBackend:
    def build(self, pipeline: Pipeline):
        if len(pipeline.components) == 0:
            raise RuntimeError(
                f"Cannot build a empty pipeline. Found {pipeline.components=}."
            )

        @kfp_dsl.pipeline(name=pipeline.name)
        def kfp_pipeline():
            outputs: Dict[str, Any] = {}

            for _component in pipeline.components:
                kfp_component = build_kubeflow_component(_component)
                kwargs = {
                    k: outputs[v.name] if isinstance(v, Component) else v
                    for k, v in _component.inputs.items()
                }
                container_op = kfp_component(**kwargs)
                outputs[_component.name] = container_op.output

        return kfp_pipeline
