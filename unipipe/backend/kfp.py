from __future__ import annotations

from inspect import isclass
from typing import Any, Dict, Optional

import kfp.v2.dsl as kfp_dsl
from kfp.v2.components.component_factory import create_component_from_func

from unipipe.dsl import Component, LazyAttribute, Pipeline
from unipipe.utils.annotations import get_annotations, resolve_annotations


def build_kubeflow_component(component: Component):
    comp = create_component_from_func(
        func=resolve_annotations(component.func),
        base_image=component.base_image or "fkodom/unipipe:latest",
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


def resolve_value(arguments: Dict, value: Any) -> Any:
    if isinstance(value, LazyAttribute):
        return arguments[value.parent.name].outputs[value.key]
    elif isinstance(value, Component):
        return arguments[value.name].output
    elif isinstance(value, Pipeline):
        if value.name in arguments:
            return arguments[value.name]
        return resolve_value(arguments, value.return_value)
    elif isinstance(value, (tuple, list)):
        return tuple(resolve_value(arguments, x) for x in value)
    else:
        return value


def _build(pipeline: Pipeline, arguments: Optional[Dict] = None):
    if arguments is None:
        arguments = pipeline.inputs

    for comp in pipeline.components:
        _kwargs = {k: resolve_value(arguments, v) for k, v in comp.inputs.items()}
        if isinstance(comp, Pipeline):
            result = _build(comp, arguments=_kwargs)
        else:
            result = comp.func(**_kwargs)
            kfp_component = build_kubeflow_component(comp)
            result = kfp_component(**_kwargs)
            set_hardware_attributes(result, comp)

        arguments[comp.name] = result

    return resolve_value(arguments, pipeline.return_value)


class KubeflowPipelinesBackend:
    def build(self, pipeline: Pipeline):
        @kfp_dsl.pipeline(name=pipeline.name)
        def kfp_pipeline():
            _build(pipeline)

        return kfp_pipeline


# class KubeflowPipelinesBackend:
#     def build(self, pipeline: Pipeline):
#         @kfp_dsl.pipeline(name=pipeline.name)
#         def kfp_pipeline():
#             outputs: Dict[str, Any] = {}

#             def resolve_value(v: Any) -> Any:
#                 if isinstance(v, LazyAttribute):
#                     return outputs[v.parent.name].outputs[v.key]
#                 elif isinstance(v, Component):
#                     return outputs[v.name].output
#                 else:
#                     return v

#             for _component in pipeline.components:
#                 _kwargs = {
#                     k: resolve_value(arguments, v) for k, v in comp.inputs.items()
#                 }
#                 kfp_component = build_kubeflow_component(_component)
#                 kwargs = {k: resolve_value(v) for k, v in _component.inputs.items()}
#                 container_op = kfp_component(**kwargs)
#                 set_hardware_attributes(container_op, _component)

#                 try:
#                     outputs[_component.name] = container_op
#                 except RuntimeError:
#                     outputs[_component.name] = container_op

#         return kfp_pipeline
