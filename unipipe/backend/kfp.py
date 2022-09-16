from __future__ import annotations

from typing import Any, Dict

import kfp.dsl as kfp_dsl
import kfp.v2.dsl as kfp_v2_dsl
from kfp.v2.components.component_factory import create_component_from_func

from unipipe.dsl import Component, ConditionalPipeline, LazyAttribute, Pipeline
from unipipe.utils.annotations import resolve_annotations


def build_kubeflow_component(component: Component):
    comp = create_component_from_func(
        func=resolve_annotations(component.func),
        base_image=component.base_image or "fkodom/unipipe:latest",
        packages_to_install=component.packages_to_install,
        pip_index_urls=component.pip_index_urls,
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
            # KFP requires the count to be given as a string
            container_op.set_gpu_limit(str(accelerator.count))
        if accelerator.type:
            container_op.add_node_selector_constraint(
                "cloud.google.com/gke-accelerator",
                accelerator.type.value,
            )

    return container_op


def resolve_value(_locals: Dict, value: Any) -> Any:
    if isinstance(value, LazyAttribute):
        return _locals[value.parent.name].outputs[value.key]
    elif isinstance(value, Component):
        return _locals[value.name].output
    elif isinstance(value, Pipeline):
        if value.name in _locals:
            return _locals[value.name]
        return resolve_value(_locals, value.return_value)
    elif isinstance(value, tuple):
        return tuple(resolve_value(_locals, x) for x in value)
    elif isinstance(value, list):
        return list(resolve_value(_locals, x) for x in value)
    else:
        return value


def run_component(component: Component, **kwargs):
    kfp_component = build_kubeflow_component(component)
    result = kfp_component(**kwargs)
    set_hardware_attributes(result, component)

    return result


def build_pipeline_with_locals(pipeline: Pipeline, _locals: Dict[str, Any]):
    for component in pipeline.components:
        kwargs = {k: resolve_value(_locals, v) for k, v in component.inputs.items()}
        __locals = {**_locals, **kwargs}
        if isinstance(component, ConditionalPipeline):
            operand1 = resolve_value(_locals, component.condition.operand1)
            operand2 = resolve_value(_locals, component.condition.operand2)
            comparator = component.condition.comparator

            # An unfortunate fact about KFP conditions -- they require two operands,
            # and as a result, they don't deal well with raw boolean input values.
            # As a workaround, you can cast the boolean values to strings and compare.
            #
            # Basically, this allows 'unipipe' users to write things like:
            #   with dsl.equal(evaluates_to_true, True):
            #       do_something()
            if isinstance(operand1, bool):
                operand1 = str(operand1).lower()
            if isinstance(operand2, bool):
                operand2 = str(operand2).lower()

            with kfp_dsl.Condition(comparator(operand1, operand2), name=component.name):
                result, _locals = build_pipeline_with_locals(
                    component, _locals=__locals
                )
        elif isinstance(component, Pipeline):
            result, _ = build_pipeline_with_locals(component, _locals=__locals)
        elif isinstance(component, Component):
            result = run_component(component, **kwargs)
        else:
            raise TypeError(
                f"Found pipeline component {component} with unexpected type: "
                f"{type(component)}. Valid component types are "
                "[Component, ConditionalPipeline, Pipeline]."
            )

        _locals[component.name] = result

    return_value = resolve_value(_locals, pipeline.return_value)
    return return_value, _locals


class KubeflowPipelinesBackend:
    def build(self, pipeline: Pipeline):
        @kfp_v2_dsl.pipeline(name=pipeline.name)
        def kfp_pipeline():
            build_pipeline_with_locals(pipeline, _locals=pipeline.inputs)

        return kfp_pipeline
