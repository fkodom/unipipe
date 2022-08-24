import json
import os
import tempfile

from kfp.v2.compiler import Compiler

from examples.ex01_hello_world import pipeline as pipeline_01
from examples.ex02_hello_pipeline import pipeline as pipeline_02
from examples.ex03_multi_output_components import pipeline as pipeline_03
from examples.ex04_pipeline_arguments import pipeline as pipeline_04
from examples.ex05_dependency_management import pipeline as pipeline_05
from examples.ex06_hardware_specs import pipeline as pipeline_06
from examples.ex07_nested_pipelines import pipeline as pipeline_07
from examples.ex08_control_flow import pipeline as pipeline_08
from examples.ex09_advanced_control_flow import good_pipeline as pipeline_09
from unipipe import dsl
from unipipe.backend.kfp import KubeflowPipelinesBackend


def _test_build_kfp_pipeline(pipeline: dsl.Pipeline):
    kfp_pipeline = KubeflowPipelinesBackend().build(pipeline=pipeline)
    with tempfile.TemporaryDirectory() as tempdir:
        path = os.path.join(tempdir, "pipeline.json")
        Compiler().compile(pipeline_func=kfp_pipeline, package_path=path)

        # Check that the JSON file was created, and we can successfully load it.
        assert os.path.exists(path)
        with open(path) as f:
            _ = json.load(f)


def test_example_01():
    _test_build_kfp_pipeline(pipeline_01())


def test_example_02():
    _test_build_kfp_pipeline(pipeline_02())


def test_example_03():
    _test_build_kfp_pipeline(pipeline_03())


def test_example_04():
    _test_build_kfp_pipeline(pipeline_04(name="Tyrion Lannister"))


def test_example_05():
    _test_build_kfp_pipeline(pipeline_05(name="Tyrion Lannister"))


def test_example_06():
    image_url = "https://raw.githubusercontent.com/EliSchwartz/imagenet-sample-images/master/n01443537_goldfish.JPEG"
    _test_build_kfp_pipeline(pipeline_06(image_url=image_url))


def test_example_07():
    _test_build_kfp_pipeline(pipeline_07())


def test_example_08():
    _test_build_kfp_pipeline(pipeline_08())


def test_example_09():
    _test_build_kfp_pipeline(pipeline_09(name="Ned Stark"))
