import unipipe
from examples.ex01_hello_world import pipeline as pipeline_01
from examples.ex02_hello_pipeline import pipeline as pipeline_02
from examples.ex03_multi_output_components import pipeline as pipeline_03
from examples.ex04_pipeline_arguments import pipeline as pipeline_04
from examples.ex05_dependency_management import pipeline as pipeline_05
from examples.ex06_hardware_specs import pipeline as pipeline_06
from examples.ex07_nested_pipelines import pipeline as pipeline_07
from examples.ex08_control_flow import pipeline as pipeline_08


def test_example_01():
    unipipe.run(pipeline=pipeline_01(), executor="python")


def test_example_02():
    unipipe.run(pipeline=pipeline_02(), executor="python")


def test_example_03():
    unipipe.run(pipeline=pipeline_03(), executor="python")


def test_example_04():
    unipipe.run(pipeline=pipeline_04(name="Tyrion Lannister"), executor="python")


def test_example_05():
    unipipe.run(pipeline=pipeline_05(name="Tyrion Lannister"), executor="python")


def test_example_06():
    image_url = "https://raw.githubusercontent.com/EliSchwartz/imagenet-sample-images/master/n01443537_goldfish.JPEG"
    unipipe.run(pipeline=pipeline_06(image_url=image_url), executor="python")


def test_example_07():
    unipipe.run(pipeline=pipeline_07(), executor="python")


def test_example_08():
    unipipe.run(pipeline=pipeline_08(), executor="python")
