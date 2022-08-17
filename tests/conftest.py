import pytest


def pytest_addoption(parser):
    parser.addoption("--docker", action="store_true")


def pytest_configure(config):
    config.addinivalue_line("markers", "docker: docker tests")


def pytest_collection_modifyitems(config, items):
    run_docker = config.getoption("--docker")
    skip_docker = pytest.mark.skip(reason="need --docker option to run")

    for item in items:
        if ("docker" in item.keywords) and (not run_docker):
            item.add_marker(skip_docker)
