import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock

import aiobotocore.awsrequest
import aiobotocore.endpoint
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import botocore.awsrequest
import botocore.model
import pandas as pd
import pytest
import requests
from sqlalchemy import create_engine

FIXTURES_PATH = (Path(__file__).parent / "fixtures").absolute()


# https://pawamoy.github.io/posts/local-http-server-fake-files-testing-purposes/
def spawn_and_wait_server():
    process = subprocess.Popen(
        [sys.executable, "-m", "http.server", "-d", FIXTURES_PATH]
    )
    while True:
        try:
            requests.get("http://localhost:8000")
        except Exception:
            time.sleep(1)
        else:
            break
    return process


# credits to pytest-xdist's README
@pytest.fixture(scope="session", autouse=True)
def http_server(tmp_path_factory, worker_id):
    if worker_id == "master":
        # single worker: just run the HTTP server
        process = spawn_and_wait_server()
        yield process
        process.kill()
        process.wait()
        return

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    # try to get a lock
    lock = root_tmp_dir / "lock"
    try:
        lock.mkdir(exist_ok=False)
    except FileExistsError:
        yield  # failed, don't run the HTTP server
        return

    # got the lock, run the HTTP server
    process = spawn_and_wait_server()
    yield process
    process.kill()
    process.wait()


@pytest.fixture(scope="module")
def fixtures_path():
    return FIXTURES_PATH


@pytest.fixture(scope="module")
def server():
    return "http://localhost:8000/%s"


@pytest.fixture(scope="function")
def con(tmp_path):
    con = f"sqlite:///{tmp_path}/data.db"
    df = pd.read_csv(FIXTURES_PATH / "testdata.csv")
    df.to_sql("test_table", create_engine(con))
    return con


# Mock s3 for fsspec
# https://github.com/aio-libs/aiobotocore/issues/755


class MockAWSResponse(aiobotocore.awsrequest.AioAWSResponse):
    """
    Mocked AWS Response.

    https://github.com/aio-libs/aiobotocore/issues/755
    https://gist.github.com/giles-betteromics/12e68b88e261402fbe31c2e918ea4168
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        self._moto_response = response
        self.status_code = response.status_code
        self.raw = MockHttpClientResponse(response)

    # adapt async methods to use moto's response
    async def _content_prop(self) -> bytes:
        return self._moto_response.content

    async def _text_prop(self) -> str:
        return self._moto_response.text


class MockHttpClientResponse(aiohttp.client_reqrep.ClientResponse):
    """
    Mocked HTP Response.

    See <MockAWSResponse> Notes
    """

    def __init__(self, response: botocore.awsrequest.AWSResponse):
        """
        Mocked Response Init.
        """

        async def read(self: MockHttpClientResponse, n: int = -1) -> bytes:
            return response.content

        self.content = MagicMock(aiohttp.StreamReader)
        self.content.read = read
        self.response = response

        self._loop = None

    @property
    def raw_headers(self) -> Any:
        """
        Return the headers encoded the way that aiobotocore expects them.
        """
        return {
            k.encode("utf-8"): str(v).encode("utf-8")
            for k, v in self.response.headers.items()
        }.items()


@pytest.fixture(scope="session", autouse=True)
def patch_aiobotocore() -> None:
    """
    Pytest Fixture Supporting S3FS Mocks.

    See <MockAWSResponse> Notes
    """

    def factory(original: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
        """
        Response Conversion Factory.
        """

        def patched_convert_to_response_dict(
            http_response: botocore.awsrequest.AWSResponse,
            operation_model: botocore.model.OperationModel,
        ) -> Any:
            return original(MockAWSResponse(http_response), operation_model)

        return patched_convert_to_response_dict

    aiobotocore.endpoint.convert_to_response_dict = factory(
        aiobotocore.endpoint.convert_to_response_dict
    )
