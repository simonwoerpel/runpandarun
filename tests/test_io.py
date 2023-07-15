from io import StringIO

import pandas as pd

from runpandarun import io

# from tests.util import setup_s3_bucket

# from moto import mock_s3


def test_io_read(fixtures_path):
    df = io.read_pandas(fixtures_path / "testdata.csv")
    assert len(df) == 10000
    assert list(df.columns) == ["state", "city", "amount", "date"]

    df = io.read_pandas(fixtures_path / "testdata.json", handler="read_json")
    assert len(df) == 10000
    assert list(df.columns) == ["state", "integer", "date"]
    assert not isinstance(df["integer"][0], str)
    df = io.read_pandas(
        fixtures_path / "testdata.json",
        handler="read_json",
        **{"dtype": {"integer": str}}
    )
    assert isinstance(df["integer"][0], str)


def test_io_write(fixtures_path, tmp_path):
    df = io.read_pandas(fixtures_path / "testdata.csv")
    io.write_pandas(df, tmp_path / "testdata.csv")
    df_out = pd.read_csv(tmp_path / "testdata.csv")
    assert len(df) == len(df_out)
    out = StringIO()
    assert not out.closed
    io.write_pandas(df.head(), out)
    assert out.closed


def test_io_read_remote(server):
    df = io.read_pandas(server % "testdata.csv")
    assert len(df) == 10000
    assert list(df.columns) == ["state", "city", "amount", "date"]


# @mock_s3
# def test_io_s3():
#     setup_s3_bucket(with_content=True)
#     df = io.read_pandas("s3://runpandarun/testdata.csv")
#     assert len(df) == 10000
#     assert list(df.columns) == ["state", "city", "amount", "date"]

#     io.write_pandas("s3://runpandarun/testdata2.csv", df.head())
#     df = io.read_pandas("s3://runpandarun/testdata2.csv")
#     assert len(df) == 5/
#     assert list(df.columns) == ["state", "city", "amount", "date"]
