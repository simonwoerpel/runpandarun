import os

from runpandarun import Playbook

EXPECTED = Playbook(
    in_uri="testdata.csv",
    out_uri="testdata_transformed.csv",
    columns=[{"value": "amount"}, "state", "date"],
    dt_index="date",
)


def test_spec_initialization(fixtures_path):
    # empty doesn't fail
    play = Playbook()
    assert play.in_uri == "-"
    assert play.out_uri == "-"

    # pydantic way
    play = Playbook(in_uri="testdata.csv")
    assert play.in_uri == EXPECTED.in_uri

    # yaml
    path = fixtures_path / "spec.yml"
    play = Playbook.from_yaml(path)
    assert play.in_uri.endswith(EXPECTED.in_uri)
    assert play.out_uri.endswith(EXPECTED.out_uri)
    assert play.columns == EXPECTED.columns
    assert play.dt_index == EXPECTED.dt_index
    assert play.handler.kwargs["skipfooter"] == 1

    # from string with env vars
    config = """
    in_uri: ${API_URL}
    request:
      header:
        key: $API_KEY
    columns:
      - a
      - b: c
      - d: $NEW_COLUMN
    """

    os.environ["API_KEY"] = "a secret key"
    os.environ["API_URL"] = "https://example.org"
    os.environ["NEW_COLUMN"] = "e"

    play = Playbook.from_string(config)

    assert play.request.header["key"] == "a secret key"
    assert play.in_uri == "https://example.org"
    assert play.columns[2]["d"] == "e"


def test_spec_merge(fixtures_path):
    play = Playbook.from_yaml(fixtures_path / "spec.yml")
    assert play.in_uri.endswith("/testdata.csv")
    play = play.merge(in_uri="-")
    assert play.in_uri == "-"
