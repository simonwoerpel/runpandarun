import pytest

from runpandarun.exceptions import SpecError
from runpandarun.playbook import Operation, Playbook


def test_spec_initialization(monkeypatch, fixtures_path):
    # empty doesn't fail and has some defaults
    play = Playbook()
    assert play.read.uri == "-"
    assert play.read.handler is None
    assert play.read.get_name() == "read_csv"
    assert play.write.uri == "-"
    assert play.write.handler is None
    assert play.write.get_name() == "to_csv"
    assert play.operations == []

    # pydantic way
    play = Playbook(read={"uri": "./testdata.csv"})
    assert play.read.uri == "./testdata.csv"

    # yaml
    path = fixtures_path / "spec.yml"
    play = Playbook.from_yaml(path)
    assert play.read.options["skipfooter"] == 1

    # from string with env vars
    config = """
    read:
      uri: ${SECRET_LOCATION}
    operations:
      - handler: DataFrame.apply
        options:
          func: ${A_SECRET_TRANSFORMATION}
    """

    monkeypatch.setenv("SECRET_LOCATION", "sftp://user:password/data.csv")
    monkeypatch.setenv("A_SECRET_TRANSFORMATION", "str.lower")

    play = Playbook.from_string(config)
    assert play.read.uri == "sftp://user:password/data.csv"
    assert play.operations[0].options["func"] == "str.lower"


def test_spec_invalid():
    with pytest.raises(SpecError):
        Operation(handler="foo")
    with pytest.raises(SpecError):
        Operation(handler="foo.bar")
    with pytest.raises(SpecError):
        Operation(handler="DataFrame.foo")
    with pytest.raises(SpecError):  # missing column
        Operation(handler="Series.map")
