import os

from runpandarun import Playbook


def test_spec_initialization(fixtures_path):
    # empty doesn't fail and has some defaults
    play = Playbook()
    assert play.read.uri == "-"
    assert play.read.handler == "read_csv"
    assert play.write.uri == "-"
    assert play.write.handler == "to_csv"
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

    os.environ["SECRET_LOCATION"] = "sftp://user:password/data.csv"
    os.environ["A_SECRET_TRANSFORMATION"] = "str.lower"

    play = Playbook.from_string(config)
    assert play.read.uri == "sftp://user:password/data.csv"
    assert play.operations[0].options["func"] == "str.lower"