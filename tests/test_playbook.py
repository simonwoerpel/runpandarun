import pandas as pd
import pytest

from runpandarun import Playbook, read_pandas


def test_playbook(fixtures_path):
    play = Playbook.from_yaml(fixtures_path / "spec.yml")
    df = play.run(write=False)
    assert len(df) == 9999
    assert df.index[0] == "ak-ahirco"
    assert len(df[df["city"] == "Zarizri"]) == 0
    orig = pd.read_csv(fixtures_path / "testdata.csv", skipfooter=1)
    assert len(orig[orig["city"] == "Zarizri"]) == len(df[df["city"] == "Zar1zr1"])

    play = Playbook.from_yaml(fixtures_path / "applymap.yml")
    df = play.read.handle()
    assert df["state"][0].isupper()
    df = play.run(df, write=False)
    assert df["state"][0].islower()

    # unsafe eval raise
    play = Playbook.from_yaml(fixtures_path / "applymap_evil.yml")
    with pytest.raises(NameError):
        play.run()

    # invoke with df argument
    play = Playbook.from_yaml(fixtures_path / "spec.yml")
    df = read_pandas(play.read.uri)
    df = play.run(df)
    assert len(df) == 10000
    assert df.index.name == "city_id"


def test_playbook_json(fixtures_path):
    play = Playbook.from_yaml(fixtures_path / "lobbyregister.yml")
    df = play.read.handle()
    assert len(df) == 17
    assert "registerNumber" in df.columns
