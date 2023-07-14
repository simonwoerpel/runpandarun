import pytest

from runpandarun import Playbook, read_pandas


def test_playbook(fixtures_path):
    play = Playbook.from_yaml(fixtures_path / "spec.yml")
    df = play.run(write=False)
    assert len(df) == 9999
    assert list(df.columns) == ["value", "state"]
    assert df.index.name == "date"

    play = Playbook.from_yaml(fixtures_path / "applymap.yml")
    df = play.run(write=False)
    assert df["state"][0] == "or"

    # unsafe eval raise
    play.ops = [{"applymap": {"func": "__import__('os').system('rm -rf /tmp/foobar')"}}]
    with pytest.raises(NameError):
        play.run()

    # invoke with df argument
    play = Playbook.from_yaml(fixtures_path / "spec.yml")
    df = read_pandas(play.in_uri)
    df = play.run(df)
    assert len(df) == 10000
    assert list(df.columns) == ["value", "state"]
    assert df.index.name == "date"
