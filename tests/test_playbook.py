from runpandarun import Playbook


def test_playbook(fixtures_path):
    play = Playbook.from_yaml(fixtures_path / "spec.yml")
    df = play.run(write=False)
    assert len(df) == 9999
    assert list(df.columns) == ["value", "state"]
    assert df.index.name == "date"
