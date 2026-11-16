import json
import pytest
from streaming.submissions import Registry, SQLiteStore


@pytest.mark.parametrize("lost_ack", [False, True])
def test_driver_fences_before_provider_run_and_binds_before_success(
    tmp_path, monkeypatch, capsys, lost_ack
):
    from streaming import beam_pipeline

    scope = dict(project="example-project", region="us-central1", run="sm0123456789")
    registry = Registry(SQLiteStore(tmp_path / "submission.sqlite"), scope)
    registry.initialize()
    name = scope["run"] + "-canary"
    registry.prepare(name)
    config = tmp_path / "config.json"
    config.write_text(json.dumps(dict(submission_scope=scope, submission_name=name)))

    class Result:
        def job_id(self):
            return "real-shaped-test-id"

    class Pipeline:
        def run(self):
            assert registry.read()[1]["attempts"][name]["state"] == "POSSIBLE"
            if lost_ack:
                raise ConnectionError("controlled unknown submit outcome")
            return Result()

    monkeypatch.setattr("sys.argv", ["driver", "--config", str(config)])
    monkeypatch.setattr(beam_pipeline, "build_pipeline", lambda *a, **kw: Pipeline())
    monkeypatch.setattr("streaming.submissions.registry_for", lambda *a: registry)
    if lost_ack:
        with pytest.raises(ConnectionError):
            beam_pipeline.main()
        registry.seal()
        assert registry.read()[1]["attempts"][name]["state"] == "POSSIBLE"
        assert not capsys.readouterr().out
    else:
        beam_pipeline.main()
        assert registry.read()[1]["attempts"][name]["job_id"] == "real-shaped-test-id"
        assert json.loads(capsys.readouterr().out)["submitted"]
