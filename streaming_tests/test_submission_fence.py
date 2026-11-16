import json
from concurrent.futures import ThreadPoolExecutor
import pytest
from streaming.submissions import Registry, SQLiteStore
from streaming.cleanup import resolve_submissions, stop_owned_jobs
from streaming.gcp import ProviderFailure


def make(tmp_path):
    m = dict(
        project="example-project",
        region="us-central1",
        run="sm0123456789",
        bucket="example-project-sm0123456789",
        labels={"acceptance": "sm0123456789"},
        created_at="2026-11-14T12:00:00Z",
        deadline="2026-11-14T15:00:00Z",
        submission_protocol="submission-fence.v1",
    )
    r = Registry(SQLiteStore(tmp_path / "registry.sqlite"), m)
    r.initialize()
    return m, r


def detail(m, state="JOB_STATE_PENDING"):
    return dict(
        id="provider-job",
        name=m["run"] + "-canary",
        projectId=m["project"],
        location=m["region"],
        labels=m["labels"],
        currentState=state,
    )


def test_empty_list_cannot_clear_possible_or_confirmed_submission(tmp_path):
    m, r = make(tmp_path)
    name = m["run"] + "-canary"
    r.prepare(name)
    r.dispatch(name)

    class Reader:
        credentials = None

        def call(self, method, url, label):
            if "inventory-jobs-" in label:
                return {"jobs": []}
            return detail(m)

    for _ in range(3):
        v = resolve_submissions(Reader(), m, registry=r, seal=True, persist=True)
        assert v["unresolved"] == [name] and not v["cleanup_allowed"]
    r.bind(name, "provider-job")
    v = resolve_submissions(Reader(), m, registry=r, persist=True)
    assert len(v["jobs"]) == 1 and not v["unresolved"] and not v["cleanup_allowed"]


def test_delayed_visibility_binds_exact_id_and_terminal_proof(tmp_path):
    m, r = make(tmp_path)
    name = m["run"] + "-canary"
    r.prepare(name)
    r.dispatch(name)

    class Reader:
        calls = 0
        state = "JOB_STATE_PENDING"
        credentials = None

        def call(self, method, url, label):
            if "inventory-jobs-" in label:
                self.calls += 1
                return {
                    "jobs": []
                    if self.calls < 3
                    else [{k: detail(m)[k] for k in ["id", "name"]}]
                }
            return detail(m, self.state)

    c = Reader()
    for _ in range(2):
        assert resolve_submissions(c, m, registry=r, persist=True)["unresolved"]
    assert not resolve_submissions(c, m, registry=r, persist=True)["unresolved"]
    r.seal()
    c.state = "JOB_STATE_FAILED"
    assert resolve_submissions(c, m, registry=r, persist=True)["cleanup_allowed"]
    c.state = "JOB_STATE_RUNNING"
    with pytest.raises(ValueError, match="terminal_provider"):
        resolve_submissions(c, m, registry=r)


def test_exact_id_404_is_not_authoritative_absence(tmp_path):
    m, r = make(tmp_path)
    name = m["run"] + "-canary"
    r.prepare(name)
    r.dispatch(name)
    r.bind(name, "provider-job")
    r.seal()

    class Reader:
        credentials = None

        def call(self, method, url, label):
            if "inventory-jobs-" in label:
                return {}
            raise ProviderFailure(label, 404, "NOT_FOUND")

    for _ in range(5):
        assert not resolve_submissions(Reader(), m, registry=r)["cleanup_allowed"]
    with pytest.raises(ValueError, match="conflicting_provider"):
        r.bind(name, "different-job")


def test_fence_dispatch_race_and_restart_are_durable(tmp_path):
    m, r = make(tmp_path)
    name = m["run"] + "-canary"
    r.prepare(name)

    def dispatch():
        try:
            r.dispatch(name)
            return True
        except ValueError:
            return False

    with ThreadPoolExecutor(2) as p:
        a = p.submit(dispatch)
        b = p.submit(r.seal)
        did_dispatch = a.result()
        b.result()
    restarted = Registry(SQLiteStore(tmp_path / "registry.sqlite"), m)
    v = restarted.read()[1]
    assert v["phase"] == "SEALED"
    assert v["attempts"][name]["state"] == ("POSSIBLE" if did_dispatch else "ABSENT")
    with pytest.raises(ValueError):
        restarted.dispatch(name)


def test_registry_corruption_never_becomes_empty_success(tmp_path):
    m, r = make(tmp_path)
    g, v = r.store.load()
    v["phase"] = "SEALED"
    r.store.cas(g, v)
    with pytest.raises(ValueError, match="corrupt"):
        r.read()


def test_unresolved_submission_exhaustion_blocks_retirement(tmp_path, monkeypatch):
    from types import SimpleNamespace

    m, r = make(tmp_path)
    name = m["run"] + "-canary"
    r.prepare(name)
    r.dispatch(name)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(m))

    class Reader:
        credentials = None

        def __init__(self, *a, **kw):
            pass

        def call(self, method, url, label):
            assert method == "GET"
            return {}

    clock = SimpleNamespace(now=0)
    monkeypatch.setattr("streaming.cleanup.Cloud", Reader)
    monkeypatch.setattr("streaming.submissions.registry_for", lambda *a: r)
    monkeypatch.setattr("streaming.cleanup.time.monotonic", lambda: clock.now)
    monkeypatch.setattr(
        "streaming.cleanup.time.sleep", lambda n: setattr(clock, "now", clock.now + n)
    )
    with pytest.raises(RuntimeError, match="not_terminal"):
        stop_owned_jobs(manifest, tmp_path / "stop")
    result = json.loads((tmp_path / "stop/guard-result.private.json").read_bytes())
    assert (
        result["unresolved_submissions"] == [name]
        and not result["active_compute_absent"]
    )
    assert clock.now == 600


def test_sealed_before_dispatch_is_authoritative_and_closure_survives_restart(tmp_path):
    from streaming.submissions import registry_for

    m, r = make(tmp_path)
    name = m["run"] + "-canary"
    r.prepare(name)
    r.seal()
    m["submission_closure"] = r.read()[1]
    frozen = registry_for(m)
    assert (
        frozen.read()[1]["attempts"][name]["absence_basis"] == "sealed_before_dispatch"
    )
    with pytest.raises(ValueError):
        frozen.dispatch(name)
    with pytest.raises(ValueError):
        r.bind(name, "provider-job")


def test_terminal_receipt_requires_actual_terminal_state_and_valid_closure(tmp_path):
    from streaming.contracts import digest
    from streaming.submissions import registry_for

    m, r = make(tmp_path)
    name = m["run"] + "-canary"
    r.prepare(name)
    r.dispatch(name)
    r.bind(name, "provider-job")
    with pytest.raises(ValueError, match="terminal_provider_receipt"):
        r.terminal(name, detail(m))
    r.seal()
    r.terminal(name, detail(m, "JOB_STATE_FAILED"))
    m["submission_closure"] = r.read()[1]
    assert registry_for(m).read()[1]["attempts"][name]["state"] == "TERMINAL"
    m["submission_closure"]["attempts"][name]["terminal_state"] = "JOB_STATE_RUNNING"
    m["submission_closure"]["integrity"] = digest(
        {k: v for k, v in m["submission_closure"].items() if k != "integrity"}
    )
    with pytest.raises(ValueError, match="invalid_submission_attempt"):
        registry_for(m)
