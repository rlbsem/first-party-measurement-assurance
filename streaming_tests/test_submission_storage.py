import pytest
from google.api_core.exceptions import PreconditionFailed
from streaming.submissions import GCSStore
from streaming.contracts import canonical


def test_generation_cas_uses_provider_preconditions_and_recovers_lost_ack():
    class Blob:
        generation = None
        committed = None

        def upload_from_string(self, data, **kw):
            assert kw == dict(if_generation_match=7, timeout=20, retry=None)
            self.committed, self.generation = data, 8
            raise ConnectionError("lost ack after commit")

        def download_as_bytes(self, **kw):
            assert kw == dict(if_generation_match=8, timeout=20, retry=None)
            return self.committed

    class Bucket:
        blob_value = Blob()

        def blob(self, key):
            return self.blob_value

    store = object.__new__(GCSStore)
    store.bucket, store.key = Bucket(), "private/control/submissions.json"
    value = dict(state="POSSIBLE")
    store.load = lambda: (8, value)
    assert store.cas(7, value)
    assert store.bucket.blob_value.committed == canonical(value).encode()


def test_cas_conflict_and_uncertain_mismatched_readback_never_succeed():
    class Blob:
        generation = None
        error = PreconditionFailed("race")

        def upload_from_string(self, *a, **kw):
            raise self.error

    class Bucket:
        value = Blob()

        def blob(self, key):
            return self.value

    store = object.__new__(GCSStore)
    store.bucket, store.key = Bucket(), "owned"
    assert store.cas(1, dict(state="POSSIBLE")) is False
    store.bucket.value.error = ConnectionError("unknown commit")
    store.load = lambda: (2, dict(state="different"))
    with pytest.raises(ConnectionError):
        store.cas(1, dict(state="POSSIBLE"))
