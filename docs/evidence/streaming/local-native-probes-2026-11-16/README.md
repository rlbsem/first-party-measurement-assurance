# Native acceptance probe contracts

180 streaming tests passed locally after adding the missing native journal and stopper probes. Actual pinned Storage Write request serialization, one-request/no-retry behavior, expected timestamp versus unrelated errors, fresh-fence CAS/readback and final cancellation proof requirements execute without credentials. See [exact source hashes](verification.json).

The application and Beam graph implementations are unchanged from the [previous verified snapshot](../local-safety-replay-2026-11-16/README.md); its executed 195 application tests, three dbt builds, 16 browser checks and graph record are referenced as prior evidence, not described as freshly rerun here. Exact-head GitHub CI separately reruns the full checks.

The new native tests are not yet executed. A separately approved campaign must exercise real scoped principals, a genuine Storage Write rejection, independent GCS evidence/readback, source reconciliation, and final native cancellation. The [safety review](../../../streaming-safety-review.md) explains the distinct Python SDK and managed Beam connector boundaries. No native success or new financial result is claimed. All earlier records remain unchanged.
