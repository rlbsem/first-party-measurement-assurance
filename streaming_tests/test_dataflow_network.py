"""Offline provider-response regression: no GCP resources or API calls."""

from copy import deepcopy

import pytest

from streaming.dataflow_network import dataflow_subnetwork


OWNED = {
    "project": "example-project",
    "region": "us-central1",
    "run": "sm0123456789",
    "subnetwork": "projects/example-project/regions/us-central1/subnetworks/sm0123456789",
}
EXPECTED = "regions/us-central1/subnetworks/sm0123456789"


@pytest.mark.parametrize(
    "source",
    [
        "projects/example-project/regions/us-central1/subnetworks/sm0123456789",
        "regions/us-central1/subnetworks/sm0123456789",
    ],
)
def test_owned_terraform_id_or_regional_shorthand_becomes_exact_dataflow_value(source):
    deployment = {**OWNED, "subnetwork": source}
    assert dataflow_subnetwork(deployment) == EXPECTED
    assert deployment["subnetwork"] == source  # preserve provider inventory truth


@pytest.mark.parametrize(
    "invalid",
    [
        "projects/other-project/regions/us-central1/subnetworks/sm0123456789",
        "projects/example-project/regions/us-east1/subnetworks/sm0123456789",
        "projects/example-project/regions/us-central1/subnetworks/other-run",
        "projects/example-project/regions/us-central1/subnetworks/sm0123456789/extra",
        "https://www.googleapis.com/compute/v1/projects/example-project/regions/us-central1/subnetworks/sm0123456789",
        "regions/us-central1/subnetworks/other-run",
        "regions/us-east1/subnetworks/sm0123456789",
        "",
    ],
)
def test_foreign_mismatched_or_unsupported_subnetwork_fails_closed(invalid):
    with pytest.raises(ValueError, match="owned_resource_mismatch"):
        dataflow_subnetwork({**OWNED, "subnetwork": invalid})


def test_missing_reviewed_manifest_field_fails_closed():
    deployment = deepcopy(OWNED)
    del deployment["project"]
    with pytest.raises(KeyError):
        dataflow_subnetwork(deployment)


def test_incomplete_reviewed_project_scope_fails_closed():
    with pytest.raises(ValueError, match="scope_incomplete"):
        dataflow_subnetwork({**OWNED, "project": ""})
