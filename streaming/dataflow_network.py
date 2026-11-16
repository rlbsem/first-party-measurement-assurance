"""Validate the Dataflow subnetwork CLI contract against the owned Terraform ID.

The Compute provider exports projects/PROJECT/regions/REGION/subnetworks/NAME.
For this single-project private VPC, Dataflow accepts the regional shorthand
regions/REGION/subnetworks/NAME, not the project-prefixed provider ID.
The name must remain exactly the campaign's reviewed, owned network name.
"""


def dataflow_subnetwork(deployment):
    project = deployment["project"]
    region = deployment["region"]
    run = deployment["run"]
    observed = deployment["subnetwork"]
    if not all(isinstance(value, str) and value for value in (project, region, run)):
        raise ValueError("dataflow_subnetwork_scope_incomplete")

    provider_id = f"projects/{project}/regions/{region}/subnetworks/{run}"
    regional_id = f"regions/{region}/subnetworks/{run}"
    if observed not in (provider_id, regional_id):
        raise ValueError("dataflow_subnetwork_owned_resource_mismatch")
    return regional_id
