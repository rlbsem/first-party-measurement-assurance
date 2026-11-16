"""Read-only validation of the exact custom-role permissions in saved plans."""

import json
from pathlib import Path
from .gcp import Cloud, write, utc


def planned_custom_roles(plans):
    roles = {}
    for plan in plans:
        for resource in plan.get("resource_changes", []):
            if resource.get("type") != "google_project_iam_custom_role":
                continue
            permissions = resource["change"]["after"].get("permissions")
            if (
                not isinstance(permissions, list)
                or not permissions
                or not all(isinstance(item, str) and item for item in permissions)
            ):
                raise ValueError("custom_role_permissions_not_known_before_apply")
            roles[resource["address"]] = sorted(set(permissions))
    return roles


def inspect_custom_roles(cloud, roles, project, output):
    """Missing names require exhausted pagination; no writes or role creation."""
    requested = {name for values in roles.values() for name in values}
    observed, token, complete, pages_read = {}, None, False, 0
    for page in range(100):
        if not requested:
            complete = True
            break
        body = dict(
            fullResourceName=f"//cloudresourcemanager.googleapis.com/projects/{project}",
            pageSize=1000,
        )
        if token:
            body["pageToken"] = token
        response = cloud.call(
            "POST",
            "https://iam.googleapis.com/v1/permissions:queryTestablePermissions",
            "custom-role-catalog-" + str(page),
            body,
        )
        pages_read += 1
        for permission in response.get("permissions", []):
            if permission["name"] in requested:
                observed[permission["name"]] = dict(
                    name=permission["name"],
                    custom_roles_support=permission.get(
                        "customRolesSupportLevel", "SUPPORTED"
                    ),
                    stage=permission.get("stage"),
                    api_disabled=permission.get("apiDisabled", False),
                )
        token = response.get("nextPageToken")
        if not token:
            complete = True
            break
        if set(observed) == requested:
            break
    missing = sorted(requested - set(observed))
    unsupported = sorted(
        name
        for name, value in observed.items()
        if value["custom_roles_support"] != "SUPPORTED"
    )
    result = dict(
        schema="streaming-custom-role-catalog.v1",
        observed_at=utc(),
        boundary="native read-only permission metadata, not a grant or cancellation test",
        roles=roles,
        observed=observed,
        missing=missing,
        unsupported=unsupported,
        catalog_exhausted=complete,
        pages_read=pages_read,
        valid=not missing and not unsupported,
        resources_created=False,
    )
    write(output, result)
    if missing and not complete:
        raise ValueError("custom_role_catalog_incomplete")
    if missing or unsupported:
        raise ValueError("custom_role_permission_invalid_or_unsupported")
    return result


def validate_saved_roles(project, plan_paths, work):
    plans = [json.loads(Path(path).read_bytes()) for path in plan_paths]
    roles = planned_custom_roles(plans)
    cloud = Cloud(project, Path(work) / "provider", quota_project=False)
    return inspect_custom_roles(
        cloud, roles, project, Path(work) / "verification.private.json"
    )
