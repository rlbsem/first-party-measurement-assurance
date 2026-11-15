"""Native entry points: no default deployment and no implicit cloud authentication."""

import argparse
import json


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "action",
        choices=[
            "preflight",
            "cleanup",
            "prepare",
            "execute",
            "inventory",
            "export",
            "verify",
        ],
    )
    p.add_argument("--project")
    p.add_argument("--work", default=".local/streaming-preflight")
    p.add_argument("--manifest")
    p.add_argument("--deadline-only", action="store_true")
    p.add_argument("--expected-project")
    p.add_argument("--expected-run")
    p.add_argument("--run")
    p.add_argument("--operator-member")
    p.add_argument("--quotas")
    p.add_argument("--rates")
    p.add_argument("--terraform")
    p.add_argument("--dbt-python")
    p.add_argument("--review")
    p.add_argument("--approved-digest")
    p.add_argument("--record")
    p.add_argument("--output")
    a = p.parse_args()
    from pathlib import Path

    def read(path):
        return json.loads(Path(path).read_bytes())

    if a.action == "preflight":
        from .preflight import inspect

        if not a.project:
            p.error("--project required")
        result = inspect(a.project, a.work)
        print(
            json.dumps(
                dict(
                    read_only=True,
                    compute_started=False,
                    observations={
                        k: v["status"] for k, v in result["observations"].items()
                    },
                    missing_permissions=result["missing_permissions"],
                )
            )
        )
    elif a.action == "cleanup":
        from .cleanup import bounded_guard

        if not a.manifest:
            p.error("--manifest required")
        print(
            json.dumps(
                bounded_guard(
                    a.manifest,
                    a.work,
                    deadline_only=a.deadline_only,
                    expected_project=a.expected_project,
                    expected_run=a.expected_run,
                )
            )
        )

    elif a.action == "prepare":
        from .release import prepare

        if not all(
            [a.project, a.run, a.operator_member, a.quotas, a.rates, a.terraform]
        ):
            p.error("prepare needs project/run/operator-member/quotas/rates/terraform")
        print(
            json.dumps(
                prepare(
                    a.project,
                    a.run,
                    a.operator_member,
                    read(a.quotas),
                    read(a.rates),
                    a.terraform,
                    a.work,
                )
            )
        )
    elif a.action == "execute":
        from .release import execute

        if not all([a.review, a.approved_digest, a.terraform, a.dbt_python]):
            p.error("execute needs review/approved-digest/terraform/dbt-python")
        result = execute(a.review, a.approved_digest, a.terraform, a.dbt_python)
        print(
            json.dumps(
                {
                    k: result[k]
                    for k in ["status", "logical_cleanup_verified", "billing_clearance"]
                }
            )
        )
    elif a.action == "inventory":
        from .retirement import collect

        if not a.manifest:
            p.error("inventory needs manifest")
        result = collect(read(a.manifest), a.work)
        print(
            json.dumps(
                dict(logical_absence=result["logical_absence"], billing_clearance=False)
            )
        )
    elif a.action == "export":
        from .evidence import export

        if not a.record or not a.output:
            p.error("export needs record/output")
        print(json.dumps(dict(sha256=export(a.record, a.output))))
    else:
        from .evidence import validate

        if not a.record:
            p.error("verify needs record")
        validate(read(a.record))
        print(json.dumps(dict(evidence_valid=True)))


if __name__ == "__main__":
    main()
