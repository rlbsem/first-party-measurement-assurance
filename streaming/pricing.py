"""Read-only published CAD SKU discovery; selection is explicit and reproducibly bound."""

from decimal import Decimal
from .gcp import utc


def catalog(cloud, service_name="Dataflow"):
    services = []
    token = None
    for page in range(20):
        url = "https://cloudbilling.googleapis.com/v1/services?pageSize=500"
        if token:
            url += "&pageToken=" + token
        response = cloud.call("GET", url, "catalog-services-" + str(page))
        services.extend(response.get("services", []))
        token = response.get("nextPageToken")
        if not token:
            break
    else:
        raise ValueError("service_catalog_incomplete")
    matches = [
        s
        for s in services
        if service_name.casefold() in s.get("displayName", "").casefold()
    ]
    if len(matches) != 1:
        raise ValueError("catalog_service_not_unique")
    skus = []
    token = None
    for page in range(30):
        url = (
            "https://cloudbilling.googleapis.com/v1/"
            + matches[0]["name"]
            + "/skus?currencyCode=CAD&pageSize=5000"
        )
        if token:
            url += "&pageToken=" + token
        response = cloud.call("GET", url, "catalog-skus-" + str(page))
        skus.extend(response.get("skus", []))
        token = response.get("nextPageToken")
        if not token:
            return dict(
                observed_at=utc(), currency="CAD", service=matches[0]["name"], skus=skus
            )
    raise ValueError("sku_catalog_incomplete")


def selected_rates(observation, selection):
    required = {"vcpu_hour", "memory_gib_hour", "streaming_engine_unit"}
    if set(selection) != required:
        raise ValueError("all_compute_components_required")
    rates = dict(currency="CAD", verified_at=observation["observed_at"], skus={})
    for key, identifier in selection.items():
        matches = [
            s
            for s in observation["skus"]
            if s["skuId"] == identifier
            and (
                "us-central1" in s.get("serviceRegions", [])
                or s.get("serviceRegions") == ["global"]
            )
            and s.get("category", {}).get("usageType") == "OnDemand"
        ]
        if len(matches) != 1:
            raise ValueError("unverified_or_nonregional_rate")
        sku = matches[0]
        pricing = sku["pricingInfo"][-1]["pricingExpression"]
        tiers = pricing["tieredRates"]
        if len(tiers) != 1:
            raise ValueError("tiered_rate_requires_explicit_cost_model")
        price = tiers[0]["unitPrice"]
        if price["currencyCode"] != "CAD":
            raise ValueError("currency_mismatch")
        rates[key] = str(
            Decimal(price.get("units", "0"))
            + Decimal(price.get("nanos", 0)) / Decimal(1_000_000_000)
        )
        rates["skus"][key] = dict(
            sku_id=identifier, description=sku["description"], unit=pricing["usageUnit"]
        )
    return rates


def exposure(rates, max_workers=4, hours=4, streaming_units_per_hour=200):
    if not 1 <= max_workers <= 4 or not 0 < hours <= 4:
        raise ValueError("bounded_resource_model_required")
    worker = (
        Decimal(max_workers)
        * Decimal(hours)
        * (
            2 * Decimal(rates["vcpu_hour"])
            + Decimal("7.5") * Decimal(rates["memory_gib_hour"])
        )
    )
    streaming = (
        Decimal(hours)
        * Decimal(streaming_units_per_hour)
        * Decimal(rates["streaming_engine_unit"])
    )
    # These explicitly conservative reservations must be replaced/refined with
    # regional service SKUs in the private deployment review, not called charges.
    components = dict(
        worker_cad=str(worker),
        streaming_engine_cad=str(streaming),
        query_reserve_cad="140",
        storage_retention_reserve_cad="25",
        pubsub_network_logs_build_reserve_cad="25",
    )
    total = sum(Decimal(v) for v in components.values())
    if total > 300:
        raise ValueError("priced_exposure_exceeds_planning_envelope")
    return dict(
        components=components,
        predicted_cad=str(total),
        streaming_units_assumption=streaming_units_per_hour,
        provider_hard_cap=False,
        actual_charge_cad=None,
    )
