"""Independent Python rowsets for the existing, unchanged synthetic fixture."""
import json
from collections import Counter, defaultdict

from scripts.verify_ga4_pipeline import expected_state, MODELS


def canonical(rows):
    """A multiset, not a set: duplicates and missing/null fields remain visible."""
    return sorted(json.dumps(dict(row), sort_keys=True, default=str) for row in rows)


def compare(actual, expected):
    if set(actual) != set(expected):
        raise AssertionError("Model inventory differs")
    for name in expected:
        if canonical(actual[name]) != canonical(expected[name]):
            raise AssertionError(f"Complete rowset mismatch: {name}")


def fixture_rows(batch):
    selected, items, daily = expected_state(batch)
    events = []
    item_rows = []
    purchases = []
    sessions = defaultdict(list)
    tx = Counter(r['transaction_id'] or None for r in selected.values()
                 if r['event_name'] == 'purchase')
    for key, source in selected.items():
        event = {k: (int(v) if k in ('arrival_batch', 'source_version', 'revenue_cents')
                     else v or None) for k, v in source.items()}
        events.append(event)
        if event['user_pseudo_id'] and event['session_id']:
            sessions[(event['user_pseudo_id'], event['session_id'])].append(event)
        for src in items[key]:
            item = {k: (int(v) if k in ('source_version', 'item_index', 'price_cents', 'quantity')
                        else v) for k, v in src.items()}
            item.update(event_date=event['event_date'],
                        extended_cents=item['price_cents'] * item['quantity'])
            item_rows.append(item)
        if event['event_name'] != 'purchase':
            continue
        total = sum(int(i['price_cents']) * int(i['quantity']) for i in items[key])
        purchase = {k: event[k] for k in ('event_key', 'event_date', 'arrival_batch',
                     'transaction_id', 'user_pseudo_id', 'session_id', 'revenue_cents')}
        purchase.update(item_rows=len(items[key]), item_value_cents=total,
                        revenue_variance_cents=event['revenue_cents'] - total,
                        transaction_id_finding=int(not event['transaction_id'] or
                                                   tx[event['transaction_id']] > 1),
                        identity_finding=int(not event['user_pseudo_id'] or not event['session_id']),
                        revenue_finding=int(not items[key] or abs(event['revenue_cents'] - total) > 1))
        purchases.append(purchase)
    session_rows = [dict(user_pseudo_id=user, session_id=session,
                         first_event_date=min(r['event_date'] for r in rows),
                         last_event_date=max(r['event_date'] for r in rows), event_count=len(rows),
                         purchase_events=sum(r['event_name'] == 'purchase' for r in rows))
                    for (user, session), rows in sessions.items()]
    fields = dict(events='event_count', purchases='purchase_events', items='purchase_item_rows',
                  revenue='reported_revenue_cents', item_value='item_value_cents',
                  duplicates='transaction_id_findings', variances='revenue_findings',
                  missing_identity='identity_findings')
    daily_rows = [dict(event_date=date, **{fields[k]: v for k, v in row.items()})
                  for date, row in daily.items()]
    return dict(zip(MODELS, (events, item_rows, purchases, session_rows, daily_rows)))
