"""Order-stable bounded-memory fingerprint; full rowset equality is checked separately."""

import hashlib


def root(bodies):
    states = {}
    for body in bodies:
        leaf = hashlib.sha256(body.encode()).hexdigest()
        bucket = leaf[:3]
        if bucket not in states:
            states[bucket] = [0, hashlib.sha256()]
        count, h = states[bucket]
        if count:
            h.update(b"\n")
        h.update(leaf.encode())
        states[bucket][0] += 1
    lines = [f"{b}:{n}:{h.hexdigest()}" for b, (n, h) in sorted(states.items())]
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def sql(table, where):
    return f"""WITH leaves AS (
      SELECT body,LOWER(TO_HEX(SHA256(body))) leaf FROM `{table}` WHERE {where}
    ), buckets AS (
      SELECT SUBSTR(leaf,1,3) bucket,COUNT(*) n,
        LOWER(TO_HEX(SHA256(STRING_AGG(leaf,'\\n' ORDER BY body)))) h
      FROM leaves GROUP BY bucket
    ) SELECT LOWER(TO_HEX(SHA256(COALESCE(STRING_AGG(CONCAT(bucket,':',CAST(n AS STRING),':',h),'\\n' ORDER BY bucket),'')))) FROM buckets"""
