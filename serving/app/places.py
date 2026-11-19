"""Place names -> taxi zones. The agent never guesses zone ids: it asks
resolve_place, which answers from the official zone table plus a small
gazetteer of everyday names ("Midtown", "JFK")."""

import functools
import logging

import yaml

from . import config, sources

log = logging.getLogger("places")


@functools.cache
def zones() -> dict[int, dict]:
    """The 263 real pickup zones (Unknown and Outside-NYC excluded, as in the features)."""
    rows = (
        sources.duck()
        .sql(
            f"SELECT zone_id, zone_name, borough FROM read_parquet('{config.STAGING_ROOT}/zones/data.parquet') "
            "WHERE NOT is_unknown AND NOT is_outside_nyc ORDER BY zone_id"
        )
        .fetchall()
    )
    return {z: {"zone_id": z, "zone_name": n, "borough": b} for z, n, b in rows}


@functools.cache
def _aliases() -> dict[str, list[int]]:
    by_name = {v["zone_name"].lower(): k for k, v in zones().items()}
    out = {}
    raw = yaml.safe_load(config.GAZETTEER.read_text())["places"]
    for alias, names in raw.items():
        ids = []
        for n in names:
            if n.lower() in by_name:
                ids.append(by_name[n.lower()])
            else:
                log.warning("gazetteer: %r (for %r) is not a zone name", n, alias)
        out[alias.lower()] = ids
    return out


def resolve(name: str) -> dict:
    q = " ".join(name.lower().split())
    z = zones()
    if q.isdigit() and int(q) in z:
        return {"status": "resolved", "matched_by": "zone_id", "zones": [z[int(q)]]}
    if q in _aliases():
        return {
            "status": "resolved",
            "matched_by": "gazetteer",
            "zones": [z[i] for i in _aliases()[q]],
        }
    exact = [v for v in z.values() if v["zone_name"].lower() == q]
    if exact:
        return {"status": "resolved", "matched_by": "zone_name", "zones": exact}
    borough = [v for v in z.values() if v["borough"].lower() == q]
    if borough:
        return {"status": "resolved", "matched_by": "borough", "zones": borough}
    partial = [v for v in z.values() if q in v["zone_name"].lower()]
    if len(partial) == 1:
        return {"status": "resolved", "matched_by": "partial_name", "zones": partial}
    if partial:
        return {
            "status": "ambiguous",
            "matched_by": "partial_name",
            "zones": partial[:20],
            "note": "Several zones match; ask which one, or pass a zone_id.",
        }
    return {
        "status": "not_found",
        "zones": [],
        "note": "No NYC taxi zone matches this name. Do not guess a zone id.",
    }
