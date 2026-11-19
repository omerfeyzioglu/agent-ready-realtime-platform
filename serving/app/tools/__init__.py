"""The tools an agent (or the HTTP API) can call: the only way to the data.
Rules are enforced here, not in a prompt (see common.py); every call is
audited (see ../audit.py). One module per kind of question."""

from .catalog import describe_metric, get_catalog
from .clock import now
from .places import resolve_place
from .shortages import find_shortages
from .zones import get_zone_history, get_zone_now

TOOLS = [
    now,
    get_catalog,
    resolve_place,
    find_shortages,
    get_zone_now,
    get_zone_history,
    describe_metric,
]
