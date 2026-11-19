"""Place names -> taxi zones (the agent never guesses zone ids)."""

from typing import Any

from .. import places
from ..audit import audited


@audited
def resolve_place(name: str) -> dict[str, Any]:
    """Turn a place name ("Midtown", "JFK", "Williamsburg", "Brooklyn") into
    official NYC taxi zones. Always use this instead of guessing zone ids."""
    return places.resolve(name)
