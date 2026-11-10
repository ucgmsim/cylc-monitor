"""Give Datasette 1.0a41 back the `permission_allowed` that datasette-dashboards 0.8.0 calls.

Datasette 1.0a20 replaced `datasette.permission_allowed(actor, action, resource, default)`
with `datasette.allowed(action=, resource=, actor=)`. datasette-dashboards 0.8.0 (its latest)
still calls the old one, for `view-instance` and for `execute-sql` on a database, so it fails
with an AttributeError. This maps those calls onto the new API, and does nothing on a
Datasette that still has the old one. Drop it once datasette-dashboards supports 1.0.
"""

from datasette.app import Datasette
from datasette.database import Database
from datasette.resources import DatabaseResource


async def permission_allowed(
    self: Datasette,
    actor: dict | None,
    action: str,
    resource: Database | str | None = None,
    default: bool | None = None,
) -> bool:
    """Answer the old permission check with the new API.

    Parameters
    ----------
    self : Datasette
        The Datasette instance.
    actor : dict or None
        The actor asking.
    action : str
        The action, e.g. view-instance or execute-sql.
    resource : Database or str or None, optional
        The database the action is on, if any.
    default : bool or None, optional
        Ignored: the new API applies Datasette's own defaults.

    Returns
    -------
    bool
        Whether the actor may.
    """
    if isinstance(resource, Database):
        resource = resource.name
    if isinstance(resource, str):
        return await self.allowed(action=action, resource=DatabaseResource(database=resource), actor=actor)
    return await self.allowed(action=action, actor=actor)


if not hasattr(Datasette, "permission_allowed"):
    setattr(Datasette, "permission_allowed", permission_allowed)  # noqa: B010 - a new attribute
