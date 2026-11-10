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

if not hasattr(Datasette, "permission_allowed"):

    async def permission_allowed(self, actor, action, resource=None, default=None):
        if isinstance(resource, Database):
            resource = resource.name
        if isinstance(resource, str):
            resource = DatabaseResource(database=resource)
        return await self.allowed(action=action, resource=resource, actor=actor)

    Datasette.permission_allowed = permission_allowed
