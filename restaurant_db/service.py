"""The network trust boundary: public commands never supply their own identity."""

from typing import Any

from .auth import Principal
from .engine import Database
from .errors import AuthorizationError, ValidationError


class CommandService:
    def __init__(self, database: Database):
        self._database = database

    def execute(self, principal: Principal, request_id: str, command: Any) -> dict[str, Any]:
        if not isinstance(command, dict):
            raise ValidationError("command must be an object")
        if command.get("op") not in ("reserve", "cancel"):
            raise AuthorizationError("This operation is available only through the local CLI")
        if "user_id" in command:
            raise ValidationError("user_id is determined by the token; omit it from the request")
        internal = {**command, "user_id": principal.user_id}
        return self._database.execute_user(principal.user_id, request_id, internal)
