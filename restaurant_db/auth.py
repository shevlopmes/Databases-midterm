"""Manually issued personal tokens. A server holds an immutable registry snapshot.

Only SHA-256 verifiers are persisted. Issue/revoke changes take effect when the
server reloads the file on restart; this module does not implement login or TTL.
"""

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import secrets
from typing import Any
import uuid

from .errors import AuthenticationError, AuthConfigurationError, ValidationError
from .journal import DirectoryLock, canonical_json, decode_json
from .model import fields, text


# 32 random bytes become 43 unpadded base64url characters. Specify entropy
# explicitly instead of relying on secrets' changeable default.
TOKEN_BYTES = 32
_TOKEN = re.compile(r"[A-Za-z0-9_-]{43}")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_BEARER = re.compile(r"Bearer +([A-Za-z0-9_-]{43})", re.IGNORECASE)


@dataclass(frozen=True)
class Principal:
    user_id: str


class AuthStore:
    def __init__(self, users: dict[str, str]):
        self._users = dict(users)

    @classmethod
    def load(cls, path: str | Path) -> "AuthStore":
        raw = Path(path).read_bytes()  # Missing/unreadable files must fail startup.
        try:
            payload = decode_json(raw)
            fields(payload, {"version", "users"}, "credentials")
            if type(payload["version"]) is not int or payload["version"] != 1:
                raise ValueError("unsupported credentials version")
            if not isinstance(payload["users"], list):
                raise ValueError("users must be a list")
            users: dict[str, str] = {}
            digests: set[str] = set()
            for item in payload["users"]:
                fields(item, {"user_id", "token_sha256"}, "credential")
                user_id = text(item["user_id"], "user_id")
                digest = item["token_sha256"]
                if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
                    raise ValueError("invalid verifier")
                if user_id in users or digest in digests:
                    raise ValueError("duplicate user or verifier")
                users[user_id] = digest
                digests.add(digest)
        except (ValueError, TypeError, UnicodeError, RecursionError, ValidationError) as exc:
            # Do not echo file contents: malformed files might contain secrets.
            raise AuthConfigurationError("Invalid credentials file") from exc
        return cls(users)

    def authenticate(self, token: str) -> Principal:
        if not isinstance(token, str) or not _TOKEN.fullmatch(token):
            raise AuthenticationError("Invalid credentials")
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        for user_id, expected in self._users.items():
            if secrets.compare_digest(digest, expected):
                return Principal(user_id)
        raise AuthenticationError("Invalid credentials")

    def authenticate_headers(self, values: list[str]) -> Principal:
        if len(values) != 1:
            raise AuthenticationError("Invalid credentials")
        match = _BEARER.fullmatch(values[0])
        if match is None:
            raise AuthenticationError("Invalid credentials")
        return self.authenticate(match.group(1))


def _save(path: Path, users: dict[str, str]) -> None:
    payload: dict[str, Any] = {"version": 1, "users": [
        {"user_id": user_id, "token_sha256": digest} for user_id, digest in sorted(users.items())]}
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        # Exclusive creation and a same-directory replace avoid partial JSON.
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(canonical_json(payload) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def issue_token(path: str | Path, user_id: str) -> str:
    """Issue or rotate one user's token. The caller delivers the returned secret."""
    user_id = text(user_id, "user_id")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = DirectoryLock(path.parent, filename=f"{path.name}.lock")
    try:
        users = AuthStore.load(path)._users if path.exists() else {}
        token = secrets.token_urlsafe(TOKEN_BYTES)
        users[user_id] = hashlib.sha256(token.encode("ascii")).hexdigest()
        _save(path, users)
        return token
    finally:
        lock.close()


def revoke_token(path: str | Path, user_id: str) -> None:
    user_id = text(user_id, "user_id")
    path = Path(path)
    # Do not create an empty registry because an operator misspelled its path.
    if not path.is_file():
        raise FileNotFoundError("Credentials file does not exist")
    lock = DirectoryLock(path.parent, filename=f"{path.name}.lock")
    try:
        users = AuthStore.load(path)._users
        if user_id not in users:
            raise ValidationError("Unknown user")
        del users[user_id]
        _save(path, users)
    finally:
        lock.close()
