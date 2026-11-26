"""Keep disposable test files inside the project, including in restricted shells."""

from pathlib import Path
import hashlib
import json
import secrets
import shutil
import uuid


def test_directory(case) -> Path:
    root = Path(__file__).resolve().parent / ".test-data"
    root.mkdir(exist_ok=True)
    path = root / uuid.uuid4().hex
    path.mkdir()

    def cleanup():
        # Verify the absolute target before recursively removing test-owned data.
        if path.is_symlink() or path.resolve().parent != root.resolve():
            raise RuntimeError("Refusing to remove a directory outside test data")
        shutil.rmtree(path)

    case.addCleanup(cleanup)
    return path


def write_credentials(path: Path, user_ids) -> dict[str, str]:
    """Create test-only secrets; only verifiers enter the registry on disk."""
    tokens = {user_id: secrets.token_urlsafe(32) for user_id in user_ids}
    payload = {"version": 1, "users": [
        {"user_id": user_id, "token_sha256": hashlib.sha256(token.encode()).hexdigest()}
        for user_id, token in tokens.items()]}
    path.write_text(json.dumps(payload), encoding="utf-8")
    return tokens
