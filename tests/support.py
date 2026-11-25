"""Keep disposable test files inside the project, including in restricted shells."""

from pathlib import Path
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
