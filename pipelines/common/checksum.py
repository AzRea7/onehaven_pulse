from hashlib import sha256
from pathlib import Path


def calculate_sha256(file_path: Path) -> str:
    hash_object = sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hash_object.update(chunk)

    return hash_object.hexdigest()
