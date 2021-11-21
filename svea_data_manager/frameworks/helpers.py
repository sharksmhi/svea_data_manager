from pathlib import Path


def verify_path(path):
    path = Path(path)

    if path.is_absolute() or '..' in path.parts:
        raise ValueError(
            'path must not be absolute or '
            'contain any traversal characters.'
        )

    return path
