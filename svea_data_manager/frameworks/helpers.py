from pathlib import Path
import logging


logger = logging.getLogger(__name__)

def check_path(path):
    path = Path(path)

    if path.is_absolute() or '..' in path.parts:
        msg = 'path must not be absolute or contain any traversal characters.'
        logger.error(msg)
        raise ValueError(msg)

    return path
