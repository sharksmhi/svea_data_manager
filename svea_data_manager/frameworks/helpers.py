from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)

TEMP_DIR = Path(Path(__file__).parent.parent, '_sdm_temp_dir')


def check_path(path):
    path = Path(path)

    if path.is_absolute() or '..' in path.parts:
        msg = 'path must not be absolute or contain any traversal characters.'
        logger.error(msg)
        raise ValueError(msg)

    return path


def get_temp_dir_path():
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    return TEMP_DIR


def clear_temp_dir():
    if not TEMP_DIR.exists():
        return
    logger.info(f'Clearing temp directory: {TEMP_DIR}')
    for path in TEMP_DIR.iterdir():
        os.remove(str(path))

