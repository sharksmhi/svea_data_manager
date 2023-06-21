import datetime
import logging
import os
import pathlib
import shutil
import sys
import zipfile
from pathlib import Path


logger = logging.getLogger(__name__)


if getattr(sys, 'frozen', False):
    ROOT_DIR = pathlib.Path(sys.executable).parent
else:
    ROOT_DIR = pathlib.Path(__file__).parent

# TEMP_DIRECTORY = pathlib.Path(ROOT_DIR, 'sdm_temp')
TEMP_DIRECTORY = pathlib.Path(pathlib.Path.home(), 'sdm_temp')


def get_temp_directory():
    create_temp_directory()
    return TEMP_DIRECTORY


def create_temp_directory():
    TEMP_DIRECTORY.mkdir(parents=True, exist_ok=True)


def clear_temp_dir(days_old=0):
    """ Deletes old files in the temp folder """
    if not TEMP_DIRECTORY.exists():
        return
    now = datetime.datetime.now()
    dt = datetime.timedelta(days=days_old)
    for path in TEMP_DIRECTORY.iterdir():
        unix_time = os.path.getctime(path)
        t = datetime.datetime.fromtimestamp(unix_time)
        if t < now - dt:
            try:
                if path.is_file():
                    os.remove(path)
                else:
                    shutil.rmtree(path)
            except PermissionError:
                pass


def create_zip_file(file_paths, output_path, rel_path):
    with zipfile.ZipFile(output_path, 'w') as zipf:
        for file_path in file_paths:
            if file_path.is_file():
                # Get the relative path of the file
                relative_path = file_path.relative_to(rel_path)
                # Add the file to the zip using the relative path
                zipf.write(file_path, arcname=relative_path)


def check_path(path):
    path = Path(path)

    if path.is_absolute() or '..' in path.parts:
        msg = 'path must not be absolute or contain any traversal characters.'
        logger.error(msg)
        raise ValueError(msg)

    return path

