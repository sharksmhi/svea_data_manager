import datetime
import os
import pathlib
import sys
import shutil
import zipfile

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


def clean_temp_directory():
    """ Deletes old files in the temp folder """
    if not TEMP_DIRECTORY.exists():
        return
    now = datetime.datetime.now()
    dt = datetime.timedelta(days=2)
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
