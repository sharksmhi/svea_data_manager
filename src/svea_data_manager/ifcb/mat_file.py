import pathlib
import os
import logging
import scipy

logger = logging


class ClassifierMatFile:

    def __init__(self, file_path):
        self._path = pathlib.Path(file_path)
        if not self._path.suffix == '.mat':
            msg = f'{self._path} is not a mat-file'
            logger.error(msg)
            raise Exception(msg)
        self._lat = None
        self._lon = None

        self._save_info()

    @property
    def path(self):
        return self._path

    @property
    def classifier_name(self):
        return self._classifier_name.split('.')[0]

    def _save_info(self):
        mat = scipy.io.loadmat(self.path, simplify_cells=True)
        self._classifier_name = pathlib.Path(mat['classifierName']).name


def load_individual_mat_files(self, directory):
    """Individual mat-files are result from the classification"""
    classifier_files = {}
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            path = pathlib.Path(root, name)
            if path.suffix != '.mat':
                continue
            if path.name[0] != 'D':
                continue
            mat = scipy.io.loadmat(path, simplify_cells=True)
            classifier_files[path.name.split('_')[0]] = pathlib.Path(mat['classifierName']).name