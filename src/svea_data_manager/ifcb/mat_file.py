import logging
import os
import pathlib

import scipy

logger = logging


class ClassifierMatFile:
    def __init__(self, file_path):
        self._path = pathlib.Path(file_path)
        if not self._path.suffix == ".mat":
            msg = f"{self._path} is not a mat-file"
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
        return self.classifier_original_path.name

    @property
    def classifier_original_path(self):
        return self._classifier_original_path

    def _save_info(self):
        mat = scipy.io.loadmat(str(self.path), simplify_cells=True)
        self._classifier_original_path = pathlib.Path(
            mat["classifierName"].split(".")[0] + ".mat"
        )


def load_individual_mat_files(self, directory):
    """Individual mat-files are result from the classification"""
    classifier_files = {}
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            path = pathlib.Path(root, name)
            if path.suffix != ".mat":
                continue
            if path.name[0] != "D":
                continue
            mat = scipy.io.loadmat(path, simplify_cells=True)
            classifier_files[path.name.split("_")[0]] = pathlib.Path(
                mat["classifierName"]
            ).name
