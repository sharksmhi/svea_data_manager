import pathlib
import os
import shutil


class ResultArchive:
    def __init__(self, directory: pathlib.Path | str):
        self._directory = pathlib.Path(directory)
        if self._directory.name != 'results':
            self._directory = pathlib.Path(self._directory, 'results')
        if not self._directory.exists():
            raise NotADirectoryError(self._directory)

    @property
    def directory(self):
        return self._directory

    @property
    def paths_by_key(self):
        paths = {}
        for root, dirs, files in os.walk(self.directory, topdown=False):
            for name in files:
                if not name.startswith('result_'):
                    continue
                path = pathlib.Path(root, name)
                paths.setdefault(path.stem, [])
                paths[path.stem].append(path)
            for name in dirs:
                if not name.starswith('result_'):
                    continue
                path = pathlib.Path(root, name)
                paths.setdefault(path.stem, [])
                paths[path.stem].append(path)
        return paths

    @property
    def result_names(self):
        return sorted(self.paths_by_key)

    @property
    def results(self):
        res = {}
        for key, paths in self.paths_by_key.items():
            res[key] = Result(*paths)
        return res

    def get_result(self, key):
        return self.results.get(key)

    def get_raw_file_keys(self, key):
        return self.results[key].raw_file_keys

    def unpack_result_to_directory(self, key, directory) -> pathlib.Path:
        return self.results[key].unpack_to_directory(directory)


class Result:
    def __init__(self, *paths):
        self._load_path(*paths)

    def _load_path(self, *paths):
        self._files = dict()
        keys = []
        for path in paths:
            keys.append(path.stem)
            self._files[path.suffix] = path

    @property
    def key(self):
        return self._files['.zip'].stem

    @property
    def raw_file_keys(self):
        """Check the txt file that contains a list of raw files included in the result archive"""
        with open(self._files['.txt']) as fid:
            return [line.strip() for line in fid.readlines()]

    def unpack_to_directory(self, directory):
        unpack_dir = pathlib.Path(directory, self.key)
        if unpack_dir.exists():
            raise FileExistsError(unpack_dir)
        shutil.unpack_archive(self._files['.zip'], extract_dir=unpack_dir)
        return unpack_dir


