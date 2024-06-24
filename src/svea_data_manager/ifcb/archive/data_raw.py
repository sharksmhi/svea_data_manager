import pathlib
import shutil

RAW_SUFFIXES = ['.adc', '.hdr', '.roi', '.txt']


class DataRaw:
    def __init__(self, root_directory: pathlib.Path | str):
        self._directory = pathlib.Path(root_directory)
        if self._directory.name != 'data_raw':
            self._directory = pathlib.Path(self._directory, 'data_raw')
        if not self._directory.exists():
            raise NotADirectoryError(self._directory)

    @property
    def directory(self) -> pathlib.Path:
        return self._directory

    def get_paths_for_keys(self, *keys) -> list[tuple[pathlib.Path, pathlib.Path]]:
        paths = []
        for key in keys:
            time, instrument = key.split('_')
            rel_target_parent = pathlib.Path(time[:5], time[:9])
            parent = self.directory / rel_target_parent
            for suffix in RAW_SUFFIXES:
                name = f'{key}{suffix}'
                path = parent / name
                rel_target_path = rel_target_parent / name
                if not path.exists():
                    continue
                paths.append((path, rel_target_path))
        return paths

    def copy_keys_to_data_directory(self, keys: list[str], directory: pathlib.Path | str) -> int:
        nr = 0
        for nr, (source_path, rel_target_path) in enumerate(self.get_paths_for_keys(*keys)):
            target_path = pathlib.Path(directory, rel_target_path)
            if target_path.exists():
                raise FileExistsError(target_path)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)
        return nr + 1

