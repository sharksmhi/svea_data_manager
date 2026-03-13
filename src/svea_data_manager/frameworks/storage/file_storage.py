import os
import shutil
from pathlib import Path

from svea_data_manager.frameworks import exceptions
from svea_data_manager.frameworks.package import Package
from svea_data_manager.frameworks.storage.base_storage import BaseStorage, logger
from svea_data_manager.sdm_event import post_event


class FileStorage(BaseStorage):
    def __init__(self, root_directory: str | Path):
        if root_directory:
            root_directory = Path(root_directory).resolve()

        if not root_directory or not root_directory.is_dir():
            msg = (
                f"root_directory must be an existing, writeable directory: "
                f"{root_directory}"
            )
            logger.error(msg)
            raise exceptions.StorageRootDirectoryDoesNotExistError(msg)
        self._root_directory = root_directory

    def __str__(self):
        return f"{self.__class__.__name__}({self._root_directory})"

    def _write(self, package: Package, force=False) -> list[Path]:
        if force:
            msg = "Not allowed to force writing to File Storage"
            logger.error(msg)
            raise exceptions.ForceNotAllowedError(msg)
        # list with tuples of (source_path, target_path, instrument, key).
        files_to_copy = []

        # first iteration: extract files to copy and check for existence.
        for resource in package.resources:
            instrument = package.instrument
            key = str(package)
            absolute_source_path = resource.absolute_source_path
            if resource.target_path is None:
                msg = (
                    f"Will not write file. "
                    f"No target path given for file: {resource.absolute_source_path}"
                )
                logger.info(msg)
                post_event(
                    "on_target_path_not_given",
                    dict(instrument=instrument, path=resource.absolute_source_path),
                )
                continue
            absolute_target_path = self._resolve_path(resource.target_path)

            if absolute_target_path.exists():
                msg = (
                    f"Will not write file. "
                    f"Resource with target path {absolute_target_path} already exists."
                )
                logger.warning(msg)
                post_event(
                    "on_target_path_exists",
                    {"instrument": instrument, "path": absolute_target_path},
                )
                continue

            files_to_copy.append(
                (absolute_source_path, absolute_target_path, instrument, key)
            )

        # second iteration: write extracted files to target.
        copied_files = []
        nr_files_to_copy = len(files_to_copy)
        for nr, (source_path, target_path, inst, key) in enumerate(files_to_copy):
            os.makedirs(target_path.parent, exist_ok=True)
            copied_file = shutil.copyfile(source_path, target_path)
            copied_files.append(Path(copied_file).relative_to(self._root_directory))
            post_event(
                "on_progress",
                dict(
                    instrument=inst,
                    msg=f"Copying files from package {key} to file storage...",
                    percentage=int((nr + 1) / nr_files_to_copy * 100),
                ),
            )
            post_event(
                "on_file_copied",
                dict(
                    instrument=inst,
                    msg="Copying files to file storage...",
                    source_path=source_path,
                    target_path=target_path,
                    nr_files_total=nr_files_to_copy,
                    nr_files_copied=nr + 1,
                ),
            )

        return copied_files

    def _delete(self, package):
        # TODO: Clean up left-overs: empty parent directories.
        removed_files = []
        for resource in package.resources:
            absolute_target_path = self._resolve_path(resource.target_path)

            if absolute_target_path.is_file():
                os.remove(absolute_target_path)
                removed_files.append(absolute_target_path)

        return removed_files

    def _resolve_path(self, path):
        return self._root_directory.joinpath(path)
