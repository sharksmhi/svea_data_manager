import pathlib

from ifcb.archive import data_raw
from ifcb.archive import results


def _get_archive_directory(directory: pathlib.Path | str = None,
                           instrument: str = None):
    archive_directory = pathlib.Path(directory, instrument.upper())
    if not archive_directory.exists():
        raise NotADirectoryError(archive_directory)
    return archive_directory


def get_archive_results(
        archive_directory: pathlib.Path | str = None,
        instrument: str = None,
        ):
    assert archive_directory
    assert instrument
    archive_directory = _get_archive_directory(archive_directory, instrument)
    archive = results.ResultArchive(archive_directory)
    return archive.result_names


def pull_result_from_archive(archive_directory: pathlib.Path | str = None,
                             instrument: str = None,
                             key: str = None,
                             target_directory: pathlib.Path | str = None):
    assert archive_directory
    assert instrument
    assert key
    assert target_directory
    archive_directory = _get_archive_directory(archive_directory, instrument)
    archive = results.ResultArchive(archive_directory)
    raw_file_keys = archive.get_raw_file_keys(key)
    result_directory = archive.unpack_result_to_directory(key=key, directory=target_directory)
    raw = data_raw.DataRaw(archive_directory)
    data_directory = result_directory / 'data'
    raw.copy_keys_to_data_directory(keys=raw_file_keys, directory=data_directory)
    return result_directory
