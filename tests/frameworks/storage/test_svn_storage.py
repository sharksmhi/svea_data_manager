import subprocess
from pathlib import PurePosixPath

import pytest

from svea_data_manager.frameworks.package import Package
from svea_data_manager.frameworks.resource import Resource
from svea_data_manager.frameworks.storage import SubversionStorage


def test_svn_storage_can_write_file(svn_repo, dir_factory):
    # Given a source file
    given_file = dir_factory("read") / "data.txt"
    given_file.write_text("content")

    # Given a Package with that file
    given_package = Package("package")
    given_package.resources.add(Resource(given_file.parent, given_file.name))

    # Given the repo is empty
    assert not SubversionStorage(svn_repo)._get_versioned_paths()

    # When writing the package
    stored_files = SubversionStorage(svn_repo).write(given_package)

    # Then the file is returned
    assert given_file.name in {file.name for file in stored_files}

    # Then the file exists in the repo
    assert SubversionStorage(svn_repo)._get_versioned_paths()


def test_subversion_storage_will_overwrite_existing_file_when_using_flag(
    svn_repo, dir_factory
):
    # Given input and output directories
    given_read_dir = dir_factory("read")

    # Given two files with the same name but different content in the two directories
    given_file_name = "file_name.txt"

    given_input_file = given_read_dir / given_file_name
    initial_file_content = "Initial file content"
    given_input_file.write_text(initial_file_content)

    # Given a Package with the file
    given_package = Package("package")
    given_package.resources.add(Resource(given_input_file.parent, given_input_file.name))

    # Given the file has been written to the repo
    SubversionStorage(svn_repo).write(given_package)

    # Given the file is changed in the read dit
    new_file_content = "New file content"
    given_input_file.write_text(new_file_content)

    # When writing the updated file to the repo with the force flag
    stored_files = SubversionStorage(svn_repo).write(given_package, force=True)

    # Then the file is returned
    assert given_file_name in {file.name for file in stored_files}

    # And the content is overwritten
    result = subprocess.run(
        ["svn", "cat", f"{svn_repo}/{given_file_name}"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout != initial_file_content
    assert result.stdout == new_file_content


def test_subversion_storage_will_not_overwrite_existing_file_when_not_using_flag(
    svn_repo, dir_factory
):
    # Given input and output directories
    given_read_dir = dir_factory("read")

    # Given two files with the same name but different content in the two directories
    given_file_name = "file_name.txt"

    given_input_file = given_read_dir / given_file_name
    initial_file_content = "Initial file content"
    given_input_file.write_text(initial_file_content)

    # Given a Package with the file
    given_package = Package("package")
    given_package.resources.add(Resource(given_input_file.parent, given_input_file.name))

    # Given the file has been written to the repo
    SubversionStorage(svn_repo).write(given_package)

    # Given the file is changed in the read dit
    new_file_content = "New file content"
    given_input_file.write_text(new_file_content)

    # When writing the updated file to the repo without the force flag
    stored_files = SubversionStorage(svn_repo).write(given_package)

    # Then the file is returned
    assert given_file_name not in {file.name for file in stored_files}

    # And the content is not overwritten
    result = subprocess.run(
        ["svn", "cat", f"{svn_repo}/{given_file_name}"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout == initial_file_content
    assert result.stdout != new_file_content


def test_subversion_storage_delete_removes_file(svn_repo, dir_factory):
    # Given output directory
    given_write_dir = dir_factory("write")

    # Given a file in the output directory
    given_file_name = "this_is_a_file.txt"
    given_file = given_write_dir / given_file_name
    given_file.write_text("And it has content.")

    # Given a Package with that file
    given_package = Package("package")
    given_package.resources.add(Resource(given_file.parent, given_file.name))

    # Given the file has been written to the repo
    SubversionStorage(svn_repo).write(given_package)
    file_as_pure_posix_path = PurePosixPath(given_file_name)
    assert file_as_pure_posix_path in SubversionStorage(svn_repo)._get_versioned_paths()

    # When deleting the file from File Storage
    deleted_files = SubversionStorage(svn_repo).delete(given_package)

    # Then the file is returned
    assert given_file_name in {file.name for file in deleted_files}

    # And the file is removed
    assert (
        file_as_pure_posix_path not in SubversionStorage(svn_repo)._get_versioned_paths()
    )


def test_subversion_storage_delete_nonexistent_file_returns_empty_list(
    svn_repo, tmp_path
):
    # Given a nonexistent file
    given_not_existing_file = tmp_path / "this_file_does_not_exist.txt"
    assert not given_not_existing_file.exists()

    # Given a Package with the nonexisting file
    given_package = Package("package")
    given_package.resources.add(Resource(tmp_path, given_not_existing_file.name))

    # When deleting the file from File Storage
    deleted_files = SubversionStorage(svn_repo).delete(given_package)

    # Then the delete list is empty
    assert not deleted_files

    # And the file still does not exist
    assert not given_not_existing_file.exists()


# This behavior differs from that of FileStorage. Which is better?
@pytest.mark.parametrize(
    "given_number_of_files, given_number_of_directories", ((1, 1), (10, 4), (32, 10))
)
def test_subversion_storage_returns_files_that_are_written(
    svn_repo, dir_factory, given_number_of_files, given_number_of_directories
):
    # Given an input directory
    given_read_dir = dir_factory("read")

    # Given a number of directories
    directories = []
    last_dir = given_read_dir
    for n in range(given_number_of_directories):
        directory_path = last_dir / f"dir_{n:03}"
        directory_path.mkdir(parents=True)
        directories.append(directory_path)
        last_dir = directory_path

    # Given a number of files
    files = []
    for n in range(given_number_of_files):
        directory = directories[n % given_number_of_directories]
        given_file_name = directory / f"file_{n:03}.txt"
        files.append(given_file_name)
        given_file_name.write_text(f"This is content {n}.")

    # Given a Package with those files
    given_package = Package("package")
    for file in files:
        given_package.resources.add(
            Resource(given_read_dir, file.relative_to(given_read_dir))
        )

    # Given a FileStorage with a root directory
    given_file_storage = SubversionStorage(svn_repo)

    # When writing the Package to SubversionStorage
    written_files = given_file_storage.write(given_package)

    # Then the storage returns only the files created
    assert len(written_files) != given_number_of_files + given_number_of_directories
    assert len(written_files) == given_number_of_files
