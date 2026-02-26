from pathlib import Path

import pytest

from svea_data_manager.frameworks import exceptions
from svea_data_manager.frameworks.package import Package
from svea_data_manager.frameworks.resource import Resource
from svea_data_manager.frameworks.storage import FileStorage


@pytest.mark.parametrize(
    "given_not_a_directory",
    (
        None,
        "",
        "/this/is/not/a/directory",
    ),
)
def test_file_storage_raises_without_an_existing_root_directory(given_not_a_directory):
    # Given a value that is not a directory
    # When creating a FileStorage with this value
    # Then FileStorage raises an exception
    with pytest.raises(exceptions.StorageRootDirectoryDoesNotExistError):
        FileStorage(given_not_a_directory)


def test_file_storage_can_write_file(dir_factory):
    # Given input and output directories
    given_read_dir = dir_factory("read")
    given_write_dir = dir_factory("write")

    # Given a file
    given_file = given_read_dir / "this_is_a_file.txt"
    given_file.write_text("And it has content.")

    # Given a Package with that file
    given_package = Package("package")
    given_package.resources.add(Resource(given_file.parent, given_file.name))

    # Given a FileStorage with a root directory
    given_file_storage = FileStorage(given_write_dir)

    # Given the root directory is empty
    assert not list(given_write_dir.iterdir())

    # When writing the Package to File Storage
    written_files = given_file_storage.write(given_package)

    # Then the file is returned
    assert given_file.name in {file.name for file in written_files}

    # Then the file is written to the root directory
    assert list(given_write_dir.iterdir())

    written_file = given_write_dir / given_file.name
    assert written_file.exists()
    assert written_file.read_text() == given_file.read_text()


def test_file_storage_does_not_overwrite_existing_file(dir_factory):
    # Given input and output directories
    given_read_dir = dir_factory("read")
    given_write_dir = dir_factory("write")

    # Given two files with the same name but different content in the two directories
    given_file_name = "same_name.txt"

    given_input_file = given_read_dir / given_file_name
    given_input_file.write_text("This is not the same...")

    given_output_file = given_write_dir / given_file_name
    given_output_file.write_text("...as this.")

    # Given a Package with the file
    given_package = Package("package")
    given_package.resources.add(Resource(given_input_file.parent, given_input_file.name))

    # Given a FileStorage with a root directory
    given_file_storage = FileStorage(given_write_dir)

    # When writing the Package to File Storage
    written_files = given_file_storage.write(given_package)

    # Then the file is not returned
    assert given_input_file.name not in {file.name for file in written_files}

    # Then the file in the root directory is not overwritten
    assert given_input_file.read_text() != given_output_file.read_text()


def test_file_storage_creates_intermediate_directories(dir_factory):
    # Given input and output directories
    given_read_dir = dir_factory("read")
    given_write_dir = dir_factory("write")

    # Given a file in subdirectories
    given_file_name = "this_is_a_file.txt"
    given_subdirectories = Path("this/is/a/tree/of/directories")
    given_file = given_read_dir / given_subdirectories / given_file_name
    given_file.parent.mkdir(parents=True)
    given_file.write_text("This is content.")

    # Given a Package with that file
    given_package = Package("package")
    given_package.resources.add(
        Resource(given_read_dir, given_file.relative_to(given_read_dir))
    )

    # Given a FileStorage with a root directory
    given_file_storage = FileStorage(given_write_dir)

    # Given the root directory is empty
    assert not list(given_write_dir.iterdir())

    # When writing the Package to File Storage
    written_files = given_file_storage.write(given_package)

    # Then the file is returned, including the intermediate directories
    assert given_subdirectories / given_file_name in written_files

    # Then the file is written to the root directory
    assert list(given_write_dir.iterdir())

    written_file = given_write_dir / given_subdirectories / given_file.name
    assert written_file.exists()
    assert written_file.read_text() == given_file.read_text()


class ResourceWithoutTargetPath(Resource):
    @property
    def target_path(self):
        return None


def test_file_storage_skips_resource_without_target_path(dir_factory):
    # Given input and output directories
    given_read_dir = dir_factory("read")
    given_write_dir = dir_factory("write")

    # Given a file
    given_file = given_read_dir / "this_is_a_file.txt"
    given_file.write_text("And it has content.")

    # Given a Package with that file but using a Resource without a target path
    given_package = Package("package")
    given_package.resources.add(
        ResourceWithoutTargetPath(given_file.parent, given_file.name)
    )

    # Given a FileStorage with a root directory
    given_file_storage = FileStorage(given_write_dir)

    # Given the root directory is empty
    assert not list(given_write_dir.iterdir())

    # When writing the Package to File Storage
    written_files = given_file_storage.write(given_package)

    # Then no file is returned
    assert not written_files

    # And the root directory is empty
    assert not list(given_write_dir.iterdir())


def test_file_storage_raises_when_force_is_true(tmp_path):
    # Given a FileStorage
    given_file_storage = FileStorage(tmp_path)

    # When calling write with force=True
    # The FileStorage raises an exception
    with pytest.raises(exceptions.ForceNotAllowedError):
        given_file_storage.write(Package("package"), force=True)


def test_file_storage_delete_removes_file(dir_factory):
    # Given output directory
    given_write_dir = dir_factory("write")

    # Given a file in the output directory
    given_file = given_write_dir / "this_is_a_file.txt"
    given_file.write_text("And it has content.")

    # Given a Package with that file
    given_package = Package("package")
    given_package.resources.add(Resource(given_file.parent, given_file.name))

    # When deleting the file from File Storage
    deleted_files = FileStorage(given_write_dir).delete(given_package)

    # Then the file is returned
    assert given_file in deleted_files

    # Then the file is removed
    assert not given_file.exists()


def test_file_storage_delete_nonexistent_file_returns_empty_list(tmp_path):
    # Given a nonexistent file
    given_not_existing_file = tmp_path / "this_file_does_not_exist.txt"
    assert not given_not_existing_file.exists()

    # Given a Package with the nonexisting file
    given_package = Package("package")
    given_package.resources.add(Resource(tmp_path, given_not_existing_file.name))

    # When deleting the file from File Storage
    deleted_files = FileStorage(tmp_path).delete(given_package)

    # Then the delete list is empty
    assert not deleted_files

    # And the file still does not exist
    assert not given_not_existing_file.exists()


# This behavior differs from that of SubversionStorage. Which is better?
@pytest.mark.parametrize(
    "given_number_of_files, given_number_of_directories", ((1, 1), (10, 4), (32, 10))
)
def test_file_storage_returns_files_that_are_written(
    dir_factory, given_number_of_files, given_number_of_directories
):
    # Given input and output directories
    given_read_dir = dir_factory("read")
    given_write_dir = dir_factory("write")

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

    # Given a Package with that file
    given_package = Package("package")
    for file in files:
        given_package.resources.add(
            Resource(given_read_dir, file.relative_to(given_read_dir))
        )

    # Given a FileStorage with a root directory
    given_file_storage = FileStorage(given_write_dir)

    # Given the root directory is empty
    assert not list(given_write_dir.iterdir())

    # When writing the Package to FileStorage
    written_files = given_file_storage.write(given_package)

    # Then the storage returns only the files created
    assert len(written_files) != given_number_of_files + given_number_of_directories
    assert len(written_files) == given_number_of_files
