import os
import pathlib
import shutil
import subprocess
import xml.etree.ElementTree as ET
import logging

from abc import ABC, abstractmethod

from svea_data_manager.frameworks import Package
from svea_data_manager.frameworks import exceptions
from svea_data_manager.sdm_event import post_event

logger = logging.getLogger(__name__)


class Storage(ABC):

    def write(self, package, force=False):
        if not isinstance(package, Package):
            raise TypeError(
                'package must be an instance '
                'of Package, not {}'.format(type(package))
            )
        return self._write(package, force=force)

    def delete(self, package):
        if not isinstance(package, Package):
            raise TypeError(
                'package must be an instance '
                'of Package, not {}'.format(type(package))
            )
        return self._delete(package)

    @abstractmethod
    def _write(self, package, **kwargs):
        pass

    @abstractmethod
    def _delete(self, package):
        pass

    ResourceAlreadyInStorage = exceptions.ResourceAlreadyInStorage


class FileStorage(Storage):

    def __init__(self, root_directory):
        root_directory = pathlib.Path(root_directory).resolve()
        if not root_directory.is_dir():
            msg = f'root_directory must be an existing, writeable directory: {root_directory}'
            logger.error(msg)
            raise ValueError(msg)
        self._root_directory = root_directory

    def _write(self, package, force=False):
        if force:
            msg = 'Not allowed to force writing to File Storage'
            logger.error(msg)
            raise exceptions.ForceNotAllowed(msg)
        # list with tuples of (source_path, target_path).
        files_to_copy = []

        # first iteration: extract files to copy and check for existence.
        for resource in package.resources:
            instrument = package.instrument
            absolute_source_path = resource.absolute_source_path
            absolute_target_path = self._resolve_path(resource.target_path)
            # if absolute_source_path.suffix == '.txt':
            #     print(f'={absolute_source_path=}')
            #     print(f'={absolute_target_path=}')
            #     print(f'={absolute_target_path.exists()=}')

            if not force and absolute_target_path.exists():
                msg = f'Will not write file. Resource with target path {absolute_target_path} already exists.'
                logger.warning(msg)
                post_event('on_target_path_exists', dict(instrument=instrument, path=absolute_target_path))
                continue

            files_to_copy.append(
                (absolute_source_path, absolute_target_path, instrument)
            )

            # if not force and absolute_target_path.exists():
            #     msg = f'resource with target path {resource.target_path} alrelatitudeady exists.'
            #     logger.error(msg)
            #     raise exceptions.ResourceAlreadyInStorage(msg)
            #
            # files_to_copy.append(
            #     (absolute_source_path, absolute_target_path)
            # )

        # second iteration: write extracted files to target.
        copied_files = []
        for source_path, target_path, inst in files_to_copy:
            os.makedirs(target_path.parent, exist_ok=True)
            copied_file = shutil.copyfile(source_path, target_path)
            copied_files.append(copied_file)
            post_event('on_file_copied', dict(instrument=inst,
                                              source_path=source_path,
                                              target_path=target_path))

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


class SubversionStorage(Storage):
    class MissingExecutable(Exception):
        """An required external program could not be found on the system"""
        pass

    class SubversionError(Exception):
        """An error occured when executing the Subversion binary"""
        pass

    def __init__(self, root_url, username=None, password=None):
        self._root_url = root_url
        self._username = username
        self._password = password

        svn_exec = shutil.which('svn')
        svnmucc_exec = shutil.which('svnmucc')

        if svn_exec is None or svnmucc_exec is None:
            raise SubversionStorage.MissingExecutable(
                'The svn executable could not be found. '
                'Make sure it is installed and in your PATH.'
            )

        self._svn_exec = svn_exec
        self._svnmucc_exec = svnmucc_exec

    def _write(self, package, force=False):
        # list of files and dirs already in version control.
        existing_paths = self._get_versioned_paths()

        # list with tuples of (source_path, target_path) to add.
        files_to_add = []

        # first iteration: extract files to add and check for existence.
        for resource in package.resources:
            absolute_source_path = resource.absolute_source_path
            relative_target_path = pathlib.PurePosixPath(resource.target_path)

            if not force and relative_target_path in existing_paths:
                raise exceptions.ResourceAlreadyInStorage(
                    'resource with target path {} '
                    'already exists.'.format(relative_target_path)
                )

            files_to_add.append(
                (absolute_source_path, relative_target_path)
            )

        # second iteration: build up multi command transaction (put, mkdir, etc).
        multi_command = []
        commited_additions = []
        for source_path, target_path in files_to_add:

            # schedule mkdir action for target's missing parents (if any).
            parent_path = None
            for parent_name in target_path.parent.parts:
                if parent_path == None:
                    parent_path = pathlib.PurePosixPath(parent_name)
                else:
                    parent_path = parent_path.joinpath(parent_name)
                if parent_path not in existing_paths + commited_additions:
                    multi_command = multi_command + ['mkdir', str(parent_path)]
                    commited_additions.append(parent_path)

            # schedule put action for target.
            multi_command = multi_command + ['put', str(source_path), str(target_path)]
            commited_additions.append(target_path)

        # run multi-command: commit
        commit_message = 'Add files for package: %s' % package
        self._run_svn_multi_command(*multi_command, commit_message=commit_message)

        return commited_additions

    def _delete(self, package):
        # list of files and dirs already in version control.
        existing_paths = self._get_versioned_paths()

        multi_command = []
        commited_removals = []
        for resource in package.resources:
            relative_target_path = pathlib.PurePosixPath(resource.target_path)
            if relative_target_path in existing_paths:
                # target exists in repo, schedule removal.
                multi_command = multi_command + ['rm', str(relative_target_path)]
                commited_removals.append(relative_target_path)

        # run multi-command: commit
        commit_message = 'Remove files for package: %s' % package
        self._run_svn_multi_command(*multi_command, commit_message=commit_message)

        return commited_removals

    def _get_versioned_paths(self):
        xml_output = self._run_svn_command(
            'list', '--depth', 'infinity', '--xml', self._root_url
        )
        return [
            pathlib.PurePosixPath(name.text)
            for name in ET.fromstring(xml_output).findall('list/entry/name')
        ]

    def _run_command(self, exec_path, *args, **kwargs):
        cmd = [exec_path, '--non-interactive']

        if self._username is not None:
            cmd = cmd + ['--username', self._username]

        if self._password is not None:
            cmd = cmd + ['--password', self._password]

        cmd = cmd + list(args)

        completed_process = subprocess.run(
            cmd,
            capture_output=True,
            universal_newlines=True,
            **kwargs
        )

        if completed_process.returncode != 0:
            raise SubversionStorage.SubversionError(
                'Command %s failed (exited with code %d): \n%s'
                % (cmd, completed_process.returncode, completed_process.stderr.strip())
            )

        return completed_process.stdout.strip()

    def _run_svn_command(self, *args, **kwargs):
        return self._run_command(self._svn_exec, *args, **kwargs)

    def _run_svn_multi_command(self, *args, **kwargs):
        opts = [
            '-U', self._root_url,
            '-m', kwargs.pop('commit_message', ''),
            '-X', '-'  # read from stdin
        ]
        kwargs = {**kwargs, 'input': os.linesep.join(args)}
        return self._run_command(self._svnmucc_exec, *opts, **kwargs)
