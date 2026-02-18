import os
import pathlib
import shutil
import subprocess
from xml.etree import ElementTree as ET

from svea_data_manager.frameworks import exceptions
from svea_data_manager.frameworks.storage.base_storage import BaseStorage, logger
from svea_data_manager.sdm_event import post_event


class SubversionStorage(BaseStorage):
    def __init__(self, root_url, username=None, password=None):
        self._root_url = root_url
        self._username = username
        self._password = password

        svn_exec = shutil.which("svn")
        svnmucc_exec = shutil.which("svnmucc")

        if svn_exec is None or svnmucc_exec is None:
            raise exceptions.MissingSubversionExecutableError(
                "The svn executable could not be found. "
                "Make sure it is installed and in your PATH."
            )

        self._svn_exec = svn_exec
        self._svnmucc_exec = svnmucc_exec

    def __str__(self):
        return f"{self.__class__.__name__}({self._root_url})"

    def _write(self, package, force=False) -> list[pathlib.PurePosixPath]:
        # list of files and dirs already in version control.
        existing_paths = self._get_versioned_paths()

        # list with tuples of (source_path, target_path) to add.
        files_to_add = []

        # first iteration: extract files to add and check for existence.
        nr_files = len(package.resources)
        messages = set()
        for nr, resource in enumerate(package.resources):
            instrument = package.instrument
            absolute_source_path = resource.absolute_source_path
            svn_message = resource.attributes.get("svn_commit_message")
            if svn_message:
                messages.add(svn_message)
            if resource.target_path is None:
                msg = (
                    f"Will not write file. "
                    f"No target path given for file: {resource.absolute_source_path}"
                )
                logger.info(msg)
                post_event(
                    "on_target_path_not_given",
                    {"instrument": instrument, "path": resource.absolute_source_path},
                )
                continue
            relative_target_path = pathlib.PurePosixPath(resource.target_path)

            post_event(
                "on_progress",
                {
                    "instrument": package.instrument,
                    "msg": "Checking file existence in SVN",
                    "percentage": int((nr + 1) / nr_files * 100),
                    "nr_files_total": nr_files,
                    "nr_files_copied": nr,
                },
            )

            if not force and relative_target_path in existing_paths:
                msg = (
                    f"Will not write file. "
                    f"Resource with target path {relative_target_path} already exists."
                )
                logger.warning(msg)
                post_event(
                    "on_target_path_exists",
                    {"instrument": instrument, "path": relative_target_path},
                )
                continue
                # raise exceptions.ResourceAlreadyInStorage(
                #     'resource with target path {} '
                #     'already exists.'.format(relative_target_path)
                # )

            files_to_add.append((absolute_source_path, relative_target_path))

        # second iteration: build up multi command transaction (put, mkdir, etc).
        multi_command = []
        commited_additions = []
        nr_files = len(files_to_add)
        for nr, (source_path, target_path) in enumerate(files_to_add):
            # schedule mkdir action for target's missing parents (if any).
            parent_path = None
            for parent_name in target_path.parent.parts:
                if parent_path is None:
                    parent_path = pathlib.PurePosixPath(parent_name)
                else:
                    parent_path = parent_path.joinpath(parent_name)
                if parent_path not in existing_paths + commited_additions:
                    multi_command = [*multi_command, "mkdir", str(parent_path)]
                    commited_additions.append(parent_path)

            # schedule put action for target.
            multi_command = [*multi_command, "put", str(source_path), str(target_path)]
            commited_additions.append(target_path)
            post_event(
                "on_svn_storage_prepared",
                {
                    "instrument": package.instrument,
                    "source_path": source_path,
                    "target_path": target_path,
                    "nr_files_total": nr_files,
                    "nr_files_copied": nr,
                },
            )

        if not multi_command:
            logger.info("No files prepared for svn storage")
            return []

        post_event(
            "on_progress",
            {
                "instrument": package.instrument,
                "msg": "Starting commit to SVN",
                "percentage": 20,
            },
        )

        # run multi-command: commit
        commit_message = f"Add {nr_files} files for package {package}"
        if messages:
            add = "; ".join(messages)
            commit_message = f"{commit_message}: {add}"
        self._run_svn_multi_command(*multi_command, commit_message=commit_message)

        post_event(
            "on_progress",
            {
                "instrument": package.instrument,
                "msg": f"Commit to SVN finished with comment: {commit_message}",
                "percentage": 100,
            },
        )

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
                multi_command = [*multi_command, "rm", str(relative_target_path)]
                commited_removals.append(relative_target_path)

        if not multi_command:
            logger.info("No files prepared for svn storage")
            return []

        # run multi-command: commit
        commit_message = f"Remove files for package: {package}"
        self._run_svn_multi_command(*multi_command, commit_message=commit_message)

        return commited_removals

    def _get_versioned_paths(self):
        xml_output = self._run_svn_command(
            "list", "--depth", "infinity", "--xml", self._root_url
        )
        return [
            pathlib.PurePosixPath(name.text)
            for name in ET.fromstring(xml_output).findall("list/entry/name")
        ]

    def _run_command(self, exec_path, *args, **kwargs):
        cmd = [exec_path, "--non-interactive"]

        if self._username is not None:
            cmd = [*cmd, "--username", self._username]

        if self._password is not None:
            cmd = [*cmd, "--password", self._password]

        cmd = cmd + list(args)

        completed_process = subprocess.run(
            cmd, capture_output=True, universal_newlines=True, **kwargs
        )

        if completed_process.returncode != 0:
            raise exceptions.SubversionError(
                f"Command {cmd} failed "
                f"(exited with code {completed_process.returncode}): \n"
                f"{completed_process.stderr.strip()}"
            )

        return completed_process.stdout.strip()

    def _run_svn_command(self, *args, **kwargs):
        return self._run_command(self._svn_exec, *args, **kwargs)

    def _run_svn_multi_command(self, *args, **kwargs):
        opts = [
            "-U",
            self._root_url,
            "-m",
            kwargs.pop("commit_message", ""),
            "-X",
            "-",  # read from stdin
        ]
        kwargs = {**kwargs, "input": os.linesep.join(args)}
        return self._run_command(self._svnmucc_exec, *opts, **kwargs)
