from svea_data_manager.sdm_event import subscribe
from pathlib import Path
import datetime


class SDMLogger:

    def __init__(self):

        self._callbacks = dict(
            resources_added={},
            resources_rejected={},
            resources_written={},
            target_path_exists={},
            transform_added_files={},
            files_copied={},
            log=[]
        )

        self._add_subscriptions()

    def _add_subscriptions(self):
        subscribe('on_resource_added', self._on_resource_added)
        subscribe('on_resource_rejected', self._on_resource_rejected)
        subscribe('on_target_path_exists', self._on_target_path_exists)
        subscribe('on_file_copied', self._on_file_copied)
        subscribe('on_transform_add_file', self.on_transform_add_file)
        subscribe('log', self._on_log)

    def _on_resource_added(self, data):
        self._callbacks['resources_added'].setdefault(data['instrument'].upper(), [])
        self._callbacks['resources_added'][data['instrument'].upper()].append(data['path'])

    def _on_resource_rejected(self, data):
        self._callbacks['resources_rejected'].setdefault(data['instrument'], [])
        self._callbacks['resources_rejected'][data['instrument'].upper()].append(data['path'])

    def _on_target_path_exists(self, data):
        self._callbacks['target_path_exists'].setdefault(data['instrument'].upper(), [])
        self._callbacks['target_path_exists'][data['instrument'].upper()].append(data['path'])

    def on_transform_add_file(self, data):
        self._callbacks['transform_added_files'].setdefault(data['instrument'].upper(), [])
        self._callbacks['transform_added_files'][data['instrument'].upper()].append(data['name'])

    def _on_file_copied(self, data):
        self._callbacks['files_copied'].setdefault(data['instrument'].upper(), [])
        self._callbacks['files_copied'][data['instrument'].upper()].append(data['target_path'])

    def _on_log(self, data):
        self._callbacks['log'].append(data['msg'])

    def get_resources_added(self, instrument=None):
        if instrument:
            return self._callbacks['resources_added'][instrument.upper()]
        else:
            return self._callbacks['resources_added']

    def get_resources_rejected(self, instrument=None):
        if instrument:
            return self._callbacks['resources_rejected'][instrument.upper()]
        else:
            return self._callbacks['resources_rejected']

    def get_target_path_exists(self, instrument=None):
        if instrument:
            return self._callbacks['target_path_exists'][instrument.upper()]
        else:
            return self._callbacks['target_path_exists']

    def get_transform_added_files(self, instrument=None):
        if instrument:
            return self._callbacks['transform_add_files'][instrument.upper()]
        else:
            return self._callbacks['transform_add_files']

    def get_files_copied(self, instrument=None):
        if instrument:
            return self._callbacks['files_copied'][instrument.upper()]
        else:
            return self._callbacks['files_copied']

    def get_nr_resources_added(self, instrument=None):
        if instrument:
            return self._get_len(self._callbacks['resources_added'].get(instrument.upper()))
        else:
            return dict((key, len(values)) for key, values in self._callbacks['resources_added'].items())

    def get_nr_resources_rejected(self, instrument=None):
        if instrument:
            return self._get_len(self._callbacks['resources_rejected'].get(instrument.upper()))
        else:
            return dict((key, len(values)) for key, values in self._callbacks['resources_rejected'].items())

    def get_nr_target_path_exists(self, instrument=None):
        if instrument:
            return self._get_len(self._callbacks['target_path_exists'].get(instrument.upper()))
        else:
            return dict((key, len(values)) for key, values in self._callbacks['target_path_exists'].items())

    def get_nr_transform_added_files(self, instrument=None):
        if instrument:
            return self._get_len(self._callbacks['transform_added_files'].get(instrument.upper()))
        else:
            return dict((key, len(values)) for key, values in self._callbacks['transform_added_files'].items())

    def get_nr_files_copied(self, instrument=None):
        if instrument:
            return self._get_len(self._callbacks['files_copied'].get(instrument.upper()))
        else:
            return dict((key, len(values)) for key, values in self._callbacks['files_copied'].items())

    def write_reports(self, directory):
        root_directory = Path(directory, datetime.datetime.now().strftime('%Y%m%d_%H%M'))
        for callback, info in self._callbacks.items():
            if type(info) == list:
                path = Path(root_directory, f'{callback}.txt')
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, 'w') as fid:
                    fid.write('\n'.join([str(inf) for inf in info]))
            elif type(info) == dict:
                for inst, values in info.items():
                    path = Path(root_directory, inst, f'{callback}_{len(values)}_files.txt')
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with open(path, 'w') as fid:
                        fid.write('\n'.join([str(val) for val in values]))
        return root_directory

    @staticmethod
    def _get_len(items):
        if not items:
            return 0
        return len(items)

