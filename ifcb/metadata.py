import pathlib
import datetime


class MetadataIFCB:

    def __init__(self, **kwargs):
        self._metadata = dict(
            id=None,
            ship=None,
            cruise_number=None,
            sampling_depth=None,
            latitude=None,
            longitude=None,
            quality_flag=None,
            classifier_version=None,
            comments=[]
        )
        self._file_path = kwargs.pop('file_path', None)
        if self._file_path:
            self._file_path = pathlib.Path(self._file_path)
        self._metadata.update(kwargs)
        self._validate()

    def __repr__(self):
        return f'IFCB metadata for file: {self.file_name}'

    def __str__(self):
        lines = [f'IFCB metadata for file: {self.file_name}']
        for key, value in self._metadata.items():
            lines.append(f"{key.ljust(20)}: {value}")
        return '\n'.join(lines)

    def __eq__(self, other):
        for key, value in self.metadata.items():
            if not other.metadata.get(key) == value:
                return False
        return True

    def __getitem__(self, key):
        if key not in self.metadata:
            return False
        return self.metadata.get(key)

    def __setitem__(self, key, value):
        key = key.lower()
        if key not in self.metadata and key != 'comment':
            raise KeyError(f'{key} is not a valid metadata')
        if key == 'comment' and value:
            self._metadata['comments'].append(value.replace('\n', ' ') + f' ({datetime.datetime.now().strftime("%Y%m%d")})')
        else:
            self._metadata[key] = value

    def _validate(self):
        if self._file_path and self._file_path.stem != self.metadata['id']:
            raise ValueError(f"Mismatch in file name and id for IFCB metadata file: {self._file_path} (id={self.metadata['metadata']})")

    @property
    def metadata(self):
        return self._metadata

    @property
    def file_name(self):
        if not self._metadata.get('id'):
            return None
        return f"{self._metadata['id']}.txt"

    def add(self, **kwargs):
        """ Adds metadata to the object """
        for key, value in kwargs.items():
            self[key] = value

    @classmethod
    def from_file(cls, path):
        with open(path) as fid:
            meta = {}
            comments = []
            for line in fid:
                strip_line = line.strip()
                if not strip_line:
                    continue
                key, value = [item.strip() for item in strip_line.split(':')]
                if value == '':
                    value = None
                if key == 'comment':
                    comments.append(value)
                else:
                    meta[key] = value
            meta['comments'] = comments
        return cls(file_path=path, **meta)

    def get_string_content(self):
        lines = []
        for key, value in self._metadata.items():
            if value is None:
                value = ''
            if key == 'comments':
                for com in value:
                    lines.append(f'comment: {com}')
            else:
                lines.append(f'{key}: {value}')
        return '\n'.join(lines)

    def save_file(self, directory):
        if not self.file_name:
            raise AttributeError('Can not save IFCB metadata file. Unknown file name')
        path = pathlib.Path(directory, self.file_name)
        with open(path, 'w') as fid:
            fid.write(self.get_string_content())
        self._file_path = path
