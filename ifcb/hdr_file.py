from pathlib import Path
import logging
import time
import csv
logger = logging


class HdrFile:

    def __init__(self, file_path):
        self._path = Path(file_path)
        if not self._path.suffix == '.hdr':
            msg = f'{self._path} is not a hdr-file'
            logger.error(msg)
            raise Exception(msg)
        self._lat = None
        self._lon = None

        self._save_info()

    @property
    def path(self):
        return self._path

    @property
    def lat(self):
        return self._lat

    @property
    def lon(self):
        return self._lon

    @property
    def metadata(self):
        meta =  dict(
            latitude=self.lat,
            longitude=self.lon,
        )
        if not self.lat or not self.lon:
            meta['quality_flag'] = 'B'
        return meta

    def _save_info(self):
        with open(self._path) as fid:
            reader = csv.reader(fid, delimiter=':')
            data = {row[0]: row[1] for row in reader}
        self._lat = data.get('gpsLatitude')
        self._lon = data.get('gpsLatitude')
        if self._lat == 'N/A':
            self._lat = ''
        if self._lon == 'N/A':
            self._lon = ''



# def get_metadata_from_hdr_file(path):
#     meta = {}
#     with open(path) as fid:
#         for line in fid:
#             key, value = [item.strip() for item in line.split(':', 1)]
#             if key == 'gpsLatitude':
#                 if '.' in value:
#                     meta['latitude'] = value
#             elif key == 'gpsLongitude':
#                 if '.' in value:
#                     meta['longitude'] = value
#     return meta
