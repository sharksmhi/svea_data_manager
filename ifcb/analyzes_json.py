from __future__ import annotations
import pathlib
import pandas as pd
import numpy as np
import datetime
from ifcb.basin import BasinIterator
from shapely import Point
import json
import logging

logger = logging.getLogger(__name__)


class IfcbJsonFormat:
    version = '0.0.1'

    def __init__(self, summary_file, basin_geojson_file):
        """summary_file is a result txt-file from the analyses"""
        self._summary_file = pathlib.Path(summary_file)
        self._basin_geojson_file = pathlib.Path(basin_geojson_file)
        self._basin_list = list(BasinIterator(self._basin_geojson_file))
        self._df = None
        self._day_data = None
        self._json_data = None
        self._load_file()

    def _load_file(self):
        self._df = pd.read_csv(path, sep='\t')
        self._day_data = {}
        for g in self._df.groupby('filelist'):
            dd = DayData(g, self._basin_list)
            self._day_data[dd.time] = dd

    def _get_json_data(self, date):
        return dict(
            analyzes=self._get_json_analyzes(date),
            meta=self._get_json_meta()
        )

    def _get_json_meta(self):
        now_str = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
        data = dict(
            compose=dict(
                version=self.version,
                time=now_str
            )
        )
        return data

    def _get_json_analyzes(self, date: datetime.date):
        data = []
        for key in sorted(self._day_data):
            day_data = self._day_data[key]
            if day_data.date != date:
                continue
            data.append(
                dict(
                    analysis=self._get_json_analysis(day_data),
                    sample=self._get_json_sample(day_data),
                    basin=self._get_json_basin(day_data)

                )
            )
        return data

    @staticmethod
    def _get_json_analysis(day_data: DayData):
        return dict(
            sampleTime=day_data.sample_time_str,
            countByTaxon=day_data.count_by_taxon,
        )

    @staticmethod
    def _get_json_sample(day_data: DayData):
        return dict(
            sampleTime=day_data.sample_time_str,
            sampleCoordinates=day_data.sample_coordinates
        )

    @staticmethod
    def _get_json_basin(day_data: DayData):
        return day_data.basin_info

    def _get_date_list(self):
        return sorted(set([day_date.date for day_date in self._day_data.values()]))

    def create_files(self, directory):
        for date in self._get_date_list():
            path = pathlib.Path(directory, f'analyzes-{date}.json')
            data = self._get_json_data(date)
            with open(path, 'w') as fid:
                json.dump(data, fid, indent=4)


class DayData:

    def __init__(self, group_item, basin_list=None):
        self._id, self._df = group_item
        self._basin_list = basin_list or None

    @property
    def time(self):
        return datetime.datetime.strptime(self._df['datetime'].values[0], 'D%Y%m%dT%H%M%S')

    @property
    def date(self):
        return self.time.date()

    @property
    def sample_time_str(self):
        return self.time.strftime('%Y-%m-%d %H:%M:%S')

    @property
    def lat(self):
        value = self._df['lat'].values[0]
        if np.isnan(value):
            value = None
        return value

    @property
    def lon(self):
        value = self._df['lon'].values[0]
        if np.isnan(value):
            value = None
        return value

    @property
    def sample_coordinates(self):
        return [self.lat, self.lon]

    @property
    def point(self) -> Point:
        return Point(float(self.lon), float(self.lat))

    @property
    def basin_info(self):
        for basin in self._basin_list:
            if not self.has_coordinates():
                logger.warning(f'No position found for file {self._id}')
                continue
            if not basin.geometry.contains(self.point):
                continue
            return dict(
                id=basin.id,
                name=basin.name
            )
        return dict(
            id='',
            name=''
        )

    @property
    def count_by_taxon(self):
        data = {}
        index = self._df['parameter'] == 'classcount'
        for taxon, count in self._df[index][['taxon', 'value']].values:
            int_count = int(count)
            if not int_count:
                continue
            data[taxon] = int_count
        return data

    def has_coordinates(self):
        if self.lat is None or self.lon is None:
            return False
        return True


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    path = r"C:\mw\temmp\temp_ifcb\count_biovol_manual_05Dec2022.txt"
    basin_file = r'C:\mw\git\svea_data_manager\ifcb\loo_composer\resources\basins.geojson'
    export_dir = r'C:\mw\temmp\temp_ifcb\json_files'

    obj = IfcbJsonFormat(path, basin_geojson_file=basin_file)
    obj.create_files(export_dir)

    df = pd.read_csv(path, sep='\t')
    dd = list(df.groupby('filelist'))
    tmp, d = dd[0]

