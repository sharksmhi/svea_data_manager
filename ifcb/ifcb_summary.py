import scipy.io
import pathlib
import numpy as np
import os
import logging
import datetime

from ifcb.hdr_file import HdrFile
from ifcb.mat_file import ClassifierMatFile
logger = logging.getLogger(__name__)

# VOLUME_PAR = 'ml_analyzedTB'
# CLASS_PAR = 'class2useTB'
# FILE_PAR = 'filelistTB'
# MDATE_PAR = 'mdateTB'
# COUNT_PAR = 'classcountTB'

VOLUME_PAR = 'ml_analyzed'
CLASS_PAR = 'class2use'
FILE_PAR = 'filelist'
MDATE_PAR = 'matdate'

COUNT_PAR = 'classcount'
BIOVOL_PAR = 'classbiovol'


class IFCBSummaryFile:

    def __init__(self):

        self._mat_summary_path = None
        self._mat_summary_data = None

        self._hdr_files = {}
        self._classifiers = {}

        self._count_data = None
        self._biovol_data = None
        self._datetime_data = None
        self._mdate_data = None
        self._file_data = None
        self._volume_data = None
        self._class_data = None

        self._lat_data = None
        self._lon_data = None

        self._all_data = None

    def load_mat_summary_file(self, path):
        """Path to a mat-summary file. This file might look different in the future"""
        logger.info(f'Loading mat summary file: {path}')
        self._mat_summary_path = pathlib.Path(path)
        if not self._mat_summary_path.exists():
            raise FileNotFoundError(self._mat_summary_path)

        self._load_mat_summary_data()
        self._create_date_data()

    def load_hdr_files(self, directory):
        """hdr-files are one of the raw output files from the ifcb instrument"""
        logger.info(f'Loading hdr-files in directory: {directory}')
        self._hdr_files = {}
        nr_loaded = 0
        nr_in_scope = len(self._id_list)
        for root, dirs, files in os.walk(directory, topdown=False):
            print(f'{root=}: {dirs=}: {files=}')
            for name in files:
                path = pathlib.Path(root, name)
                if path.suffix != '.hdr':
                    continue
                if not self._file_data.get(path.stem):
                    logger.debug(f'hdr-file is not in scope. Not loading: {path}')
                    continue
                if self._hdr_files.get(path.stem):
                    logger.warning(f'hdr-file already loaded. Not loading: {path}')
                    continue
                logger.debug(f'Loading hdr-file: {path}')
                self._hdr_files[path.stem] = HdrFile(path)
                nr_loaded += 1
                if nr_loaded == nr_in_scope:
                    logger.info(f'Found all hdr files in scope! Stop checking files.')
                    return

    def load_classifier_mat_files(self, directory):
        """Individual mat-files are result from the classification"""
        logger.info(f'Loading classifier mat-files in directory: {directory}')
        self._classifiers = {}
        nr_loaded = 0
        nr_in_scope = len(self._id_list)
        for root, dirs, files in os.walk(directory, topdown=False):
            for name in files:
                path = pathlib.Path(root, name)
                if not self._file_data.get(path.stem):
                    logger.debug(f'mat-file is not in scope. Not loading: {path}')
                    continue
                if path.suffix != '.mat':
                    continue
                if path.name[0] != 'D':
                    continue
                self._classifiers[path.stem] = ClassifierMatFile(path)
                nr_loaded += 1
                if nr_loaded == nr_in_scope:
                    logger.info(f'Found all hdr files in scope! Stop checking files.')
                    return

    def create_summary_file(self, file_path, **kwargs):
        logger.info('Creating summary file')
        if not self._hdr_files:
            logger.warning('No hdr-files seems to be loaded')

        if not self._classifiers:
            logger.warning('No classifier mat-files seems to be loaded')

        self._combine_data()
        self._write_summary_file(file_path, **kwargs)

    def _write_summary_file(self, file_path=None, **kwargs):
        if file_path:
            path = pathlib.Path(file_path)
        else:
            path = pathlib.Path(self._mat_summary_path).parent
        if path.is_dir():
            path = pathlib.Path(path, f'{self._mat_summary_path.name.stem}.txt')
        if path.exists() and not kwargs.get('overwrite'):
            raise FileExistsError(path)
        lines = []
        for line in self._all_data:
            line = [str(item) for item in line]
            lines.append('\t'.join(line))
        with open(file_path, 'w') as fid:
            fid.write('\n'.join(lines))

    # def _combine_data(self):
    #     header = np.array([FILE_PAR, 'datetime', 'lat', 'lon', VOLUME_PAR] + list(self._class_data))
    #     data = np.array([self._file_data, self._datetime_data, self._lat_data, self._lon_data, self._volume_data] +
    #                     list(self._count_data))
    #     self._all_data = np.array([header] + list(data.transpose()))

    def _get_lat(self, _id):
        hdr = self._hdr_files.get(_id)
        if not hdr:
            return ''
        return hdr.lat or ''

    def _get_lon(self, _id):
        hdr = self._hdr_files.get(_id)
        if not hdr:
            return ''
        return hdr.lon or ''

    def _get_classifier_name(self, _id):
        cls = self._classifiers.get(_id)
        if not cls:
            return ''
        return cls.classifier_name or ''

    def _combine_data(self):
        self._all_data = []
        self._all_data.append(['id', 'datetime', 'lat', 'lon', 'taxon', 'parameter', 'value'])
        for _id in self._id_list:
            common_line = [_id, self._datetime_data[_id], self._get_lat(_id), self._get_lon(_id)]
            for taxon in self._class_list:
                volume = self._volume_data[_id]
                count = self._count_data[_id][taxon]
                biovol = self._biovol_data[_id][taxon]
                classifier_name = self._get_classifier_name(_id)
                date = self._datetime_data[_id]

                self._all_data.append(common_line[:] + [taxon, VOLUME_PAR, volume])
                self._all_data.append(common_line[:] + [taxon, COUNT_PAR, count])
                self._all_data.append(common_line[:] + [taxon, BIOVOL_PAR, biovol])
                self._all_data.append(common_line[:] + [taxon, 'classifier_name', classifier_name])
                self._all_data.append(common_line[:] + [taxon, 'classifier_run_date', date])

    def _load_mat_summary_data(self):
        self._mat_summary_data = scipy.io.loadmat(self._mat_summary_path, simplify_cells=True)

        self._id_list = [file['name'].split('.')[0] for file in self._mat_summary_data[FILE_PAR]]
        self._file_data = dict(zip(self._id_list, self._mat_summary_data[FILE_PAR]))
        # self._mdate_data = dict(zip(self._id_list, self._mat_summary_data[MDATE_PAR]))
        self._volume_data = dict(zip(self._id_list, self._mat_summary_data[VOLUME_PAR]))
        self._class_list = list(self._mat_summary_data[CLASS_PAR])
        self._class_data = dict(zip(self._id_list, self._mat_summary_data[CLASS_PAR]))

        count_data_matrix = self._mat_summary_data[COUNT_PAR]
        biovol_data_matrix = self._mat_summary_data[BIOVOL_PAR]

        self._count_data = {}
        self._biovol_data = {}
        for i, _id in enumerate(self._id_list):
            self._count_data[_id] = {}
            self._biovol_data[_id] = {}
            for c, cls in enumerate(self._class_list):
                self._count_data[_id][cls] = count_data_matrix[i][c]
                biovol = biovol_data_matrix[i][c]
                if str(biovol) == '0.0':
                    biovol = '0'
                self._biovol_data[_id][cls] = biovol

    def _create_date_data(self):
        self._datetime_data = dict((_id, _id.split('_')[0]) for _id in self._id_list)

    @staticmethod
    def _translate_date(date):
        return datetime.datetime.strptime(date, '%d-%b-%Y %H:%M:%S').strftime('%Y%m%d')


def create_summary_file(
        mat_summary_file_path=None,
        hdr_root_directory=None,
        mat_root_directory=None,
        output_file_path=None,
        **kwargs
    ):
    assert mat_summary_file_path, "You have to provide a mat_summary_file_path"

    s = IFCBSummaryFile()
    s.load_mat_summary_file(mat_summary_file_path)

    if hdr_root_directory and pathlib.Path(hdr_root_directory).exists():
        s.load_hdr_files(hdr_root_directory)
    else:
        logger.warning(f'No hdr_root_directory given to add positional data to ifcb summary file')

    if mat_root_directory and pathlib.Path(mat_root_directory).exists():
        s.load_classifier_mat_files(mat_root_directory)
    else:
        logger.warning(f'No mat_root_directory given to add positional data to ifcb summary file')

    s.create_summary_file(output_file_path, **kwargs)

