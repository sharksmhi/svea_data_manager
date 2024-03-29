import datetime
import logging
import logging.handlers
import os
import pathlib
import re
import shutil
import sys
import tkinter as tk
import traceback
from tkinter import filedialog
from tkinter import messagebox

import yaml
from yaml import SafeLoader

from svea_data_manager import SveaDataManager
from svea_data_manager.sdm_logger import SDMLogger

logger = logging.getLogger(__file__)


if getattr(sys, 'frozen', False):
    DIRECTORY = pathlib.Path(sys.executable).parent
elif __file__:
    DIRECTORY = pathlib.Path(__file__).parent


class App(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        tk.Tk.wm_title(self, 'Svea Data Manager')

        self._stringvars_source_directory = {}
        self._stringvar_config = tk.StringVar()
        self._stringvar_root = tk.StringVar()
        self._stringvar_loglevel = tk.StringVar()

        self._config = None
        self._report = None
        self._report_frame = None

        self.logger = None
        self._log_level = 'DEBUG'
        self._loglevel_options = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        self._logging_format = '%(asctime)s [%(levelname)10s]    %(pathname)s [%(lineno)d] => %(funcName)s():    %(message)s'

        self._cleanup_log_after_n_days = 1

        self._setup_logger()

        self._logger = SDMLogger()

        self._create_config_stringvars()
        self._build()
        self._startup()

    @property
    def _default_config_path(self):
        for path in DIRECTORY.iterdir():
            if path.name.startswith('config'):
                logger.debug(f'Default config path is: {path}')
                return path

    @property
    def _report_directory(self):
        return pathlib.Path(DIRECTORY, 'reports')

    def _startup(self):
        self._cleanup_log()
        if not self._default_config_path.exists():
            return
        config_path = self._default_config_path
        if config_path:
            self._stringvar_config.set(self._default_config_path)
            self._on_select_config()

    def _setup_logger(self, **kwargs):
        self.logger = logging.getLogger()
        self.logger.setLevel(self._log_level)
        directory = pathlib.Path(DIRECTORY, 'log')
        if not directory.exists():
            os.makedirs(directory)
        file_path = pathlib.Path(directory, 'sdm.log')
        handler = logging.handlers.TimedRotatingFileHandler(str(file_path), when='D', interval=1, backupCount=7)
        formatter = logging.Formatter(self._logging_format)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def _cleanup_log(self):
        now = datetime.datetime.now()
        before_time = now - datetime.timedelta(days=self._cleanup_log_after_n_days)
        for path in self._report_directory.iterdir():
            if datetime.datetime.fromtimestamp(path.stat().st_mtime) > before_time:
                continue
            if path.is_file():
                os.remove(str(path))
            else:
                shutil.rmtree(path)

    def _on_select_loglevel(self, *args):
        level = self._stringvar_loglevel.get().upper()
        if level not in self._loglevel_options:
            raise ValueError(f'"{level}" is not a valid loglevel')
        self.logger.setLevel(level)

    def _create_config_stringvars(self):
        self._stringvars_source_directory = {}
        if not self._config:
            return
        for key in self._config:
            self._stringvars_source_directory[key.lower()] = tk.StringVar()

    def _build(self):

        layout = {'padx': 5,
                  'pady': 5,
                  'sticky': 'nsew'}

        self._frame_paths = tk.Frame(self)
        self._frame_paths.grid(row=0, column=0, **layout)

        self._frame_config = tk.Frame(self)
        self._frame_config.grid(row=1, column=0, **layout)

        self._inner_config_frame = None

        grid_configure(self, nr_rows=2)

        self._build_frame_paths()

    def _build_frame_paths(self):

        frame = self._frame_paths
        grid = dict(padx=5, pady=5)

        r = 0
        tk.Label(frame, text='Loggningsnivå:').grid(row=r, column=0, **grid, sticky='e')
        tk.OptionMenu(frame, self._stringvar_loglevel,
                      *self._loglevel_options,
                      command=self._on_select_loglevel).grid(row=r, column=1, **grid, sticky='w')
        self._stringvar_loglevel.set('WARNING')
        self._on_select_loglevel()

        r += 1
        config_title = tk.Label(frame, text='Konfigurationsfil')
        config_title.grid(row=r, column=0, **grid, sticky='e')
        config_title.bind('<Control-Button-1>', self._select_config_path)
        tk.Label(frame, textvariable=self._stringvar_config).grid(row=r, column=1, **grid,
                                                                                     sticky='w')

        r += 1
        tk.Button(frame, text='Välj rotmapp för data', command=self._select_root_dir).grid(row=r, column=0, **grid)
        tk.Label(frame, textvariable=self._stringvar_root).grid(row=r, column=1, **grid, sticky='w')

        r += 1
        self._button_run_all = tk.Button(frame, text='Hantera all', command=self._run_all_instruments)
        self._button_run_all.grid(row=r, column=1, **grid, sticky='e')
        self._button_run_all.configure(state='disabled')

        grid_configure(frame, nr_columns=2, nr_rows=r+1)

    def _build_frame_config(self):

        if self._inner_config_frame:
            self._inner_config_frame.destroy()

        if not self._config:
            return

        self._create_config_stringvars()

        self._buttons_path = {}
        self._buttons_run = {}

        self._inner_config_frame = tk.Frame(self._frame_config)
        self._inner_config_frame.grid()
        grid_configure(self._frame_config)

        gridl = dict(padx=5, pady=2)
        gridb = dict(padx=(10, 5), pady=2)

        line_length = 120

        self._stringvar_attributes = {}

        conf_r = 0
        for inst in self._config:
            frame = tk.Frame(self._inner_config_frame)
            frame.grid(row=conf_r, column=0, sticky='nsew', **gridl)
            r = 0
            inst_lower = inst.lower()
            self._stringvar_attributes[inst_lower] = {}
            tk.Label(frame, text='-'*line_length).grid(row=r, column=0, columnspan=2, **gridl, sticky='ew')
            r += 1
            tk.Label(frame, text=inst.upper()).grid(row=r, column=0, **gridl, sticky='w')
            r += 1
            for key, item in self._config[inst].items():
                if key == 'source_directory':
                    self._buttons_path[inst_lower] = tk.Button(frame, text=key, command=lambda x=inst: self._select_instrument_dir(x))
                    self._buttons_path[inst_lower].grid(row=r, column=0, **gridb, sticky='w')
                    tk.Label(frame, textvariable=self._stringvars_source_directory[inst_lower]).grid(row=r, column=1, **gridl, sticky='w')
                elif key == 'attributes':
                    tk.Label(frame, text=str(key)).grid(row=r, column=0, **gridl, sticky='nw')
                    attr_frame = tk.Frame(frame)
                    attr_frame.grid(row=r, column=1, **gridl, sticky='w')
                    ar = 0
                    for attr, value in item.items():
                        if not value:
                            value = ''
                        tk.Label(attr_frame, text=attr).grid(row=ar, column=0, **gridl, sticky='w')
                        self._stringvar_attributes[inst_lower][attr] = tk.StringVar()
                        entry = tk.Entry(attr_frame, textvariable=self._stringvar_attributes[inst_lower][attr])
                        entry.grid(row=ar, column=1, **gridl, sticky='w')
                        self._stringvar_attributes[inst_lower][attr].set(value)
                        if attr == 'ship':
                            entry.config(state='disabled')
                        ar += 1
                    grid_configure(attr_frame, nr_columns=2, nr_rows=ar)

                else:
                    tk.Label(frame, text=str(key)).grid(row=r, column=0, **gridl, sticky='w')
                    tk.Label(frame, text=str(item)).grid(row=r, column=1, **gridl, sticky='w')
                grid_configure(frame, nr_columns=2, nr_rows=r+1)

                r += 1
            self._buttons_run[inst_lower] = tk.Button(frame, text='Fortsätt',
                                                       command=lambda x=inst: self._run_instrument(x))
            self._buttons_run[inst_lower].grid(row=r, column=1, **gridb, sticky='e')
            conf_r += 1
        tk.Label(self._inner_config_frame, text='-' * line_length).grid(row=conf_r, column=0, columnspan=2, **gridl, sticky='ew')
        grid_configure(self._inner_config_frame, nr_columns=2, nr_rows=conf_r+1)

    def _select_config_path(self, *args):
        path = filedialog.askopenfilename(title='Välj konfigurationsfil', filetypes=[('konfigurationsfil', '*.yaml')])
        if not path:
            return
        self._stringvar_config.set(path)
        self._stringvar_root.set('')
        self._on_select_config()

    def _select_root_dir(self):
        directory = filedialog.askdirectory(title='Välj rotmapp')
        if not directory:
            return
        self._stringvar_root.set(directory)
        self._on_select_root_dir()

    def _select_instrument_dir(self, inst):
        directory = filedialog.askdirectory(title=f'Välj mapp för {inst.upper()}-data')
        if not directory:
            return
        self._set_source_path_for_instrument(inst, directory)

    def _on_select_config(self):
        self._button_run_all.configure(state='disabled')
        path_str = self._stringvar_config.get()
        if not path_str:
            return
        path = pathlib.Path(path_str)
        if not path.exists():
            self._stringvar_config.set('')
            self._config = None
            msg = 'Ingen gilltig configurationsfil vald!'
            messagebox.showerror('Val av konfigurationsfil', msg)
            logger.debug(msg)
            return
        with open(path) as fid:
            config = yaml.load(fid, Loader=SafeLoader)
        self._config = {}
        for key, item in config.items():
            self._config[key.lower()] = item

        self._build_frame_config()
        self._check_paths_based_on_config()
        self._button_run_all.configure(state='normal')

    def _on_select_root_dir(self):
        for svar in self._stringvars_source_directory.values():
            svar.set('')
        path_str = self._stringvar_root.get()
        if not path_str:
            return
        for path in pathlib.Path(path_str).iterdir():
            item = path.name.lower()
            self._set_source_path_for_instrument(item, path)

    def _set_source_path_for_instrument(self, inst, path):
        """ Sets source_path to corresponding stringvariable and instrument in self._config """
        if not self._stringvars_source_directory.get(inst):
            return
        self._stringvars_source_directory[inst].set('')
        if not self._config:
            return
        data = self._config.get(inst)
        if not data:
            return
        self._config[inst]['source_directory'] = str(path)
        self._stringvars_source_directory[inst].set(str(path))

    def _check_paths_based_on_config(self):
        if not self._config:
            return
        for inst in self._config:
            inst = inst.lower()
            paths_str = self._stringvars_source_directory[inst].get()
            if not paths_str or not pathlib.Path(paths_str).exists():
                self._stringvars_source_directory[inst].set('')
                self._config[inst]['source_directory'] = ''
                
    def _add_attributes_to_config(self):
        for inst, attrs in self._stringvar_attributes.items():
            for key, var in attrs.items():
                value = var.get().strip()
                if not value:
                    value = None
                self._config[inst]['attributes'][key] = value

    def _run_all_instruments(self):
        self._add_attributes_to_config()
        if not self._config:
            msg = 'Ingen gilltig konfigurationsfil hittades'
            messagebox.showwarning('Hanterar alla instrument', msg)
            logger.debug(msg)
            return
        for inst in self._config:
            if not self._config[inst]['source_directory']:
                msg = f'Ingen källmapp satt för instrument {inst.upper()}. Avbryter!'
                messagebox.showwarning('Hanterar alla instrument', msg)
                logger.debug(msg)
                return
        self._write_latest_config(self._config)
        try:
            report_dir = self._run_with_config(self._config)
            nr_accepted_str = '\n'.join(
                [f'{inst}: {nr}' for inst, nr in self._logger.get_nr_resources_added().items()])
            nr_rejected_str = '\n'.join(
                [f'{inst}: {nr}' for inst, nr in self._logger.get_nr_resources_rejected().items()])
            nr_transformed_str = '\n'.join(
                [f'{inst}: {nr}' for inst, nr in self._logger.get_nr_transform_added_files().items()])
            nr_copied_str = '\n'.join(
                [f'{inst}: {nr}' for inst, nr in self._logger.get_nr_files_copied().items()])
            nr_not_copied_str = '\n'.join(
                [f'{inst}: {nr}' for inst, nr in self._logger.get_nr_target_path_exists().items()])

            msg = f'Hanteringen är klar för samtliga instrument. ' \
                  f'Antal filer som hanterats: \n{nr_accepted_str}\n\n' \
                  f'Antal filer som inte hanterats: \n{nr_rejected_str}\n\n' \
                  f'Antal filer som lagts till under prosessen: \n{nr_transformed_str}\n\n' \
                  f'Antal filer som kopierats: \n{nr_copied_str}\n\n' \
                  f'Antal filer som inte kopierats: \n{nr_not_copied_str}\n\n' \
                  f'Se fullständig rapport under: {report_dir}.'
            messagebox.showinfo('Hanterar alla instrument', msg)
            logger.debug(msg)
            self._logger.reset()
        except Exception as e:
            messagebox.showerror('Något gick fel', f'{e}\n\n{traceback.format_exc()}')
            logger.critical(e)
            logger.critical(traceback.format_exc())
            raise

    def _run_instrument(self, inst, show_message=False):
        self._add_attributes_to_config()
        data = self._config.get(inst)
        if not data:
            return
        if not self._config[inst]['source_directory']:
            msg = f'Ingen källmapp satt för instrument {inst.upper()}'
            messagebox.showwarning(f'Kör {inst}', msg)
            logger.debug(msg)
            return
        config = {inst: data}
        self._write_latest_config(config)
        try:
            report_dir = self._run_with_config(config)
            msg = f'Hanteringen är klar för instrument: {inst.upper()}. \n\n' \
                  f'Antal filer som hanterats: {self._logger.get_nr_resources_added(inst)}\n' \
                  f'Antal filer som inte hanterats: {self._logger.get_nr_resources_rejected(inst)}\n' \
                  f'Antal filer som lagts till under prosessen: {self._logger.get_nr_transform_added_files(inst)}\n' \
                  f'Antal filer som kopierats: {self._logger.get_nr_files_copied(inst)}\n' \
                  f'Antal filer som inte kopierats: {self._logger.get_nr_target_path_exists(inst)}\n\n' \
                  f'Se fullständig rapport under: {report_dir}.'
            messagebox.showinfo('Hanterar alla instrument', msg)
            logger.debug(msg)
            self._logger.reset()
        except Exception as e:
            messagebox.showerror('Något gick fel', f'{e}\n\n{traceback.format_exc()}')
            logger.critical(e)
            logger.critical(traceback.format_exc())
            raise

    @staticmethod
    def _write_latest_config(config):
        with open(pathlib.Path(DIRECTORY, 'latest_config.yaml'), 'w') as fid:
            yaml.dump(config, fid)

    def _run_with_config(self, config):
        sdm = SveaDataManager.from_config(config)
        sdm.read_packages()
        sdm.transform_packages()
        sdm.write_packages()
        report_dir = self._logger.write_reports(self._report_directory)
        return report_dir
        # messagebox.showinfo('Arkivering klar!', f'Arkiveringen av instrument {", ".join(config.keys())} är färdig\n'
        #                                         f'Se rapport under: {report_dir}')

    #     self._report = sdm.get_report_text()
    #     self._show_report_frame()
    #
    # def _show_report_frame(self):
    #     if not self._report:
    #         pass
    #     self._report_frame = tk.Toplevel(self)
    #     r = 0
    #     for inst, text in self._report.items():
    #         tk.Label(self._report_frame, text=text).grid(row=r, column=0, padx=10, pady=10, sticky='nsew')
    #         r += 1
    #     grid_configure(self._report_frame, nr_rows=r)


def grid_configure(frame, nr_rows=1, nr_columns=1, **kwargs):
    """
    Updated 20180825

    Put weighting on the given frame. Put weighting on the number of rows and columns given.
    kwargs with tag "row"(r) or "columns"(c, col) sets the number in tag as weighting.
    Example:
        c1=2 sets frame.grid_columnconfigure(1, weight=2)
    """
    row_weight = {}
    col_weight = {}

    # Get information from kwargs
    for key, value in kwargs.items():
        rc = int(re.findall('\d+', key)[0])
        if 'r' in key:
            row_weight[rc] = value
        elif 'c' in key:
            col_weight[rc] = value

            # Set weight
    for r in range(nr_rows):
        frame.grid_rowconfigure(r, weight=row_weight.get(r, 1))

    for c in range(nr_columns):
        frame.grid_columnconfigure(c, weight=col_weight.get(c, 1))


def main():
    app = App()
    app.mainloop()
