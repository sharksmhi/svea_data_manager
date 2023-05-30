import datetime
import logging
import logging.handlers
import os
import pathlib
import shutil
import sys
import traceback
import subprocess

import flet as ft
import yaml

from svea_data_manager import SveaDataManager
from svea_data_manager.sdm_logger import SDMLogger
from svea_data_manager import sdm_event
from svea_data_manager.sdm_event import subscribe

logger = logging.getLogger(__name__)

if getattr(sys, 'frozen', False):
    DIRECTORY = pathlib.Path(sys.executable).parent
else:
    DIRECTORY = pathlib.Path(__file__).parent

DEFAULT_CONFIG_SAVE_PATH = pathlib.Path(DIRECTORY, 'default_config')

# https://colorpalettes.net/color-palette-4553/
INSTRUMENT_SECTION_BG_COLOR = '#DFE8CC'
CONFIG_BG_COLOR = '#DAE2B6'
DEFAULT_INSTRUMENT_BG_COLOR = '#CCD6A6'
ATTRIBUTES_COLOR = '#F7EDDB'

MISSING_CONFIG_TEXT = '< Ingen konfigurationsfil vald >'
MISSING_DATA_ROOT_TEXT = '< Ingen rotkatalog för källdata vald >'

# INSTRUMENT_BG_COLORS = {
#     'ifcb': '#75ff75',
#     'adcp': '#ff96fc',
#     'ctd': '#8ce9fa',
#     'mvp': '#8c91fa',
#     'ferrybox': '#fa8c8c',
# }

# https://colorpalettes.net/color-palette-4573/
# INSTRUMENT_BG_COLORS = {
#     'ifcb': '#AEC670',
#     'adcp': '#AEC09A',
#     'ctd': '#778D45',
#     'mvp': '#344C11',
#     'ferrybox': '#1A2902',
# }

INSTRUMENT_BG_COLORS = {
    # 'ifcb': '#DFE8CC',
    # 'adcp': '#DFE8CC',
    # 'ctd': '#DFE8CC',
    # 'mvp': '#DFE8CC',
    # 'ferrybox': '#DFE8CC',
}

CLEANUP_LOG_AFTER_NR_DAYS = 7


def get_instrument_bg_color(inst):
    return INSTRUMENT_BG_COLORS.get(inst.lower(), DEFAULT_INSTRUMENT_BG_COLOR)


DISABLED_ATTRIBUTES = [
    'ship'
]


TRANSLATE = {
    'source_directory': 'Källmapp',
    'target_directory': 'Målmapp',
    'ship': 'Fartyg',
    'cruise': 'Cruise',
    'comment': 'Kommentar'
}


def translate(text):
    return TRANSLATE.get(text, text)


def load_default_config():
    if not DEFAULT_CONFIG_SAVE_PATH.exists():
        return None
    with open(DEFAULT_CONFIG_SAVE_PATH) as fid:
        path = fid.readline().strip()
        if not path:
            return None
        return pathlib.Path(path)


def save_default_config(path):
    with open(DEFAULT_CONFIG_SAVE_PATH, 'w') as fid:
        fid.write(str(path))


class FletApp:
    def __init__(self):
        self.page = None
        self.file_picker = None
        self._config = {}
        self._attributes = {}
        self._progress_bars = {}
        self._progress_texts = {}
        self._instrument_items = {}
        self._current_source_instrument = None

        self._toggle_buttons = []

        sdm_event.subscribe('after_write_packages', self._on_archiving_finished)

        self.logging_level = 'DEBUG'
        self.logging_format = '%(asctime)s [%(levelname)10s]    %(pathname)s [%(lineno)d] => %(funcName)s():    %(message)s'
        self.logging_format_stdout = '[%(levelname)10s] %(filename)s: %(funcName)s() [%(lineno)d] %(message)s'
        self._setup_logger()

        self._logger = SDMLogger(report_directory=self._report_directory)

        subscribe('on_progress', self._callback_on_progress)

        self._cleanup_reports()

        self.app = ft.app(target=self.main)

    def main(self, page):
        self.page = page
        self.page.title = 'Svea Data Manager: Making your data handling easier!'
        self._add_report_bottom_sheet()
        self._build()
        self._initiate_banner()

    def _initiate_banner(self):
        self.banner_content = ft.Column()

        self.page.banner = ft.Banner(
            bgcolor=ft.colors.AMBER_100,
            leading=ft.Icon(ft.icons.WARNING_AMBER_ROUNDED, color=ft.colors.AMBER, size=40),
            content=self.banner_content,
            actions=[
                ft.TextButton("OK!", on_click=self._close_banner),
            ],
        )

    def _close_banner(self, e=None):
        self.page.banner.open = False
        self.page.update()

    def _show_banner(self, e=None):
        self.page.banner.open = True
        self.page.update()

    def _add_report_bottom_sheet(self):
        self._report_container = ft.Container(padding=10, expand=True)
        self._report_bottom_sheet = ft.BottomSheet(
            self._report_container,
            open=False,
            on_dismiss=self._on_dismiss_report_bottom_sheet,
        )
        self.page.overlay.append(self._report_bottom_sheet)

    def _on_dismiss_report_bottom_sheet(self, e):
        pass

    def _build(self):
        self._config_row = ft.Row()
        self._root_source_row = ft.Row()
        self._instrument_listview = ft.ListView(expand=1, spacing=10, padding=20, auto_scroll=False)

        padding = 10
        self.config_container = ft.Container(content=self._config_row,
                                             bgcolor=CONFIG_BG_COLOR,
                                             border_radius=20,
                                             padding=padding)

        self.root_source_container = ft.Container(content=self._root_source_row,
                                             bgcolor=DEFAULT_INSTRUMENT_BG_COLOR,
                                             border_radius=20,
                                             padding=padding)

        self.instrument_container = ft.Container(content=self._instrument_listview,
                                                 bgcolor=INSTRUMENT_SECTION_BG_COLOR,
                                                 padding=padding,
                                                 border_radius=20,
                                                 expand=True)

        self.page.controls.append(self.config_container)
        self.page.controls.append(self.root_source_container)
        self.page.controls.append(self.instrument_container)

        self._pick_source = ft.FilePicker(on_result=self._on_pick_source_dir)
        self._pick_source_root = ft.FilePicker(on_result=self._on_pick_source_root_dir)
        self.page.overlay.append(self._pick_source)
        self.page.overlay.append(self._pick_source_root)

        self._build_select_config()
        self._build_select_root_source()
        self._set_default_config()
        self.update_page()

    def update_page(self):
        self.page.update()

    def _setup_logger(self, **kwargs):
        name = 'sdm'
        # self.logger = logging.getLogger(name)
        self.logger = logging.getLogger()
        self.logger.setLevel(self.logging_level)

        debug_file_path = pathlib.Path(self._log_directory, f'{name}_debug.log')
        handler = logging.handlers.TimedRotatingFileHandler(str(debug_file_path), when='H', interval=3, backupCount=10)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(self.logging_format)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        debug_file_path = pathlib.Path(self._log_directory, f'{name}_warning.log')
        handler = logging.handlers.TimedRotatingFileHandler(str(debug_file_path), when='D', interval=1, backupCount=14)
        handler.setLevel(logging.WARNING)
        formatter = logging.Formatter(self.logging_format)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def _cleanup_reports(self):
        self._logger.cleanup_reports(nr_days_old=CLEANUP_LOG_AFTER_NR_DAYS)
        # now = datetime.datetime.now()
        # before_time = now - datetime.timedelta(days=CLEANUP_LOG_AFTER_NR_DAYS)
        # for path in self._report_directory.iterdir():
        #     if datetime.datetime.fromtimestamp(path.stat().st_mtime) > before_time:
        #         continue
        #     if path.is_file():
        #         os.remove(str(path))
        #     else:
        #         shutil.rmtree(path)

    @property
    def _report_directory(self):
        path = pathlib.Path(DIRECTORY, 'reports')
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def _log_directory(self):
        path = pathlib.Path(DIRECTORY, 'logs')
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _build_select_config(self):

        file_picker = ft.FilePicker(on_result=self._pick_config_file)
        self.page.overlay.append(file_picker)
        btn = ft.ElevatedButton("Välj konfigurationsfil", on_click=lambda _: file_picker.pick_files(
            allow_multiple=False))

        self._config_path = ft.Text(MISSING_CONFIG_TEXT)

        self._config_row.controls.append(btn)
        self._config_row.controls.append(self._config_path)

    def _build_select_root_source(self):
        btn = ft.ElevatedButton("Välj rotkatalog för data", on_click=self._pick_source_root_dir)

        self._data_root_directory = ft.Text(MISSING_DATA_ROOT_TEXT)

        self._root_source_row.controls.append(btn)
        self._root_source_row.controls.append(self._data_root_directory)

    def _set_config_file(self, text=None):
        if not text:
            return
        self._config_path.value = text
        self._load_config_file()
        self._update_gui_from_config()
        self.update_page()
        save_default_config(text)

    def _set_default_config(self):
        """Search the current directory and loads the first config file found"""
        path = load_default_config()
        if path:
            self._set_config_file(str(path))
            return
        for path in pathlib.Path(DIRECTORY).iterdir():
            if path.name.startswith('config') and path.suffix == '.yaml':
                self._set_config_file(str(path))

    def _pick_config_file(self, e: ft.FilePickerResultEvent):
        if not e.files:
            return
        path = e.files[0].path
        self._set_config_file(path)

    def _pick_source_dir(self, inst):
        self._current_source_instrument = inst
        self._pick_source.get_directory_path()

    def _on_pick_source_dir(self, e: ft.FilePickerResultEvent):
        if not e.path:
            return
        self._instrument_items[self._current_source_instrument]['source_directory'].value = e.path
        self.update_page()
        self._current_source_instrument = None

    def _load_config_file(self):
        self._config = {}
        self._attributes = {}
        self._progress_bars = {}
        self._instrument_items = {}

        path = self._config_path.value.strip()
        if not path:
            return
        with open(path) as fid:
            config = yaml.load(fid, Loader=yaml.SafeLoader)
        for key, item in config.items():
            self._config[key.lower()] = item

    def _update_gui_from_config(self):
        self._toggle_buttons = []
        self._instrument_listview.controls = []
        btn = ft.ElevatedButton(text=f'Arkivera data från alla instrument',
                                                  on_click=self._archive_all_data)
        self._toggle_buttons.append(btn)
        self._instrument_listview.controls.append(btn)
        if not self._config:
            return
        for key, value in self._config.items():
            self._add_instrument_section(key, value)

    def _add_instrument_section(self, instrument: str, config: dict):
        self._instrument_items[instrument] = {}
        padding = 10
        inst_col = ft.Column()
        inst_col.controls.append(ft.Text(instrument.upper()))
        for key, value in config.items():
            if key == 'attributes':
                continue
            if not value:
                value = ''
            row = ft.Row()
            if key == 'source_directory':
                btn = ft.ElevatedButton(
                    translate(key),
                    # icon=ft.icons.UPLOAD_FILE,
                    on_click=lambda e, inst=instrument: self._pick_source_dir(inst))
                self._toggle_buttons.append(btn)
                row.controls.append(btn)
                self._instrument_items[instrument][key] = ft.Text(value or '')
                row.controls.append(self._instrument_items[instrument][key])
            elif type(value) == bool:
                cb = ft.Checkbox(label=key, value=value)
                self._instrument_items[instrument][key] = cb
                row.controls.append(cb)
            else:
                row.controls.append(ft.Text(translate(key)))
                row.controls.append(ft.Text(value))
            inst_col.controls.append(row)
        attributes = config.get('attributes')
        if attributes:
            self._add_attributes_container(parent=inst_col.controls, instrument=instrument, attributes=attributes)
        run_row = ft.Row()
        btn = ft.ElevatedButton(text=f'Arkivera {instrument}-data',
                                                  on_click=lambda e, inst=instrument: self._archive_data(inst))
        self._toggle_buttons.append(btn)
        run_row.controls.append(btn)
        pbar = ft.ProgressBar(width=400, value=0)
        run_row.controls.append(pbar)

        progress_text = ft.Text('')
        run_row.controls.append(progress_text)

        self._progress_bars[instrument.upper()] = pbar
        self._progress_texts[instrument.upper()] = progress_text
        inst_col.controls.append(run_row)

        container = ft.Container(content=inst_col,
                                 bgcolor=get_instrument_bg_color(instrument),
                                 padding=padding,
                                 border_radius=20,)
        self._instrument_listview.controls.append(container)

    def _pick_source_root_dir(self, e: ft.FilePickerResultEvent):
        self._pick_source_root.get_directory_path()

    def _on_pick_source_root_dir(self, e: ft.FilePickerResultEvent):
        if not e.path:
            text = MISSING_DATA_ROOT_TEXT
            self._data_root_directory.value = text
            return
        self._data_root_directory.value = e.path
        self._update_all_source_dirs()
        self.update_page()

    def _update_all_source_dirs(self):
        if self._data_root_directory.value == MISSING_DATA_ROOT_TEXT:
            return
        root = pathlib.Path(self._data_root_directory.value)
        subdirs = dict((path.name.upper(), path) for path in root.iterdir())
        for name, inst in self._instrument_items.items():
            path = subdirs.get(name.upper())
            if not path:
                continue
            self._instrument_items[name]['source_directory'].value = str(path)

    def _add_attributes_container(self, parent: list, instrument: str, attributes: dict):
        padding = 10
        self._attributes.setdefault(instrument, {})
        attr_col = ft.Column()
        for key, value in attributes.items():
            if not value:
                value = ''
            attr = ft.TextField(label=translate(key))
            attr.value = str(value)
            if key.lower() in DISABLED_ATTRIBUTES:
                attr.disabled = True
            attr_col.controls.append(attr)
            self._attributes[instrument][key] = attr

        container = ft.Container(content=attr_col,
                                 bgcolor=ATTRIBUTES_COLOR,
                                 padding=padding,
                                 border_radius=10)
        parent.append(container)

    def _update_config_items(self):
        for inst, items in self._instrument_items.items():
            for key, widget in items.items():
                self._config[inst][key] = widget.value

    def _update_config_attributes(self):
        """Adds attributes in GUI to config"""
        for inst, data in self._attributes.items():
            for key, widget in data.items():
                self._config[inst]['attributes'][key] = widget.value

    def _archive_all_data(self, e):
        self._archive_data()

    def _archive_data(self, inst=None):
        self._disable_toggle_buttons()
        self._close_banner()
        self._update_config_items()
        self._update_config_attributes()
        config = self._config.copy()
        if inst:
            config = {inst: self._config[inst]}
        config = {inst: config[inst] for inst in config if config[inst]['source_directory']}
        logger.info(f'Running instruments: {",".join(list(config))}')
        if not config:
            self._show_info('Du har inte angivig källmapp för något instrument!')
            return
        self._progress_bars[inst.upper()].value = 0.1
        self.update_page()
        sdm = SveaDataManager.from_config(config)
        print('Reading')
        sdm.read_packages()
        print('Transforming')
        sdm.transform_packages()
        print('Writing')
        sdm.write_packages()
        report_dir = self._write_report()
        self._show_result_ok(report_dir)

        for ins in config:
            self._progress_bars[ins.upper()].value = 0
            self._progress_texts[ins.upper()].value = 'Allt klart!'
        self.update_page()

    def _disable_toggle_buttons(self):
        for btn in self._toggle_buttons:
            btn.disabled = True

    def _enable_toggle_buttons(self):
        for btn in self._toggle_buttons:
            btn.disabled = False

    def _write_report(self):
        report_dir = self._logger.write_reports(self._report_directory)
        return report_dir

    def _get_result_info(self, logger_info, bad_color_if_nr=False):
        ok_color = 'black'
        bad_color = 'red'

        lst = []
        for inst, nr in logger_info.items():
            item = (f'{inst}: {nr}', bad_color if nr and bad_color_if_nr else ok_color)
            lst.append(item)
        return lst

    def _show_info(self, text):
        self.banner_content.controls = [ft.Text(text)]
        self._show_banner()

    def _show_result_ok(self, report_dir):
        nr_accepted_str = self._get_result_info(self._logger.get_nr_resources_added())
        nr_rejected_str = self._get_result_info(self._logger.get_nr_resources_rejected(), bad_color_if_nr=True)
        nr_transformed_str = self._get_result_info(self._logger.get_nr_transform_added_files())
        nr_copied_str = self._get_result_info(self._logger.get_nr_files_copied())
        nr_svn_prepared = self._get_result_info(self._logger.get_nr_svn_prepared())
        nr_not_copied_str = self._get_result_info( self._logger.get_nr_target_path_exists(), bad_color_if_nr=True)

        lv = ft.ListView()
        button_row = ft.Row()
        button_row.controls.append(ft.ElevatedButton('Öppna rapportmapp',
                                                     on_click=lambda e,
                                                                     directory=report_dir: self._open_report_directory(
                                                         directory)))
        button_row.controls.append(ft.ElevatedButton('OK',
                                                     on_click=self._close_report_bottom_sheet))
        # lv.controls.append(button_row)  # This does not work in executable. Opens new instance instead

        ok_color = 'black'
        bad_color = 'red'
        info_lst = []
        info_lst.append((f'Antal filer som hanterats:', ok_color))
        info_lst.extend(nr_accepted_str)
        info_lst.append(('', ok_color))
        info_lst.append((f'Antal filer som inte hanterats', ok_color))
        info_lst.extend(nr_rejected_str)
        info_lst.append(('', ok_color))
        info_lst.append((f'Antal filer som lagts till under prosessen:', ok_color))
        info_lst.extend(nr_transformed_str)
        info_lst.append(('', ok_color))
        info_lst.append((f'Antal filer som förberetts för svn:', ok_color))
        info_lst.extend(nr_svn_prepared)
        info_lst.append(('', ok_color))
        info_lst.append((f'Antal filer som kopierats:', ok_color))
        info_lst.extend(nr_copied_str)
        info_lst.append(('', ok_color))
        info_lst.append((f'Antal filer som inte kopierats:', ok_color))
        info_lst.extend(nr_not_copied_str)
        info_lst.append(('', ok_color))
        info_lst.append((f'Se fullständig rapport under: {report_dir}', ok_color))

        for info in info_lst:
            lv.controls.append(ft.Text(f'{info[0]}\n', color=info[1]))

        button_row = ft.Row()
        button_row.controls.append(ft.ElevatedButton('Öppna rapportmapp',
                                                     on_click=lambda e,
                                                                     directory=report_dir: self._open_report_directory(
                                                         directory)))
        button_row.controls.append(ft.ElevatedButton('OK',
                                                     on_click=self._close_report_bottom_sheet))
        # lv.controls.append(button_row) # This does not work in executable. Opens new instance instead
        self._report_container.content = lv
        self._report_bottom_sheet.open = True
        self._report_bottom_sheet.update()
        self._logger.reset()

    def old__show_result_ok(self, report_dir):
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

        lv = ft.ListView()
        ok_color = 'black'
        bad_color = 'red'
        info_lst = [
             (f'Antal filer som hanterats: \n{nr_accepted_str}', ok_color),
             (f'Antal filer som inte hanterats: \n{nr_rejected_str}', bad_color if nr_rejected_str else ok_color),
             (f'Antal filer som lagts till under prosessen: \n{nr_transformed_str}', ok_color),
             (f'Antal filer som kopierats: \n{nr_copied_str}', ok_color),
             (f'Antal filer som inte kopierats: \n{nr_not_copied_str}', bad_color if nr_not_copied_str else ok_color),
             (f'Se fullständig rapport under: {report_dir}.', ok_color)
        ]
        for info in info_lst:
            lv.controls.append(ft.Text(f'{info[0]}\n', color=info[1]))

        button_row = ft.Row()
        button_row.controls.append(ft.ElevatedButton('Öppna rapportmapp',
                                                     on_click=lambda e,
                                                                     directory=report_dir: self._open_report_directory(
                                                         directory)))
        button_row.controls.append(ft.ElevatedButton('OK',
                                                     on_click=self._close_report_bottom_sheet))
        lv.controls.append(button_row)
        self._report_container.content = lv
        self._report_bottom_sheet.open = True
        self._report_bottom_sheet.update()
        self._logger.reset()

    def _show_result_except(self, ex):
        cont = ft.Container(bgcolor='red', padding=10)
        col = ft.Column(expand=True)
        cont.content = col
        col.controls.append(ft.Text('Något gick fel!'))
        lv = ft.ListView()
        col.controls.append(lv)
        lv.controls.append(f'{ex}\n{traceback.format_exc()}')
        logger.critical(traceback.format_exc())
        self._report_container.content = cont
        self._open_report_bottom_sheet()
        self._logger.reset()

    def _open_report_bottom_sheet(self, *args):
        self._report_bottom_sheet.open = True
        self._report_bottom_sheet.update()

    def _close_report_bottom_sheet(self, *args):
        self._report_bottom_sheet.open = False
        self._report_bottom_sheet.update()

    def _open_report_directory(self, report_dir):
        print(f'{report_dir=}')
        # subprocess.Popen(f'explorer /select,"{report_dir}"')
        import os
        os.system(f'start {report_dir}')

    def _callback_on_progress(self, data):
        pbar = self._progress_bars.get(data['instrument'].upper())
        text = self._progress_texts.get(data['instrument'].upper())
        if not pbar:
            return
        pbar.value = data['percentage'] / 100
        text.value = data.get('msg', '')
        self.update_page()

    # def _callback_file_storage(self, data):
    #     pbar = self._progress_bars.get(data['instrument'].upper())
    #     if not pbar:
    #         return
    #     pbar.value = data['nr_files_copied'] / data['nr_files_total']
    #     self.update_page()
    #
    # def _callback_svn_prepared(self, data):
    #     pbar = self._progress_bars.get(data['instrument'].upper())
    #     if not pbar:
    #         return
    #     pbar.value = data['nr_files_copied'] / data['nr_files_total']
    #     self.update_page()

    def _on_archiving_finished(self, data):
        self._enable_toggle_buttons()


def main():
    app = FletApp()


if __name__ == '__main__':
    main()

