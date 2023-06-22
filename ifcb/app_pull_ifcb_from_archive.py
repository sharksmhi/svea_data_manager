import pathlib
import sys
import yaml

import flet as ft

from ifcb import archive

FROZEN = False
if getattr(sys, 'frozen', False):
    APP_DIRECTORY = pathlib.Path(sys.executable).parent
    FROZEN = True
else:
    APP_DIRECTORY = pathlib.Path(__file__).parent

PULL_IFCB_CONFIG_PATH = APP_DIRECTORY / 'pull_ifcb_config.yaml'

"""
493548
4B4E6D
6A8D92
80B192
A1E887
"""

MAIN_WINDOW_BG_COLOR = '#493548'
CONFIG_BG_COLOR = '#A1E887'
INSTRUMENT_COLOR = '#6A8D92'
ARCHIVE_COLOR = '#80B192'


def get_banner_color(status='bad'):
    if status == 'bad':
        return 'red'
    return 'green'


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

        self.app = ft.app(target=self.main)

    def main(self, page):
        self.page = page
        self.page.bgcolor = MAIN_WINDOW_BG_COLOR
        self.page.title = 'Svea Data Manager: Hämta IFCB resultat från arkivet'
        self._set_config()
        self._build()
        self._update_page()

    def _update_page(self):
        self.page.update()

    @property
    def config(self):
        return self._config

    @property
    def archive_root(self):
        return pathlib.Path(self.config['archive_root'])

    @property
    def instrument(self):
        return self._instrument.value or None

    @property
    def archive(self):
        return self._archive.value or None

    @property
    def target_directory(self):
        return self._target_directory.value or None

    @property
    def instruments(self):
        return [path.name for path in self.archive_root.iterdir() if path.is_dir()]

    def _set_config(self):
        self._load_config()
        if self._config:
            return
        if not FROZEN:
            raise Exception('No config file found')
        self._config['archive_root'] = APP_DIRECTORY

    def _load_config(self):
        print(f'{PULL_IFCB_CONFIG_PATH=}')
        if not PULL_IFCB_CONFIG_PATH.exists():
            print('No config file found')
            return
        with open(PULL_IFCB_CONFIG_PATH) as fid:
            self._config = yaml.safe_load(fid)

    def _set_banner(self, color):
        self.banner_content = ft.Column()

        self.page.banner = ft.Banner(
            # bgcolor=ft.colors.AMBER_100,
            bgcolor=color,
            leading=ft.Icon(ft.icons.WARNING_AMBER_ROUNDED, color=ft.colors.AMBER, size=40),
            content=self.banner_content,
            force_actions_below=True,
            actions=[
                ft.TextButton("OK!", on_click=self._close_banner),
            ],
        )

    def _show_info(self, text, status='bad'):
        self._set_banner(get_banner_color(status))
        self.banner_content.controls = [ft.Text(text)]
        self._show_banner()

    def _close_banner(self, e=None):
        self.page.banner.open = False
        self._update_page()

    def _show_banner(self, e=None):
        self.page.banner.open = True
        self._update_page()

    def _build(self):
        self._config_row = ft.Row()
        self._instrument_row = ft.Row(alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
        self._archive_row = ft.Row(alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
        self._target_row = ft.Row()

        self._target_directory_picker = ft.FilePicker(on_result=self._on_pick_target_dir)
        self.page.overlay.append(self._target_directory_picker)

        padding = 10
        self.config_container = ft.Container(content=self._config_row,
                                             bgcolor=CONFIG_BG_COLOR,
                                             border_radius=20,
                                             padding=padding)

        self.instrument_container = ft.Container(content=self._instrument_row,
                                                 bgcolor=INSTRUMENT_COLOR,
                                                 border_radius=20,
                                                 padding=padding,
                                                 expand=False)

        self.archive_container = ft.Container(content=self._archive_row,
                                              bgcolor=ARCHIVE_COLOR,
                                              border_radius=20,
                                              padding=padding,
                                              expand=False)

        self.target_container = ft.Container(content=self._target_row,
                                              bgcolor=ARCHIVE_COLOR,
                                              border_radius=20,
                                              padding=padding,
                                              expand=False)

        self.page.controls.append(self.config_container)
        self.page.controls.append(self.instrument_container)
        self.page.controls.append(self.archive_container)
        self.page.controls.append(self.target_container)

        self._build_config_row()
        self._build_instrument_row()
        self._build_archive_row()
        self._build_target_row()

    def _build_config_row(self):
        self._config_row.controls.append(ft.Text('Rotkatalog för IFCB-arkivet:'))
        self._config_row.controls.append(ft.Text(self.config['archive_root']))

    def _build_instrument_row(self):
        options = []
        for inst in self.instruments:
            options.append(ft.dropdown.Option(inst))

        self._instrument = ft.Dropdown(
            # width=100,
            options=options,
            on_change=self._on_select_instrument
        )

        self._instrument_row.controls.append(ft.Text('Välj instrument i arkivet:'))
        self._instrument_row.controls.append(self._instrument)

    def _build_archive_row(self):
        self._archive = ft.Dropdown(
            # width=100,
        )

        self._archive_row.controls.append(ft.Text('Välj resultatkörning i arkivet:'))
        self._archive_row.controls.append(self._archive)

    def _build_target_row(self):

        dir_btn = ft.ElevatedButton('Välj var du vill lägga ditt arkiv', on_click=lambda _:
                                    self._target_directory_picker.get_directory_path(
                                        dialog_title='Var vill du lägga arkivet?'
                                    ))

        self._target_directory = ft.Text()

        self._pull_archive_btn = ft.ElevatedButton('Hämta arkiv!', on_click=self._pull_archive)
        self._toggle_buttons.append(dir_btn)
        self._toggle_buttons.append(self._pull_archive_btn)

        path_row = ft.Row()
        path_row.controls.append(ft.Text('Arkivet kommer läggas här:'))
        path_row.controls.append(self._target_directory)

        col = ft.Column()
        col.controls.append(dir_btn)
        col.controls.append(path_row)
        col.controls.append(self._pull_archive_btn)

        self._target_row.controls.append(col)

    def _disable_toggle_buttons(self):
        for btn in self._toggle_buttons:
            btn.disabled = True
            btn.update()

    def _enable_toggle_buttons(self):
        for btn in self._toggle_buttons:
            btn.disabled = False
            btn.update()

    def _pull_archive(self, *args):
        self._disable_toggle_buttons()
        try:
            if not self.instrument:
                self._show_info('Inget instrument valt')
                return
            if not self.archive:
                self._show_info('Inget arkiv valt')
                return
            if not self.target_directory:
                self._show_info('Ingen målmapp vald')
                return
            archive.pull_result_from_archive(
                archive_directory=self.archive_root,
                instrument=self.instrument,
                key=self.archive,
                target_directory=self.target_directory
            )
            self._show_info(f'Arkivet "{self.archive} finns nu under mappen "{self.target_directory}"',
                            status='good')
        except FileExistsError as e:
            self._show_info(f'Filen/mappen finns redan: \n{e}')
        except Exception as e:
            self._show_info(f'Något gick fel: \n{e}')
        finally:
            self._enable_toggle_buttons()

    def _on_pick_target_dir(self, e: ft.FilePickerResultEvent):
        if not e.path:
            return
        self._target_directory.value = e.path
        self._target_directory.update()

    def _set_archive_options(self, archive_names):
        options = []
        for name in archive_names:
            options.append(ft.dropdown.Option(name))
        self._archive.options = options
        self._archive.update()

    def _on_select_instrument(self, *args):
        archive_list = archive.get_archive_results(archive_directory=self.archive_root,
                                                   instrument=self._instrument.value)
        self._set_archive_options(archive_list)


def main():
    app = FletApp()


if __name__ == '__main__':
    main()