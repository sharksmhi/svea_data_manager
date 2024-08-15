import pathlib

import yaml
import flet as ft


class UserSettings:

    def __init__(self, settings_path: pathlib.Path) -> None:
        self._settings_path = settings_path
        self._settings = {}
        self._widgets: dict[str, ft.Control] = {}

        self._load_settings()

    def _load_settings(self):
        if not self._settings_path.exists():
            return
        with open(self._settings_path) as fid:
            self._settings = yaml.safe_load(fid)

    def add_widget(self, name: str, widget: ft.Control):
        self._widgets[name] = widget

    def apply_settings(self) -> None:
        for name, value in self._settings.items():
            widget = self._widgets.get(name)
            if not widget:
                continue
            widget.value = value
            widget.update()

    def save_settings(self) -> None:
        data = {}
        for name, widget in self._widgets.items():
            data[name] = widget.value

        with open(self._settings_path, 'w') as fid:
            yaml.safe_dump(data, fid)
