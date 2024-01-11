import math
import flet as ft


class TooltipTexts:

    @property
    def config_file(self):
        return 'No info'

    @property
    def root_directory(self):
        return 'No info'

    def source_directory(self, path):
        return f'Path is {path}'

    def archive_instrument(self, path):
        return f'Path is {path}'

    @property
    def default_attribute(self):
        return 'No info'

    @property
    def ship(self):
        return 'No info'

    @property
    def cruise(self):
        return 'No info'

    @property
    def config_file(self):
        return 'No info'


def get_tooltip_widget(msg):
    return ft.Tooltip(
        message=msg,
        padding=20,
        border_radius=10,
        text_style=ft.TextStyle(size=14, color=ft.colors.WHITE),
        gradient=ft.LinearGradient(
            begin=ft.alignment.top_left,
            end=ft.alignment.Alignment(0.8, 1),
            colors=[
                "0xff1f005c",
                "0xff5b0060",
                "0xff870160",
                "0xffac255e",
                "0xffca485c",
                "0xffe16b5c",
                "0xfff39060",
                "0xffffb56b",
            ],
            tile_mode=ft.GradientTileMode.MIRROR,
            rotation=math.pi / 3,
        ),
    )