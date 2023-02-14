from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from functools import cached_property
import json

from shapely.geometry import Point, shape


@dataclass
class Basin:
    geometry: shape
    id: str
    name: str

    @classmethod
    def from_feature(cls, feature) -> Basin:
        name = feature['properties']['name']
        id = feature['properties']['id']
        geometry = shape(feature['geometry'])
        return cls(geometry=geometry, name=name, id=id)


class BasinIterator:
    def __init__(self, geojson_file: Path) -> None:
        self._geojson_file = geojson_file
        self._index = 0

    def __iter__(self) -> BasinIterator:
        return self

    def __len__(self) -> int:
        return len(self.features)

    def __next__(self) -> Basin:
        if self._index + 1 == len(self):
            raise StopIteration()
        self._index = self._index + 1
        return Basin.from_feature(self.features[self._index])

    @cached_property
    def features(self) -> list[dict]:
        with open(self._geojson_file, 'r') as infile:
            return json.load(infile).get('features', [])