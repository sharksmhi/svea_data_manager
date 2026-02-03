import logging
from pathlib import Path

import click

from svea_data_manager import SveaDataManager


@click.group()
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="WARNING",
)
def cli(log_level: str):
    logging.basicConfig(level=log_level.upper())


def with_data_target_args(func):
    func = click.argument("TARGET", type=click.Path(path_type=Path))(func)
    func = click.argument("DATA", type=click.Path(exists=True, path_type=Path))(func)
    return func


@cli.command()
@with_data_target_args
def adcp(data: Path, target: Path):
    target.mkdir(parents=True, exist_ok=True)
    config = {"ADCP": {"source_directory": data, "target_directory": target}}
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


@cli.command()
@with_data_target_args
def ctd(data: Path, target: Path):
    target.mkdir(parents=True, exist_ok=True)
    config = {"CTD": {"source_directory": data, "target_directory": target}}
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


@cli.command()
@with_data_target_args
def ferrybox(data: Path, target: Path):
    target.mkdir(parents=True, exist_ok=True)
    config = {"Ferrybox": {"source_directory": data, "target_directory": target}}
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


@cli.command()
@with_data_target_args
def ifcb(data: Path, target: Path):
    target.mkdir(parents=True, exist_ok=True)
    config = {"IFCB": {"source_directory": data, "target_directory": target}}
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


@cli.command()
@with_data_target_args
def ifcbclassification(data: Path, target: Path):
    target.mkdir(parents=True, exist_ok=True)
    config = {
        "IFCBCLASSIFICATION": {"source_directory": data, "target_directory": target}
    }
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


@cli.command()
@with_data_target_args
def mvp(data: Path, target: Path):
    target.mkdir(parents=True, exist_ok=True)
    config = {"MVP": {"source_directory": data, "target_directory": target}}
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


if __name__ == "__main__":
    cli()
