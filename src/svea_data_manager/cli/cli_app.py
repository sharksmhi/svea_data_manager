import logging
from pathlib import Path

import click

from svea_data_manager.manager import SveaDataManager


@click.group()
@click.option(
    "-l",
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
)
def cli(log_level: str):
    logging.basicConfig(level=log_level.upper())


def with_data_target_args_path(func):
    func = click.argument("TARGET", type=click.Path(path_type=Path))(func)
    func = click.argument("DATA", type=click.Path(exists=True, path_type=Path))(func)
    return func


def with_data_target_args_svn(func):
    func = click.argument("TARGET", type=str)(func)
    func = click.argument("DATA", type=click.Path(exists=True, path_type=Path))(func)
    func = click.option("-f", "--force", is_flag=True)(func)
    return func


@cli.command()
@with_data_target_args_path
def adcp(data: Path, target: Path):
    target.mkdir(parents=True, exist_ok=True)
    config = {"ADCP": {"source_directory": data, "target_directory": target}}
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


@cli.command()
@with_data_target_args_svn
def ctd(data: Path, target: str, force: bool):
    config = {
        "CTD": {"source_directory": data, "subversion_repo_url": target, "force": force}
    }
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


@cli.command()
@with_data_target_args_path
def ferrybox(data: Path, target: Path):
    target.mkdir(parents=True, exist_ok=True)
    config = {"Ferrybox": {"source_directory": data, "target_directory": target}}
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


@cli.command()
@with_data_target_args_path
def ifcb(data: Path, target: Path):
    target.mkdir(parents=True, exist_ok=True)
    config = {"IFCB": {"source_directory": data, "target_directory": target}}
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


@cli.command()
@with_data_target_args_path
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
@with_data_target_args_svn
def mvp(data: Path, target: str, force: bool):
    config = {
        "MVP": {"source_directory": data, "subversion_repo_url": target, "force": force}
    }
    sdm = SveaDataManager.from_config(config)
    sdm.read_packages()
    sdm.transform_packages()
    sdm.write_packages()


if __name__ == "__main__":
    cli()
