"""Applies one or more project cards to a base scenario and writes the resulting build scenario.

Usage:
    python apply_projects.py sf_demo

Looks up <name>.yml in the build_configs/ directory at the repo root.
"""
import argparse
from pathlib import Path

import yaml
from network_wrangler import load_scenario
from projectcard import read_cards

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_CONFIGS_DIR = REPO_ROOT / "build_configs"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", help="Name of the build config (without .yml) in build_configs/.")
    return parser.parse_args(argv)


def _repo_path(value: str) -> Path:
    """Resolve a config path relative to REPO_ROOT if it isn't already absolute."""
    path = Path(value)
    return path if path.is_absolute() else REPO_ROOT / path


def main(argv=None):
    args = parse_args(argv)
    config_path = BUILD_CONFIGS_DIR / f"{args.name}.yml"
    config = yaml.safe_load(config_path.read_text())

    # Load the base scenario config
    scenario = load_scenario(str(_repo_path(config["scenario"])))

    # Read the project card configs
    project_cards = read_cards([str(_repo_path(p)) for p in config["project_cards"]])

    # Add projects to the scenario
    scenario.add_project_cards(project_cards.values())

    # Check the queue, then apply
    scenario.queued_projects
    scenario.apply_all_projects()
    scenario.applied_projects 

    # Write the resulting build scenario to a new directory
    scenario.write(
        str(_repo_path(config["out_dir"])),
        name=config["out_name"],
        overwrite=True,
        roadway_file_format=config.get("roadway_file_format", "geojson"),
        roadway_true_shape=True,
        transit_write=True,
        transit_file_format=config.get("transit_file_format", "txt"),
    )
    return scenario


if __name__ == "__main__":
    main()
