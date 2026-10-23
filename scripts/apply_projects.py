"""Applies one or more project cards to a base scenario and writes the resulting build scenario.

Usage:
    python apply_projects.py sf_demo
    python apply_projects.py sf_demo --diff "I-80 E bound Bay Bridge road diet (dummy project)"

Looks up <name>.yml in the build_configs/ directory at the repo root.
"""
import argparse

from build_config import get_diffs_directory, load_build_config, resolve_repo_path
from network_wrangler import load_scenario
from network_wrangler.reporting import apply_project_with_diff
from projectcard import read_cards


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", help="Name of the build config (without .yml) in build_configs/.")
    parser.add_argument(
        "--diff",
        metavar="PROJECT_NAME",
        help="Name of a project (as it appears in the project card's 'project' field) to apply "
        "individually and write a before/after QA/QC diff for, written to "
        "<out_dir>/diffs/<project_name>/.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    config = load_build_config(args.name)

    # Load the base scenario config
    scenario = load_scenario(str(resolve_repo_path(config["scenario"])))

    # Read the project card configs
    project_cards = read_cards([str(resolve_repo_path(p)) for p in config["project_cards"]])

    # Add projects to the scenario
    scenario.add_project_cards(project_cards.values())

    # Apply the named project individually first and write its before/after diff, so the
    # queue computed below no longer includes it (avoiding re-applying it a second time).
    if args.diff:
        diff_name = args.diff.lower().replace(" ", "_")
        diff_out_dir = get_diffs_directory(config) / diff_name
        apply_project_with_diff(scenario, args.diff, diff_out_dir)

    # Check the queue, then apply
    scenario.queued_projects
    scenario.apply_all_projects()
    scenario.applied_projects 

    # Write the resulting build scenario to a new directory
    scenario.write(
        str(resolve_repo_path(config["out_dir"])),
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
