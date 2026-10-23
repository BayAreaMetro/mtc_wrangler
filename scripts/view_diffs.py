"""Opens the project diff viewer for a build config.

Usage:
    python view_diffs.py <build config name>
    python view_diffs.py sf_demo

Looks up <name>.yml in the build_configs/ directory at the repo root, then opens the viewer on
<out_dir>/diffs, where `apply_projects.py <name> --diff "<project name>"` writes each project's
before/after diff.

Requires the `viewer` dependencies (streamlit, streamlit-folium) and a network_wrangler that
includes `network_wrangler.reporting.viewer`.
"""
import argparse
from pathlib import Path

from build_config import get_diffs_directory, load_build_config

__all__ = [
    # Building blocks
    "parse_args",
    "find_diffs_directory",
    # Entry point
    "main",
]


def parse_args(argv=None):
    """Parses the command line.

    Args:
        argv: command line arguments, excluding the program name. Defaults to `sys.argv[1:]`.

    Returns:
        Namespace with the build config `name`.
    """
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("name", help="Name of the build config (without .yml) in build_configs/.")
    return parser.parse_args(argv)


def find_diffs_directory(build_config_name: str) -> Path:
    """Locates the project diffs written for a build config.

    Args:
        build_config_name: name of the build config (without .yml) in build_configs/.

    Returns:
        Absolute path to the build config's `diffs` folder.

    Raises:
        ValueError: If the build config does not exist, or no diffs have been written for it yet.
    """
    diffs_directory = get_diffs_directory(load_build_config(build_config_name))
    if not diffs_directory.is_dir():
        msg = (
            f"No diffs found for build config '{build_config_name}': {diffs_directory} does not "
            f'exist. Write one with: python scripts\\apply_projects.py {build_config_name} '
            f'--diff "<project name>"'
        )
        raise ValueError(msg)
    return diffs_directory


def main(argv=None):
    """Opens the project diff viewer on a build config's diffs, until the viewer is stopped.

    Args:
        argv: command line arguments, excluding the program name. Defaults to `sys.argv[1:]`.

    Returns:
        None. Exits the process with the viewer's exit code.

    Raises:
        ValueError: If the build config does not exist, or no diffs have been written for it.
        ImportError: If the installed network_wrangler does not include the diff viewer.

    See Also:
        find_diffs_directory: locates the folder this function opens.
    """
    args = parse_args(argv)
    diffs_directory = find_diffs_directory(args.name)

    try:
        from network_wrangler.reporting.viewer.__main__ import main as launch_viewer
    except ImportError as error:
        msg = (
            "The installed network_wrangler does not include the diff viewer "
            "(network_wrangler.reporting.viewer). Install a version that does, e.g. an editable "
            "install of a network_wrangler checkout that has it: "
            "uv pip install -e <path to network_wrangler>"
        )
        raise ImportError(msg) from error

    launch_viewer([str(diffs_directory)])


if __name__ == "__main__":
    main()
