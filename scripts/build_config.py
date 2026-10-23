"""Helpers shared by the scripts that read a build config from build_configs/."""

from pathlib import Path

import yaml

__all__ = [
    # Building blocks
    "resolve_repo_path",
    "load_build_config",
    # Derived locations
    "get_diffs_directory",
]

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_CONFIGS_DIR = REPO_ROOT / "build_configs"
# Subfolder of a build config's `out_dir` that project diffs are written to and read from.
DIFFS_SUBDIRECTORY = "diffs"


def resolve_repo_path(value: str) -> Path:
    """Turns a path from a build config into an absolute path.

    Args:
        value: path as written in a build config, either absolute or relative to the repo root.

    Returns:
        The path unchanged if it is already absolute, otherwise the path under the repo root.
    """
    path = Path(value)
    return path if path.is_absolute() else REPO_ROOT / path


def load_build_config(name: str) -> dict:
    """Reads a build config from the build_configs/ folder.

    Args:
        name: name of the build config without the `.yml` extension, e.g. `sf_demo`.

    Returns:
        The parsed build config.

    Raises:
        ValueError: If there is no build config with that name.
    """
    config_path = BUILD_CONFIGS_DIR / f"{name}.yml"
    if not config_path.is_file():
        available_names = sorted(path.stem for path in BUILD_CONFIGS_DIR.glob("*.yml"))
        msg = f"Build config not found: {config_path}. Available build configs: {available_names}"
        raise ValueError(msg)
    return yaml.safe_load(config_path.read_text())


def get_diffs_directory(build_config: dict) -> Path:
    """Locates the folder that holds a build config's project diffs.

    Args:
        build_config: build config as returned by `load_build_config()`. Must have `out_dir`.

    Returns:
        Absolute path to `<out_dir>/diffs`, which contains one subfolder per project diff.

    See Also:
        load_build_config: reads the build config this function expects.
    """
    return resolve_repo_path(build_config["out_dir"]) / DIFFS_SUBDIRECTORY
