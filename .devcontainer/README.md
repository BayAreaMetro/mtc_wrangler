# Development Container Setup

This repository includes a development container configuration that provides a consistent development environment using Docker and VS Code.

## What's Included

- **Base Environment**: Ubuntu with Miniconda3
- **Conda Environment**: Automatically created from `environment.yaml`
- **VS Code Extensions**: 
  - Python support with debugging
  - Jupyter notebooks
  - Code formatting (Black, Ruff)
  - YAML and JSON support
  - Git integration
- **Pre-configured Settings**: Python interpreter path, kernel selection, etc.

## Getting Started

### Using GitHub Codespaces

1. Navigate to your repository on GitHub
2. Click the green "Code" button
3. Select "Codespaces" tab
4. Click "Create codespace on [branch-name]"
5. Wait for the environment to build (first time takes ~5-10 minutes)
6. Start coding! The conda environment will be automatically activated.

### Using VS Code with Dev Containers Extension

1. Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
2. Clone this repository locally
3. Open the repository in VS Code
4. When prompted, click "Reopen in Container" (or use Command Palette: "Dev Containers: Reopen in Container")
5. Wait for the container to build
6. Start developing!

## Environment Details

- **Python Version**: 3.10.18
- **Conda Environment Name**: `network_wrangler`
- **Key Packages**: 
  - network-wrangler
  - geopandas
  - osmnx
  - matplotlib
  - jupyter
  - pandas
  - And many more (see `environment.yaml`)

## Customization

To modify the development environment:

1. **Add Python packages**: Update `environment.yaml` and rebuild the container
2. **Add VS Code extensions**: Modify `.devcontainer/devcontainer.json`
3. **Change system packages**: Update `.devcontainer/Dockerfile`

## Troubleshooting

- **Container won't build**: Check Docker is running and you have sufficient disk space
- **Python packages missing**: Rebuild the container after updating `environment.yaml`
- **Permissions issues**: The container runs as user `vscode` with sudo access

## Performance Tips

- Use the "consistency=cached" mount option for better performance on macOS
- Close unused browser tabs in Codespaces to preserve memory
- Large datasets should be stored in persistent volumes rather than the workspace
