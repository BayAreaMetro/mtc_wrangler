# Use the official miniconda image as base
FROM continuumio/miniconda3:latest

# Set the working directory
WORKDIR /workspaces/mtc_wrangler

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    build-essential \
    gcc \
    g++ \
    make \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Create vscode user with sudo privileges
RUN groupadd --gid 1000 vscode \
    && useradd --uid 1000 --gid vscode --shell /bin/bash --create-home vscode \
    && echo vscode ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/vscode \
    && chmod 0440 /etc/sudoers.d/vscode

# Copy environment file
COPY ../environment.yaml /tmp/environment.yaml

# Create conda environment from environment.yaml
RUN conda env create -f /tmp/environment.yaml

# Make RUN commands use the new environment
SHELL ["conda", "run", "-n", "network_wrangler", "/bin/bash", "-c"]

# Initialize conda for bash
RUN conda init bash

# Set up conda environment activation in bashrc
RUN echo "conda activate network_wrangler" >> /home/vscode/.bashrc

# Change ownership of conda environments to vscode user
RUN chown -R vscode:vscode /opt/conda/

# Switch to vscode user
USER vscode

# Set the default command
CMD ["/bin/bash"]
