# MTC Wrangler

MTC-specific network wrangling tools and utilities for Bay Area transportation modeling.

## Overview

This repository contains utilities and resources specific to wrangling networks for the Metropolitan Transportation Commission (MTC) networks. It extends [Network Wrangler](https://github.com/network-wrangler/network_wrangler) with MTC-specific schemas, validation, and workflows.

## Installation & Setup

For installation instructions and basic setup, please refer to the [Network Wrangler documentation](https://network-wrangler.github.io/network_wrangler/).

MTC Wrangler uses the [BayAreaMetro fork of Network Wrangler](https://github.com/BayAreaMetro/network_wrangler).

## Key Features

### MTC-Specific Network Models

The `models/` directory provides MTC-customized versions of Network Wrangler classes:

- **MTCRoadwayNetwork**: Extended roadway network with MTC-required fields
- **MTCRoadLinksTable**: Schema requiring `county`, `jurisdiction`, and `mtc_facility_type`
- **MTCRoadNodesTable**: Schema requiring `county` for all nodes

### Scripts

The `create_baseyear_network/` directory contains scripts for building MTC base year networks. See [create_baseyear_network/README.md](https://github.com/BayAreaMetro/mtc_wrangler/tree/main/create_baseyear_network) for details

## Quick Example

```python
from mtc_wrangler.models import MTCRoadwayNetwork

# Load network with MTC validation
net = MTCRoadwayNetwork.read(
    link_file="links.geojson",
    node_file="nodes.geojson"
)

# All MTC required fields are validated
print(net.links_df[['county', 'jurisdiction', 'mtc_facility_type']].head())
```

## Relevant Repositories

- [network-wrangler/network_wrangler](https://github.com/network-wrangler/network_wrangler)
- [BayAreaMetro/network_wrangler](https://github.com/BayAreaMetro/network_wrangler) - BayAreaMetro fork
- [network-wrangler/projectcard](https://github.com/network-wrangler/projectcard) - Project card infrastructure

## Documentation Structure

- **API Reference**: Detailed documentation of MTC models and schemas
