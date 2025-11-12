"""MTC-specific network models and schemas.

This module provides MTC-customized versions of Network Wrangler classes
with additional validation for Bay Area transportation modeling requirements.

Classes:
    MTCRoadwayNetwork: Roadway network with MTC-specific validation
    MTCRoadLinksTable: Schema for MTC roadway links
    MTCRoadNodesTable: Schema for MTC roadway nodes

Example:
    ```python
    from mtc_wrangler.models import MTCRoadwayNetwork

    # Read network with MTC validation
    net = MTCRoadwayNetwork.read(
        link_file="links.geojson",
        node_file="nodes.geojson"
    )

    # Ensure all required MTC fields are present
    assert 'county' in net.links_df.columns
    assert 'jurisdiction' in net.links_df.columns
    assert 'mtc_facility_type' in net.links_df.columns
    ```
"""

from .mtc_network import MTCRoadwayNetwork
from .mtc_roadway_schema import MTCRoadLinksTable, MTCRoadNodesTable

__all__ = [
    "MTCRoadwayNetwork",
    "MTCRoadLinksTable",
    "MTCRoadNodesTable",
]
