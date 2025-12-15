"""MTC-specific network models and schemas.

This module provides MTC-customized versions of Network Wrangler classes
with additional validation for Bay Area transportation modeling requirements.

Classes:
    MTCRoadwayNetwork: Roadway network with MTC-specific validation
    MTCRoadLinksTable: Schema for MTC roadway links
    MTCRoadNodesTable: Schema for MTC roadway nodes
    MTCFacilityType: Enum for functional class (ft) codes
    MTCUseClass: Enum for vehicle-class restrictions (useclass) codes

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

from .mtc_network import (
    LOCAL_CRS_FEET,
    FEET_PER_MILE,
    MTC_TIME_PERIODS,
    TIME_PERIOD_TO_LABEL,
    get_county_geodataframe,
    get_county_bbox,
    assign_county_to_geodataframes,
    MTCRoadwayNetwork
)
from .mtc_roadway_schema import (
    MTCCounty,
    MTC_COUNTIES,
    COUNTY_NAME_TO_CENTROID_START_NUM,
    COUNTY_NAME_TO_NODE_START_NUM,
    COUNTY_NAME_TO_NUM,
    MTCFacilityType,
    MTCRoadLinksTable,
    MTCRoadNodesTable,
    MTCUseClass,
)

__all__ = [
    "MTCRoadwayNetwork",
    "MTCRoadLinksTable",
    "MTCRoadNodesTable",
    "MTCCounty",
    "MTC_COUNTIES",
    "COUNTY_NAME_TO_CENTROID_START_NUM",
    "COUNTY_NAME_TO_NODE_START_NUM",
    "COUNTY_NAME_TO_NUM",
    "MTCFacilityType",
    "MTCUseClass",
]
