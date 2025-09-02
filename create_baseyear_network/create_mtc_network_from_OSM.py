"""Create MTC base year networks (2023) from OpenStreetMap (OSM) data.

This script creates transportation network models from OpenStreetMap data for the 
San Francisco Bay Area region. It can process either individual counties or the entire 
Bay Area as a unified network.

Main Features:
- Fetches road network data from OpenStreetMap using OSMnx
- Standardizes network attributes (highway types, lanes, access permissions)
- Integrates GTFS transit data from 511 Bay Area feed
- Creates compatible networks for travel demand models
- Outputs in multiple formats (Parquet, GeoJSON, Tableau Hyper)

The script performs the following workflow:
1. Downloads OSM network data for specified geography
2. Simplifies network topology while preserving connectivity
3. Standardizes attributes (highway types, lanes, access modes)
4. Assigns county-specific node/link numbering schemes
5. Integrates GTFS transit data
6. Creates transit stops and links on the roadway network
7. Outputs final network files

Tested in July 2025 with:
  * network_wrangler v1.0-beta.3
  * OSMnx v1.9+
  * GTFS feed from 511 Bay Area (September 2023)

References:
  * Asana: GMNS+ / NetworkWrangler2 > Build 2023 network using existing tools 
    https://app.asana.com/1/11860278793487/project/15119358130897/task/1210468893117122
  * MTC Year 2023 Network Creation Steps Google Doc
    https://docs.google.com/document/d/1TU0nsUHmyKfYZDbwjeCFiW09w53fyWu7X3XcRlNyf2o/edit
  * network_wrangler\\notebook\\Create Network from OSM.ipynb

Usage:
    python create_mtc_network_from_OSM.py <county>
    
    where <county> is one of:
    - 'Bay Area' (entire 9-county region)
    - Individual county names: 'San Francisco', 'San Mateo', 'Santa Clara',
      'Alameda', 'Contra Costa', 'Solano', 'Napa', 'Sonoma', 'Marin'

Example:
    python create_mtc_network_from_OSM.py "San Francisco"
    python create_mtc_network_from_OSM.py "Bay Area"
"""

USAGE = __doc__
import argparse
import datetime
import pathlib
import pickle
import pprint
import statistics
import sys
from typing import Any, Optional, Tuple, Union

import networkx
import osmnx
import numpy as np
import pandas as pd
import geopandas as gpd
import pygris
import shapely.geometry

import network_wrangler
from network_wrangler import WranglerLogger
from network_wrangler.params import LAT_LON_CRS
from network_wrangler.roadway.io import load_roadway_from_dataframes, write_roadway
from network_wrangler.transit.io import load_feed_from_path, write_transit, load_transit
from network_wrangler.models.gtfs.types import RouteType
from network_wrangler.utils.transit import \
  drop_transit_agency, filter_transit_by_boundary, create_feed_from_gtfs_model
from network_wrangler.transit.validate import shape_links_without_road_links

NOW = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"


# Map county names to county network node start based on
# https://bayareametro.github.io/tm2py/inputs/#county-node-numbering-system
COUNTY_NAME_TO_NODE_START_NUM = {
    'San Francisco': 1_000_000,
    'San Mateo'    : 1_500_000,
    'Santa Clara'  : 2_000_000,
    'Alameda'      : 2_500_000,
    'Contra Costa' : 3_000_000,
    'Solano'       : 3_500_000,
    'Napa'         : 4_000_000,
    'Sonoma'       : 4_500_000,
    'Marin'        : 5_000_000,
    'External'     : 900_001,
 }
BAY_AREA_COUNTIES = list(COUNTY_NAME_TO_NODE_START_NUM.keys())
BAY_AREA_COUNTIES.remove("External")

COUNTY_NAME_TO_NUM = dict(zip(BAY_AREA_COUNTIES, range(1,len(BAY_AREA_COUNTIES)+1)))

FEET_PER_MILE = 5280.0

NETWORK_SIMPLIFY_TOLERANCE = 20 # feet

LOCAL_CRS_FEET = "EPSG:2227"
""" NAD83 / California zone 3 (ftUS) https://epsg.io/2227 """

COUNTY_NAME_TO_GTFS_AGENCIES = {
    'San Francisco': [
        'SF', # SF Muni
        'BA', # BART
        # 'GG', # Golden Gate Transit
        'CT', # Caltrain
    ],
    'San Mateo': [
        'SM', # SamTrans
        'BA', # BART
        'CT', # Caltrain
    ]
}


# way (link) tags we want from OpenStreetMap (OSM)
# osmnx defaults are viewable here: https://osmnx.readthedocs.io/en/stable/osmnx.html?highlight=util.config#osmnx.utils.config
# and configurable as useful_tags_way
# These are used in step2_osmnx_extraction.py
TAG_NUMERIC = 1
TAG_STRING  = 2
OSM_WAY_TAGS = {
    'highway'            : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:highway
    'tunnel'             : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:tunnel
    'bridge'             : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:bridge
    'junction'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:junction
    'oneway'             : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:oneway
    'name'               : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:name
    'ref'                : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:ref
    'width'              : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:width
    'est_width'          : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:est_width
    'access'             : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:access
    'area'               : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:area
    'service'            : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:service
    'maxspeed'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:maxspeed
    # lanes accounting
    'lanes'              : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes
    'lanes:backward'     : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'lanes:forward'      : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'lanes:both_ways'    : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes#Lanes_in_different_directions
    'bus'                : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:bus
    'lanes:bus'          : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'lanes:bus:forward'  : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'lanes:bus:backward' : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:lanes:psv
    'hov'                : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:hov
    'hov:lanes'          : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:hov
    'taxi'               : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:taxi
    'lanes:hov'          : TAG_NUMERIC,  # https://wiki.openstreetmap.org/wiki/Key:hov
    'shoulder'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:shoulder
    'toll'               : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:toll
    'toll:hgv'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:toll:hgv
    'toll:hov'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:toll:hov
    'toll:lanes'         : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:toll:lanes
    'turn'               : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:turn
    'turn:lanes'         : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    'turn:lanes:forward' : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    'turn:lanes:backward': TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:turn#Turning_indications_per_lane
    # active modes
    'sidewalk'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:sidewalk
    'cycleway'           : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:cycleway
}

# from biggest to smallest
HIGHWAY_HIERARCHY = [
    'motorway',       # freeways or express-ways
    'motorway_link',  # on- and off-ramps
    'trunk',          # highway, not quite motorway
    'trunk_link',     # highway ramps
    'primary',        # major arterial
    'primary_link',   # connects primary to other
    'secondary',      # smaller highways or country roads
    'secondary_link', # connects secondary to tertiary
    'tertiary',       # connects minor streets to major roads
    'tertiary_link',  # typically at-grade turning lane
    'busway',         # bus-only link
    'unclassified',   # minor public roads
    'residential',    # residential street
    'living_street',  # pedestrian-focused residential
    'service',        # vehicle access to building, parking lot, etc.
    'track',          # minor land-access roads
]

def get_county_bbox(counties) -> tuple[float, float, float, float]:
    """
    Read in list of counties and return bounding box in WGS84 coordinates.
    
    This function reads the Bay Area county boundaries from a shapefile and
    calculates the total bounding box encompassing all counties. The coordinates
    are converted to WGS84 (EPSG:4326) if needed for OSM data retrieval.
    
    Args:
        county_shapefile: Path to the county shapefile containing Bay Area counties.
                         Expected to have geometry in any valid CRS.
        
    Returns:
        tuple: Bounding box as (west, south, east, north) in decimal degrees.
               These are longitude/latitude coordinates in WGS84 projection.
               
    Note:
        The returned tuple order (west, south, east, north) matches the format
        expected by osmnx.graph_from_bbox() function.
    """
    WranglerLogger.info(f"Reading county shapefile for Bay Area")
    county_gdf = pygris.counties(state = 'CA', cache = True, year = 2010)
    county_gdf = county_gdf[county_gdf['NAME10'].isin(counties)].copy()

    # Get the total bounds (bounding box) of all counties
    # Returns (minx, miny, maxx, maxy)
    bbox = county_gdf.total_bounds
    WranglerLogger.info(f"Bounding box for Bay Area counties: minx={bbox[0]:.6f}, miny={bbox[1]:.6f}, maxx={bbox[2]:.6f}, maxy={bbox[3]:.6f}")
    
    # Convert to WGS84 (EPSG:4326) if not already
    if county_gdf.crs != LAT_LON_CRS:
        WranglerLogger.info(f"Converting from {county_gdf.crs} to {LAT_LON_CRS}")
        county_gdf_wgs84 = county_gdf.to_crs(LAT_LON_CRS)
        bbox = county_gdf_wgs84.total_bounds
        WranglerLogger.info(f"Bounding box in WGS84: minx={bbox[0]:.6f}, miny={bbox[1]:.6f}, maxx={bbox[2]:.6f}, maxy={bbox[3]:.6f}")
    
    # OSMnx expects (left, bottom, right, top) which is (west, south, east, north)
    # bbox is currently (minx, miny, maxx, maxy) which is (west, south, east, north)
    west = bbox[0]
    south = bbox[1]
    east = bbox[2]
    north = bbox[3]
    
    return (west, south, east, north)

def get_min_or_median_value(lane: Union[int, str, list[Union[int, str]]]) -> int:
    """
    Convert lane value to integer, handling various OSM input formats.
    
    OSM lane data can be inconsistent, appearing as integers, strings, or lists
    when multiple values have been tagged over time. This function normalizes
    these various formats into a single integer value.
    
    For lists with two items, returns the minimum value (conservative approach).
    For lists with more than two items, returns the median value (typical case).
    
    Args:
        lane: Lane value from OSM that can be:
            - An integer (e.g., 2)
            - A string representation of an integer (e.g., '2' or '1.5')
            - A list of integers or string representations (e.g., [2, 4] or ['2', '3', '4'])
    
    Returns:
        The processed lane count as an integer. Fractional values are converted
        to integers via float conversion first.
    
    Examples:
        >>> get_min_or_median_value(3)
        3
        >>> get_min_or_median_value('2')
        2
        >>> get_min_or_median_value('1.5')  # Handles fractional strings
        1
        >>> get_min_or_median_value([2, 4])  # Returns min for 2-item list
        2
        >>> get_min_or_median_value([1, 2, 3, 4, 5])  # Returns median for longer list
        3
    """
    if isinstance(lane, list):
        lane = [item for item in lane if item.isnumeric()]# keep only numeric 
        lane = [int(float(s)) for s in lane] # float conversion first for values like 1.5 (yes really)
        if len(lane) == 2:
            return int(min(lane))
        else:
            return int(statistics.median(lane))
    elif isinstance(lane, str):
        return int(float(lane)) # float conversion first for values like 1.5 
    return lane

def standardize_highway_value(links_gdf: gpd.GeoDataFrame) -> None:
    """Standardize the highway value in the links GeoDataFrame and set access permissions.

    Standardized values - drive:
    - residential      residential street (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dresidential)
    - service          vehicle access to building, parking lot, etc. (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dservice)
    - tertiary         connects minor streets to major roads (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dtertiary)
    - tertiary_link    typically at-grade turning lane (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dtertiary_link)
    - secondary        smaller highways, e.g. country roads (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dsecondary)
    - secondary_link   connects secondary to tertiary, unclassified or other minor highway (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dsecondary_link)
    - primary          major arterial (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dprimary)
    - primary_link     connects primary to others (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dprimary_link)
    - motorway         freeways or expressways (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dmotorway)
    - motorway_link    on- and off-ramps (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dmotorway_link)
    - trunk            highway, not quite motorway (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dtrunk)
    - trunk_link       highway ramps (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dtrunk_link)
    - unclassified     minor public roads (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dunclassified)
    - track            minor land-access roads (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dtrack)
    - living_street    more pedestrian focused than residential, e.g. woonerf (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dliving_street)

    Standardized values - non-auto (drive_access=False):
    - path             typically non-auto (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dpath)
    - cycleway         separate way for cycling (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dcycleway)
    - pedestrian       designated for pedestrians (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dpedestrian)
                       -> converted to footway or path (if cycleway too)
    - footway          pedestrian path (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dfootway)
    - busway           dedicated right-of-way for buses (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dbusway)

    Args:
        links_gdf: Links GeoDataFrame from OSMnx with columns:
            - highway: OSM highway type (str or list of str)
            - Other OSM tags as columns
    
    Returns:
        None (modifies links_gdf in place)
    
    Side Effects:
        Adds the following columns to links_gdf:
            - highway_orig: Original highway value prefixed with 'highway:'
            - steps: Boolean indicating if there are stairs
            - drive_access: Boolean for auto access
            - bike_access: Boolean for bicycle access
            - walk_access: Boolean for pedestrian access
            - truck_access: Boolean for truck access
            - bus_only: Boolean for bus access
    
    Notes:
        - Processes highway values according to a hierarchy
        - Converts lists to single values based on priority
        - Sets appropriate access permissions for each facility type
    """
    # make a copy of highway: highway_orig
    links_gdf['highway_orig'] = 'highway:' + links_gdf['highway'].astype(str)

    # default all access to true
    links_gdf['drive_access'] = True
    links_gdf['bike_access']  = True
    links_gdf['walk_access']  = True
    links_gdf['truck_access'] = True
    links_gdf['bus_only']     = True

    ################ non-auto ################
    # make steps an attribute
    links_gdf['steps'] = False

    # steps -> footway, steps=True
    links_gdf.loc[links_gdf.highway == 'steps', 'steps'] = True
    links_gdf.loc[links_gdf.highway == 'steps', 'highway'] = 'footway'

    links_gdf.loc[links_gdf.highway.apply(lambda x: isinstance(x, list) and 'steps' in x), 'steps'  ] = True
    links_gdf.loc[links_gdf.highway.apply(lambda x: isinstance(x, list) and 'steps' in x), 'highway'] = 'footway'

    # includes path => path
    links_gdf.loc[links_gdf.highway.apply(lambda x: isinstance(x, list) and 'path' in x), 'highway'] = 'path'

    # includes footway or pedestrian *and* cycleway => path
    links_gdf.loc[links_gdf.highway.apply(lambda x: isinstance(x, list) and 
                                          (('footway' in x) or ('pedestrian' in x)) and 
                                          ('cycleway' in x)), 'highway'] = 'path'

    # includes footway => footway
    links_gdf.loc[links_gdf.highway.apply(lambda x: isinstance(x, list) and ('footway' in x)), 'highway'] = 'footway'

    # convert pedestrian to footway
    links_gdf.loc[links_gdf.highway == 'pedestrian', 'highway'] = 'footway'

    # includes pedestrian => footway
    links_gdf.loc[links_gdf.highway.apply(lambda x: isinstance(x, list) and ('pedestrian' in x)), 'highway'] = 'footway'

    # includes cycleway => cycleway
    links_gdf.loc[links_gdf.highway.apply(lambda x: isinstance(x, list) and ('cycleway' in x)), 'highway'] = 'cycleway'

    # remove drive_access, truck_access, bus_only from non-auto links
    links_gdf.loc[links_gdf.highway.isin(['path','footway','cycleway']), 'drive_access'] = False
    links_gdf.loc[links_gdf.highway.isin(['path','footway','cycleway']), 'truck_access'] = False
    links_gdf.loc[links_gdf.highway.isin(['path','footway','cycleway']), 'bus_only'] = False
    # restrict ped from bikes and vice versa
    links_gdf.loc[links_gdf.highway == 'footway',  'bike_access'] = False
    links_gdf.loc[links_gdf.highway == 'cycleway', 'walk_access'] = False

    ################ bus ################
    # includes busway => busway
    links_gdf.loc[ links_gdf.highway.apply(lambda x: isinstance(x, list) and ('busway' in x)), 'highway'] = 'busway'
    # remove access for anything but buses
    links_gdf.loc[links_gdf.highway == 'busway', 'drive_access'] = False
    links_gdf.loc[links_gdf.highway == 'busway', 'truck_access'] = False
    links_gdf.loc[links_gdf.highway == 'busway', 'bike_access'] = False
    links_gdf.loc[links_gdf.highway == 'busway', 'walk_access'] = False

    ################ auto ################

    # go from highest to lowest and choose highest
    for highway_type in HIGHWAY_HIERARCHY:
        links_gdf.loc[ links_gdf.highway.apply(lambda x: isinstance(x, list) and highway_type in x), 'highway'] = highway_type

    WranglerLogger.debug(f"After standardize_highway_value():\n{links_gdf.highway.value_counts(dropna=False)}")
    return

def standardize_lanes_value(
        links_gdf: gpd.GeoDataFrame,
        trace_tuple: Optional[tuple[int, int]] = None
    ) -> gpd.GeoDataFrame:
    """
    Standardize the lanes value in the links GeoDataFrame.
    
    This function processes complex OSM lane tagging to produce consistent lane counts
    for network modeling. It handles:
    - Directional lane tagging (lanes:forward, lanes:backward)
    - Bus-only lanes (lanes:bus, lanes:bus:forward, lanes:bus:backward)
    - Bidirectional links represented as forward/reverse pairs
    - Missing lane values filled based on highway type statistics
    
    The function performs several key operations:
    1. Resolves list-valued attributes from OSM (using min/median logic)
    2. Matches forward/reverse link pairs to combine directional attributes
    3. Calculates directional lanes from total lanes for two-way streets
    4. Extracts bus lanes separately from general traffic lanes
    5. Fills missing values using highway type statistics
    
    Args:
        links_gdf: GeoDataFrame containing OSM link data with columns:
            - A, B: Node IDs defining the link
            - key: Distinguishes parallel edges
            - oneway: Boolean or list indicating if link is one-way
            - reversed: Boolean or list indicating if link direction is reversed  
            - lanes: Total number of lanes (may be for both directions)
            - lanes:forward: Number of forward lanes (optional)
            - lanes:backward: Number of backward lanes (optional)
            - lanes:both_ways: Lanes shared by both directions (optional)
            - lanes:bus*: Various bus lane attributes (optional)
            - highway: OSM highway type classification
        trace_tuple: Optional tuple of (A,B) node IDs to trace for debugging
    
    Returns:
        Modified links_gdf with standardized lane values:
        - 'lanes': Integer count of general traffic lanes (excludes bus lanes)
        - 'buslanes': Integer count of bus-only lanes (default 0)
        All -1 placeholders are replaced with appropriate values.
    
    Side Effects:
        Modifies the input GeoDataFrame in place. Adds/modifies columns:
        - lanes: Standardized general traffic lane count
        - buslanes: Standardized bus lane count
        - lanes_orig: Original lanes value before processing
    
    Notes:
        - Two-way streets have lanes divided equally between directions
        - Bus-only facilities (highway='busway') get lanes=0, buslanes=1
        - Missing values filled using mode of lanes by highway type
        - Default assumption is 1 lane if no information available
    """
    WranglerLogger.debug(f"standardize_lanes_value() for {len(links_gdf):,} links")

    # move reversed to be right after oneway
    reversed_series = links_gdf.pop('reversed')
    links_gdf.insert(links_gdf.columns.get_loc('oneway') + 1, 'reversed', reversed_series)

    # Handle cases where oneway is a list (set to True if any value is True)
    oneway_is_list = links_gdf['oneway'].apply(lambda x: isinstance(x, list))
    if oneway_is_list.any():
        WranglerLogger.debug(f"Found {oneway_is_list.sum()} links with oneway as list, setting to True if any value is True")
        links_gdf.loc[oneway_is_list, 'oneway'] = links_gdf.loc[oneway_is_list, 'oneway'].apply(lambda x: any(x) if x else False)
    
    # oneway should now always be a bool
    # reversed is a bool or a list
    links_gdf['oneway_type']   = links_gdf['oneway'].apply(type).astype(str)
    links_gdf['reversed_type'] = links_gdf['reversed'].apply(type).astype(str)
    WranglerLogger.debug(f"links_gdf[['oneway_type','reversed_type']].value_counts():\n{links_gdf[['oneway_type','reversed_type']].value_counts()}")

    WranglerLogger.debug(f"reversed_type is list:\n{links_gdf.loc[ links_gdf.reversed.apply(type) == list]}")
    # it looks like reversed is sometimes [False, True] but these are typically in pairs where one of them is the reverse of the other
    # For links with reverse=[False, True]: these come in pairs, with reversed A and B values for each pair
    # pair them up and then set reverse=True for one and reverse=False for the other
    
    # Find links where reversed is a list
    list_reversed_mask = links_gdf['reversed'].apply(lambda x: isinstance(x, list))
    if list_reversed_mask.any():
        WranglerLogger.debug(f"Found {list_reversed_mask.sum()} links with reversed as list")
        
        # Process these links
        list_reversed_links = links_gdf[list_reversed_mask].copy()
        
        # Create a set to track processed indices
        processed_indices = set()
        
        for idx, row in list_reversed_links.iterrows():
            if idx in processed_indices:
                continue
                
            # Look for the paired link (with swapped A and B)
            pair_mask = (links_gdf['A'] == row['B']) & (links_gdf['B'] == row['A']) & list_reversed_mask
            
            if pair_mask.any():
                # Get the index of the paired link
                pair_idx = links_gdf[pair_mask].index[0]
                
                # Set reversed=False for the first link and reversed=True for the paired link
                links_gdf.at[idx, 'reversed'] = False
                links_gdf.at[pair_idx, 'reversed'] = True
                
                # Mark both as processed
                processed_indices.add(idx)
                processed_indices.add(pair_idx)
                
                # WranglerLogger.debug(f"Paired link {idx} (A={row['A']}, B={row['B']}) with {pair_idx}")
            else:
                # If no pair found, default to False
                links_gdf.at[idx, 'reversed'] = False
                WranglerLogger.debug(f"No pair found for link {idx} (A={row['A']}, B={row['B']}), setting reversed=False")

    # after looping to fix
    links_gdf['reversed_type'] = links_gdf['reversed'].apply(type).astype(str)
    assert (links_gdf['reversed_type']=="<class 'bool'>").all()
    assert (links_gdf['oneway_type'] =="<class 'bool'>").all()
    links_gdf.drop(columns=['reversed_type','oneway_type'], inplace=True)

    WranglerLogger.debug(f"links_gdf len={len(links_gdf):,}")
    WranglerLogger.debug(f"links_gdf[['oneway','reversed']].value_counts()\n{links_gdf[['oneway','reversed']].value_counts()}")

    # rename lanes to lanes_orig since it may not be what we want for two-way links
    links_gdf.rename(columns={'lanes': 'lanes_orig'}, inplace=True)
    # lanes columns are sometimes a list of lanes, e.g. [2,3,4]. For lists with two items, use min. For lists with more, use median.
    LANES_COLS = [
        'lanes_orig',
        'lanes:forward', 'lanes:backward',
        'lanes:both_ways',
        'lanes:bus',
        'lanes:bus:forward','lanes:bus:backward'
    ]
    LANES_COLS_REV = [f"{col}_rev" for col in LANES_COLS]
    for lane_col in LANES_COLS:
        if lane_col not in links_gdf.columns: links_gdf[lane_col] = np.nan
        WranglerLogger.debug(f"Before get_min_or_median_value: links_gdf['{lane_col}'].value_counts():\n{links_gdf[lane_col].value_counts(dropna=False)}")
        links_gdf[lane_col] = links_gdf[lane_col].apply(get_min_or_median_value)
        WranglerLogger.debug(f"After get_min_or_median_value: links_gdf['{lane_col}'].value_counts():\n{links_gdf[lane_col].value_counts(dropna=False)}")

    # split links_gdf into A<B and A>B to join links with their reverse
    links_gdf_AltB = links_gdf.loc[ links_gdf.A < links_gdf.B].copy()
    links_gdf_BltA = links_gdf.loc[ links_gdf.B < links_gdf.A].copy()

    if trace_tuple:
        WranglerLogger.debug(
            f"trace links_gdf:\n{links_gdf.loc[ ((links_gdf.A==trace_tuple[0]) & (links_gdf.B==trace_tuple[1])) | ((links_gdf.A==trace_tuple[1]) & (links_gdf.B==trace_tuple[0]))]}"
        )
        if (trace_tuple[0] < trace_tuple[1]):
            WranglerLogger.debug(f"trace link in links_gdf_AltB:\n{links_gdf_AltB.loc[ (links_gdf_AltB.A==trace_tuple[0]) & (links_gdf_AltB.B==trace_tuple[1]) ]}")
        else:
            WranglerLogger.debug(f"trace link in links_gdf_BltA:\n{links_gdf_BltA.loc[ (links_gdf_BltA.A==trace_tuple[0]) & (links_gdf_BltA.B==trace_tuple[1]) ]}")

    # for the links in links_gdf_reversed, make all columns have suffix '_rev'
    rev_col_rename = {}
    for col in links_gdf_BltA.columns.to_list():
        rev_col_rename[col] = f"{col}_rev"
    links_gdf_BltA.rename(columns=rev_col_rename, inplace=True)

    # join with reversed version of this link to pick up lanes:backward, lanes:bus:backward
    links_gdf_wide = pd.merge(
        left=links_gdf_AltB,
        right=links_gdf_BltA,
        how='outer',
        left_on=['A','B','key'],
        right_on=['B_rev','A_rev','key_rev'],
        indicator=True,
        validate='one_to_one'
    )
    # every link is present in the left_only or both or right_only next to the reverse
    WranglerLogger.debug(f"links_gdf_wide['_merge'].value_counts():\n{links_gdf_wide['_merge'].value_counts()}")
    if trace_tuple:
        if trace_tuple[0] < trace_tuple[1]:
            WranglerLogger.debug(f"trace links_gdf_wide:\n{links_gdf_wide.loc[ (links_gdf_wide.A==trace_tuple[0]) & (links_gdf_wide.B==trace_tuple[1]) ]}")
        else:
            WranglerLogger.debug(f"trace links_gdf_wide:\n{links_gdf_wide.loc[ (links_gdf_wide.B_rev==trace_tuple[1]) & (links_gdf_wide.A_rev==trace_tuple[0]) ]}")

    ALL_COLS = links_gdf.columns.tolist()
    ALL_COLS_REV = [f"{col}_rev" for col in ALL_COLS]

    # set the lanes from lanes:forward or lanes:backward
    links_gdf_wide['lanes'    ] = -1 # initialize to -1
    links_gdf_wide['lanes_rev'] = -1

    links_gdf_wide.loc[ links_gdf_wide['lanes:forward' ].notna() & (links_gdf_wide.reversed == False), 'lanes'    ] = links_gdf_wide['lanes:forward']
    links_gdf_wide.loc[ links_gdf_wide['lanes:backward'].notna() & (links_gdf_wide.reversed == True ), 'lanes_rev'] = links_gdf_wide['lanes:backward']

    # set the lanes from lanes:both_ways
    links_gdf_wide.loc[ links_gdf_wide['lanes:both_ways' ].notna() & (links_gdf_wide['lanes'    ]==-1), 'lanes'    ] = links_gdf_wide['lanes:both_ways']
    links_gdf_wide.loc[ links_gdf_wide['lanes:both_ways' ].notna() & (links_gdf_wide['lanes_rev']==-1), 'lanes_rev'] = links_gdf_wide['lanes:both_ways']

    # since lanes is for both directions, divide by 2 if not one way (hmm... what about when it goes to zero?)
    links_gdf_wide.loc[ links_gdf_wide['lanes_orig'].notna() & (links_gdf_wide['lanes'    ]==-1) & (links_gdf_wide['oneway']==False), 'lanes'    ] = links_gdf_wide['lanes_orig']
    links_gdf_wide.loc[ links_gdf_wide['lanes_orig'].notna() & (links_gdf_wide['lanes'    ]==-1) & (links_gdf_wide['oneway']==True ), 'lanes'    ] = np.floor(0.5*links_gdf_wide['lanes_orig'])
    links_gdf_wide.loc[ links_gdf_wide['lanes_orig'].notna() & (links_gdf_wide['lanes_rev']==-1) & (links_gdf_wide['oneway']==False), 'lanes_rev'] = links_gdf_wide['lanes_orig']
    links_gdf_wide.loc[ links_gdf_wide['lanes_orig'].notna() & (links_gdf_wide['lanes_rev']==-1) & (links_gdf_wide['oneway']==True ), 'lanes_rev'] = np.floor(0.5*links_gdf_wide['lanes_orig'])

    # if it got set to 0, make it 1
    links_gdf_wide.loc[ (links_gdf_wide['lanes_orig']==1) & (links_gdf_wide['oneway']==True ), 'lanes'    ] = 1
    links_gdf_wide.loc[ (links_gdf_wide['lanes_orig']==1) & (links_gdf_wide['oneway']==True ), 'lanes_rev'] = 1

    WranglerLogger.debug(f"links_gdf_wide.lanes       .value_counts():\n{links_gdf_wide[   'lanes'    ].value_counts(dropna=False)}")
    WranglerLogger.debug(f"links_gdf_wide.lanes_rev   .value_counts():\n{links_gdf_wide[   'lanes_rev'].value_counts(dropna=False)}")

    # set the buslanes
    links_gdf_wide['buslanes'    ] = -1
    links_gdf_wide['buslanes_rev'] = -1

    links_gdf_wide.loc[ links_gdf_wide['lanes:bus:forward' ].notna() & (links_gdf_wide.reversed == False), 'buslanes'    ] = links_gdf_wide['lanes:bus:forward']
    links_gdf_wide.loc[ links_gdf_wide['lanes:bus:backward'].notna() & (links_gdf_wide.reversed == True ), 'buslanes_rev'] = links_gdf_wide['lanes:bus:backward']
    
    WranglerLogger.debug(f"lanes:bus:forward rows:\n{links_gdf_wide.loc[ links_gdf_wide['lanes:bus:forward'].notna()]}")
    WranglerLogger.debug(f"lanes:bus:backward rows:\n{links_gdf_wide.loc[ links_gdf_wide['lanes:bus:backward'].notna()]}")

    WranglerLogger.debug(f"links_gdf_wide.buslanes    .value_counts():\n{links_gdf_wide['buslanes'    ].value_counts(dropna=False)}")
    WranglerLogger.debug(f"links_gdf_wide.buslanes_rev.value_counts():\n{links_gdf_wide['buslanes_rev'].value_counts(dropna=False)}")

    # since lanes is for both directions, divide by 2 if not one way (hmm... what about when it goes to zero?)
    links_gdf_wide.loc[ links_gdf_wide['lanes:bus'].notna() & (links_gdf_wide['buslanes'    ]==-1) & (links_gdf_wide['oneway']==False), 'buslanes'    ] = links_gdf_wide['lanes:bus']
    links_gdf_wide.loc[ links_gdf_wide['lanes:bus'].notna() & (links_gdf_wide['buslanes'    ]==-1) & (links_gdf_wide['oneway']==True ), 'buslanes'    ] = np.floor(0.5*links_gdf_wide['lanes:bus'])
    links_gdf_wide.loc[ links_gdf_wide['lanes:bus'].notna() & (links_gdf_wide['buslanes_rev']==-1) & (links_gdf_wide['oneway']==False), 'buslanes_rev'] = links_gdf_wide['lanes:bus']
    links_gdf_wide.loc[ links_gdf_wide['lanes:bus'].notna() & (links_gdf_wide['buslanes_rev']==-1) & (links_gdf_wide['oneway']==True ), 'buslanes_rev'] = np.floor(0.5*links_gdf_wide['lanes:bus'])

    # if highway=='busway' set buslanes to 1, lanes to 0
    links_gdf_wide.loc[ links_gdf_wide['highway']     == 'busway', 'buslanes'] = 1
    links_gdf_wide.loc[ links_gdf_wide['highway']     == 'busway', 'lanes'   ] = 0
    links_gdf_wide.loc[ links_gdf_wide['highway_rev'] == 'busway', 'buslanes_rev'] = 1
    links_gdf_wide.loc[ links_gdf_wide['highway_rev'] == 'busway', 'lanes_rev'   ] = 0

    WranglerLogger.debug(f"links_gdf_wide:\n{links_gdf_wide[LANES_COLS + LANES_COLS_REV]}")
    WranglerLogger.debug(f"links_gdf_wide for busway:\n{links_gdf_wide.loc[links_gdf_wide.highway=='busway', LANES_COLS]}")

    WranglerLogger.debug(f"links_gdf_wide.buslanes    .value_counts():\n{links_gdf_wide['buslanes'    ].value_counts(dropna=False)}")
    WranglerLogger.debug(f"links_gdf_wide.buslanes_rev.value_counts():\n{links_gdf_wide['buslanes_rev'].value_counts(dropna=False)}")

    if trace_tuple:
        if (trace_tuple[0] < trace_tuple[1]):
            # it comes in through AltB
            WranglerLogger.debug(f"trace link in links_gdf_wide:\n{links_gdf_wide.loc[ (links_gdf_wide.A==trace_tuple[0]) & (links_gdf_wide.B==trace_tuple[1]) ]}")
        else:
            # it comes in through BltA
            WranglerLogger.debug(f"trace link in links_gdf_wide:\n{links_gdf_wide.loc[ (links_gdf_wide.B_rev==trace_tuple[0]) & (links_gdf_wide.A_rev==trace_tuple[1]) ]}")


    WranglerLogger.debug(f"links_gdf_wide:\n{links_gdf_wide}")
    ALL_COLS = ALL_COLS + ['lanes','buslanes']
    ALL_COLS_REV = [f"{col}_rev" for col in ALL_COLS]
    rev_to_nonrev = dict(zip(ALL_COLS_REV, ALL_COLS))
    WranglerLogger.debug(f"rev_to_nonrev:\n{rev_to_nonrev}")
    # put it back together
    links_gdf = pd.concat([
        links_gdf_wide.loc[ links_gdf_wide.A.notna(), ALL_COLS],
        links_gdf_wide.loc[ links_gdf_wide.A_rev.notna(), ALL_COLS_REV ].rename(columns=rev_to_nonrev)
    ])
    links_gdf['A']   = links_gdf['A'].astype(int)
    links_gdf['B']   = links_gdf['B'].astype(int)
    links_gdf['key'] = links_gdf['key'].astype(int)

    WranglerLogger.debug(f"After reassmbly, links_gdf len={len(links_gdf):,}")
    WranglerLogger.debug(f"After reassmbly, links_gdf[['oneway','reversed']].value_counts()\n{links_gdf[['oneway','reversed']].value_counts()}")
    WranglerLogger.debug(f"After reassmbly, links_gdf:\n{links_gdf}")

    links_gdf.fillna({'lanes':-1, 'buslanes':-1}, inplace=True)
    WranglerLogger.debug(f"links_gdf.lanes.value_counts():\n{links_gdf['lanes'].value_counts(dropna=False)}")
    WranglerLogger.debug(f"links_gdf.buslanes.value_counts():\n{links_gdf['buslanes'].value_counts(dropna=False)}")
    
    # for buslanes, default to zero
    links_gdf.loc[ links_gdf['buslanes'] == -1, 'buslanes'] = 0
    links_gdf['buslanes'] = links_gdf['buslanes'].astype(int)

    if trace_tuple:
        WranglerLogger.debug(
            f"trace: links_gdf:\n"
            f"{links_gdf.loc[ ((links_gdf.A==trace_tuple[0]) & (links_gdf.B==trace_tuple[1])) | ((links_gdf.A==trace_tuple[1]) & (links_gdf.B==trace_tuple[0]))]}"
        )

    # Create a mapping from highway value -> most common number of lanes for that value
    # and use that to assign the remaining unset lanes
    
    # First, get links that have valid lane counts (not -1)
    links_with_lanes = links_gdf[links_gdf['lanes'] >= 0].copy()
    
    if len(links_with_lanes) > 0:
        # Calculate the most common (mode) number of lanes for each highway type
        highway_lanes_mode = links_with_lanes.groupby('highway')['lanes'].agg(lambda x: x.mode()[0] if len(x.mode()) > 0 else x.median())
        highway_lanes_mode = highway_lanes_mode.astype(int)
        highway_to_lanes = highway_lanes_mode.to_dict()
        # override these since lanes are vehicle lanes
        highway_to_lanes['cycleway'] = 0
        highway_to_lanes['footway'] = 0
        
        WranglerLogger.debug(f"Highway to lanes mapping based on mode:\n{pprint.pformat(highway_to_lanes)}")

        # Apply the mapping to links with missing lanes (-1)
        links_missing_lanes = links_gdf['lanes'] == -1
        missing_count = links_missing_lanes.sum()
        
        if missing_count > 0:
            WranglerLogger.info(f"Filling {missing_count:,} links with missing lane counts using highway type mapping")
            
            # Apply the mapping
            for highway, default_lanes in highway_to_lanes.items():
                mask = links_missing_lanes & (links_gdf['highway'] == highway)
                if mask.any():
                    links_gdf.loc[mask, 'lanes'] = default_lanes
                    WranglerLogger.debug(f"  Set {mask.sum()} {highway} links to {default_lanes} lanes")
            
            # Check how many are still missing
            still_missing = (links_gdf['lanes'] == -1).sum()
            if still_missing > 0:
                # For any highway types not in our mapping, set to 1
                WranglerLogger.info(f"Found {still_missing:,} links with missing lanes, assuming 1 lane")
                WranglerLogger.debug(f"\n{links_gdf.loc[links_gdf['lanes'] == -1]})")
                links_gdf.loc[links_gdf['lanes'] == -1, 'lanes'] = 1
                
            links_gdf['lanes'] = links_gdf['lanes'].astype(int)
    else:
        WranglerLogger.warning("No links with valid lane counts found, cannot create highway to lanes mapping")
        highway_to_lanes = {}

    WranglerLogger.info(f"After standardize_lanes_value:\n{links_gdf['lanes'].value_counts(dropna=False)}")
    WranglerLogger.info(f"After standardize_lanes_value:\n{links_gdf['buslanes'].value_counts(dropna=False)}")
    return links_gdf

def create_managed_lanes_fields(
    links_gdf: gpd.GeoDataFrame
):
    """Converts buslanes to managed lanes per Wrangler format.

    Args:
        links_gdf: The links to be used to create a RoadwayNetwork
    
    Returns:
        Nothing; links_gdf is modified in place
    """
    WranglerLogger.debug(f"create_managed_lanes_fields():\n{links_gdf[['lanes','buslanes']].value_counts()}")
    
    # default - all access
    links_gdf['access'] = 'any'
    # this one is a bit odd, but I think parquet doesn't like the mixed types
    links_gdf['ML_access'] = 'any'
    links_gdf['ML_lanes'] = 0

    # for buslanes & GP lanes both:
    mask_both = (links_gdf["buslanes"] > 0) & (links_gdf["lanes"] > 0)
    links_gdf.loc[mask_both, 'ML_access'] = 'bus'
    links_gdf.loc[mask_both, 'ML_lanes'] = links_gdf.loc[mask_both, "buslanes"].values

    # for buslanes only: 
    mask_bus_only = (links_gdf["buslanes"] > 0) & (links_gdf["lanes"] == 0)
    links_gdf.loc[mask_bus_only, 'access'] = 'bus'
    links_gdf.loc[mask_bus_only, 'lanes'] = links_gdf.loc[mask_bus_only, "buslanes"].values

    WranglerLogger.debug(f"links_gdf['access'].value_counts(dropna=False):\n{links_gdf['access'].value_counts(dropna=False)}")
    WranglerLogger.debug(f"links_gdf['ML_access'].value_counts(dropna=False):\n{links_gdf['ML_access'].value_counts(dropna=False)}")
    WranglerLogger.debug(f"links_gdf['lanes'].value_counts(dropna=False):\n{links_gdf['lanes'].value_counts(dropna=False)}")
    WranglerLogger.debug(f"links_gdf['ML_lanes'].value_counts(dropna=False):\n{links_gdf['ML_lanes'].value_counts(dropna=False)}")

    # drop buslanes column
    links_gdf.drop(columns=['buslanes'], inplace=True)

def get_roadway_value(highway: Union[str, list[str]]) -> str:
    """
    Extract a single highway value from potentially multiple OSM values.
    
    OSM ways can have multiple highway tags when they serve multiple purposes
    or have been tagged inconsistently. This function resolves to a single value.
    
    When multiple values are present (as a list), returns the first one.
    This is typically the most important/primary classification.
    
    Args:
        highway: Either a single highway type string or a list of highway types
                from OSM tags.
    
    Returns:
        A single highway type string representing the primary classification.
    
    Examples:
        >>> get_roadway_value('primary')
        'primary'
        >>> get_roadway_value(['primary', 'secondary'])
        'primary'
        
    Note:
        This simple selection strategy works because standardize_highway_value()
        later applies a hierarchy-based selection for lists.
    """
    if isinstance(highway,list):
        WranglerLogger.debug(f"list: {highway}")
        return highway[0]
    return highway

def handle_links_with_duplicate_A_B(
        links_gdf: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
    """
    Handle links with duplicate A,B node pairs by merging or prioritizing.
    
    Multiple OSM ways can connect the same two nodes (parallel edges), representing
    different types of infrastructure (e.g., a freeway and a parallel frontage road).
    This function resolves these duplicates by:
    1. Prioritizing based on highway hierarchy (motorway > primary > residential)
    2. Aggregating lanes from matching infrastructure (same name or unnamed)
    3. Preserving bus lanes from bus-only facilities
    
    The function ensures each A-B pair appears only once in the final network,
    selecting the most important link while preserving capacity information.
    
    Args:
        links_gdf: GeoDataFrame with a 'dupe_A_B' column marking duplicate links.
                  Must contain columns: A, B, key, highway, name, lanes, buslanes
    
    Returns:
        GeoDataFrame with duplicates resolved. Each A-B pair appears exactly once.
        The 'dupe_A_B' column is set to False for all remaining links.
    
    Algorithm:
        1. Sort duplicates by highway hierarchy (highest priority first)
        2. Keep the highest priority link for each A-B pair
        3. Aggregate lanes from links with matching/empty names to the kept link
        4. Aggregate bus lanes from busway facilities to the kept link
    """
    WranglerLogger.info("Handling links with duplicate (A,B)")

    debug_cols = ['A','B','key','highway','oneway','reversed','name','ref','length','lanes','buslanes']
    WranglerLogger.debug(f"links to fix:\n{links_gdf.loc[ links_gdf.dupe_A_B, debug_cols]}")

    # Vectorized processing of duplicate links
    dupe_links = links_gdf.loc[links_gdf.dupe_A_B].copy()
    WranglerLogger.debug(f"Processing {len(dupe_links)} duplicate links")
    
    # Create highway hierarchy mapping
    highway_level_map = {hw: i for i, hw in enumerate(HIGHWAY_HIERARCHY)}
    dupe_links['highway_level'] = dupe_links['highway'].map(highway_level_map).fillna(100)
    dupe_links['highway_level'] = dupe_links['highway_level'].astype(int)
    debug_cols.insert( debug_cols.index('oneway'), 'highway_level')
    WranglerLogger.debug(f"Highway level mapping applied:\n{dupe_links[debug_cols]}")
    
    # Sort by A, B, and highway_level to get highest priority first
    dupe_links = dupe_links.sort_values(['A', 'B', 'highway_level'])
    WranglerLogger.debug(
        f"Sorted duplicate links by A, B, highway_level:\n"
        f"{dupe_links[debug_cols]}"
    )
    
    # For each group, identify the first (highest priority) and other rows
    dupe_links['group_rank'] = dupe_links.groupby(['A', 'B']).cumcount()
    debug_cols.insert( debug_cols.index('oneway'), 'group_rank')
    WranglerLogger.debug(f"Group ranks assigned:\n{dupe_links[debug_cols]}")
    
    # Aggregate busway buslanes to first row in each group
    busway_links = dupe_links[dupe_links['highway'] == 'busway']
    WranglerLogger.debug(f"Found {len(busway_links)} busway links")
    busway_buslanes = busway_links.groupby(['A', 'B'])['buslanes'].first()
    if not busway_buslanes.empty:
        WranglerLogger.debug(f"Busway buslanes to aggregate:\n{busway_buslanes}")
    
    # Aggregate lanes from rows with matching or empty names
    # Get first row's name for each group
    first_rows = dupe_links[dupe_links['group_rank'] == 0]
    first_names = first_rows.set_index(['A', 'B'])['name']
    WranglerLogger.debug(f"First row names for {len(first_names)} groups")
    
    # Find rows that match first row's name or have no name (treating NaN as empty)
    dupe_links_indexed = dupe_links.set_index(['A', 'B'])
    matching_name_mask = (
        (dupe_links_indexed['name'] == dupe_links_indexed.index.map(first_names.to_dict())) |
        (dupe_links_indexed['name'].isna()) |
        (dupe_links_indexed['name'] == '')
    )
    WranglerLogger.debug(f"Rows matching name criteria: {matching_name_mask.sum()}")
    matching_lanes = dupe_links_indexed[matching_name_mask].groupby(['A', 'B'])['lanes'].sum()
    if not matching_lanes.empty:
        WranglerLogger.debug(f"Aggregated lanes for matching names:\n{matching_lanes.head(10)}")
    
    # Keep only the first row from each group
    unduped_df = dupe_links[dupe_links['group_rank'] == 0].copy()
    WranglerLogger.debug(f"Keeping {len(unduped_df)} first rows from duplicate groups")
    
    # Vectorized application of aggregated values
    # Apply busway buslanes using merge
    if not busway_buslanes.empty:
        WranglerLogger.debug(f"Applying {len(busway_buslanes)} busway buslane aggregations")
        busway_df = busway_buslanes.reset_index()
        busway_df.columns = ['A', 'B', 'buslanes_new']
        unduped_df = unduped_df.merge(busway_df, on=['A', 'B'], how='left')
        mask = unduped_df['buslanes_new'].notna()
        if mask.any():
            WranglerLogger.debug(f"Updated buslanes for {mask.sum()} links")
            unduped_df.loc[mask, 'buslanes'] = unduped_df.loc[mask, 'buslanes_new']
        unduped_df = unduped_df.drop(columns=['buslanes_new'])
    
    # Apply aggregated lanes using merge
    if not matching_lanes.empty:
        WranglerLogger.debug(f"Applying {len(matching_lanes)} lane aggregations")
        lanes_df = matching_lanes.reset_index()
        lanes_df.columns = ['A', 'B', 'lanes_new']
        # Log original lanes for debugging
        orig_lanes_series = unduped_df.set_index(['A', 'B'])['lanes']
        unduped_df = unduped_df.merge(lanes_df, on=['A', 'B'], how='left')
        mask = unduped_df['lanes_new'].notna()
        if mask.any():
            # Log changes for debugging
            changes = unduped_df[mask][['A', 'B', 'lanes', 'lanes_new']]
            WranglerLogger.debug(f"Updated lanes for {mask.sum()} links:\n{changes}")
            unduped_df.loc[mask, 'lanes'] = unduped_df.loc[mask, 'lanes_new']
        unduped_df = unduped_df.drop(columns=['lanes_new'])

    # Drop temporary columns
    unduped_df = unduped_df.drop(columns=['highway_level', 'group_rank'])
    WranglerLogger.debug(f"Final unduped_df has {len(unduped_df)} links")

    # put it back together
    WranglerLogger.debug(f"unduped_df:\n{unduped_df}")
    unduped_df['dupe_A_B'] = False

    full_gdf = pd.concat([
        links_gdf.loc[links_gdf.dupe_A_B == False],
        unduped_df
    ])
    # verify it worked
    full_gdf['dupe_A_B'] = full_gdf.duplicated(subset=['A','B'], keep=False)
    assert( (full_gdf['dupe_A_B']==False).all() )

    return full_gdf


def standardize_and_write(
        g: networkx.MultiDiGraph,
        county: str, 
        prefix: str
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Standardize OSM network data and write to multiple output formats.
    
    This is a key processing function that transforms raw OSM network data into
    a standardized format suitable for travel demand modeling. It handles the
    complexity of OSM tagging, assigns model-specific IDs, and performs spatial
    operations for multi-county networks.
    
    Processing steps:
    1. Projects graph to WGS84 (EPSG:4326) for geographic consistency
    2. Converts NetworkX graph to GeoDataFrames (nodes and links)
    3. Removes self-loop edges (where A==B) and tags duplicate A-B pairs
    4. Standardizes highway classifications and access permissions
    5. Standardizes lane counts and separates bus lanes
    6. For Bay Area: performs spatial join to assign county to each link/node
    7. Assigns model-specific node and link IDs based on county numbering system
    8. Removes unnamed service roads to simplify network
    9. Writes outputs in multiple formats for different use cases
    
    County numbering system:
    - San Francisco: nodes 1,000,000+, links 1,000,000+
    - San Mateo: nodes 1,500,000+, links 2,000,000+
    - Santa Clara: nodes 2,000,000+, links 3,000,000+
    - Alameda: nodes 2,500,000+, links 4,000,000+
    - Contra Costa: nodes 3,000,000+, links 5,000,000+
    - Solano: nodes 3,500,000+, links 6,000,000+
    - Napa: nodes 4,000,000+, links 7,000,000+
    - Sonoma: nodes 4,500,000+, links 8,000,000+
    - Marin: nodes 5,000,000+, links 9,000,000+
    - External: nodes 900,001+, links 0+
    
    Args:
        g: NetworkX MultiDiGraph from OSMnx containing the road network.
           Nodes have 'osmid', 'x', 'y' attributes.
           Edges have highway, lanes, name, etc. from OSM tags.
        county: County name (e.g., "San Francisco") or "Bay Area" for 
               multi-county processing
        prefix: String prefix for output filenames (e.g., "3_simplified_")
    
    Returns:
        Tuple of (links_gdf, nodes_gdf) containing:
            - links_gdf: GeoDataFrame of standardized road links with columns:
                A, B: Model node IDs
                highway: Standardized road type
                lanes: Number of general traffic lanes
                buslanes: Number of bus-only lanes  
                bus_only: Boolean if the link is bus-only
                drive/bike/walk/truck_access: Boolean access flags
                model_link_id: Unique link identifier
                shape_id: Link geometry identifier
                county: Assigned county name
            - nodes_gdf: GeoDataFrame of network nodes with columns:
                model_node_id: Unique node identifier
                X, Y: Longitude, latitude coordinates
                county: Assigned county name
    
    Side Effects:
        Writes multiple files to OUTPUT_DIR (depending on output formats):
            - {prefix}{county}_links.hyper: Tableau Hyper format for visualization
            - {prefix}{county}_nodes.hyper: Tableau Hyper format for visualization
            - {prefix}{county}_links.parquet: Parquet format for analysis
            - {prefix}{county}_nodes.parquet: Parquet format for analysis
            - {prefix}{county}_links.gpkg: GeoPackage format for GIS
            - {prefix}{county}_nodes.gpkg: GeoPackage format for GIS
            - {prefix}{county}_links.geojson: GEOJSON format for GIS
            - {prefix}{county}_nodes.geojson: GEOJSON format for GIS
    
    Notes:
        - Multi-county links assigned to county with longest intersection
        - Unnamed service roads removed to reduce complexity
        - Original OSM IDs preserved in osm_node_id and osm_link_id columns
    """
    WranglerLogger.info(f"======= standardize_and_write(g, {county=}, {prefix=}) =======")
    county_no_spaces = county.replace(" ","")
    # project to long/lat
    g = osmnx.projection.project_graph(g, to_crs=LAT_LON_CRS)

    nodes_gdf, edges_gdf = osmnx.graph_to_gdfs(g)
    WranglerLogger.info(f"After converting to gdfs, len(edges_gdf)={len(edges_gdf):,} and len(nodes_gdf)={len(nodes_gdf):,}")

    # u,v,key are the index columns; Move them to real columns
    links_gdf = edges_gdf.reset_index(drop=False)

    # Note: u,v are not unique
    # The column 'key' differentiates parallel edges between the same u and v nodes. 
    # If there are multiple distinct street segments connecting the same two nodes
    # (e.g., a street with a median where traffic flows in both directions along
    # separate physical paths), the key allows each of these parallel edges to be
    # uniquely identified. For edges where no parallel edges exist, the key is typically 0.
    WranglerLogger.debug(f"Links with identical u,v:\n{links_gdf.loc[links_gdf.duplicated(subset=['u','v'], keep=False)]}")
    # For now, leave them and tag
    links_gdf['dupe_A_B'] = False
    links_gdf.loc[links_gdf.duplicated(subset=['u','v'], keep=False), 'dupe_A_B'] = True
    
    # Drop edges where u==v
    WranglerLogger.debug(f"Dropping edges where u==v:\n{links_gdf.loc[links_gdf['u']==links_gdf['v']]}")
    links_gdf = links_gdf.loc[links_gdf['u']!=links_gdf['v']].reset_index(drop=True)
    WranglerLogger.info(f"links_gdf has {len(links_gdf):,} links after dropping loop links (with u==v)")

    # use A,B instead of u,v
    links_gdf.rename(columns={'u': 'A', 'v': 'B'}, inplace=True)
    # nodes has osmid as index; move to be a real column
    nodes_gdf.reset_index(names='osmid', inplace=True)

    # keep original OSM node IDs
    if 'osmid_original' in nodes_gdf.columns:
        # If the graph is simplified, this may be a list of nodes/links rather than a single node/link
        # Note: osmid will also be present, but we'll keep as int and use later
        nodes_gdf.rename(columns={'osmid_original':'osm_node_id'}, inplace=True)
    else:
        # For unsimplified graph, let's just copy which we'll convert to string
        nodes_gdf['osm_node_id'] = nodes_gdf['osmid']

    links_gdf.rename(columns={'osmid':'osm_link_id'}, inplace=True)
    # also convert to a string
    nodes_gdf['osm_node_id'] = nodes_gdf['osm_node_id'].astype(str)
    links_gdf['osm_link_id'] = links_gdf['osm_link_id'].astype(str)

    # Handle county assignment
    if county == "Bay Area":
        # Read the county shapefile for spatial joins
        WranglerLogger.info("Performing spatial join to assign counties for Bay Area network...")
        county_gdf = pygris.counties(state = 'CA', cache = True, year = 2010)
        county_gdf = county_gdf[county_gdf['NAME10'].isin(BAY_AREA_COUNTIES)].copy()
        county_gdf = county_gdf.rename(columns={'NAME10': 'county'})
        WranglerLogger.debug(f"county_gdf:\n{county_gdf}")
                
        # Ensure both are in the same CRS (LOCAL_CRS_FEET)
        county_gdf.to_crs(LOCAL_CRS_FEET, inplace=True)
        links_gdf.to_crs(LOCAL_CRS_FEET, inplace=True)
        nodes_gdf.to_crs(LOCAL_CRS_FEET, inplace=True)

        # Dissolve counties to one region shape and create convex hull
        region_shape = county_gdf.dissolve().convex_hull.iloc[0]
        region_gdf = gpd.GeoDataFrame([{'geometry': region_shape, 'region': 'Bay Area'}], crs=county_gdf.crs)
        
        # Filter to links that intersect with region
        links_gdf = links_gdf[links_gdf.intersects(region_shape)].copy()
        WranglerLogger.info(f"Filtered to {len(links_gdf):,} links intersecting Bay Area region")
        
        # Filter nodes to only those referenced in the filtered links
        used_nodes = pd.concat([links_gdf['A'], links_gdf['B']]).unique()
        nodes_gdf = nodes_gdf[nodes_gdf['osmid'].isin(used_nodes)]
        WranglerLogger.info(f"Filtered to {len(nodes_gdf):,} nodes that are referenced in links")

        # Spatial join for nodes - use point geometry
        nodes_gdf = gpd.sjoin(
            nodes_gdf, 
            county_gdf[['geometry', 'county']], 
            how='left', 
            predicate='within'
        )
        # Use "External" for nodes outside counties
        nodes_gdf['county'] = nodes_gdf['county'].fillna('External')
        
        # First, do a spatial join to find all intersecting counties
        WranglerLogger.info(f"Before joining links to counties, {len(links_gdf)=:,}")
        links_gdf = gpd.sjoin(
            links_gdf,
            county_gdf[['geometry', 'county']],
            how='left',
            predicate='intersects'
        )
        WranglerLogger.debug(f"{len(links_gdf)=:,}")
        WranglerLogger.debug(f"links_gdf:\n{links_gdf}")
        
        # Use "External" for links outside counties
        links_gdf['county'] = links_gdf['county'].fillna('External')
        WranglerLogger.debug(f"links_gdf:\n{links_gdf}")

        # The only links to adjust are those that matched to multiple counties
        multicounty_links_gdf = links_gdf[links_gdf.duplicated(subset=['A','B','key'], keep=False)].copy()
        WranglerLogger.debug(f"multicounty_links_gdf:\n{multicounty_links_gdf}")

        if len(multicounty_links_gdf) > 0:
            # Calculate intersection lengths for multi-county links
            WranglerLogger.info(f"Found {len(multicounty_links_gdf):,} links in multicounty_links_gdf")
            
            # Calculate intersection length for each link-county pair
            multicounty_links_gdf['intersection_length'] = multicounty_links_gdf.apply(
                lambda row: row.geometry.intersection(
                    county_gdf[county_gdf['county'] == row['county']].iloc[0].geometry
                ).length if not pd.isna(row['county']) else 0,
                axis=1
            )
            
            # Sorting by index (ascending), intersection_length (descending)
            multicounty_links_gdf.sort_values(
                by=['A','B','key','intersection_length'],
                ascending=[True, True, True, False],
                inplace=True)
            WranglerLogger.debug(f"multicounty_links_gdf:\n{multicounty_links_gdf}")
            # drop duplicates now, keeping first
            multicounty_links_gdf.drop_duplicates(subset=['A','B','key'], keep='first', inplace=True)
            WranglerLogger.debug(f"After dropping duplicates: multicounty_links_gdf:\n{multicounty_links_gdf}")
            
            # put them back together
            links_gdf = pd.concat([
                links_gdf.drop_duplicates(subset=['A','B','key'], keep=False), # single-county links
                multicounty_links_gdf
            ])
            # verify that each link is only represented once
            multicounty_links_gdf = links_gdf[links_gdf.duplicated(subset=['A','B','key'], keep=False)]
            assert(len(multicounty_links_gdf)==0)

            # drop temporary columns
            links_gdf.drop(columns=['index_right','intersection_length'], inplace=True)
            links_gdf.reset_index(drop=True, inplace=True)
            
        # Drop the extra columns from spatial join
        links_gdf = links_gdf.drop(columns=['index_right'], errors='ignore')

        WranglerLogger.debug(f"links_gdf with one county per link:\n{links_gdf}")

        # Sort nodes by county for consistent numbering
        nodes_gdf = nodes_gdf.sort_values('county').reset_index(drop=True)
        WranglerLogger.debug(f"nodes_gdf:\n{nodes_gdf}")

        # revert to LAT_LON_CRS
        county_gdf.to_crs(LAT_LON_CRS, inplace=True)
        links_gdf.to_crs(LAT_LON_CRS, inplace=True)
        nodes_gdf.to_crs(LAT_LON_CRS, inplace=True)    
    else:
        # Original single-county logic
        links_gdf['county'] = county
        nodes_gdf['county'] = county

    # Renumber nodes based on their assigned county  
    nodes_gdf.rename(columns={'x':'X', 'y':'Y'}, inplace=True)
        
    # Vectorized approach to assign model_node_id based on county
    nodes_gdf['model_node_id'] = 0  # Initialize
        
    # For each county, assign sequential model node IDs starting from the county's base number
    for county_name in sorted(nodes_gdf['county'].unique()):
        county_mask = nodes_gdf['county'] == county_name
        county_node_count = county_mask.sum()
            
        # Get the starting node ID for this county
        start_node_id = COUNTY_NAME_TO_NODE_START_NUM.get(county_name, 900_001)
            
        # Assign sequential IDs to all nodes in this county
        county_indices = nodes_gdf[county_mask].index
        nodes_gdf.loc[county_indices, 'model_node_id'] = range(start_node_id, start_node_id + county_node_count)
        
    # Create mapping from original osmid to new model_node_id for updating links
    osmid_to_model_id = dict(zip(nodes_gdf['osmid'], nodes_gdf['model_node_id']))
    WranglerLogger.debug(f"TRACE nodes_gdf.loc[nodes_gdf.model_node_id==1000017]:\n{nodes_gdf.loc[nodes_gdf.model_node_id==1000017]}")
    WranglerLogger.debug(f"TRACE nodes_gdf.loc[nodes_gdf.model_node_id==1000014]:\n{nodes_gdf.loc[nodes_gdf.model_node_id==1000014]}")
        
    # Update links A,B using the mapping
    links_gdf['A'] = links_gdf['A'].map(osmid_to_model_id)
    links_gdf['B'] = links_gdf['B'].map(osmid_to_model_id)

    # create model_link_id based on COUNTY_NAME_TO_NUM, assuming 100,000
    links_gdf = links_gdf.sort_values('county').reset_index(drop=True)
    links_gdf['model_link_id'] = 0  # Initialize
        
    # Create link IDs based on assigned county
    for link_county in sorted(links_gdf['county'].unique()):
        county_mask = links_gdf['county'] == link_county
        county_links_count = county_mask.sum()
            
        # Get the county number, use 0 for External or unknown counties
        county_num = COUNTY_NAME_TO_NUM.get(link_county, 0)
            
        # Calculate start_id based on county number
        start_id = county_num * 1_000_000
            
        # Create sequential IDs for this county's links
        county_indices = links_gdf[county_mask].index
        links_gdf.loc[county_indices, 'model_link_id'] = range(start_id, start_id + county_links_count)

    WranglerLogger.debug(f"links_gdf.dtypes\n{links_gdf.dtypes}")
    WranglerLogger.debug(f"links_gdf:\n{links_gdf}")
    # create shape_id, a str version of model_link_id
    links_gdf['shape_id'] = 'sh' + links_gdf['model_link_id'].astype(str)

    if county=="Bay Area":
        # Log detailed county summaries using groupby aggregation
        node_summary = nodes_gdf.groupby('county')['model_node_id'].agg([
            ('count', 'count'),
            ('min', 'min'),
            ('max', 'max')
        ]).sort_index()
        WranglerLogger.info(f"\nCOUNTY SUMMARIES - NODES:\n{node_summary}")
        
        # Link summaries by county
        link_summary = links_gdf.groupby('county').agg({
            'model_link_id': ['count', 'min', 'max'],
            'A': ['min', 'max'],
            'B': ['min', 'max']
        }).sort_index()
        WranglerLogger.info(f"\nCOUNTY SUMMARIES - LINKS:\n{link_summary}")
    else:
        WranglerLogger.debug(f"SUMMARY - NODES:\n{nodes_gdf.describe()}")
        WranglerLogger.debug(f"SUMMARY - LINKS:\n{links_gdf.describe()}")

    standardize_highway_value(links_gdf)
    links_gdf = standardize_lanes_value(links_gdf, trace_tuple=(1002230,1011140))
    links_gdf = handle_links_with_duplicate_A_B(links_gdf)
    # and distance
    links_gdf['distance'] = links_gdf['length']/FEET_PER_MILE

    # additional simplifications
    # Delete links with highway=='service' and no name
    nameless_service_links_gdf = links_gdf.loc[ 
        (links_gdf['highway']=='service') & 
        (links_gdf['name'].isna() |
         (links_gdf['name']=='')) ]
    WranglerLogger.info(f"Removing {len(nameless_service_links_gdf):,} nameless service links")
    WranglerLogger.debug(f"nameless_service_links_gdf:\n{nameless_service_links_gdf}")
    links_gdf = links_gdf.loc[
        (links_gdf['highway']!='service') |
        (links_gdf['name'].notna() &
         (links_gdf['name']!=''))]

    # If hyper format is specified, write to tableau hyper file
    if 'hyper' in OUTPUT_FORMAT:
        tableau_utils.write_geodataframe_as_tableau_hyper(
            links_gdf, 
            OUTPUT_DIR / f"{prefix}{county_no_spaces}_links.hyper", 
            f"{county_no_spaces}_links"
        )

        tableau_utils.write_geodataframe_as_tableau_hyper(
            nodes_gdf, 
            OUTPUT_DIR / f"{prefix}{county_no_spaces}_nodes.hyper", 
            f"{county_no_spaces}_nodes"
        )

    # write to parquet -- this requires simpler column types
    # Outputs depending on specified OUTPUT_FORMAT
    
    # Subset the links columns and convert name, ref to strings
    WranglerLogger.debug(f"links_gdf.dtypes:\n{links_gdf.dtypes}")
    links_non_list_cols = [
        'A','B','key','dupe_A_B','highway','oneway','name','ref','geometry',
        'drive_access','bike_access','walk_access','truck_access','bus_only',
        'lanes','buslanes','length','distance','county','model_link_id','shape_id'
    ]
    parquet_links_gdf = links_gdf[links_non_list_cols].copy()
    parquet_links_gdf['name'] = parquet_links_gdf['name'].astype(str)
    parquet_links_gdf['ref'] = parquet_links_gdf['ref'].astype(str)
    
    if 'parquet' in OUTPUT_FORMAT:
        links_parquet_file = OUTPUT_DIR / f"{prefix}{county_no_spaces}_links.parquet"
        parquet_links_gdf.to_parquet(links_parquet_file)
        WranglerLogger.info(f"Wrote {links_parquet_file}")
    if 'gpkg' in OUTPUT_FORMAT:
        links_gpkg_file = OUTPUT_DIR / f"{prefix}{county_no_spaces}_links.gpkg"
        parquet_links_gdf.to_file(links_gpkg_file, driver='GPKG')
        WranglerLogger.info(f"Wrote {links_gpkg_file}")
    if 'geojson' in OUTPUT_FORMAT:
        links_geojson_file = OUTPUT_DIR / f"{prefix}{county_no_spaces}_links.geojson"
        parquet_links_gdf.to_file(links_geojson_file, driver='GeoJSON')
        WranglerLogger.info(f"Wrote {links_geojson_file}")

    # Subset the nodes columns
    WranglerLogger.debug(f"nodes_gdf.dtypes:\n{nodes_gdf.dtypes}")
    nodes_non_list_cols = [
        'osmid','X','Y','street_count','ref','geometry','county','model_node_id'
    ]
    parquet_nodes_gdf = nodes_gdf[nodes_non_list_cols].copy()
    parquet_nodes_gdf['ref'] = parquet_nodes_gdf['ref'].astype(str)

    if 'parquet' in OUTPUT_FORMAT:
        nodes_parquet_file = OUTPUT_DIR / f"{prefix}{county_no_spaces}_nodes.parquet"
        parquet_nodes_gdf.to_parquet(nodes_parquet_file)
        WranglerLogger.info(f"Wrote {nodes_parquet_file}")
    if 'gpkg' in OUTPUT_FORMAT:
        nodes_gpkg_file = OUTPUT_DIR / f"{prefix}{county_no_spaces}_nodes.gpkg"
        parquet_nodes_gdf.to_file(nodes_gpkg_file, driver='GPKG')
        WranglerLogger.info(f"Wrote {nodes_gpkg_file}")
    if 'geojson' in OUTPUT_FORMAT:
        nodes_geojson_file = OUTPUT_DIR / f"{prefix}{county_no_spaces}_nodes.geojson"
        parquet_nodes_gdf.to_file(nodes_geojson_file, driver='GeoJSON')
        WranglerLogger.info(f"Wrote {nodes_geojson_file}")

    return (links_gdf, nodes_gdf)

if __name__ == "__main__":
    """
    Main execution block for creating MTC networks from OpenStreetMap.
    
    This script follows a multi-step workflow with caching for efficiency:
    
    1. OSM Network Extraction:
       - Downloads road network from OSM via OSMnx
       - Caches raw graph for faster re-runs
       
    2. Network Simplification:
       - Consolidates intersections within tolerance (20 feet)
       - Preserves connectivity and attributes
       - Caches simplified graph
       
    3. Standardization:
       - Processes highway types and lane counts
       - Assigns access permissions by mode
       - Creates model-specific IDs
       
    4. GTFS Integration:
       - Loads 511 Bay Area GTFS feed
       - Filters to specified geography
       - Creates transit stops and links on road network
       
    5. Output Generation:
       - Writes road network with transit
       - Creates feed object for transit
       - Outputs in multiple formats
       
    The script uses caching extensively - if intermediate files exist,
    they are loaded instead of regenerating, significantly speeding up
    iterative development and debugging.
    """

    pd.options.display.max_columns = None
    pd.options.display.width = None
    pd.options.display.min_rows = 20 # number of rows to show in truncated view
    pd.set_option('display.float_format', '{:.2f}'.format)
    # Elevate SettingWithCopyWarning to error
    pd.options.mode.chained_assignment = 'raise'

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("county", type=str, choices=['Bay Area'] + BAY_AREA_COUNTIES)
    parser.add_argument("input_gtfs", type=pathlib.Path, help="Directory with GTFS feed files")
    parser.add_argument("output_dir", type=pathlib.Path, help="Directory to write output files")
    parser.add_argument("output_format", type=str, choices=['parquet','hyper','geojson','gpkg'], help="Output format for network files", nargs = '+')
    args = parser.parse_args()
    args.county_no_spaces = args.county.replace(" ","") # remove spaces
    OUTPUT_DIR = args.output_dir.resolve()
    INPUT_2023GTFS = args.input_gtfs

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    osmnx.settings.use_cache = True
    osmnx.settings.cache_folder = OUTPUT_DIR / "osmnx_cache"
    osmnx.settings.log_file = True
    osmnx.settings.logs_folder = OUTPUT_DIR / "osmnx_logs"
    osmnx.settings.useful_tags_way=OSM_WAY_TAGS.keys()

    # INFO_LOG  = OUTPUT_DIR / f"create_mtc_network_from_OSM_{args.county_no_spaces}_{NOW}.info.log"
    # DEBUG_LOG = OUTPUT_DIR / f"create_mtc_network_from_OSM_{args.county_no_spaces}_{NOW}.debug.log"
    INFO_LOG  = OUTPUT_DIR / f"create_mtc_network_from_OSM_{args.county_no_spaces}.info.log"
    DEBUG_LOG = OUTPUT_DIR / f"create_mtc_network_from_OSM_{args.county_no_spaces}.debug.log"

    network_wrangler.setup_logging(
        info_log_filename=INFO_LOG,
        debug_log_filename=DEBUG_LOG,
        std_out_level="info",
        file_mode='w'
    )
    WranglerLogger.info(f"Created by {__file__}")
    # For now, doing drive as we'll add handle transit and walk/bike separately
    # Aug 24: switch to all because Market Street bus-only links are missing
    OSM_network_type = "all"
    
    # Formatting output and ensuring if hyper is selected, there is another format
    OUTPUT_FORMAT = args.output_format
    ROADWAY_OUTPUT_FORMATS = OUTPUT_FORMAT.copy()
    if 'hyper' in OUTPUT_FORMAT: 
        import tableau_utils                   # only import if needed
        ROADWAY_OUTPUT_FORMATS.remove('hyper') # hyper is only for tableau viz
    WranglerLogger.debug(f"OUTPUT_FORMAT={OUTPUT_FORMAT}")
    WranglerLogger.debug(f"ROADWAY_OUTPUT_FORMATS={ROADWAY_OUTPUT_FORMATS}")
    
    if len(ROADWAY_OUTPUT_FORMATS)==0: 
        WranglerLogger.info("No roadway output formats specified. Please include at least one of 'parquet','geojson','gpkg'. ")
        sys.exit()



    # Skip steps if files are present: Read the simplified network graph rather than creating it from OSM
    g = None
    simplified_graph_file = OUTPUT_DIR / f"1_graph_OSM_{args.county_no_spaces}_simplified{NETWORK_SIMPLIFY_TOLERANCE}.pkl"
    if simplified_graph_file.exists():
        try:
            # Read the MultiDiGraph from the pickle file
            with open(simplified_graph_file, 'rb') as f:
                g = pickle.load(f)
            WranglerLogger.info(f"Read {simplified_graph_file}; graph has {g.number_of_edges():,} edges and {len(g.nodes()):,} nodes")
        except Exception as e:
            WranglerLogger.info(f"Could not read {simplified_graph_file}")
            WranglerLogger.error(e)

    if g == None:
        if args.county == 'Bay Area':
            # Use bounding box approach for Bay Area
            WranglerLogger.info("Creating network for Bay Area using bounding box approach...")
        
            # Get bounding box from shapefile
            # If this is cached, it takes about 3 minutes
            bbox = get_county_bbox(BAY_AREA_COUNTIES)
            WranglerLogger.info(f"Using bounding box: west={bbox[0]:.6f}, south={bbox[1]:.6f}, east={bbox[2]:.6f}, north={bbox[3]:.6f}")
        
            # Use OSMnx to pull the network graph for the bounding box
            # See https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.graph.graph_from_bbox
            g = osmnx.graph_from_bbox(
                bbox,  # (west, south, east, north) tuple
                network_type=OSM_network_type
            )
        else:    
            # Use OXMnx to pull the network graph for a place.
            # See https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.graph.graph_from_place
            #
            # g is a [networkx.MultiDiGraph](https://networkx.org/documentation/stable/reference/classes/multidigraph.html#), 
            # a directed graph with self loops and parallel edges (muliple edges can exist between two nodes)
            WranglerLogger.info(f"Calling osmnx.graph_from_place('{args.county}, California, USA', {OSM_network_type})")
            g = osmnx.graph_from_place(f'{args.county}, California, USA', network_type=OSM_network_type)

        initial_graph_file = OUTPUT_DIR / f"0_graph_OSM_{args.county_no_spaces}.pkl"
        with open(initial_graph_file, "wb") as f: pickle.dump(g, f)
        WranglerLogger.info(f"Wrote {initial_graph_file}")
        WranglerLogger.info(f"Initial graph has {g.number_of_edges():,} edges and {len(g.nodes()):,} nodes")
        if args.county != "Bay Area":
            # For counties, write stadardized version of this too so we can check against simplified
            standardize_and_write(g, args.county, "2_original_")

        # Project to CRS https://epsg.io/2227 where length is feet
        g = osmnx.projection.project_graph(g, to_crs=LOCAL_CRS_FEET)

        WranglerLogger.info(f"Calling osmnx.simplification.consolidate_intersections() with tolerance={NETWORK_SIMPLIFY_TOLERANCE} ")
        # If we do simplification, it must be access-based.
        # Drive links shouldn't be simplified to pedestrian links and vice versa
        #
        # Simplify to consolidate intersections
        # https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.simplification.consolidate_intersections
        g = osmnx.simplification.consolidate_intersections(
            g, 
            tolerance=NETWORK_SIMPLIFY_TOLERANCE, # feet
            rebuild_graph=True,
            dead_ends=True, # keep dead-ends
            reconnect_edges=True,
        )
        WranglerLogger.info(f"After simplifying, graph has {g.number_of_edges():,} edges and {len(g.nodes()):,} nodes")
        with open(simplified_graph_file, "wb") as f: pickle.dump(g, f)
        WranglerLogger.info(f"Wrote {simplified_graph_file}")

    # Skip steps if files are present: Read the roadway network 
    roadway_network = None
    roadway_net_file = f"4_roadway_network_{args.county_no_spaces}"
    try:
        nodes_gdf = gpd.read_parquet(path=OUTPUT_DIR / f"{roadway_net_file}_node.parquet")
        links_gdf = gpd.read_parquet(path=OUTPUT_DIR / f"{roadway_net_file}_link.parquet")
        shapes_gdf = links_gdf.copy()
        roadway_network = load_roadway_from_dataframes(links_gdf, nodes_gdf, shapes_gdf)
        WranglerLogger.info(f"Read roadway network from {roadway_net_file}_[node,link].parquet")
        WranglerLogger.debug(f"roadway_network:\n{roadway_network}")
    except Exception as e:
        # that's ok
        WranglerLogger.debug(f"Failed to read roadway network:")
        WranglerLogger.debug(e)
        pass

    if roadway_network == None:

        # Standardize network graph and create roadway network dataframes
        (links_gdf, nodes_gdf) = standardize_and_write(g, args.county, f"3_simplified_")

        # Create managed lanes fields: access, ML_access, ML_lanes, etc
        # https://network-wrangler.github.io/network_wrangler/latest/networks/#road-links
        create_managed_lanes_fields(links_gdf)

        WranglerLogger.debug(f"links_gdf.head()\n{links_gdf.head()}")
        WranglerLogger.debug(f"nodes_gdf.head()\n{nodes_gdf.head()}")

        # Drop columns that we likely won't need anymore
        # Keep Wrangler columns: 
        LINK_COLS = [
            'A', 'B', 'highway','name','ref','oneway','reversed','length','geometry',
            'access','ML_access','drive_access', 'bike_access', 'walk_access', 'truck_access', 'bus_only',
            'lanes','ML_lanes','distance', 'county', 'model_link_id', 'shape_id'
        ]
        links_gdf = links_gdf[LINK_COLS]
        NODE_COLS = [
            'osmid', 'X', 'Y', 'street_count', 'geometry', 'county', 'model_node_id'
        ]
        nodes_gdf = nodes_gdf[NODE_COLS]

        # create roadway network
        roadway_network = network_wrangler.load_roadway_from_dataframes(
            links_df=links_gdf,
            nodes_df=nodes_gdf,
            shapes_df=links_gdf,
            filter_to_nodes=True,
        )
        WranglerLogger.info(f"Created RoadwayNetwork")
        WranglerLogger.debug(f"roadway_network:\n{roadway_network}")

        # Write the 2023 roadway network to ROADWAY_OUTPUT_FORMATS files
        for roadway_format in ROADWAY_OUTPUT_FORMATS:
            WranglerLogger.info(f"Writing 2023 roadway network to {roadway_format} files...")
            try:
                write_roadway(
                    roadway_network, 
                    out_dir=OUTPUT_DIR,
                    prefix=roadway_net_file,
                    file_format=roadway_format,
                    overwrite=True,
                    true_shape=True
                )
                WranglerLogger.info(f"Roadway network saved to {OUTPUT_DIR} in {roadway_format}")
            except Exception as e:
                WranglerLogger.error(f"Error writing roadway network in {roadway_format}: {e}")
                raise(e)

    # Skip steps if files are present: Read the GtfsModel object
    gtfs_model = None
    gtfs_model_dir = OUTPUT_DIR / f"4_gtfs_model_{args.county_no_spaces}"
    if gtfs_model_dir.exists():
        try:
            gtfs_model = load_feed_from_path(gtfs_model_dir, wrangler_flavored=False, low_memory=False)
            WranglerLogger.info(f"Read gtfs_model from {gtfs_model_dir}")
        except Exception as e:
            WranglerLogger.debug(f"Failed to read gtfs_model from {gtfs_model_dir}")
            WranglerLogger.debug(e)
            pass
    
    if gtfs_model == None:
        # The gtfs feed covers the month of September 2023; select to Wednesday, September 27, 2023
        # gtfs_model doesn't include calendar_dates so read this ourselves
        # tableau viz of this feed: https://10ay.online.tableau.com/#/site/metropolitantransportationcommission/views/regional_feed_511_2023-10/Dashboard1?:iid=1
        calendar_dates_df = pd.read_csv(INPUT_2023GTFS / "calendar_dates.txt")
        WranglerLogger.debug(f"calendar_dates_df (len={len(calendar_dates_df):,}):\n{calendar_dates_df}")
        calendar_dates_df = calendar_dates_df.loc[ (calendar_dates_df.date == 20230927) & (calendar_dates_df.exception_type == 1) ]
        WranglerLogger.debug(f"After filtering calendar_dates_df (len={len(calendar_dates_df):,}):\n{calendar_dates_df}")
        # make service_id a string
        calendar_dates_df['service_id'] = calendar_dates_df['service_id'].astype(str)
        service_ids_df = calendar_dates_df[['service_id']].drop_duplicates().reset_index(drop=True)
        # Convert DataFrame to list for the updated load_feed_from_path function
        service_ids = service_ids_df['service_id'].tolist()
        WranglerLogger.debug(f"Filtering to {len(service_ids):,} service_ids")

        # Read a GTFS network (not wrangler_flavored)
        gtfs_model = load_feed_from_path(INPUT_2023GTFS, wrangler_flavored=False, service_ids_filter=service_ids, low_memory=False)
        WranglerLogger.debug(f"gtfs_model:\n{gtfs_model}")
        # drop some columns that are not required or useful
        gtfs_model.stops.drop(columns=['stop_code','stop_desc','stop_url','tts_stop_name','platform_code','stop_timezone','wheelchair_boarding'], inplace=True)

        if args.county == "Bay Area":
            # Bay Area: drop SFO Airport rail/bus for now
            drop_transit_agency(gtfs_model, agency_id='SI')
        elif args.county in COUNTY_NAME_TO_GTFS_AGENCIES:
            keep_agencies = COUNTY_NAME_TO_GTFS_AGENCIES[args.county]
            WranglerLogger.debug(f"For {args.county}, keeping agencies {keep_agencies}")
            # if this is configured, drop transit agencies not included in this list
            for agency_id in gtfs_model.agency['agency_id'].tolist():
                if agency_id not in keep_agencies:
                    drop_transit_agency(gtfs_model, agency_id=agency_id)
   

        
        county_gdf = pygris.counties(state = 'CA', cache = True, year = 2010)
        county_gdf = county_gdf[county_gdf['NAME10'].isin(BAY_AREA_COUNTIES)].copy()
        if (args.county != "Bay Area"):
            # filter to the given county
            county_gdf = county_gdf.loc[county_gdf['NAME10'] == args.county]
            assert(len(county_gdf) == 1)

        # filter out routes outside of Bay Area
        filter_transit_by_boundary(
            gtfs_model,
            county_gdf, 
            partially_include_route_type_action={RouteType.RAIL: 'truncate'})
        WranglerLogger.debug(f"gtfs_model:\n{gtfs_model}")

        # this one trip_id seems to have the wrong direction_id; it's 0 but should be 1
        gtfs_model.trips.loc[ gtfs_model.trips['trip_id'] == 'PE:t263-sl17-p182-r1A:20230930', 'direction_id'] = 1

        # write filtered gtfs_model
        gtfs_model_dir.mkdir(exist_ok=True)
        write_transit(
            gtfs_model,
            gtfs_model_dir,
            prefix=f"gtfs_model_{args.county_no_spaces}",
            overwrite=True
        )

    # Define time periods for frequency calculation: 3a-6a, 6a-10a, 10a-3p, 3p-7p, 7p-3a
    # https://bayareametro.github.io/travel-model-two/develop/input/#time-periods
    TIME_PERIODS = {
        'EA':['03:00','06:00'],  # 3a-6a
        'AM':['06:00','10:00'],  # 6a-10a
        'MD':['10:00','15:00'],  # 10a-3p
        'PM':['15:00','19:00'],  # 3p-7p
        'EV':['19:00','03:00'],  # 7p-3a (crosses midnight)
    }
    # If max_rows is exceeded, switch to truncate view
    pd.options.display.max_rows = 500

    try:
        feed = create_feed_from_gtfs_model(
            gtfs_model,
            roadway_network,
            local_crs=LOCAL_CRS_FEET,
            crs_units="feet",
            timeperiods=TIME_PERIODS,
            frequency_method='median_headway',
            default_frequency_for_onetime_route=180*60, # 180 minutes
            add_stations_and_links=True,
            trace_shape_ids=[
                # 'SF:140:20230930',  # powell-hyde cable car
                # 'SF:2751:20230930', # 27 bus
                'SF:60:20230930',    # LOWL bus line
                'SF:19800:20230930', # F line LRT
            ]
        )
        WranglerLogger.debug(f"Created feed from gtfs_model: {feed}")
    except Exception as e:
        WranglerLogger.error(e)

        WranglerLogger.error(vars(e).keys())

        if hasattr(e,'bus_stop_links_gdf'):
            WranglerLogger.debug(f"bus_stop_links_gdf type={type(e.bus_stop_links_gdf)}")
            if 'hyper' in OUTPUT_FORMAT:
                tableau_utils.write_geodataframe_as_tableau_hyper(
                    e.bus_stop_links_gdf,
                    OUTPUT_DIR / f"bus_stop_links.hyper",
                    "bus_stop_links_gdf"
                )
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'bus_stop_links.hyper'}")
            elif 'parquet' in OUTPUT_FORMAT:
                e.bus_stop_links_gdf.to_parquet(OUTPUT_DIR / f'bus_stop_links.parquet')
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'bus_stop_links.parquet'}")
            elif 'gpkg' in OUTPUT_FORMAT:
                e.bus_stop_links_gdf.to_file(OUTPUT_DIR / f'bus_stop_links.gpkg', driver='GPKG')
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'bus_stop_links.gpkg'}")
            elif 'geojson' in OUTPUT_FORMAT:
                e.bus_stop_links_gdf.to_file(OUTPUT_DIR / f'bus_stop_links.geojson', driver='GEOJSON')
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'bus_stop_links.geojson'}")

        if hasattr(e, "bus_stops_gdf"):
            if 'hyper' in OUTPUT_FORMAT:
                WranglerLogger.debug(f"bus_stops_gdf type={type(e.bus_stops_gdf)}")
                tableau_utils.write_geodataframe_as_tableau_hyper(
                    e.bus_stops_gdf,
                    OUTPUT_DIR / f"bus_stops.hyper",
                    "bus_stops_gdf"
                )
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'bus_stops.hyper'}")
            elif 'parquet' in OUTPUT_FORMAT:
                e.bus_stop_gdf.to_parquet(OUTPUT_DIR / f'bus_stop.parquet')
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'bus_stop.parquet'}")
            elif 'gpkg' in OUTPUT_FORMAT:
                e.bus_stop_gdf.to_file(OUTPUT_DIR / f'bus_stop.gpkg', driver='GPKG')
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'bus_stop.gpkg'}")
            elif 'geojson' in OUTPUT_FORMAT:
                e.bus_stop_gdf.to_file(OUTPUT_DIR / f'bus_stop.geojson', driver='GEOJSON')
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'bus_stop.geojson'}")

        
        if hasattr(e, "no_bus_path_gdf"):
            if 'hyper' in OUTPUT_FORMAT:
                WranglerLogger.debug(f"no_bus_path_gdf type={type(e.no_bus_path_gdf)}")
                tableau_utils.write_geodataframe_as_tableau_hyper(
                    e.no_bus_path_gdf,
                    OUTPUT_DIR / f"no_bus_path.hyper",
                    "no_bus_path"
                )
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'no_bus_path.hyper'}")
            elif 'parquet' in OUTPUT_FORMAT:
                e.no_bus_path_gdf.to_parquet(OUTPUT_DIR / f'no_bus_path.parquet')
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'no_bus_path.parquet'}")
            elif 'gpkg' in OUTPUT_FORMAT:
                e.no_bus_path_gdf.to_file(OUTPUT_DIR / f'no_bus_path.gpkg', driver='GPKG')
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'no_bus_path.gpkg'}")
            elif 'geojson' in OUTPUT_FORMAT:
                e.no_bus_path_gdf.to_file(OUTPUT_DIR / f'no_bus_path.geojson', driver='GEOJSON')
                WranglerLogger.info(f"Wrote {OUTPUT_DIR / f'no_bus_path.geojson'}")

        raise(e)

    # write roadway network again because now it has the transit
    roadway_net_file = f"5_roadway_network_inc_transit_{args.county_no_spaces}"
    for roadway_format in ROADWAY_OUTPUT_FORMATS:
        WranglerLogger.info(f"Writing roadway network with transit links to {roadway_format} files...")
        try:
            write_roadway(
                roadway_network, 
                out_dir=OUTPUT_DIR,
                prefix=roadway_net_file,
                file_format=roadway_format,
                overwrite=True,
                true_shape=True
            )
        except Exception as e:
            # that's ok
            WranglerLogger.error(f"Error writing roadway network in {roadway_format}: {e}")
            WranglerLogger.debug(e)
            pass

    # write feed object
    feed_dir = OUTPUT_DIR / f"6_feed_{args.county_no_spaces}"
    feed_dir.mkdir(exist_ok=True)
    write_transit(
        feed,
        feed_dir,
        prefix=f"feed_{args.county_no_spaces}",
        overwrite=True
    )

    # create a transit network
    transit_network = load_transit(feed=feed)
    WranglerLogger.info(f"Created transit_network:\n{transit_network}")

    shape_link_wo_road_df = shape_links_without_road_links(feed.shapes, roadway_network.links_df)
    WranglerLogger.debug(f"shape_link_wo_road_df len={len(shape_link_wo_road_df)} type={type(shape_link_wo_road_df)}")
    WranglerLogger.debug(f"shape_link_wo_road_df:\n{shape_link_wo_road_df}")

    shape_link_wo_road_df['geometry'] = shape_link_wo_road_df.apply(
        lambda row: shapely.geometry.LineString([row["geometry_A"], row["geometry_B"]]),
        axis=1
    )
    shape_link_wo_road_gdf = gpd.GeoDataFrame(shape_link_wo_road_df, crs=LAT_LON_CRS)
    tableau_utils.write_geodataframe_as_tableau_hyper(
        shape_link_wo_road_gdf, 
        OUTPUT_DIR / "shape_link_wo_road.hyper",
        "shape_link_wo_road"
    )

    raise

    # finally, create a scenario
    my_scenario = network_wrangler.scenario.create_scenario(
        base_scenario = {
            "road_net": roadway_network,
            "transit_net": transit_network,
            "applied_projects": [],
            "conflicts": {}
        },
    )
    # write it to disk
    scenario_dir = OUTPUT_DIR / "7_wrangler_scenario"
    scenario_dir.mkdir(exist_ok=True)
    my_scenario.write(path=scenario_dir )
    WranglerLogger.info(f"Wrote scenario to {scenario_dir}")