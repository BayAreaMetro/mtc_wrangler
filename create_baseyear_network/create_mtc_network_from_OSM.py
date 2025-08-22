USAGE = """

Create MTC base year networks (2023) from OSM.

Tested in July 2025 with:
  * network_wrangler, https://github.com/network-wrangler/network_wrangler/tree/main

References:
  * Asana: GMNS+ / NetworkWrangler2 > Build 2023 network using existing tools (https://app.asana.com/1/11860278793487/project/15119358130897/task/1210468893117122?focus=true)
  * MTC Year 2023 Network Creation Steps Google Doc (https://docs.google.com/document/d/1TU0nsUHmyKfYZDbwjeCFiW09w53fyWu7X3XcRlNyf2o/edit?tab=t.0#heading=h.kjbu68swdkst)
  * network_wrangler\\notebook\\Create Network from OSM.ipynb
"""
import argparse
import datetime
import getpass
import pathlib
import pickle
import pprint
import statistics
from typing import Any, Optional, Tuple, Union

import networkx
import osmnx
import numpy as np
import pandas as pd
import geopandas as gpd

import tableau_utils
import network_wrangler
from network_wrangler import WranglerLogger
from network_wrangler.params import LAT_LON_CRS
from network_wrangler.roadway.io import load_roadway_from_dataframes, write_roadway
from network_wrangler.transit.io import load_feed_from_path, write_transit
from network_wrangler.models.gtfs.types import RouteType
from network_wrangler.utils.transit import \
  drop_transit_agency, filter_transit_by_boundary, create_feed_from_gtfs_model, truncate_route_at_stop

COUNTY_SHAPEFILE = pathlib.Path("M:\\Data\\Census\\Geography\\tl_2010_06_county10\\tl_2010_06_county10_9CountyBayArea.shp")
INPUT_2023GTFS = pathlib.Path("M:\\Data\\Transit\\511\\2023-09")
OUTPUT_DIR = pathlib.Path("M:\\Development\\Travel Model Two\\Supply\\Network Creation 2025\\from_OSM")
NOW = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
USERNAME = getpass.getuser()
if USERNAME=="lmz":
    COUNTY_SHAPEFILE = pathlib.Path("../../tl_2010_06_county10/tl_2010_06_county10_9CountyBayArea.shp").resolve()
    INPUT_2023GTFS = pathlib.Path("../../511gtfs_2023-09").resolve()
    OUTPUT_DIR = pathlib.Path("../../output_from_OSM").resolve()

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

NETWORK_SIMPLIFY_TOLERANCE = 30 # feet

LOCAL_CRS_FEET = "EPSG:2227"
""" NAD83 / California zone 3 (ftUS) https://epsg.io/2227 """

COUNTY_NAME_TO_GTFS_AGENCIES = {
    'San Francisco': [
        'SF', # SF Muni
        'BA', # BART
        'GG', # Golden Gate Transit
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
    'unclassified',   # minor public roads
    'residential',    # residential street
    'living_street',  # pedestrian-focused residential
    'service',        # vehicle access to building, parking lot, etc.
    'track',          # minor land-access roads
]

def get_county_bbox(county_shapefile: pathlib.Path) -> tuple[float, float, float, float]:
    """
    Read county shapefile and return bounding box in WGS84 coordinates.
    
    Args:
        county_shapefile: Path to the county shapefile
        
    Returns:
        tuple: Bounding box as (north, south, east, west) for OSMnx
               Note: OSMnx expects (north, south, east, west) not (minx, miny, maxx, maxy)
    """
    WranglerLogger.info(f"Reading county shapefile from {county_shapefile}")
    county_gdf = gpd.read_file(county_shapefile)
    
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
    Convert lane value to integer, handling various input formats.
    
    For lists with two items, returns the minimum value.
    For lists with more than two items, returns the median value.
    
    Args:
        lane: Lane value that can be:
            - An integer
            - A string representation of an integer  
            - A list of integers or string representations of integers
    
    Returns:
        The processed lane count as an integer.
    
    Examples:
        >>> get_min_or_median_value(3)
        3
        >>> get_min_or_median_value('2')
        2
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
        return int(lane)
    return lane

def standardize_lanes_value(
        links_gdf: gpd.GeoDataFrame,
        trace_tuple: Optional[tuple[int, int]] = None
    ) -> gpd.GeoDataFrame:
    """
    Standardize the lanes value in the links GeoDataFrame.
    
    Processes lane-related columns in the GeoDataFrame to ensure consistent
    lane count representation. Handles various OSM lane tagging formats including
    forward/backward lanes, handles reversed links, and fills missing lane values
    based on highway type.
    
    Args:
        links_gdf: GeoDataFrame containing OSM link data with columns:
            - oneway: Boolean or list indicating if link is one-way
            - reversed: Boolean or list indicating if link direction is reversed  
            - lanes: Total number of lanes
            - lanes:forward: Number of forward lanes (optional)
            - lanes:backward: Number of backward lanes (optional)
            - highway: OSM highway type
    
    Returns:
        Modified links_gdf with standardized lane values.
        The 'lanes' column will have integer values (no -1 placeholders). This *does not include* buslanes.
        The 'buslanes' column will have integer values (no -1 placeholders).
    
    Side Effects:
        Modifies the input GeoDataFrame in place.
    
    Notes:
        - Handles OSM bidirectional links that are represented as lists
        - Merges forward/backward lane counts when appropriate
        - Creates highway-to-lanes mapping for filling missing values
        - Uses sensible defaults for highway types without samples
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
    links_gdf_AltB = links_gdf.loc[ links_gdf.A < links_gdf.B]
    links_gdf_BltA = links_gdf.loc[ links_gdf.B < links_gdf.A]

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

def standardize_highway_value(links_gdf: gpd.GeoDataFrame) -> None:
    """Standardize the highway value in the links GeoDataFrame.

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
            - bus_access: Boolean for bus access
    
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
    links_gdf['bus_access']   = True

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

    # remove drive_access, truck_access, bus_access from non-auto links
    links_gdf.loc[links_gdf.highway.isin(['path','footway','cycleway']), 'drive_access'] = False
    links_gdf.loc[links_gdf.highway.isin(['path','footway','cycleway']), 'truck_access'] = False
    links_gdf.loc[links_gdf.highway.isin(['path','footway','cycleway']), 'bus_access'] = False
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

def get_roadway_value(highway: Union[str, list[str]]) -> str:
    """
    Extract a single highway value from potentially multiple values.
    
    When multiple values are present (as a list), returns the first one.
    
    Args:
        highway: Either a single highway type string or a list of highway types.
    
    Returns:
        A single highway type string.
    
    Examples:
        >>> get_roadway_value('primary')
        'primary'
        >>> get_roadway_value(['primary', 'secondary'])
        'primary'
    """
    if isinstance(highway,list):
        WranglerLogger.debug(f"list: {highway}")
        return highway[0]
    return highway

def handle_links_with_duplicate_A_B(
        links_gdf: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
    """Handle links with duplicate u,v by merging or dropping
    """
    WranglerLogger.info("Handling links with duplicate (A,B)")
    WranglerLogger.debug(f"\n{links_gdf}")

    debug_cols = ['A','B','key','highway','oneway','reversed','name','ref','length','lanes','buslanes']
    WranglerLogger.debug(f"links to fix:\n{links_gdf.loc[ links_gdf.dupe_A_B, debug_cols]}")

    grouped_links_gdf = links_gdf.loc[links_gdf.dupe_A_B].groupby(by=['A','B'])
    unduped_df = pd.DataFrame()
    # Iterate through the groups
    for group_name, group_links_df in grouped_links_gdf:
        group_links_df['highway_level'] = group_links_df['highway'].apply(lambda x: HIGHWAY_HIERARCHY.index(x) if x in HIGHWAY_HIERARCHY else 100)
        group_links_df.sort_values(by='highway_level', inplace=True)
        # WranglerLogger.debug(f"Group for {group_name} type={type(group_links_df)}:\n{group_links_df[debug_cols + ['highway_level']]}")

        # if the last one is a busway, then just add the buslane to the GP link
        first_row = group_links_df.iloc[0]
        last_row = group_links_df.iloc[-1]
        if last_row['highway'] == 'busway':
            # add buslanes to first row and delete
            group_links_df.loc[group_links_df.index[0], 'buslanes'] = last_row['buslanes']
            group_links_df.drop(group_links_df.index[-1], inplace=True)

        if len(group_links_df) == 1:
            unduped_df = pd.concat([unduped_df, group_links_df])
            continue

        # if the last row has the same name or no name then assume it's a variant of the same and add lanes
        if (last_row['name'] == first_row['name']) or (not last_row['name']):
            group_links_df.loc[group_links_df.index[0], 'lanes'] = first_row['lanes'] + last_row['lanes']
            group_links_df.drop(group_links_df.index[-1], inplace=True)

        if len(group_links_df) == 1:
            unduped_df = pd.concat([unduped_df, group_links_df])
            continue

        # otherwise, just drop the other links
        while len(group_links_df) > 1:
            group_links_df.drop(group_links_df.index[-1], inplace=True)

        unduped_df = pd.concat([unduped_df, group_links_df])
        continue

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
    WranglerLogger.info(f"{full_gdf['dupe_A_B'].sum()=:,}")
    return full_gdf


def standardize_and_write(
        g: networkx.MultiDiGraph,
        county: str, 
        prefix: str
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Standardize fields and write the given graph's links and nodes to Tableau Hyper format.
    
    Processes an OSMnx graph by:
    1. Projecting to WGS84 (EPSG:4326)
    2. Converting to GeoDataFrames
    3. Removing duplicate edges (keeping shortest) and loop edges (A==B)
    4. Standardizing highway and lane values
    5. For Bay Area: assigning counties via spatial join
    6. Writing to Tableau Hyper files
    
    Args:
        g: NetworkX MultiDiGraph from OSMnx containing the road network.
        county: County name or "Bay Area" for multi-county processing
        prefix: String prefix to prefix to output filenames
    
    Returns:
        A tuple containing:
            - links_gdf: GeoDataFrame of standardized road links/edges
            - nodes_gdf: GeoDataFrame of road network nodes
    
    Side Effects:
        Writes two Tableau Hyper files to OUTPUT_DIR:
            - {prefix}{county_no_spaces}_links.hyper
            - {prefix}{county_no_spaces}_nodes.hyper
    
    Notes:
        - Renames columns u,v to A,B for consistency
        - Drops duplicate edges between same nodes, keeping shortest
        - For Bay Area, performs spatial join to assign counties
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
        county_gdf = gpd.read_file(COUNTY_SHAPEFILE)
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
        multicounty_links_gdf = links_gdf[links_gdf.duplicated(subset=['index'], keep=False)].copy()
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
                by=['index','intersection_length'],
                ascending=[True, False],
                inplace=True)
            WranglerLogger.debug(f"multicounty_links_gdf:\n{multicounty_links_gdf}")
            # drop duplicates now, keeping first
            multicounty_links_gdf.drop_duplicates(subset=['index'], keep='first', inplace=True)
            WranglerLogger.debug(f"After dropping duplicates: multicounty_links_gdf:\n{multicounty_links_gdf}")
            
            # put them back together
            links_gdf = pd.concat([
                links_gdf.drop_duplicates(subset=['index'], keep=False), # single-county links
                multicounty_links_gdf
            ])
            # verify that each link is only represented once
            multicounty_links_gdf = links_gdf[links_gdf.duplicated(subset=['index'], keep=False)]
            assert(len(multicounty_links_gdf)==0)

            # drop temporary columns
            links_gdf.drop(columns=['index','index_right','intersection_length'], inplace=True)
            links_gdf.reset_index(drop=True, inplace=True)
            
        # Drop the extra columns from spatial join
        links_gdf = links_gdf.drop(columns=['index','index_right'], errors='ignore')

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
    links_gdf = standardize_lanes_value(links_gdf, trace_tuple=(1000017,1000014))
    links_gdf = handle_links_with_duplicate_A_B(links_gdf)
    # and distance
    links_gdf['distance'] = links_gdf['length']/FEET_PER_MILE

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

    # Subset the links columns and convert name, ref to strings
    WranglerLogger.debug(f"links_gdf.dtypes:\n{links_gdf.dtypes}")
    links_non_list_cols = [
        'A','B','key','dupe_A_B','highway','oneway','name','ref','geometry',
        'drive_access','bike_access','walk_access','truck_access','bus_access',
        'lanes','buslanes','length','distance','county','model_link_id','shape_id'
    ]
    parquet_links_gdf = links_gdf[links_non_list_cols].copy()
    parquet_links_gdf['name'] = parquet_links_gdf['name'].astype(str)
    parquet_links_gdf['ref'] = parquet_links_gdf['ref'].astype(str)

    links_parquet_file = OUTPUT_DIR / f"{prefix}{county_no_spaces}_links.parquet"
    parquet_links_gdf.to_parquet(links_parquet_file)
    WranglerLogger.info(f"Wrote {links_parquet_file}")
    links_gpkg_file = pathlib.Path(str(links_parquet_file).replace("parquet","gpkg"))
    parquet_links_gdf.to_file(links_gpkg_file, driver='GPKG')
    WranglerLogger.info(f"Wrote {links_gpkg_file}")

    # Subset the nodes columns
    WranglerLogger.debug(f"nodes_gdf.dtypes:\n{nodes_gdf.dtypes}")
    nodes_non_list_cols = [
        'osmid','X','Y','street_count','ref','geometry','county','model_node_id'
    ]
    parquet_nodes_gdf = nodes_gdf[nodes_non_list_cols].copy()
    parquet_nodes_gdf['ref'] = parquet_nodes_gdf['ref'].astype(str)

    nodes_parquet_file = OUTPUT_DIR / f"{prefix}{county_no_spaces}_nodes.parquet"
    parquet_nodes_gdf.to_parquet(nodes_parquet_file)
    WranglerLogger.info(f"Wrote {nodes_parquet_file}")
    nodes_gpkg_file = pathlib.Path(str(nodes_parquet_file).replace("parquet","gpkg"))
    parquet_nodes_gdf.to_file(nodes_gpkg_file, driver='GPKG')
    WranglerLogger.info(f"Wrote {nodes_gpkg_file}")

    return (links_gdf, nodes_gdf)

if __name__ == "__main__":

    pd.options.display.max_columns = None
    pd.options.display.width = None
    pd.set_option('display.float_format', '{:.2f}'.format)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    osmnx.settings.use_cache = True
    osmnx.settings.cache_folder = OUTPUT_DIR / "osmnx_cache"
    osmnx.settings.log_file = True
    osmnx.settings.logs_folder = OUTPUT_DIR / "osmnx_logs"
    osmnx.settings.useful_tags_way=OSM_WAY_TAGS.keys()

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("county", type=str, choices=['Bay Area'] + BAY_AREA_COUNTIES)
    args = parser.parse_args()
    args.county_no_spaces = args.county.replace(" ","") # remove spaces

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
    OSM_network_type = "drive"

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
            bbox = get_county_bbox(COUNTY_SHAPEFILE)
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

        WranglerLogger.info(f"Calling smnx.simplification.consolidate_intersections() with tolerance={NETWORK_SIMPLIFY_TOLERANCE} ")
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
    roadway_net_parquet_file = f"4_roadway_network_{args.county_no_spaces}"
    try:
        nodes_gdf = gpd.read_parquet(path=OUTPUT_DIR / f"{roadway_net_parquet_file}_node.parquet")
        links_gdf = gpd.read_parquet(path=OUTPUT_DIR / f"{roadway_net_parquet_file}_link.parquet")
        shapes_gdf = links_gdf.copy()
        roadway_network = load_roadway_from_dataframes(links_gdf, nodes_gdf, shapes_gdf)
        WranglerLogger.info(f"Read roadway network from {roadway_net_parquet_file}_[node,link].parquet")
        WranglerLogger.debug(f"roadway_network:\n{roadway_network}")
    except Exception as e:
        # that's ok
        WranglerLogger.debug(f"Failed to read roadway network:")
        WranglerLogger.debug(e)
        pass

    if roadway_network == None:

        # Standardize network graph and create roadway network dataframes
        (links_gdf, nodes_gdf) = standardize_and_write(g, args.county, f"3_simplified_")

        WranglerLogger.debug(f"links_gdf.head()\n{links_gdf.head()}")
        WranglerLogger.debug(f"nodes_gdf.head()\n{nodes_gdf.head()}")

        # Drop columns that we likely won't need anymore
        LINK_COLS = [
            'A', 'B', 'highway','name','ref','oneway','reversed','length','geometry',
            'drive_access', 'bike_access', 'walk_access', 'truck_access', 'bus_access',
            'lanes', 'distance', 'county', 'model_link_id', 'shape_id'
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
            shapes_df=links_gdf
        )
        WranglerLogger.info(f"Created RoadwayNetwork")
        WranglerLogger.debug(f"roadway_network:\n{roadway_network}")

        # Write the 2023 roadway network to parquet files
        WranglerLogger.info("Writing 2023 roadway network to parquet files...")
        try:
            write_roadway(
                roadway_network, 
                out_dir=OUTPUT_DIR,
                prefix=roadway_net_parquet_file,
                file_format="parquet",
                overwrite=True,
                true_shape=True
            )
            WranglerLogger.info(f"Roadway network saved to {OUTPUT_DIR}")
        except Exception as e:
            WranglerLogger.error(f"Error writing roadway network: {e}")
            raise(e)

    # Skip steps if files are present: Read the GtfsModel object
    gtfs_model = None
    gtfs_model_dir = OUTPUT_DIR / f"4_gtfs_model_{args.county_no_spaces}"
    if gtfs_model_dir.exists():
        try:
            gtfs_model = load_feed_from_path(gtfs_model_dir, wrangler_flavored=False)
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
        service_ids = calendar_dates_df[['service_id']].drop_duplicates().reset_index(drop=True)
        WranglerLogger.debug(f"After filtering service_ids (len={len(service_ids):,}):\n{service_ids}")

        # Read a GTFS network (not wrangler_flavored)
        gtfs_model = load_feed_from_path(INPUT_2023GTFS, wrangler_flavored=False, service_ids_filter=service_ids)
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
   

        county_gdf = gpd.read_file(COUNTY_SHAPEFILE)
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
            skip_stop_agencies = 'CT'
        )
        WranglerLogger.debug(f"Created feed from gtfs_model: {feed}")
    except Exception as e:
        WranglerLogger.error(e)
        raise(e)

    # create a transit network
    # transit_network = network_wrangler.transit.io.load_transit("M:\\Data\\Transit\\511\\2023-10")

    # finally, create a scenario
    # my_scenario = network_wrangler.scenario.create_scenario(
    #     base_year_scenario = {
    #         "road_net": roadway_network,
    #         "transit_net": transit_network,
    #         "applied_projects": [],
    #         "conflicts": {}
    #     },
    # )
    # write it to disk
    # scenario_dir = OUTPUT_DIR / "wrangler_scenario"
    # scenario_dir.mkdir(exist_ok=True)
    # my_scenario.write(path=scenario_dir )
    # WranglerLogger.info(f"Wrote scenario to {scenario_dir}")