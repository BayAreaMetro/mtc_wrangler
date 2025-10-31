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

Tested in July-September 2025 with:
  * BayArea network_wrangler fork: https://github.com/BayAreaMetro/network_wrangler
    (pull request pending: https://github.com/network-wrangler/network_wrangler/pull/408)
    -> centroids branch
  * OSMnx v1.9+
  * GTFS feed from 511 Bay Area (September 2023)

References:
  * Asana: GMNS+ / NetworkWrangler2 > Build 2023 network using existing tools 
    https://app.asana.com/1/11860278793487/project/15119358130897/task/1210468893117122
  * MTC Year 2023 Network Creation Steps Google Doc
    https://docs.google.com/document/d/1TU0nsUHmyKfYZDbwjeCFiW09w53fyWu7X3XcRlNyf2o/edit
  * network_wrangler\\notebook\\Create Network from OSM.ipynb

Usage:
    python create_mtc_network_from_OSM.py <county> <input_gtfs> <output_dir> <output_format> [--trace-shape-ids <shape_id1> <shape_id2> ...]

    where <county> is one of:
    - 'Bay Area' (entire 9-county region)
    - Individual county names: 'San Francisco', 'San Mateo', 'Santa Clara',
      'Alameda', 'Contra Costa', 'Solano', 'Napa', 'Sonoma', 'Marin'

Example:
    python create_mtc_network_from_OSM.py "San Francisco" ../../511gtfs_2023-09 ../../output_from_OSM/SanFrancisco parquet hyper
    python create_mtc_network_from_OSM.py "Santa Clara" ../../511gtfs_2023-09 ../../output_from_OSM/SantaClara parquet --trace-shape-ids "SF:366:20230930" "SF:2808:20230930"
"""

USAGE = __doc__
import argparse
import datetime
import pathlib
import pickle
import pprint
import requests
import statistics
import sys
from typing import Any, Optional, Tuple, Union
import us

import networkx
import osmnx
import numpy as np
import pandas as pd
import geopandas as gpd
import pygris
import shapely.geometry

import tableau_utils
import network_wrangler
from network_wrangler import WranglerLogger
from network_wrangler.params import LAT_LON_CRS
from network_wrangler.roadway.network import RoadwayNetwork
from network_wrangler.roadway.io import load_roadway_from_dataframes, write_roadway
from network_wrangler.roadway.nodes.name import add_roadway_link_names_to_nodes
from network_wrangler.roadway.nodes.filters import filter_nodes_to_links
from network_wrangler.utils.geo import add_direction_to_links
from network_wrangler.models.gtfs.gtfs import GtfsModel
from network_wrangler.transit.feed.feed import Feed
from network_wrangler.transit.network import TransitNetwork
from network_wrangler.transit.io import load_feed_from_path, write_transit, load_transit
from network_wrangler.models.gtfs.types import RouteType
from network_wrangler.utils.transit import \
  drop_transit_agency, filter_transit_by_boundary, create_feed_from_gtfs_model
from network_wrangler.roadway.centroids import FitForCentroidConnection, add_centroid_nodes, add_centroid_connectors

# Suppress FutureWarning about downcasting in fillna
# The traceback shows this occurs in pandera/backends/pandas/container.py line 570
# when pandera calls fillna() during DataFrame validation
import warnings
warnings.filterwarnings('ignore', 
                       message='Downcasting object dtype arrays on .fillna',
                       category=FutureWarning,
                       module='pandera')

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

NETWORK_SIMPLIFY_TOLERANCE = 30 # feet

LOCAL_CRS_FEET = "EPSG:2227"
""" NAD83 / California zone 3 (ftUS) https://epsg.io/2227 """

COUNTY_NAME_TO_GTFS_AGENCIES = {
    'Alameda': [
        'AC', # AC Transit
        'BA', # BART
        # 'AM', # Capital Corridor - remove this because it dips in and out of county
        # 'CE', # ACE -  remove this because it dips in and out of county
        'EM', # Emery Go-Round
        'WH', # LAVTA
        'UC', # Union City Transi
    ],
    'Contra Costa': [
        'CC', # County Connection
        'FS', # FAST
        'RV', # Rio Vista Delta Breeze
        '3D', # Tri Delta Transit
        'VC', # Vacaville City Coach
        'WC', # WestCat (Western Contra Costa)
    ],
    'Marin': [
        'GG', # Golden Gate Transit
        'MA', # Marin Transit
        'SA', # SMART
    ],
    'Napa': [
        'VN', # VINE Transit
    ],
    'San Francisco': [
        'SF', # SF Muni
        'BA', # BART
        # 'GG', # Golden Gate Transit
        'CT', # Caltrain
        'MB', # Mission Bay TMA
    ],
    'San Mateo': [
        'SM', # SamTrans
        'BA', # BART
        # 'CT', # Caltrain
    ],
    'Santa Clara': [
        'MV', # Mountain View Go
        'SC', # VTA
    ],
    'Solano': [
        'ST', # SolTrans
    ],
    'Sonoma': [
        'PE', # Petaluma
        'SR', # Santa Rosa CityBus
        'SO', # Sonoma County Transit
        'SA', # SMART
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
    'cycleway:both'      : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:cycleway:both
    'cycleway:left'      : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:cycleway:left
    'cycleway:right'     : TAG_STRING,   # https://wiki.openstreetmap.org/wiki/Key:cycleway:right
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

def get_county_geodataframe(
        output_dir: pathlib.Path,
        state: str
) -> gpd.GeoDataFrame:
    """
    Fetch the US Census TIGER shapefile for 2010 county shapes using pygris,
    or uses cached version if available.

    Saves to output_dir / tl_2010_us_county10 / tl_2010_us_county10.shp
    """
    county_shapefile = output_dir / "tl_2010_us_county10" / "tl_2010_us_county10.shp"
    if county_shapefile.exists():
        county_gdf = gpd.read_file(county_shapefile)
        WranglerLogger.info(f"Read {county_shapefile}")
    else:
        WranglerLogger.info(f"Fetching California 2010 county shapes using pygris")
        county_gdf = pygris.counties(state = 'CA', cache = True, year = 2010)
        # save it to the cache dir
        county_shapefile.parent.mkdir(exist_ok=True)
        county_gdf.to_file(county_shapefile)

    my_state = us.states.lookup(state)
    county_gdf = county_gdf.loc[ county_gdf["STATEFP10"] == my_state.fips]
    WranglerLogger.debug(f"county_gdf:\n{county_gdf}")
    return county_gdf

def get_county_bbox(
        counties: list[str],
        base_output_dir: pathlib.Path,
) -> tuple[float, float, float, float]:
    """
    The coordinates are converted to WGS84 (EPSG:4326) if needed.

    Args:
        counties: list of California counties to include.
        base_output_dir: Base directory for shared resources (county shapefiles)

    Returns:
        tuple: Bounding box as (west, south, east, north) in decimal degrees.
               These are longitude/latitude coordinates in WGS84 projection.

    Note:
        The returned tuple order (west, south, east, north) matches the format
        expected by osmnx.graph_from_bbox() function.
    """
    county_gdf = get_county_geodataframe(base_output_dir, "CA")
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
        try:
            return int(float(lane)) # float conversion first for values like 1.5
        except ValueError:
            # if it's not a string at all
            WranglerLogger.error(f"get_min_or_median_value() for lane string:{lane}")
            return 0
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
    - corridor         hallway inside a building (https://wiki.openstreetmap.org/wiki/Tag:highway%3Dcorridor)
                       -> converted to footway
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
    # corridor -> footway
    links_gdf.loc[links_gdf.highway == 'corridor', 'highway'] = 'footway'

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

    ################ set drive_centroid_fit, walk_centroid_fit ################
    # for centroid connectors, assess fit for centroid connectors based on highway value
    centroid_dict = {"drive":{}, "walk":{}} # mode -> highway -> fitness
    current_fit = FitForCentroidConnection.DO_NOT_USE # start with this for freeways
    for highway in HIGHWAY_HIERARCHY:
        # special values
        if highway in ["busway","living_street","track"]:
            centroid_dict["drive"][highway] = FitForCentroidConnection.DO_NOT_USE

            if highway in ["busway"]:
                centroid_dict["walk"][highway] = FitForCentroidConnection.DO_NOT_USE
            else:
                centroid_dict["walk"][highway] = FitForCentroidConnection.GOOD
            continue

        # higher in hierarchy is worse
        # for now, walk and drive are the same
        if highway == "primary":
            current_fit = FitForCentroidConnection.OKAY
        elif highway == "secondary":
            current_fit = FitForCentroidConnection.GOOD
        elif highway == "unclassified":
            current_fit = FitForCentroidConnection.BEST
        
        # set it
        centroid_dict["drive"][highway] = current_fit
        centroid_dict["walk"][highway] = current_fit
    
    for highway in ["path","cycleway","footway"]:
        centroid_dict["walk"][highway] = FitForCentroidConnection.BEST

    for mode in centroid_dict.keys():
        WranglerLogger.debug(f"centroid_dict[{mode}]:\n{pprint.pformat(centroid_dict[mode])}")
        links_gdf[f"{mode}_centroid_fit"] = links_gdf["highway"].map(centroid_dict[mode])
        # fill na with Not ok
        links_gdf[f"{mode}_centroid_fit"] = links_gdf[f"{mode}_centroid_fit"].fillna(FitForCentroidConnection.DO_NOT_USE)
        links_gdf[f"{mode}_centroid_fit"] = links_gdf[f"{mode}_centroid_fit"].astype(int)
        WranglerLogger.debug(f"{mode}_centroid_fit:\n{links_gdf[f'{mode}_centroid_fit'].value_counts(dropna=False)}")

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
    links_gdf_wide.loc[ links_gdf_wide['lanes_orig'].notna() & (links_gdf_wide['lanes'    ]==-1) & (links_gdf_wide['oneway']==True ), 'lanes'    ] = links_gdf_wide['lanes_orig']
    links_gdf_wide.loc[ links_gdf_wide['lanes_orig'].notna() & (links_gdf_wide['lanes'    ]==-1) & (links_gdf_wide['oneway']==False), 'lanes'    ] = np.floor(0.5*links_gdf_wide['lanes_orig'])
    links_gdf_wide.loc[ links_gdf_wide['lanes_orig'].notna() & (links_gdf_wide['lanes_rev']==-1) & (links_gdf_wide['oneway']==True ), 'lanes_rev'] = links_gdf_wide['lanes_orig']
    links_gdf_wide.loc[ links_gdf_wide['lanes_orig'].notna() & (links_gdf_wide['lanes_rev']==-1) & (links_gdf_wide['oneway']==False), 'lanes_rev'] = np.floor(0.5*links_gdf_wide['lanes_orig'])

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
    WranglerLogger.info(f"buslanes:\n{links_gdf['buslanes'].value_counts(dropna=False)}")
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
    links_gdf['access'] = None
    # this one is a bit odd, but I think parquet doesn't like the mixed types
    links_gdf['ML_access'] = None
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


def stepa_standardize_attributes(
        g: networkx.MultiDiGraph,
        county: str,
        prefix: str,
        output_dir: pathlib.Path,
        base_output_dir: pathlib.Path,
        output_formats: list[str],
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
    9. Creates managed lanes fields (via create_managed_lanes_fields())
    10. Writes outputs in multiple formats for different use cases
    
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
        output_dir: Base directory for output
        output_formats: Handled formats: hyper, geojson, parquet, gpkg
    
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
        Writes multiple files to output_dir (depending on output_formats):
            - {prefix}{county}_{nodes|links}.{hyper|parquet|gpkg|geojson}: links and nodes
              in the given output format
    
    Notes:
        - Multi-county links assigned to county with longest intersection
        - Unnamed service roads removed to reduce complexity
        - Original OSM IDs preserved in osm_node_id and osm_link_id columns
    """
    WranglerLogger.info(f"======= STEP {prefix[:2]}: Standardize attributes for {county} =======")
    county_no_spaces = county.replace(" ","")

    # Check for cached roadway network -- just parquet for now
    try:
        cached_nodes_gdf = gpd.read_parquet(path=output_dir / f"{prefix}nodes.parquet")
        cached_links_gdf = gpd.read_parquet(path=output_dir / f"{prefix}links.parquet")
        WranglerLogger.info(f"Loaded cached roadway network from:")
        WranglerLogger.info(f"  {output_dir / f'{prefix}nodes.parquet'}")
        WranglerLogger.info(f"  {output_dir / f'{prefix}links.parquet'}")
        return (cached_links_gdf, cached_nodes_gdf)
    except Exception as e:
        WranglerLogger.debug(f"Could not load cached roadway network: {e}")

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
        county_gdf = get_county_geodataframe(base_output_dir, "CA")
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
    # calculate length (in feet) directly; don't trust the existing value
    links_gdf.to_crs(LOCAL_CRS_FEET, inplace=True)
    links_gdf['length'] = links_gdf.length
    links_gdf['distance'] = links_gdf['length']/FEET_PER_MILE
    links_gdf.to_crs(LAT_LON_CRS, inplace=True)

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

    # Create managed lanes fields: access, ML_access, ML_lanes, etc
    # https://network-wrangler.github.io/network_wrangler/latest/networks/#road-links
    create_managed_lanes_fields(links_gdf)

    # Add direction to links
    links_gdf = add_direction_to_links(links_gdf, cardinal_only = False)

    # If hyper format is specified, write to tableau hyper file
    if 'hyper' in output_formats:
        tableau_utils.write_geodataframe_as_tableau_hyper(
            links_gdf,
            output_dir / f"{prefix}links.hyper",
            f"{prefix}links"
        )

        tableau_utils.write_geodataframe_as_tableau_hyper(
            nodes_gdf,
            output_dir / f"{prefix}nodes.hyper",
            f"{prefix}nodes"
        )

    # write to other formats -- this requires simpler column types
    # Outputs depending on specified OUTPUT_FORMAT
    
    # Subset the links columns and convert name, ref to strings
    WranglerLogger.debug(f"links_gdf.dtypes:\n{links_gdf.dtypes}")
    links_non_list_cols = [
        'A','B','key','dupe_A_B','highway','oneway','name','ref','geometry',
        'drive_access','bike_access','walk_access','truck_access','bus_only',
        'lanes','ML_lanes','length','distance','county','model_link_id','shape_id',
        'access','ML_access'
    ]
    parquet_links_gdf = links_gdf[links_non_list_cols].copy()
    parquet_links_gdf['name'] = parquet_links_gdf['name'].astype(str)
    parquet_links_gdf['ref'] = parquet_links_gdf['ref'].astype(str)
    
    if 'parquet' in output_formats:
        links_parquet_file = output_dir / f"{prefix}links.parquet"
        parquet_links_gdf.to_parquet(links_parquet_file)
        WranglerLogger.info(f"Wrote {links_parquet_file}")
    if 'gpkg' in output_formats:
        links_gpkg_file = output_dir / f"{prefix}links.gpkg"
        parquet_links_gdf.to_file(links_gpkg_file, driver='GPKG')
        WranglerLogger.info(f"Wrote {links_gpkg_file}")
    if 'geojson' in output_formats:
        links_geojson_file = output_dir / f"{prefix}links.geojson"
        parquet_links_gdf.to_file(links_geojson_file, driver='GeoJSON')
        WranglerLogger.info(f"Wrote {links_geojson_file}")

    # Subset the nodes columns
    WranglerLogger.debug(f"nodes_gdf.dtypes:\n{nodes_gdf.dtypes}")
    nodes_non_list_cols = [
        'osmid','X','Y','street_count','ref','geometry','county','model_node_id'
    ]
    parquet_nodes_gdf = nodes_gdf[nodes_non_list_cols].copy()
    parquet_nodes_gdf['ref'] = parquet_nodes_gdf['ref'].astype(str)

    if 'parquet' in output_formats:
        nodes_parquet_file = output_dir / f"{prefix}nodes.parquet"
        parquet_nodes_gdf.to_parquet(nodes_parquet_file)
        WranglerLogger.info(f"Wrote {nodes_parquet_file}")
    if 'gpkg' in output_formats:
        nodes_gpkg_file = output_dir / f"{prefix}nodes.gpkg"
        parquet_nodes_gdf.to_file(nodes_gpkg_file, driver='GPKG')
        WranglerLogger.info(f"Wrote {nodes_gpkg_file}")
    if 'geojson' in output_formats:
        nodes_geojson_file = output_dir / f"{prefix}nodes.geojson"
        parquet_nodes_gdf.to_file(nodes_geojson_file, driver='GeoJSON')
        WranglerLogger.info(f"Wrote {nodes_geojson_file}")

    return (links_gdf, nodes_gdf)

def get_travel_model_zones(base_output_dir: pathlib.Path,):
    """Fetches travel model two zones -- MAZs and TAZs, and returns
    GeoDataFrames with shapes and centroids.

    Args:
        base_output_dir: Base directory for shared resources (zone files)

    Returns:
       dictionary with keys MAZ and TAZ, and values as GeoDataFrame with
       columns, MAZ (for MAZ only), TAZ, county, geometry, geometry_centroid

    """
    ZONES_DIR = base_output_dir / "mtc_zones"
    ZONES_DIR.mkdir(exist_ok=True)

    WranglerLogger.info(f"Looking for MTC zones files in {ZONES_DIR}")

    ZONE_VERSION = "2_5"
    BASE_URL = "https://github.com/BayAreaMetro/tm2py-utils/raw/refs/heads/main/tm2py_utils/inputs/maz_taz"
    SHAPEFILE_FILETYPES = [".cpg",".dbf",".prj",".shp",".shx"]
    gdfs = {}

    # Fetch the maz/taz/county mapping
    county_mapping_file = f"mazs_tazs_county_tract_PUMA_{ZONE_VERSION.replace('_','.')}.csv"
    if (ZONES_DIR / county_mapping_file).exists() == False:
        try:
            url = f"{BASE_URL}/{county_mapping_file}"
            WranglerLogger.debug(f"Fetching {url}")
            response = requests.get(url)
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
            with open(ZONES_DIR / county_mapping_file, "wb") as f:
                f.write(response.content)
            WranglerLogger.debug(f"File downloaded successfully to {ZONES_DIR / county_mapping_file}")
        except requests.exceptions.RequestException as e:
            WranglerLogger.fatal(f"Error downloading {url} with requests: {e}")
            raise e
        WranglerLogger.info(f"Succeeded downloading {ZONES_DIR / county_mapping_file}")
    county_mapping_df = pd.read_csv(ZONES_DIR / county_mapping_file)
    WranglerLogger.debug(f"Read {ZONES_DIR / county_mapping_file}")
    WranglerLogger.debug(f"county_mapping_df:\n{county_mapping_df}")

    # For each zone type, fetch the shapefile from GitHub to local dir if needed
    # and then read it into the geodataframe
    for zone_type in ["MAZ","TAZ"]:
        shapefile = f"{zone_type.lower()}s_TM2_{ZONE_VERSION}.shp"

        # fetch it if it doesn't exist
        if (ZONES_DIR / shapefile).exists() == False:
            for filetype in SHAPEFILE_FILETYPES:
                try:
                    shapefile_file = f"{shapefile}".replace(".shp",filetype)
                    url = f"{BASE_URL}/shapefiles/{shapefile_file}"
                    WranglerLogger.debug(f"Fetching {url}")
                    response = requests.get(url)
                    response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
                    with open(ZONES_DIR / shapefile_file, "wb") as f:
                        f.write(response.content)
                    WranglerLogger.debug(f"File downloaded successfully to {ZONES_DIR / shapefile_file}")
                except requests.exceptions.RequestException as e:
                    WranglerLogger.fatal(f"Error downloading {url} with requests: {e}")
                    raise e
            WranglerLogger.info(f"Succeeded downloading {ZONES_DIR / shapefile}")
        
        # now read it
        gdfs[zone_type] = gpd.read_file(ZONES_DIR / shapefile)
        WranglerLogger.info(f"Read {zone_type} shapefile from {ZONES_DIR / shapefile}")

        # create centroid geometry
        gdfs[zone_type]["geometry_centroid"] = gdfs[zone_type].apply(
            lambda row: shapely.geometry.Point(
                [row[f"{zone_type}_X"],
                 row[f"{zone_type}_Y"]]
            ),
            axis=1
        )
        WranglerLogger.debug(f"gdfs[{zone_type}]:\n{gdfs[zone_type]}")

        # join to county_mapping_df
        gdfs[zone_type] = pd.merge(
            gdfs[zone_type],
            right=county_mapping_df[[f"{zone_type}_NODE", "county_name"]].drop_duplicates(),
            how='left',
            validate='one_to_one',
            indicator=True
        )
        assert (gdfs[zone_type]['_merge'] == 'both').all(), "Not all merge indicators are 'both'"

        # keep only relevant columns
        keep_cols=[f"{zone_type}_NODE", f"{zone_type}_SEQ", "county_name", "geometry", "geometry_centroid"]
        if zone_type == "MAZ":
            keep_cols.insert(1, "TAZ_NODE")
        gdfs[zone_type] = gdfs[zone_type][keep_cols]

        # rename county_name to county
        gdfs[zone_type].rename(columns={"county_name":"county"}, inplace=True)

        WranglerLogger.debug(f"gdfs[{zone_type}] type={type(gdfs[zone_type])}\n{gdfs[zone_type]}")

    return gdfs

# =============================================================================
# 7-STEP NETWORK CREATION WORKFLOW FUNCTIONS
# =============================================================================

def step1_download_osm_network(
        county: str, output_dir: pathlib.Path, base_output_dir: pathlib.Path
    ) -> networkx.MultiDiGraph:
    """
    Step 1: Downloads OSM network data for specified geography.

    Downloads road network data from OpenStreetMap using OSMnx for either
    individual counties or the entire Bay Area. Uses caching to avoid
    repeated downloads.

    Args:
        county: County name (e.g., "San Francisco") or "Bay Area"
        output_dir: County-specific output directory
        base_output_dir: Base directory for shared resources

    Returns:
        NetworkX MultiDiGraph containing the raw OSM road network
    """
    WranglerLogger.info(f"======= STEP 1: Download OSM network for {county} =======")

    # Configure OSMnx
    osmnx.settings.use_cache = True
    osmnx.settings.cache_folder = output_dir / "osmnx_cache"
    osmnx.settings.log_file = True
    osmnx.settings.logs_folder = output_dir / "osmnx_logs"
    osmnx.settings.useful_tags_way=OSM_WAY_TAGS.keys()
        
    county_no_spaces = county.replace(" ", "")
    OSM_network_type = "all"  # Include all road types

    # Check for cached graph
    initial_graph_file = output_dir / "1_graph_OSM.pkl"
    
    if initial_graph_file.exists():
        try:
            with open(initial_graph_file, 'rb') as f:
                g = pickle.load(f)
            WranglerLogger.info(f"Loaded cached OSM graph from {initial_graph_file}")
            WranglerLogger.info(f"Graph has {g.number_of_edges():,} edges and {len(g.nodes()):,} nodes")
            return g
        except Exception as e:
            WranglerLogger.warning(f"Could not read cached graph: {e}")
    
    # Download new graph
    if county == 'Bay Area':
        WranglerLogger.info("Downloading network for Bay Area using bounding box...")
        bbox = get_county_bbox(BAY_AREA_COUNTIES, base_output_dir)
        WranglerLogger.info(f"Bounding box: west={bbox[0]:.6f}, south={bbox[1]:.6f}, east={bbox[2]:.6f}, north={bbox[3]:.6f}")
        g = osmnx.graph_from_bbox(bbox, network_type=OSM_network_type)
    else:
        WranglerLogger.info(f"Downloading network for {county}...")
        g = osmnx.graph_from_place(f'{county} County, California, USA', network_type=OSM_network_type)
    
    # Cache the downloaded graph
    with open(initial_graph_file, "wb") as f:
        pickle.dump(g, f)
    WranglerLogger.info(f"Cached OSM graph to {initial_graph_file}")
    WranglerLogger.info(f"Downloaded graph has {g.number_of_edges():,} edges and {len(g.nodes()):,} nodes")
    
    return g


def step2_simplify_network_topology(
        g: networkx.MultiDiGraph,
        county: str,
        output_dir: pathlib.Path
    ) -> networkx.MultiDiGraph:
    """
    Step 2: Simplifies network topology while preserving connectivity.
    
    Consolidates nearby intersections and removes unnecessary intermediate nodes
    while preserving the essential network structure and connectivity.
    
    Args:
        g: NetworkX MultiDiGraph from step 1
        county: County name for caching purposes
        
    Returns:
        Simplified NetworkX MultiDiGraph
    """
    WranglerLogger.info(f"======= STEP 2: Simplify network topology for {county} =======")

    county_no_spaces = county.replace(" ", "")
    simplified_graph_file = output_dir / f"2_graph_OSM_simplified{NETWORK_SIMPLIFY_TOLERANCE}.pkl"
    
    # Check for cached simplified graph
    if simplified_graph_file.exists():
        try:
            with open(simplified_graph_file, 'rb') as f:
                simplified_g = pickle.load(f)
            WranglerLogger.info(f"Loaded cached simplified graph from {simplified_graph_file}")
            WranglerLogger.info(f"Simplified graph has {simplified_g.number_of_edges():,} edges and {len(simplified_g.nodes()):,} nodes")
            return simplified_g
        except Exception as e:
            WranglerLogger.warning(f"Could not read cached simplified graph: {e}")
    
    # Project to local CRS for accurate distance calculations
    g = osmnx.projection.project_graph(g, to_crs=LOCAL_CRS_FEET)
    
    # Simplify by consolidating intersections
    WranglerLogger.info(f"Simplifying with tolerance={NETWORK_SIMPLIFY_TOLERANCE} feet...")
    simplified_g = osmnx.simplification.consolidate_intersections(
        g,
        tolerance=NETWORK_SIMPLIFY_TOLERANCE,  # feet
        rebuild_graph=True,
        dead_ends=True,  # keep dead-ends
        reconnect_edges=True,
    )
    
    # Cache the simplified graph
    with open(simplified_graph_file, "wb") as f:
        pickle.dump(simplified_g, f)
    WranglerLogger.info(f"Cached simplified graph to {simplified_graph_file}")
    WranglerLogger.info(f"Simplified graph has {simplified_g.number_of_edges():,} edges and {len(simplified_g.nodes()):,} nodes")
    
    return simplified_g

def hack_rename_nodes(roadway_network):
    """Apply manual corrections to node link names for specific locations.

    This function contains hacks to fix node names that don't match their transit stops properly.

    Args:
        roadway_network: RoadwayNetwork object to modify in place
    """
    import numpy as np

    # hack: this node is labeled as Vista Access Road but we want it to be matched with
    # the Golden Gate Bridge/Parking Lot stop

    # TODO: Add support for node selection by X/Y location in RoadwayNetwork.get_selection()
    # For now, find the node manually by calculating distances

    target_x = -122.4740
    target_y = 37.8072
    tolerance = 0.001  # ~100 meters

    # Find nodes within tolerance of target coordinates
    mask = (
        (roadway_network.nodes_df['X'] >= target_x - tolerance) &
        (roadway_network.nodes_df['X'] <= target_x + tolerance) &
        (roadway_network.nodes_df['Y'] >= target_y - tolerance) &
        (roadway_network.nodes_df['Y'] <= target_y + tolerance)
    )

    matching_nodes = roadway_network.nodes_df[mask]

    if len(matching_nodes) > 0:
        # If multiple nodes found, get the closest one
        if len(matching_nodes) > 1:
            distances = np.sqrt(
                (matching_nodes['X'] - target_x) ** 2 +
                (matching_nodes['Y'] - target_y) ** 2
            )
            closest_idx = distances.idxmin()
            selected_node_ids = [int(matching_nodes.loc[closest_idx, 'model_node_id'])]  # Convert numpy int64 to regular int
        else:
            selected_node_ids = [int(nid) for nid in matching_nodes['model_node_id'].tolist()]  # Convert numpy int64 to regular int

        WranglerLogger.info(f"Found node(s) near Golden Gate Bridge/Parking Lot stop: {selected_node_ids}")

        # Use get_selection() with the found model_node_id(s)
        node_selection = roadway_network.get_selection({
            "nodes": {"model_node_id": selected_node_ids}
        })

        # Directly update the node attributes since custom list attributes aren't supported in property_changes
        # This is a hack, so we'll modify the dataframe directly
        for node_id in selected_node_ids:
            node_idx = roadway_network.nodes_df[roadway_network.nodes_df['model_node_id'] == node_id].index[0]
            roadway_network.nodes_df.at[node_idx, 'link_names'] = ["Golden Gate Bridge/Parking Lot"]
            roadway_network.nodes_df.at[node_idx, 'incoming_link_names'] = ["Golden Gate Bridge/Parking Lot"]
            roadway_network.nodes_df.at[node_idx, 'outgoing_link_names'] = ["Golden Gate Bridge/Parking Lot"]

        WranglerLogger.info(f"Updated node(s) {selected_node_ids} link_names to 'Golden Gate Bridge/Parking Lot'")
    else:
        WranglerLogger.warning(f"No nodes found near coordinates ({target_x}, {target_y})")

    # Add more node rename hacks here as needed

def step3_assign_county_node_link_numbering(
        links_gdf: gpd.GeoDataFrame,
        nodes_gdf: gpd.GeoDataFrame,
        county: str,
        output_dir: pathlib.Path,
        base_output_dir: pathlib.Path,
        output_formats: list[str],
) -> RoadwayNetwork:
    """
    Step 3: Assigns county-specific node/link numbering schemes.

    Creates a RoadwayNetwork object with model-specific node and link IDs
    based on the county numbering system. This step is already integrated
    into step3_standardize_attributes.

    Args:
        links_gdf: Links GeoDataFrame from step 3
        nodes_gdf: Nodes GeoDataFrame from step 3
        county: County name
        output_dir: County-specific output directory
        base_output_dir: Base directory for shared resources
        output_formats: Handled formats: hyper, geojson, parquet, gpkg

    Returns:
        RoadwayNetwork object with county-specific numbering
    """
    WranglerLogger.info(f"======= STEP 3: Create roadway network with county numbering for {county} =======")

    county_no_spaces = county.replace(" ", "")
    roadway_net_file = "3_roadway_network"
    
    # Check for cached roadway network
    try:
        cached_nodes_gdf = gpd.read_parquet(path=output_dir / f"{roadway_net_file}_node.parquet")
        cached_links_gdf = gpd.read_parquet(path=output_dir / f"{roadway_net_file}_link.parquet")
        shapes_gdf = cached_links_gdf.copy()
        roadway_network = load_roadway_from_dataframes(cached_links_gdf, cached_nodes_gdf, shapes_gdf)
        WranglerLogger.info(f"Loaded cached roadway network from {roadway_net_file}")
        return roadway_network
    except Exception as e:
        WranglerLogger.debug(f"Could not load cached roadway network: {e}")
    
    # Prepare data for roadway network creation
    LINK_COLS = [
        'A', 'B','osm_link_id','highway','name','ref','oneway','reversed','length','geometry',
        'access','ML_access','drive_access', 'bike_access', 'walk_access', 'truck_access', 'bus_only',
        'lanes','ML_lanes','distance', 'county', 'model_link_id', 'shape_id', 
        'drive_centroid_fit', 'walk_centroid_fit', 'direction'
    ]
    NODE_COLS = [
        'osmid','osm_node_id','X', 'Y', 'street_count', 'geometry', 'county', 'model_node_id'
    ]
    
    clean_links_gdf = links_gdf[LINK_COLS].copy()
    clean_nodes_gdf = nodes_gdf[NODE_COLS].copy()
    
    # Create roadway network
    roadway_network = network_wrangler.load_roadway_from_dataframes(
        links_df=clean_links_gdf,
        nodes_df=clean_nodes_gdf,
        shapes_df=clean_links_gdf
    )

    # filter nodes to links
    roadway_network.nodes_df = filter_nodes_to_links(roadway_network.links_df, roadway_network.nodes_df)

    # use link names to set a new attribute to nodes: link_names
    add_roadway_link_names_to_nodes(roadway_network)

    # Apply node name hacks for specific locations
    hack_rename_nodes(roadway_network)

    # Write roadway network to cache
    for roadway_format in output_formats:
        if roadway_format == "hyper": continue
        try:
            write_roadway(
                roadway_network,
                out_dir=output_dir,
                prefix=roadway_net_file,
                file_format=roadway_format,
                overwrite=True,
                true_shape=True
            )
            WranglerLogger.info(f"Saved roadway network in {roadway_format} format")
        except Exception as e:
            WranglerLogger.error(f"Error writing roadway network in {roadway_format}: {e}")
    
    WranglerLogger.info(f"Created roadway network with {len(roadway_network.links_df)} links and {len(roadway_network.nodes_df)} nodes")
    return roadway_network

def step4_add_centroids_and_connectors(
        roadway_network: RoadwayNetwork,
        county: str,
        output_dir: pathlib.Path,
        base_output_dir: pathlib.Path,
        output_formats: list[str],
):
    """
    Step 4: Adds centroids and centroid connectors to the RoadwayNetwork.

    Args:
        roadway_network: RoadwayNetwork to modify
        county: County name
        output_dir: County-specific output directory
        base_output_dir: Base directory for shared resources (zone files)
        output_formats: Handled formats: hyper, geojson, parquet, gpkg

    Returns:
        RoadwayNetwork
    """
    WranglerLogger.info(f"======= STEP 4: Create centroids and centroid connectors for {county} =======")

    county_no_spaces = county.replace(" ", "")
    roadway_net_file = "4_roadway_network"

    # Check for cached roadway network
    try:
        cached_nodes_gdf = gpd.read_parquet(path=output_dir / f"{roadway_net_file}_node.parquet")
        cached_links_gdf = gpd.read_parquet(path=output_dir / f"{roadway_net_file}_link.parquet")
        shapes_gdf = cached_links_gdf.copy()
        roadway_network = load_roadway_from_dataframes(cached_links_gdf, cached_nodes_gdf, shapes_gdf)
        WranglerLogger.info(f"Loaded cached roadway network from {roadway_net_file}")
        return roadway_network
    except Exception as e:
        WranglerLogger.debug(f"Could not load cached roadway network: {e}")

    # Create centroid connectors -- fetch travel model zone data
    zones_gdf_dict = get_travel_model_zones(base_output_dir)

    if county != "Bay Area":
        for zone_type in zones_gdf_dict.keys():
            zones_gdf_dict[zone_type] = zones_gdf_dict[zone_type].loc[ zones_gdf_dict[zone_type].county == county ]
            WranglerLogger.info(f"Filtered {zone_type} to {county}: {len(zones_gdf_dict[zone_type]):,} rows")

    # TAZ & TAZ drive connectors
    add_centroid_nodes(roadway_network, zones_gdf_dict["TAZ"], "TAZ_NODE")
    summary_gdf = add_centroid_connectors(
        roadway_network, 
        zones_gdf_dict["TAZ"], "TAZ_NODE", 
        mode="drive", 
        local_crs=LOCAL_CRS_FEET,
        zone_buffer_distance=20,
        num_centroid_connectors=4,
        max_mode_graph_degrees=4,
        default_link_attribute_dict = {
            "lanes":7, "oneway":False,
            # TODO: this is an odd choice, but right now it's interfering with transit conflation to roadway network
            "drive_access": False
        }
    )
    WranglerLogger.debug(f"TAZs with 0 connectors:\n{summary_gdf.loc[summary_gdf.num_connectors == 0]}")
    # MAZ & MAZ walk/bike connectors
    add_centroid_nodes(roadway_network, zones_gdf_dict["MAZ"], "MAZ_NODE")
    summary_gdf = add_centroid_connectors(
        roadway_network, 
        zones_gdf_dict["MAZ"], "MAZ_NODE", 
        mode="walk", 
        local_crs=LOCAL_CRS_FEET,
        zone_buffer_distance=20,
        num_centroid_connectors=2,
        max_mode_graph_degrees=8, # make this larger because more footway links are oks
        default_link_attribute_dict = {"lanes":1, "oneway":False, "bike_access":True}
    )
    WranglerLogger.debug(f"MAZs with 0 connectors:\n{summary_gdf.loc[summary_gdf.num_connectors == 0]}")

    # Write roadway network to cache
    for roadway_format in output_formats:
        if roadway_format == "hyper":
            tableau_utils.write_geodataframe_as_tableau_hyper(
                roadway_network.links_df,
                output_dir / f"{roadway_net_file}_links.hyper",
                f"{roadway_net_file}_links"
            )
            tableau_utils.write_geodataframe_as_tableau_hyper(
                roadway_network.nodes_df,
                output_dir / f"{roadway_net_file}_nodes.hyper",
                f"{roadway_net_file}_nodes"
            )
            continue

        try:
            write_roadway(
                roadway_network,
                out_dir=output_dir,
                prefix=roadway_net_file,
                file_format=roadway_format,
                overwrite=True,
                true_shape=True
            )
            WranglerLogger.info(f"Saved roadway network in {roadway_format} format")
        except Exception as e:
            WranglerLogger.error(f"Error writing roadway network in {roadway_format}: {e}")

    return roadway_network

def step5_prepare_gtfs_transit_data(
        county: str,
        input_gtfs: pathlib.Path,
        output_dir: pathlib.Path,
        base_output_dir: pathlib.Path
) -> GtfsModel:
    """
    Step 5: Prepare GTFS transit data for integration: filter to service date and relevant operators

    Loads and processes GTFS transit feed data, filtering to the specified
    geography and preparing for integration with the roadway network.

    Args:
        county: County name
        input_gtfs: Path to input GTFS data
        output_dir: County-specific output directory
        base_output_dir: Base directory for shared resources (county shapefiles)

    Returns:
        Filtered GTFS model object
    """
    WranglerLogger.info(f"======= STEP 5: Preparing GTFS transit data for {county} =======")

    county_no_spaces = county.replace(" ", "")
    gtfs_model_dir = output_dir / "5_gtfs_model"
    
    # Check for cached GTFS model
    if gtfs_model_dir.exists():
        try:
            gtfs_model = load_feed_from_path(gtfs_model_dir, wrangler_flavored=False, low_memory=False)
            WranglerLogger.info(f"Loaded cached GTFS model from {gtfs_model_dir}")
            return gtfs_model
        except Exception as e:
            WranglerLogger.debug(f"Could not load cached GTFS model: {e}")
    
    # Load and filter GTFS data
    WranglerLogger.info("Loading GTFS feed for September 27, 2023...")
    
    # Filter to specific service date
    calendar_dates_df = pd.read_csv(input_gtfs / "calendar_dates.txt")
    calendar_dates_df = calendar_dates_df.loc[
        (calendar_dates_df.date == 20230927) & (calendar_dates_df.exception_type == 1)
    ]
    calendar_dates_df['service_id'] = calendar_dates_df['service_id'].astype(str)
    service_ids_df = calendar_dates_df[['service_id']].drop_duplicates().reset_index(drop=True)
    service_ids = service_ids_df['service_id'].tolist()
    
    # Load GTFS model
    gtfs_model = load_feed_from_path(input_gtfs, wrangler_flavored=False, service_ids_filter=service_ids, low_memory=False)
    
    # Clean up unnecessary columns
    gtfs_model.stops.drop(columns=[
        'stop_code','stop_desc','stop_url','tts_stop_name',
        'platform_code','stop_timezone','wheelchair_boarding'
    ], inplace=True, errors='ignore')
    
    # Filter agencies by county
    if county == "Bay Area":
        drop_transit_agency(gtfs_model, agency_id='SI')  # Drop SFO Airport
    elif county in COUNTY_NAME_TO_GTFS_AGENCIES:
        keep_agencies = COUNTY_NAME_TO_GTFS_AGENCIES[county]
        WranglerLogger.info(f"Keeping agencies for {county}: {keep_agencies}")
        drop_agencies = []
        for agency_id in gtfs_model.agency['agency_id'].tolist():
            if agency_id not in keep_agencies:
                drop_agencies.append(agency_id)
        WranglerLogger.info(f"Dropping agencies for {drop_agencies}")
        drop_transit_agency(gtfs_model, agency_id=drop_agencies)

    # Filter by geographic boundary
    county_gdf = get_county_geodataframe(base_output_dir, "CA")
    county_gdf = county_gdf[county_gdf['NAME10'].isin(BAY_AREA_COUNTIES)].copy()
    if county != "Bay Area":
        county_gdf = county_gdf.loc[county_gdf['NAME10'] == county]
        assert len(county_gdf) == 1
    
    filter_transit_by_boundary(
        gtfs_model,
        county_gdf,
        partially_include_route_type_action={RouteType.RAIL: 'truncate'}
    )
    
    # Fix known data issue
    gtfs_model.trips.loc[
        gtfs_model.trips['trip_id'] == 'PE:t263-sl17-p182-r1A:20230930',
        'direction_id'
    ] = 1

    # Check for and remove duplicate consecutive stops (same stop_id appearing twice in a row)
    gtfs_model.stop_times = gtfs_model.stop_times.sort_values(['trip_id', 'stop_sequence'])
    gtfs_model.stop_times['prev_stop_id'] = gtfs_model.stop_times.groupby('trip_id')['stop_id'].shift(1)
    gtfs_model.stop_times['is_duplicate_consecutive'] = (
        gtfs_model.stop_times['stop_id'] == gtfs_model.stop_times['prev_stop_id']
    )

    duplicate_consecutive = gtfs_model.stop_times[gtfs_model.stop_times['is_duplicate_consecutive'] == True]
    if len(duplicate_consecutive) > 0:
        WranglerLogger.warning(
            f"Found {len(duplicate_consecutive)} consecutive duplicate stops. Removing them.\n"
            f"Sample: {duplicate_consecutive[['trip_id', 'stop_id', 'stop_sequence']].head(10)}"
        )
        # Remove the duplicate consecutive stops
        gtfs_model.stop_times = gtfs_model.stop_times[
            gtfs_model.stop_times['is_duplicate_consecutive'] == False
        ].copy()

    # Clean up temporary columns
    gtfs_model.stop_times.drop(columns=['prev_stop_id', 'is_duplicate_consecutive'], inplace=True)

    # Cache the filtered GTFS model
    gtfs_model_dir.mkdir(exist_ok=True)
    write_transit(
        gtfs_model,
        gtfs_model_dir,
        prefix="gtfs_model",
        overwrite=True
    )
    
    WranglerLogger.info(f"Integrated GTFS data: {len(gtfs_model.routes)} routes, {len(gtfs_model.stops)} stops")
    return gtfs_model

def step6_create_transit_network(
        gtfs_model: GtfsModel,
        roadway_network: RoadwayNetwork,
        county: str,
        output_dir: pathlib.Path,
        output_formats: list[str],
        trace_shape_ids: Optional[list[str]] = None,
) -> tuple[TransitNetwork, gpd.GeoDataFrame]:
    """
    Step 6: Create TransitNetwork by converting GtfsModel to Wrangler-flavored Feed object,
    integrating with RoadwayNetwork

    Integrates GTFS transit data with the roadway network by creating transit
    stops and connecting them to the road network with appropriate links.

    Args:
        gtfs_model: GTFS model from step 4
        roadway_network: RoadwayNetwork from step 3
        county: County name
        output_dir: Base directory for output.
            Only used if an exception is thrown with debug data.
        output_formats: Handled formats: hyper, geojson, parquet, gpkg
        trace_shape_ids: Optional list of shape IDs to trace for debugging transit routing

    Returns:
        Tuple of (TransitNetwork with stops and links integrated, shape_links_gdf)
    """
    WranglerLogger.info(f"======= STEP 6: Creating Wrangler-flavored GTFS Feed for {county} =======")

    # Define time periods for frequency calculation
    TIME_PERIODS = {
        'EA': ['03:00','06:00'],  # 3a-6a
        'AM': ['06:00','10:00'],  # 6a-10a
        'MD': ['10:00','15:00'],  # 10a-3p
        'PM': ['15:00','19:00'],  # 3p-7p
        'EV': ['19:00','03:00'],  # 7p-3a (crosses midnight)
    }

    try:
        feed = create_feed_from_gtfs_model(
            gtfs_model,
            roadway_network,
            local_crs=LOCAL_CRS_FEET,
            crs_units="feet",
            timeperiods=TIME_PERIODS,
            frequency_method='median_headway',
            default_frequency_for_onetime_route=180*60,  # 180 minutes
            add_stations_and_links=True,
            max_stop_distance = 0.10*FEET_PER_MILE,
            trace_shape_ids=trace_shape_ids,
            # for 9-county Bay Area, ignore these - they're logged
            errors = "ignore"
        )
        WranglerLogger.info(f"Created transit Feed object with stops and links integrated")
        
    except Exception as e:
        WranglerLogger.error(f"Error creating transit stops and links: {e}")
        county_no_spaces = county.replace(" ", "")
        
        for error_gdf_name in ["bus_stop_links_gdf", "bus_stops_gdf", "no_bus_path_gdf"]:
            # Write debug outputs if available
            if hasattr(e, error_gdf_name):
                error_gdf = getattr(e, error_gdf_name)
                WranglerLogger.debug(f"error_gdf for {error_gdf_name}: {type(error_gdf)}")
            else:
                continue
            
            if "hyper" in output_formats:
                tableau_utils.write_geodataframe_as_tableau_hyper(
                    error_gdf,
                    output_dir / error_gdf_name.replace("_gdf", ".hyper"),
                    error_gdf_name
                )
            if "parquet" in output_formats:
                debug_file = output_dir / error_gdf_name.replace("_gdf", ".parquet")
                error_gdf.to_parquet(debug_file)
                WranglerLogger.error(f"Wrote {debug_file}")
            if "gpkg" in output_formats:
                debug_file = output_dir / error_gdf_name.replace("_gdf", ".gpkg")
                error_gdf.to_file(debug_file, driver="GPKG")
                WranglerLogger.error(f"Wrote {debug_file}")
            if "geojson" in output_formats:
                debug_file = output_dir / error_gdf_name.replace("_gdf", ".geojson")
                error_gdf.to_file(debug_file, driver="GeoJSON")
                WranglerLogger.error(f"Wrote {debug_file}")

        raise e
    
    # Write roadway network with transit
    county_no_spaces = county.replace(" ","")
    roadway_net_file = "6_roadway_network_inc_transit"
    for roadway_format in output_formats:
        if roadway_format == "hyper":
            tableau_utils.write_geodataframe_as_tableau_hyper(
                roadway_network.links_df,
                output_dir / f"{roadway_net_file}_links.hyper",
                "6_roadway_links"
            )
            tableau_utils.write_geodataframe_as_tableau_hyper(
                roadway_network.nodes_df,
                output_dir / f"{roadway_net_file}_nodes.hyper",
                "6_roadway_nodes"
            )
            continue
        try:
            write_roadway(
                roadway_network,
                out_dir=output_dir,
                prefix=roadway_net_file,
                file_format=roadway_format,
                overwrite=True,
                true_shape=True
            )
            WranglerLogger.info(f"Wrote roadway network with transit in {roadway_format} format")
        except Exception as e:
            WranglerLogger.warning(f"Error writing roadway network in {roadway_format}: {e}")
    
    # Create transit network
    transit_network = load_transit(feed=feed)
    WranglerLogger.info(f"Created transit network:\n{transit_network}")

    transit_network_dir = output_dir / "6_transit_network"
    transit_network_dir.mkdir(exist_ok=True)
    write_transit(
        transit_network,
        out_dir=transit_network_dir,
        prefix="transit_network",
        overwrite=True
    )
    WranglerLogger.info(f"Wrote transit network to {transit_network_dir}")

    # Construct links from transit network shape points
    shape_links_gdf = transit_network.feed.shapes.sort_values(by=["shape_id","shape_pt_sequence"]).reset_index(drop=True)
    shape_links_gdf["next_shape_model_node_id"] = shape_links_gdf.groupby("shape_id")["shape_model_node_id"].shift(-1)
    shape_links_gdf["next_shape_pt_sequence"] = shape_links_gdf.groupby("shape_id")["shape_pt_sequence"].shift(-1)
    # Filter to only rows that have a next node (excludes last point of each shape)
    shape_links_gdf = shape_links_gdf.loc[ shape_links_gdf["next_shape_model_node_id"].notna() ]
    shape_links_gdf["next_shape_model_node_id"] = shape_links_gdf["next_shape_model_node_id"].astype(int)
    shape_links_gdf["next_shape_pt_sequence"] = shape_links_gdf["next_shape_pt_sequence"].astype(int)
    shape_links_gdf.rename(columns={"shape_model_node_id":"A", "next_shape_model_node_id":"B"}, inplace=True)

    # Drop these columns
    shape_links_gdf.drop(columns=["shape_id","shape_pt_lat","shape_pt_lon","geometry","stop_id","stop_name"], inplace=True)

    # Join them with roadway network shapes to replicate
    # network_wrangler/transit/validate.py:shape_links_without_road_links()
    roadnet_shapes_gdf = roadway_network.links_df
    roadnet_shapes_gdf = roadnet_shapes_gdf.loc[
        (roadnet_shapes_gdf["drive_access"])
        | (roadnet_shapes_gdf["bus_only"])
        | (roadnet_shapes_gdf["rail_only"])
        | (roadnet_shapes_gdf["ferry_only"])
    ]
    shape_roadnet_links_gdf = gpd.GeoDataFrame(
        pd.merge(
            left=shape_links_gdf,
            right=roadnet_shapes_gdf,
            on=["A","B"],
            how="left",
            indicator=True
        ), crs=roadnet_shapes_gdf.crs
    )
    WranglerLogger.debug(f"shape_roadnet_links_gdf['_merge'].value_counts():\n{shape_roadnet_links_gdf['_merge'].value_counts()}")
    # write it
    shape_roadnet_links_name = "6_transit_road_links_gdf"
    if "hyper" in output_formats:
        tableau_utils.write_geodataframe_as_tableau_hyper(
            shape_roadnet_links_gdf,
                output_dir / shape_roadnet_links_name.replace("_gdf",".hyper"),
                shape_roadnet_links_name
            )
    if "parquet" in output_formats:
        debug_file = output_dir / shape_roadnet_links_name.replace("_gdf",".parquet")
        shape_roadnet_links_gdf.to_parquet(debug_file)
        WranglerLogger.error(f"Wrote {debug_file}")
    if "gpkg" in output_formats:
        debug_file = output_dir / shape_roadnet_links_name.replace("_gdf",".gpkg")
        shape_roadnet_links_gdf.to_file(debug_file, driver="GPKG")
        WranglerLogger.error(f"Wrote {debug_file}")
    if "geojson" in output_formats:
        debug_file = output_dir / shape_roadnet_links_name.replace("_gdf",".geojson")
        shape_roadnet_links_gdf.to_file(debug_file, driver="GeoJSON")
        WranglerLogger.error(f"Wrote {debug_file}")
    return transit_network, shape_roadnet_links_gdf

if __name__ == "__main__":
    """
    Main execution using the 8-step workflow:
    
    1. Downloads OSM network data for specified geography
    1a. Standardize attributes and write
    2. Simplifies network topology while preserving connectivity
    2a. Standardize attributes and write
    3. Assigns county-specific node/link numbering schemes
    4. Add centroids and centroid connectors
    5. Prepare GTFS transit data for integration by filtering to service
       date and county
    6. Create TransitNetwork integrated with RoadwayNetwork
    7. Create base year Scenario
    8. TODO: Apply projects and write future year Scenario
       
    The script uses caching extensively - if intermediate files exist,
    they are loaded instead of regenerating, significantly speeding up
    iterative development and debugging.
    """

    # Setup pandas display options
    pd.options.display.max_columns = None
    pd.options.display.width = None
    pd.options.display.min_rows = 20 # number of rows to show in truncated view
    pd.options.display.max_rows = 500 # number of rows to show before truncating
    pd.set_option('display.float_format', '{:.2f}'.format)
    # numpy
    np.set_printoptions(linewidth=500)

    # Elevate SettingWithCopyWarning to error
    pd.options.mode.chained_assignment = 'raise'

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("county", type=str, choices=['Bay Area'] + BAY_AREA_COUNTIES)
    parser.add_argument("input_gtfs", type=pathlib.Path, help="Directory with GTFS feed files")
    parser.add_argument("output_dir", type=pathlib.Path, help="Directory to write output files")
    parser.add_argument("output_format", type=str, choices=['parquet','hyper','geojson','gpkg'], help="Output format for network files", nargs = '+')
    parser.add_argument("--trace-shape-ids", type=str, nargs='*', help="Optional shape IDs to trace for debugging transit routing", default=None)
    args = parser.parse_args()
    args.county_no_spaces = args.county.replace(" ","") # remove spaces

    # Set up output directories
    # Base directory for shared resources (zones, county shapefiles)
    base_output_dir = args.output_dir.resolve()
    # County-specific subdirectory for network outputs
    output_dir = base_output_dir / args.county_no_spaces

    # Create output directories
    base_output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Note: pygris uses its own default cache location determined by appdirs
    # The cache=True parameter in pygris.counties() calls enables caching
    
    # Setup logging
    INFO_LOG  = output_dir / "create_mtc_network_from_OSM.info.log"
    DEBUG_LOG = output_dir / "create_mtc_network_from_OSM.debug.log"

    network_wrangler.setup_logging(
        info_log_filename=INFO_LOG,
        debug_log_filename=DEBUG_LOG,
        std_out_level="info",
        file_mode='w'
    )
    WranglerLogger.info(f"Starting 7-step network creation workflow for {args.county}")
    WranglerLogger.info(f"Created by {__file__}")

    # For now, doing drive as we'll add handle transit and walk/bike separately
    # Aug 24: switch to all because Market Street bus-only links are missing
    OSM_network_type = "all"
    
    # Formatting output and ensuring if hyper is selected, there is another format
    if (len(args.output_format) == 0) or (args.output_format == ["hyper"]):
        WranglerLogger.fatal("No roadway output formats specified. Please include at least one of 'parquet','geojson','gpkg'")
        sys.exit()

    try:
        # STEP 1: Download OSM network data
        g = step1_download_osm_network(args.county, output_dir, base_output_dir)

        # STEP 1a: standardize attributes (and write)
        # Note: we don't keep the results of this, since we'll use version from the simplified graph
        stepa_standardize_attributes(g, args.county, "1a_original_", output_dir, base_output_dir, args.output_format)

        # STEP 2: Simplify network topology
        simplified_g = step2_simplify_network_topology(g, args.county, output_dir)

        # STEP 2a: standardize attributes and write
        (links_gdf, nodes_gdf) = stepa_standardize_attributes(simplified_g, args.county, "2a_simplified_", output_dir, base_output_dir, args.output_format)

        # STEP 3: Assign county-specific numbering and create RoadwayNetwork object
        # This also drops columns we're done with and writes the roadway network
        roadway_network = step3_assign_county_node_link_numbering(links_gdf, nodes_gdf, args.county, output_dir, base_output_dir, args.output_format)

        # STEP 4: Add centroids and centroid connectors
        roadway_network = step4_add_centroids_and_connectors(roadway_network, args.county, output_dir, base_output_dir, args.output_format)

        # STEP 5: Prepare GTFS transit data: Read and filter to service date, relevant operators. Creates GtfsModel object
        # This also writes the GtfsModel as GTFS
        gtfs_model = step5_prepare_gtfs_transit_data(args.county, args.input_gtfs, output_dir, base_output_dir)

        # STEP 6: Create TransitNetwork by integrating GtfsModel with RoadwayNetwork to create a Wrangler-flavored Feed object
        # This writes the RoadwayNetwork and TransitNetwork
        transit_network, shape_links_gdf = step6_create_transit_network(gtfs_model, roadway_network, args.county, output_dir, args.output_format, args.trace_shape_ids)

        # before doing this, convert list-columns to strings or writing the scenario will fail
        list_columns = ["link_names", "incoming_link_names", "outgoing_link_names"]
        for list_col in list_columns:
            if list_col in roadway_network.nodes_df.columns:
                roadway_network.nodes_df[list_col] = roadway_network.nodes_df[list_col].astype(str)

        # STEP 7: Create base year scenario
        my_scenario = network_wrangler.scenario.create_scenario(
            base_scenario = {
                "road_net": roadway_network,
                "transit_net": transit_network,
                "applied_projects": [],
                "conflicts": {}
            },
        )
    
        # write it to disk
        scenario_dir = output_dir / "7_scenario"
        scenario_dir.mkdir(exist_ok=True)
        my_scenario.write(
            path=scenario_dir,
            name="mtc_2023",
            roadway_file_format="geojson",
            roadway_true_shape=True
        )
        WranglerLogger.info(f"Wrote scenario to {scenario_dir}")

        # TODO: apply some projects
        # TODO: Write scneario with projects
        # TODO: Write as cube network?

        WranglerLogger.info("=" * 60)
        WranglerLogger.info("7-STEP WORKFLOW COMPLETED SUCCESSFULLY")
        WranglerLogger.info("=" * 60)
        WranglerLogger.info(f"Final network summary for {args.county}:")
        WranglerLogger.info(f"  - Roadway links: {len(roadway_network.links_df):,}")
        WranglerLogger.info(f"  - Roadway nodes: {len(roadway_network.nodes_df):,}")
        WranglerLogger.info(f"  - Transit routes: {len(transit_network.feed.routes):,}")
        WranglerLogger.info(f"  - Transit stops: {len(transit_network.feed.stops):,}")
        sys.exit()
        
    except Exception as e:
        WranglerLogger.error("WORKFLOW FAILED")
        WranglerLogger.error(f"Error: {e}")
        raise

        


