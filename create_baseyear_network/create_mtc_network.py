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
import pathlib
import statistics

import networkx
import osmnx
import pandas as pd
import geopandas as gpd

import tableau_utils
import network_wrangler
from network_wrangler import WranglerLogger

INPUT_2023GTFS = pathlib.Path("M:\\Data\\Transit\\511\\2023-10")
OUTPUT_DIR = pathlib.Path("M:\\Development\\Travel Model Two\\Supply\\Network Creation 2025\\from_OSM")
NOW = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
BAY_AREA_COUNTIES = [
    'Alameda', 
    'Contra Costa', 
    'Marin',
    'Napa',
    'San Francisco',
    'San Mateo',
    'Santa Clara',
    'Solano',
    'Sonoma'
]

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

def get_min_or_median_value(lane):
    """
    For lists with two items, use min. For lists with more, use median.
    """
    if isinstance(lane, list):
        lane = [int(s) for s in lane]
        if len(lane) == 2:
            return int(min(lane))
        else:
            return int(statistics.median(lane))
    elif isinstance(lane, str):
        return int(lane)
    return lane

def standardize_lanes_value(links_gdf: gpd.GeoDataFrame):
    """Standardize the lanes value in the links GeoFataFrame.
    """
    WranglerLogger.debug(f"standardize_lanes_value()")
    # oneway is always a bool
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
        WranglerLogger.debug(f"links_gdf[['oneway_type','reversed_type']].value_counts():\n{links_gdf[['oneway_type','reversed_type']].value_counts()}")
        WranglerLogger.debug(f"links_gdf[['oneway','reversed']].value_counts():\n{links_gdf[['oneway','reversed']].value_counts()}")

    # rename lanes to lanes_orig since it may not be what we want for two-way links
    links_gdf.rename(columns={'lanes': 'lanes_orig'}, inplace=True)
    # lanes columns are sometimes a list of lanes, e.g. [2,3,4]. For lists with two items, use min. For lists with more, use median.
    LANES_COLS = [
        'lanes_orig',
        'lanes:backward',
        'lanes:forward',
        'lanes:both_ways',
        'lanes:bus',
        'lanes:bus:forward','lanes:bus:backward'
    ]
    for lane_col in LANES_COLS:
        WranglerLogger.debug(f"Before get_min_or_median_value: links_gdf['{lane_col}'].value_counts():\n{links_gdf[lane_col].value_counts(dropna=False)}")
        links_gdf[lane_col] = links_gdf[lane_col].apply(get_min_or_median_value)
        WranglerLogger.debug(f"After get_min_or_median_value: links_gdf['{lane_col}'].value_counts():\n{links_gdf[lane_col].value_counts(dropna=False)}")

    # split links_gdf into reversed and not reversed and make a wide dataframe with both
    links_gdf_notreversed = links_gdf[links_gdf['reversed'] == False].copy()  # this should be longer
    links_gdf_reversed    = links_gdf[links_gdf['reversed'] == True].copy()

    # for the links in links_gdf_reversed, make all columns have suffix '_rev'
    rev_col_rename = {}
    for col in links_gdf_reversed.columns.to_list():
        rev_col_rename[col] = f"{col}_rev"
    links_gdf_reversed.rename(columns=rev_col_rename, inplace=True)

    links_gdf_wide = pd.merge(
        left=links_gdf_notreversed,
        right=links_gdf_reversed,
        how='outer',
        left_on=['A','B'],
        right_on=['B_rev','A_rev'],
        indicator=True,
        validate='one_to_one'
    )
    WranglerLogger.debug(f"links_gdf_wide['_merge'].value_counts():\n{links_gdf_wide['_merge'].value_counts()}")
    LANES_COLS = ['A','B','drive_access','oneway','reversed','name','lanes','lanes_orig','lanes:backward','lanes:forward','lanes:both_ways','lanes:bus','lanes:bus:forward','lanes:bus:backward']
    LANES_COLS += [f"{col}_rev" for col in LANES_COLS if col not in ['A','B']]

    # set the lanes from lanes:forward or lanes:backward
    links_gdf['lanes'    ] = -1 # initialize to -1
    links_gdf['lanes_rev'] = -1
    links_gdf_wide.loc[ links_gdf_wide['lanes:forward' ].notna() & (links_gdf_wide.reversed == False), 'lanes'    ] = links_gdf_wide['lanes:forward']
    links_gdf_wide.loc[ links_gdf_wide['lanes:backward'].notna() & (links_gdf_wide.reversed == False), 'lanes_rev'] = links_gdf_wide['lanes:backward']

    WranglerLogger.debug(f"links_gdf_wide:\n{links_gdf_wide[LANES_COLS]}")
    WranglerLogger.debug(f"links_gdf_wide.lanes.value_counts():\n{links_gdf_wide['lanes'].value_counts(dropna=False)}")
    raise

def standardize_highway_value(links_gdf: gpd.GeoDataFrame):
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
        links_gdf (gpd.DataFrame): links from OSMnx with columns, highway, steps, and other OSM tags.  New columns:

          steps: (bool) if there are stairs for the facility
          drive_access: (bool)
          bike_access: (bool)
          walk_access: (bool)
          truck_access: (bool)
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
    # go from highest to lowest and choose highest
    for highway_type in HIGHWAY_HIERARCHY:
        links_gdf.loc[ links_gdf.highway.apply(lambda x: isinstance(x, list) and highway_type in x), 'highway'] = highway_type
    return

def get_roadway_value(highway):
    """ 
    When multiple values are present, return the first one.
    """
    if isinstance(highway,list):
        WranglerLogger.debug(f"list: {highway}")
        return highway[0]
    return highway



def standardize_and_write(g: networkx.MultiDiGraph, suffix: str) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Standardizes fields and writes the given graph's links and nodes to Tableau.

    Args:
        g (networkx.MultiDiGraph): _description_
        suffix (str): _description_
    
    Returns: links and nodes GeoDataFrame objects
    """
    WranglerLogger.info(f"======= standardize_and_write(g, {suffix}) =======")
    # project to long/lat
    g = osmnx.projection.project_graph(g, to_crs="EPSG:4326")

    nodes_gdf, edges_gdf = osmnx.graph_to_gdfs(g)
    WranglerLogger.info(f"After converting to gdfs, len(edges_gdf)={len(edges_gdf):,} and len(nodes_gdf=){len(nodes_gdf):,}")

    # When checking for uniqueness in uv, it looks like all of these are loops where
    # it would be fine to delete the longer one for the purposes of routing....so that's what we will do.
    links_gdf = edges_gdf.loc[edges_gdf.groupby(['u', 'v'])['length'].idxmin()].reset_index(drop=False)
    WranglerLogger.info(f"links_gdf has {len(links_gdf):,} links after dropping duplicates")

    # use A,B instead of u,v
    links_gdf.rename(columns={'u': 'A', 'v': 'B'}, inplace=True)

    standardize_highway_value(links_gdf)
    standardize_lanes_value(links_gdf)

    WranglerLogger.debug(f"2 links_gdf:\n{links_gdf}")
    WranglerLogger.debug(f"2 links_gdf.dtypes:\n{links_gdf.dtypes}")

    for col in links_gdf.columns:
        # report on value counts for non-unique columns
        if col not in ['geometry', 'A', 'B', 'name', 'width','osm_link_id', 'length']:
            WranglerLogger.debug(f"column {col} has value_counts:\n{links_gdf[col].value_counts(dropna=False)}")

        elif col in ['length']:
            # leave as float
            pass
        # A, B are too big for int64, so convert to str
        elif col in ['A', 'B']:
            WranglerLogger.debug(f"column {col} has min={links_gdf[col].min():,} and max={links_gdf[col].max():,}")
            links_gdf[col] = links_gdf[col].astype(str)
        # convert objects to strings but leave others as is
        elif links_gdf[col].dtype == object:
            links_gdf[col] = links_gdf[col].astype(str)

    WranglerLogger.info(f"3 links_gdf:\n{links_gdf}")
    WranglerLogger.info(f"3 links_gdf.dtypes:\n{links_gdf.dtypes}")

    tableau_utils.write_geodataframe_as_tableau_hyper(
        links_gdf, 
        OUTPUT_DIR/f"{args.county.replace(' ','_').lower()}_links{suffix}.hyper", 
        f"{args.county.replace(' ','_').lower()}_links{suffix}"
    )

    for col in nodes_gdf.columns:
        if col in ['highway','ref','railway']:
            nodes_gdf[col] = nodes_gdf[col].astype(str)

    WranglerLogger.info(f"1 nodes_gdf:\n{nodes_gdf}")
    WranglerLogger.info(f"1 {len(nodes_gdf)=:,} nodes_gdf.dtypes:\n{nodes_gdf.dtypes}")
    WranglerLogger.info(f"1 nodes_gdf.index:\n{nodes_gdf.index}")
    tableau_utils.write_geodataframe_as_tableau_hyper(
        nodes_gdf, 
        OUTPUT_DIR/f"{args.county.replace(' ','_').lower()}_nodes{suffix}.hyper", 
        f"{args.county}_nodes{suffix}"
    )
    return (links_gdf, nodes_gdf)

if __name__ == "__main__":

    pd.options.display.max_columns = None
    pd.options.display.width = None

    osmnx.settings.use_cache = True
    osmnx.settings.cache_folder = OUTPUT_DIR / "osmnx_cache"
    osmnx.settings.log_file = True
    osmnx.settings.logs_folder = OUTPUT_DIR / "osmnx_logs"
    osmnx.settings.useful_tags_way=OSM_WAY_TAGS.keys()

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("county", type=str, choices=['Bay Area'] + BAY_AREA_COUNTIES)
    args = parser.parse_args()

    # INFO_LOG  = OUTPUT_DIR / f"create_mtc_network_{args.county}_{NOW}.info.log"
    # DEBUG_LOG = OUTPUT_DIR / f"create_mtc_network_{args.county}_{NOW}.debug.log"
    INFO_LOG  = OUTPUT_DIR / f"create_mtc_network_{args.county}.info.log"
    DEBUG_LOG = OUTPUT_DIR / f"create_mtc_network_{args.county}.debug.log"

    network_wrangler.setup_logging(
        info_log_filename=INFO_LOG,
        debug_log_filename=DEBUG_LOG,
        std_out_level="info",
        file_mode='w'
    )
    WranglerLogger.info(f"Created by {__file__}")

    counties = [args.county] if args.county != 'Bay Area' else BAY_AREA_COUNTIES
    for county in counties:
        # use network_type='all_public' for all edges
        # Use OXMnx to pull the network graph for a place.
        # See https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.graph.graph_from_place
        #
        # g is a [networkx.MultiDiGraph](https://networkx.org/documentation/stable/reference/classes/multidigraph.html#), 
        # a directed graph with self loops and parallel edges (muliple edges can exist between two nodes)
        WranglerLogger.info(f"Creating network for {county}...")
        g = osmnx.graph_from_place(f'{county}, California, USA', network_type='all')
        WranglerLogger.info(f"Initial graph has {g.number_of_edges():,} edges and {len(g.nodes()):,} nodes")

        (links_gdf, nodes_gdf) = standardize_and_write(g, "_unsimplified")
        nodes_gdf.reset_index(names='osmid', inplace=True)

        # Project to CRS https://epsg.io/2227 where length is feet
        g = osmnx.projection.project_graph(g, to_crs="EPSG:2227")

        # TODO: If we do simplification, it should be access-based. Drive links shouldn't be simplified to pedestrian links and vice versa
        # consolidate intersections
        # https://osmnx.readthedocs.io/en/stable/user-reference.html#osmnx.simplification.consolidate_intersections
        # g = osmnx.simplification.consolidate_intersections(
        #     g, 
        #     tolerance=30, # feet
        #     rebuild_graph=True,
        #     dead_ends=True, # keep dead-ends
        #     reconnect_edges=True,
        # )
        # WranglerLogger.info(f"After consolidating, graph has {g.number_of_edges():,} edges and {len(g.nodes()):,} nodes")

        # TODO: For now, we're retaining the result from the unsimplified for creating networks, but we could revisit
        # standardize_and_write(g, "_simplified30ft")

    # rename osmid to model_node_id; osmid is an int
    nodes_gdf.rename(columns={'osmid':'model_node_id', 'x':'X', 'y':'Y'}, inplace=True)
    WranglerLogger.debug(f"{len(nodes_gdf)=:,} nodes_gdf.dtypes:\n{nodes_gdf.dtypes}")

    # create roadway network
    roadway_network =  network_wrangler.load_roadway_from_dataframes(
        links_df=links_gdf,
        nodes_df=nodes_gdf,
        shapes_df=links_gdf
    )
    WranglerLogger.debug(f"roadway_network:\n{roadway_network}")

    # Read a GTFS network (not wrangler_flavored)
    gtfs_model = network_wrangler.transit.io.load_feed_from_path(INPUT_2023GTFS, wrangler_flavored=False)
    WranglerLogger.debug(f"gtfs_model:\n{gtfs_model}")

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