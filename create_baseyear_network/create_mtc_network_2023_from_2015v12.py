USAGE = """

Create MTC base year networks (2023) from the previously created 2015 networks.

Tested in July 2025 with:
  * network_wrangler, https://github.com/network-wrangler/network_wrangler/tree/main

References:
  * Asana: Year 2023 Model Run and Calibration Tasks > Network in Network Wrangler Format (https://app.asana.com/1/11860278793487/project/15119358130897/task/1209256117977561?focus=true)
  * MTC Year 2023 Network Creation Steps Google Doc (https://docs.google.com/document/d/1TU0nsUHmyKfYZDbwjeCFiW09w53fyWu7X3XcRlNyf2o/edit?tab=t.0#heading=h.kt1d1r2i57ei)
  * https://github.com/Metropolitan-Council/met_council_wrangler/blob/main/notebooks
"""
import datetime, time
import getpass
import pathlib
import pprint
import typing
import pandas as pd
import numpy as np
import geopandas as gpd
import shapely.geometry

import tableau_utils
import network_wrangler

from network_wrangler import WranglerLogger
from network_wrangler import write_transit
from network_wrangler.transit.network import TransitNetwork
from network_wrangler.utils.transit import \
  drop_transit_agency, filter_transit_by_boundary, create_feed_from_gtfs_model, truncate_route_at_stop
from network_wrangler.errors import \
  NodeNotFoundError, TransitValidationError
from network_wrangler.roadway.nodes.create import generate_node_ids
from network_wrangler.roadway.io import load_roadway_from_dir, write_roadway
from network_wrangler.transit.io import load_feed_from_path

INPUT_2015v12 = pathlib.Path(r"E:\Box\Modeling and Surveys\Development\Travel Model Two Conversion\Model Inputs\2015-tm22-dev-sprint-03\standard_network_after_project_cards")
INPUT_2023GTFS = pathlib.Path("M:\\Data\\Transit\\511\\2023-10")
COUNTY_SHAPEFILE = pathlib.Path("M:\\Data\\Census\\Geography\\tl_2010_06_county10\\tl_2010_06_county10_9CountyBayArea.shp")
OUTPUT_DIR = pathlib.Path("M:\\Development\\Travel Model Two\\Supply\\Network Creation 2025\\from_2015v12")

USERNAME = getpass.getuser()
if USERNAME=="lmz":
  INPUT_2015v12 = pathlib.Path("../../standard_network_after_project_cards")
  INPUT_2023GTFS = pathlib.Path("../../511gtfs_2023-10")
  COUNTY_SHAPEFILE = pathlib.Path("../../tl_2010_06_county10/tl_2010_06_county10_9CountyBayArea.shp")
  OUTPUT_DIR = pathlib.Path("../../output_from_2015v12")


NODES_FILE = INPUT_2015v12 / "v12_node.geojson"
LINKS_FILE = INPUT_2015v12 / "v12_link.json"
SHAPES_FILE = INPUT_2015v12 / "v12_shape.geojson"

NOW = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

def create_line(row):
  """Simple method to create shapely.geometry.LineString from coordinates in a DataFrame row
  """
  start_point = shapely.geometry.Point(row['X_A'], row['Y_A'])
  end_point = shapely.geometry.Point(row['X_B'], row['Y_B'])
  return shapely.geometry.LineString([start_point, end_point])

def fix_link_lanes(road_links_gdf: pd.DataFrame, lanes_col: str):
  """Makes lanes columns network_wrangler 1.0 compliant.

  Updates the given column so that it only contains integers, and scoped values are set into sc_[lanes_col]
  Args:
      links_df (pd.DataFrame): the RoadLinks DataFrame
      lanes_col (str): 'lanes' or 'ML_lanes'
  """
  lanes_dict_list = road_links_gdf.loc[road_links_gdf[lanes_col].apply(lambda x: isinstance(x, dict)), lanes_col].to_list()
  # Make the dictionaries unique by converting to string representations, getting unique ones, then converting back
  unique_lanes_dict_list = []
  seen_dicts = set()
  for lanes_dict in lanes_dict_list:
    dict_str = str(sorted(lanes_dict.items()))
    if dict_str not in seen_dicts:
      seen_dicts.add(dict_str)
      unique_lanes_dict_list.append(lanes_dict)
  lanes_dict_list = unique_lanes_dict_list

  WranglerLogger.debug(f"fix_link_lanes(lanes_col={lanes_col})")
  WranglerLogger.debug(f"{len(lanes_dict_list)=}  lanes_dict_list:{lanes_dict_list}")
  # lanes_dict_list: [
  # {'default': 3, 'timeofday': [{'time': [21600, 36000], 'value': 2}, {'time': [54000, 68400], 'value': 2}]}, 
  # {'default': 3, 'timeofday': [{'time': [54000, 68400], 'value': 2}]}, 
  # {'default': 3, 'timeofday': [{'time': [21600, 36000], 'value': 2}]}, 
  # etc
  for lanes_dict in lanes_dict_list:
    WranglerLogger.debug(f"  lanes_dict: {lanes_dict}")
    # create sc_lanes from this dictionary
    # network_wrangler/api_roadway/#network_wrangler.models.roadway.tables.RoadLinksTable
    sc_lanes = None
    if ('timeofday' in lanes_dict) and (len(lanes_dict['timeofday'])>0):
      sc_lanes = []
      for my_dict in lanes_dict['timeofday']:
        sc_dict = {}
        sc_dict['timespan'] = [
          time.strftime("%H:%M", time.gmtime(my_dict['time'][0])),
          time.strftime("%H:%M", time.gmtime(my_dict['time'][1]))
        ]
        sc_dict['value'] = my_dict['value']
        sc_lanes.append(sc_dict)
        # e.g. [{'timespan':['12:00':'15:00'], 'value': 3},{'timespan':['15:00':'19:00'], 'value': 2}]
    # set them
    road_links_gdf.loc[ road_links_gdf[lanes_col]==lanes_dict, lanes_col] = lanes_dict['default']
    # since sc_lanes may be a dictionary, make copies of it for each row to set or
    # pandas will error that the length doesn't match the rows
    road_links_gdf.loc[ road_links_gdf[lanes_col]==lanes_dict, f'sc_{lanes_col}'] = [sc_lanes] * len(road_links_gdf[road_links_gdf[lanes_col] == lanes_dict])

  # Set null, blank, '0' or 'NaN' to 0
  road_links_gdf.loc[ road_links_gdf[lanes_col].isnull(), lanes_col ] = 0
  road_links_gdf.loc[ road_links_gdf[lanes_col] == '',    lanes_col ] = 0
  road_links_gdf.loc[ road_links_gdf[lanes_col] == '0',   lanes_col ] = 0
  road_links_gdf.loc[ road_links_gdf[lanes_col] == 'NaN', lanes_col ] = 0

  # reset and check
  road_links_gdf[f'{lanes_col}_type'] = road_links_gdf[lanes_col].apply(type).astype(str)
  WranglerLogger.debug(f"road_links_gdf[['{lanes_col}_type]']].value_counts():")
  WranglerLogger.debug(road_links_gdf[[f'{lanes_col}_type']].value_counts())

  WranglerLogger.debug(f"strings value_counts():")
  WranglerLogger.debug(road_links_gdf.loc[ road_links_gdf[lanes_col].apply(lambda x: isinstance(x, str)), lanes_col])

def fix_mixed_type_columns(road_links_gdf: pd.DataFrame):
  """Fix columns with mixed types that prevent parquet writing.
  
  Identifies columns with mixed types and converts them to strings.
  """
  WranglerLogger.debug("Checking for columns with mixed types...")
  
  for col in road_links_gdf.columns:
    if road_links_gdf[col].dtype == 'object':
      # Check if column has mixed types
      types_in_col = set()
      sample_size = min(1000, len(road_links_gdf))
      for val in road_links_gdf[col].head(sample_size):
        if pd.notna(val):
          types_in_col.add(type(val).__name__)
      
      if len(types_in_col) > 1:
        WranglerLogger.debug(f"  Column {col} has mixed types: {types_in_col}. Converting to string.")
        road_links_gdf[col] = road_links_gdf[col].astype(str)
        # Replace 'nan' strings with empty strings for cleaner output
        road_links_gdf[col] = road_links_gdf[col].replace('nan', '')

def fix_numeric_columns(road_links_gdf: pd.DataFrame):
  """Fix numeric columns that have empty strings or invalid values.
  
  Converts empty strings to NaN for numeric columns so they can be written to parquet.
  """
  # Find all columns that should be numeric based on their current dtype or content
  # Include all columns that end with common numeric suffixes
  numeric_suffixes = ['time', 'cost', 'distance', 'speed', 'capacity', 'toll', 'lanes']
  numeric_cols = []
  
  for col in road_links_gdf.columns:
    # Check if column name suggests it should be numeric
    if any(suffix in col.lower() for suffix in numeric_suffixes):
      numeric_cols.append(col)
    # Also include single letter columns that are typically coordinates
    elif col in ['u', 'v', 'w', 'x', 'y', 'z', 'A', 'B']:
      numeric_cols.append(col)
    # Check if the column is already numeric but has some string values
    elif road_links_gdf[col].dtype == 'object':
      # Sample the column to see if it contains numeric-looking values
      sample = road_links_gdf[col].dropna().head(100)
      if len(sample) > 0:
        try:
          # Try to convert a sample to numeric
          pd.to_numeric(sample, errors='coerce')
          # If more than 50% convert successfully, consider it numeric
          converted = pd.to_numeric(sample, errors='coerce')
          if converted.notna().sum() / len(sample) > 0.5:
            numeric_cols.append(col)
        except:
          pass
  
  WranglerLogger.debug(f"  Identified {len(numeric_cols)} potentially numeric columns to fix: {numeric_cols}")
  
  for col in numeric_cols:
    if col in road_links_gdf.columns:
      # Replace empty strings and string NaN values with np.nan
      road_links_gdf[col] = road_links_gdf[col].replace('', np.nan)
      road_links_gdf[col] = road_links_gdf[col].replace('nan', np.nan)
      road_links_gdf[col] = road_links_gdf[col].replace('NaN', np.nan)
      road_links_gdf[col] = road_links_gdf[col].replace('NAN', np.nan)
      
      # Try to convert to numeric
      try:
        road_links_gdf[col] = pd.to_numeric(road_links_gdf[col], errors='coerce')
        non_nan_count = road_links_gdf[col].notna().sum()
        if non_nan_count > 0:
          WranglerLogger.debug(f"    Converted column {col} to numeric (has {non_nan_count:,} non-NaN values)")
      except Exception as e:
        WranglerLogger.warning(f"    Could not convert column {col} to numeric: {e}")

def fix_link_access(road_links_gdf: pd.DataFrame, access_col: str):
  """Converts access columns to strings and renames them with 'orig_' prefix.
  
  Converts all values to strings for parquet compatibility and renames the column
  to have 'orig_' prefix. The original column name is removed.
  
  Args:
      road_links_gdf (pd.DataFrame): the RoadLinks DataFrame
      access_col (str): 'access', 'ML_access'
  """
  if access_col not in road_links_gdf.columns:
    WranglerLogger.debug(f"fix_link_access: Column {access_col} not found, skipping")
    return
    
  WranglerLogger.debug(f"fix_link_access(access_col='{access_col}')")
  col_type = road_links_gdf[access_col].apply(type).astype(str)
  WranglerLogger.debug(f"col_type.value_counts(dropna=False):\n{col_type.value_counts(dropna=False)}")
  # convert to string
  road_links_gdf[access_col] = road_links_gdf[access_col].astype(str)
  # rename
  road_links_gdf.rename({access_col:f'orig_{access_col}'}, inplace=True)
  WranglerLogger.debug(f"Converted column '{access_col}' to str and renamed to 'orig_{access_col}'")

def create_nodes_for_new_stations(
    new_stop_ids: typing.List[str],
    gtfs_model: network_wrangler.models.gtfs.gtfs.GtfsModel,
    nodes_gdf: gpd.GeoDataFrame
  ) -> gpd.GeoDataFrame:
  """Creates nodes table for a list of stop_ids in a gtfs feed.
    
    Finds unused node numbers given MTC node numbering convention (by county) and creates
    node table for the given stations.

    Args:
        new_stop_ids (List[str]): list of stop_ids
        gtfs_model (network_wrangler.models.gtfs.gtfs.GtfsModelGtfsModel): GTFS feed with stop_ids
        nodes_gdf (gpd.GeoDataFrame): Nodes table
        
    Returns:
        Node table with the given stop ids in the same crs as nodes_gdf. Columns:
          * model_node_id (int): Unique identifier for the node
          * X, Y (float): Longitude and Latitude
          * geometry (GeoSeries)
        MTC-node attributes:
          * county (str)
          * drive_access, walk_access, bike_access, rail_only (TODO: rename to transit_only?)
          * name (str): will be set to stop_name
        Other: (drop before adding to nodes)
          * stop_id
  """
  WranglerLogger.debug("=== create_nodes_for_new_stations() ====")

  # Read county shapefile for spatial join
  counties_gdf = gpd.read_file(COUNTY_SHAPEFILE)
  
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

  # Get stop coordinates from GTFS for the new stations
  new_stops_df = gtfs_model.stops[gtfs_model.stops['stop_id'].isin(new_stop_ids)].copy()
  
  # Create GeoDataFrame from stops
  new_stops_geometry = [shapely.geometry.Point(lon, lat) for lon, lat in 
                        zip(new_stops_df['stop_lon'], new_stops_df['stop_lat'])]
  new_stops_gdf = gpd.GeoDataFrame(new_stops_df, geometry=new_stops_geometry, crs='EPSG:4326')
  
  # Reproject to match county shapefile CRS if needed
  if counties_gdf.crs != new_stops_gdf.crs:
    new_stops_gdf = new_stops_gdf.to_crs(counties_gdf.crs)
  
  # Perform spatial join to determine county for each stop
  new_stops_gdf = gpd.sjoin(new_stops_gdf, counties_gdf[['NAME10', 'geometry']], 
                                 how='left', predicate='within')
  # For stops that couldn't be matched, set to External
  new_stops_gdf.loc[ new_stops_gdf['NAME10'].isna(), 'NAME10' ] = 'External'

  # rename columns for returning
  new_stops_gdf.rename(columns={
    'stop_lon':'X', 'stop_lat':'Y',
    'NAME10':'county'
  }, inplace=True)
  # drop the other columns
  new_stops_gdf = new_stops_gdf[['stop_id','stop_name','X','Y','county','geometry']]
  new_stops_gdf['model_node_id'] = -1
  WranglerLogger.debug(f"new_stops_gdf:\n{new_stops_gdf}")
  
  # Group stops by county to generate node IDs per county
  for county_name, county_stops_df in new_stops_gdf.groupby('county'):      
    start_range = COUNTY_NAME_TO_NODE_START_NUM[county_name] + 1
    if county_name == 'External':
      end_range = start_range + 99_999
    else:
      end_range = start_range + 500_000
    WranglerLogger.debug(f"For {county_name}, range:{start_range:,}-{end_range:,}")
    
    # Generate unique node IDs for all stops in this county
    num_stops = len(county_stops_df)
    try:
      new_node_ids = generate_node_ids(nodes_gdf, range(start_range, end_range), n=num_stops)
      WranglerLogger.debug(f"new_node_ids: {new_node_ids}")
      
      # Assign the generated node IDs to the stops
      for i, (idx, stop_row) in enumerate(county_stops_df.iterrows()):
        stop_id = stop_row['stop_id']
        new_stops_gdf.loc[new_stops_gdf.stop_id == stop_id, 'model_node_id'] = new_node_ids[i]
        
    except Exception as e:
      WranglerLogger.error(f"Could not generate node IDs for county {county_name}: {e}")
      raise
  
  new_stops_gdf['drive_access'] = 0
  new_stops_gdf['walk_access']  = 1
  new_stops_gdf['bike_access']  = 1
  new_stops_gdf['rail_only']    = 1
  new_stops_gdf['ferry_only']   = 1
  # rename stop_name to name
  new_stops_gdf.rename(columns={'stop_name':'name'}, inplace=True)
  # convert to the same crs as nodes_gdf
  new_stops_gdf.to_crs(nodes_gdf.crs, inplace=True)

  WranglerLogger.debug(f"returning {type(new_stops_gdf)} new_stops_gdf:\n{new_stops_gdf}")
  return new_stops_gdf

def create_transit_links_for_new_stations(
    stop_pairs: typing.List[typing.Tuple[str, str, bool]],
    stop_id_to_model_node_id: typing.Dict[str, int],
    node_gdf: gpd.GeoDataFrame,
    existing_links_gdf: gpd.GeoDataFrame,
  ) -> gpd.GeoDataFrame:
  """Creates transit links between specified stop pairs.
  
  This function creates uni-directional links between stop pairs to ensure transit
  connectivity in the roadway network.
  
  Args:
      stop_pairs: List of tuples (from_stop_id, to_stop_id, oneway) representing links to create
      stop_id_to_model_node_id: Dictionary mapping relevant stop_id to model_node_id
      node_gdf: Roadway nodes
      existing_links_gdf: Existing roadway links
      
  Returns:
      GeoDataFrame of new transit links with appropriate attributes
  """
  WranglerLogger.debug("=== create_transit_links_for_new_stations() ====")

  # Get the maximum model_link_id from existing links
  max_link_id = existing_links_gdf['model_link_id'].max()
  next_link_id = max_link_id + 1
  
  link_dicts = []
  # Track created links to avoid duplicates within this batch
  created_links = set()
  
  for (from_stop_id, to_stop_id, oneway) in stop_pairs:
    from_model_node_id = stop_id_to_model_node_id.get(from_stop_id)
    to_model_node_id = stop_id_to_model_node_id.get(to_stop_id)
    
    if from_model_node_id is None:
      WranglerLogger.warning(f"Stop {from_stop_id} not found in stop_id_to_model_node_id mapping - skipping link")
      continue
    if to_model_node_id is None:
      WranglerLogger.warning(f"Stop {to_stop_id} not found in stop_id_to_model_node_id mapping - skipping link")
      continue
    
    point_A_series = node_gdf.loc[node_gdf.model_node_id == from_model_node_id].geometry
    point_B_series = node_gdf.loc[node_gdf.model_node_id == to_model_node_id].geometry
    
    if len(point_A_series) == 0:
      WranglerLogger.warning(f"Node {from_model_node_id} for stop {from_stop_id} not found in node_gdf - skipping link")
      continue
    if len(point_B_series) == 0:
      WranglerLogger.warning(f"Node {to_model_node_id} for stop {to_stop_id} not found in node_gdf - skipping link")
      continue

    # Check if link already exists in the network
    forward_exists = ((existing_links_gdf['A'] == from_model_node_id) & 
                      (existing_links_gdf['B'] == to_model_node_id)).any()
    if forward_exists:
      raise ValueError(
        f"Link from node {from_model_node_id} (stop {from_stop_id}) to node {to_model_node_id} (stop {to_stop_id}) "
        f"already exists in the network. Duplicate transit links are not allowed."
      )
    
    # Check if link was already created in this batch
    link_tuple = (from_model_node_id, to_model_node_id)
    if link_tuple in created_links:
      raise ValueError(
        f"Duplicate link from node {from_model_node_id} (stop {from_stop_id}) to node {to_model_node_id} (stop {to_stop_id}) "
        f"in stop_pairs list. Each link should only be specified once."
      )
    
    # For two-way links, also check if reverse link exists
    if not oneway:
      reverse_exists = ((existing_links_gdf['A'] == to_model_node_id) & 
                        (existing_links_gdf['B'] == from_model_node_id)).any()
      if reverse_exists:
        raise ValueError(
          f"Reverse link from node {to_model_node_id} (stop {to_stop_id}) to node {from_model_node_id} (stop {from_stop_id}) "
          f"already exists in the network. Cannot create bidirectional link when reverse already exists."
        )
      
      # Check if reverse was already created in this batch
      reverse_tuple = (to_model_node_id, from_model_node_id)
      if reverse_tuple in created_links:
        raise ValueError(
          f"Duplicate reverse link from node {to_model_node_id} (stop {to_stop_id}) to node {from_model_node_id} (stop {from_stop_id}) "
          f"in stop_pairs list. Bidirectional links should be specified with oneway=False, not as two separate entries."
        )
    
    # Extract the actual Point object from the GeoSeries
    point_A = point_A_series.values[0]
    point_B = point_B_series.values[0]
    
    # Create LineString and calculate distance
    line_geom = shapely.geometry.LineString([point_A, point_B])
    
    # Calculate distance in miles (assuming coordinates are in feet - EPSG:2227)
    distance_ft = line_geom.length
    distance_miles = distance_ft / 5280.0
    
    # Create forward link
    forward_link = {
      'model_link_id': next_link_id,
      'shape_id': str(next_link_id),
      'A': from_model_node_id,
      'B': to_model_node_id,
      'name': f'Transit link {from_stop_id} to {to_stop_id}',
      'geometry': line_geom,
      'distance': distance_miles,
      # Set as rail-only / ferry_only link
      'rail_only': 1,
      'ferry_only': 1,
      'drive_access': 0,
      'walk_access': 0,
      'bike_access': 0,
      'transit': 1,
      # Copy lane and other attributes from template
      'lanes': 0,
      'managed': 0,
      'bus_only': 0,
    }
    link_dicts.append(forward_link)
    created_links.add(link_tuple)  # Track this link as created
    next_link_id += 1

    if oneway: continue

    # Create backward link
    backward_link = {
      'model_link_id': next_link_id,
      'shape_id': str(next_link_id),
      'A': to_model_node_id,
      'B': from_model_node_id,
      'name': f'Transit link {to_stop_id} to {from_stop_id}',
      'geometry': shapely.geometry.LineString([point_B, point_A]),
      'distance': distance_miles,
      # Set as rail-only/ferry-only link
      'rail_only': 1,
      'ferry_only': 1,
      'drive_access': 0,
      'walk_access': 0,
      'bike_access': 0,
      'transit': 1,
      # Copy lane and other attributes from template
      'lanes': 0,
      'managed': 0,
      'bus_only': 0,
    }
    link_dicts.append(backward_link)
    created_links.add(reverse_tuple)  # Track the reverse link as created
    next_link_id += 1

  new_links_gdf = gpd.GeoDataFrame(data=link_dicts, crs=node_gdf.crs)    
  WranglerLogger.debug(f"new_links_gdf:\n{new_links_gdf}")
  return new_links_gdf

if __name__ == "__main__":
  pd.options.display.max_columns = None
  pd.options.display.width = None
  pd.options.display.min_rows = 20

  # INFO_LOG  = OUTPUT_DIR / f"create_mtc_network_from_2015_{NOW}.info.log"
  # DEBUG_LOG = OUTPUT_DIR / f"create_mtc_network_from_2015_{NOW}.debug.log"
  INFO_LOG  = OUTPUT_DIR / f"create_mtc_network_from_2015.info.log"
  DEBUG_LOG = OUTPUT_DIR / f"create_mtc_network_from_2015.debug.log"

  network_wrangler.setup_logging(
    info_log_filename=INFO_LOG,
    debug_log_filename=DEBUG_LOG,
    std_out_level="info",
    file_mode='w'
  )
  WranglerLogger.info(f"Created by {__file__}")

  # Create roadway and transit subdirectories for output
  roadway_network_dir    = OUTPUT_DIR / "roadway_network"
  transit_gtfs_model_dir = OUTPUT_DIR / "gtfs_model"
  transit_network_dir    = OUTPUT_DIR / "transit_network"

  roadway_network = None
  gtfs_model = None
  # skip initial steps if we've done them already
  try:
    roadway_network = load_roadway_from_dir(
      roadway_network_dir,
      file_format="parquet",
    )
    WranglerLogger.debug("After load_roadway_from_dir()")
    WranglerLogger.debug(f"roadway_network.nodes_df.head():\n{roadway_network.nodes_df.head()}")
    WranglerLogger.debug(f"roadway_network.links_df.head():\n{roadway_network.links_df.head()}")
  except Exception as e:
    WranglerLogger.info(f"Could not read roadway_network from {roadway_network_dir}. Processing 2015 roadway network.")
    WranglerLogger.error(e)
  try:
    gtfs_model = load_feed_from_path(
      feed_path = transit_gtfs_model_dir,
      wrangler_flavored = False
    )
    WranglerLogger.debug("After load_feed_from_path()")
    WranglerLogger.debug(f"gtfs_model:\n{gtfs_model}")
  except Exception as e:
    WranglerLogger.info(f"Could not read gtfs_model from {transit_gtfs_model_dir}. Processing 2015 roadway network.")
    WranglerLogger.error(e)

  if not roadway_network or not gtfs_model:
    nodes_gdf = network_wrangler.utils.io_table.read_table(filename=NODES_FILE)
    WranglerLogger.debug(f"Read {NODES_FILE}:\n{nodes_gdf}")
    WranglerLogger.debug(f"type(nodes_gdf)={type(nodes_gdf)} crs={nodes_gdf.crs}")
    WranglerLogger.debug(f"nodes_df.dtypes:\n{nodes_gdf.dtypes:}")

    links_df = network_wrangler.utils.io_table.read_table(filename=LINKS_FILE)
    WranglerLogger.debug(f"Read {LINKS_FILE}:\n{links_df}")
    WranglerLogger.debug(f"type(links_df)={type(links_df)}")
    WranglerLogger.debug(f"links_df.dtypes:\n{links_df.dtypes:}")

    shapes_gdf = network_wrangler.utils.io_table.read_table(filename=SHAPES_FILE)
    WranglerLogger.debug(f"Read {SHAPES_FILE}:\n{shapes_gdf}")
    WranglerLogger.debug(f"type(shapes_gdf)={type(shapes_gdf)} crs={shapes_gdf.crs}")
    WranglerLogger.debug(f"shapes_df.dtypes:\n{shapes_gdf.dtypes:}")

    # make transit into an int instead of an object, default to 0
    WranglerLogger.debug(f"Initial links_df.transit.value_counts(dropna=False)\n{links_df.transit.value_counts(dropna=False)}")
    links_df.transit = links_df.transit.fillna(0)
    links_df.transit = links_df.transit.astype(bool)
    WranglerLogger.debug(f"Updated links_df.transit.value_counts(dropna=False)\n{links_df.transit.value_counts(dropna=False)}")

    # This is a model network and we'll come back to that later, but we're starting with roadway.
    # So drop the TAZ and MAZ nodes, and the centroid connectors (FT=99, transit==False)
    WranglerLogger.debug(f"links_df[['ft','transit']].value_counts(dropna=False)=\n{links_df[['ft','transit']].value_counts(dropna=False)}")
    road_links_df = links_df.loc[ (links_df.ft != 99) | (links_df.transit == True) ]
    WranglerLogger.info(f"Filtering to {len(road_links_df):,} road links from {len(links_df):,} model links")

    # filter out tap, taz, maz links
    WranglerLogger.debug(f"road_links_df.roadway.value_counts(dropna=False)=\n{road_links_df.roadway.value_counts(dropna=False)}")
    road_links_df = road_links_df.loc[road_links_df.roadway != 'tap']
    road_links_df = road_links_df.loc[road_links_df.roadway != 'taz']
    road_links_df = road_links_df.loc[road_links_df.roadway != 'maz']
    WranglerLogger.info(f"Filtering to {len(road_links_df):,} road links after dropping roadway=tap,taz,maz")

    # https://bayareametro.github.io/tm2py/inputs/#county-node-numbering-system
    # MAZs and TAZs have node numbers < 1M
    road_nodes_gdf = nodes_gdf.loc[ nodes_gdf.model_node_id > 999999 ]
    WranglerLogger.info(f"Filtering to {len(road_nodes_gdf):,} road nodes from {len(nodes_gdf):,} model nodes")

    # Noting that 'id','fromIntersectionId','toIntersectionId' is not unicque
    # because there are a bunch with id='walktorailN' or 'tap_N', and fromIntersectionId/toIntersectionId=None
    duplicates = road_links_df.loc[road_links_df.duplicated(subset=['id','fromIntersectionId','toIntersectionId'], keep=False)]
    WranglerLogger.debug(f"duplicated: len={len(duplicates):,}:\n{duplicates}")

    road_links_df = pd.merge(
      left=road_links_df,
      right=shapes_gdf[['id','fromIntersectionId','toIntersectionId','geometry']],
      on=['id','fromIntersectionId','toIntersectionId'],
      how='left',
      indicator=True,
    )
    WranglerLogger.debug(f"After merging with shapes_gdf, road_links_df[['_merge']].value_counts():\n{road_links_df[['_merge']].value_counts()}")
    road_links_df.drop(columns=['_merge'], inplace=True)
    WranglerLogger.debug(f"{len(road_links_df.geometry.isna())=:,}")

    # Merging with shapes in the reverse direction
    shapes_gdf.geometry = shapes_gdf.geometry.reverse()
    road_links_df = pd.merge(
      left=road_links_df,
      right=shapes_gdf[['id','fromIntersectionId','toIntersectionId','geometry']],
      left_on=['id','fromIntersectionId','toIntersectionId'],
      right_on=['id','toIntersectionId', 'fromIntersectionId'],
      how='left',
      indicator=True,
      suffixes=('','_revgeom')
    )
    WranglerLogger.debug(f"After merging with shapes_gdf (reversed), road_links_df[['_merge']].value_counts():\n{road_links_df[['_merge']].value_counts()}")
    # now we have geometry and geometry_revgeom.  Use the latter if the former is na
    road_links_df.loc[ road_links_df.geometry.isna(), 'geometry'] = road_links_df.geometry_revgeom
    # drop new columns as we've used them to set geometry
    road_links_df.drop(columns=['_merge', 'fromIntersectionId_revgeom','toIntersectionId_revgeom','geometry_revgeom'], inplace=True)
    WranglerLogger.debug(f"{len(road_links_df.geometry.isna())=:,}")

    # For the rows that do not have geometry, create a simple two-point line geometry from the node locations
    # Use all nodes, not just road nodes
    no_geometry_links = road_links_df.loc[ pd.isnull(road_links_df.geometry) ]
    no_geometry_links = pd.merge(
      left=no_geometry_links,
      right=nodes_gdf[['model_node_id','X','Y']],
      how='left',
      left_on='A',
      right_on='model_node_id',
      validate='many_to_one',
      indicator=True,
      suffixes=('','_A')
    ).rename(columns={'_merge':'_merge_A','X':'X_A','Y':'Y_A','model_node_id':'model_node_id_A'})

    no_geometry_links = pd.merge(
      left=no_geometry_links,
      right=nodes_gdf[['model_node_id','X','Y']],
      how='left',
      left_on='B',
      right_on='model_node_id',
      validate='many_to_one',
      indicator=True,
      suffixes=('','_B')
    ).rename(columns={'_merge':'_merge_B','X':'X_B','Y':'Y_B','model_node_id':'model_node_id_B'})

    # check that they all merged
    WranglerLogger.debug(f"After merging with nodes, no_geometry_links[['_merge_A','_merge_B']].value_counts():\n{no_geometry_links[['_merge_A','_merge_B']].value_counts()}")
    WranglerLogger.debug(f"no_geometry_links:\n{no_geometry_links}")
    no_geometry_links['geometry'] = no_geometry_links.apply(create_line, axis=1)
    # we're done with these columns -- drop them
    no_geometry_links.drop(columns=[
      'model_node_id_A','X_A','Y_A','_merge_A',
      'model_node_id_B','X_B','Y_B','_merge_B'], 
      inplace=True)

    # create road_links_gdf now that we have geometry for everything
    road_links_gdf = gpd.GeoDataFrame(pd.concat([
      road_links_df.loc[ pd.notnull(road_links_df.geometry) ],
      no_geometry_links]),
      crs=shapes_gdf.crs)
    WranglerLogger.debug(f"Created road_links_gdf with dtypes:\n{road_links_gdf.dtypes}")
    WranglerLogger.debug(f"road_links_gdf:\n{road_links_gdf}")

    # fill in missing managed values with 0
    WranglerLogger.debug(f"road_links_gdf['managed'].value_counts():\n{road_links_gdf['managed'].value_counts()}")
    WranglerLogger.debug(f"road_links_gdf['managed'].apply(type).value_counts():\n{road_links_gdf['managed'].apply(type).value_counts()}")
    road_links_gdf.loc[road_links_gdf.managed == '', 'managed'] = 0 # blank -> 0
    road_links_gdf['managed'] = road_links_gdf['managed'].astype(int)
    WranglerLogger.debug(f"road_links_gdf['managed'].value_counts():\n{road_links_gdf['managed'].value_counts()}")

    # The columns lanes and ML_lanes are a combination of types, including dictionaries representing time-scoped versions
    # Fix this according to network_wrangler standard
    fix_link_lanes(road_links_gdf, lanes_col='lanes')
    fix_link_lanes(road_links_gdf, lanes_col='ML_lanes')

    # Access columns will be fixed after all links are added (including transit links)

    # network_wrangler requires distance field
    road_links_gdf_feet = road_links_gdf.to_crs(epsg=2227)
    road_links_gdf_feet['distance'] = road_links_gdf_feet.length / 5280 # distance is in miles
    # join back to road_links_gdf
    road_links_gdf = road_links_gdf.merge(
      right=road_links_gdf_feet[['A','B','distance']],
      how='left',
      on=['A','B'],
      validate='one_to_one'
    )
    # shape_id is a string
    road_links_gdf['shape_id'] = road_links_gdf.model_link_id.astype(str)

    # are there links with distance==0?
    WranglerLogger.debug(f"road_links_gdf.loc[ road_links_gdf['distance'] == 0 ]:\n{road_links_gdf.loc[ road_links_gdf['distance'] == 0 ]}")

    #TODO: This includes connectors so it's technically a model roadway network rather than a roadway network...

    # Before creating the RoadwayNetwork object, there are a few transit stops and links that are missing because they didn't
    # exist in 2015.  Add these to the roadway network because we'll need them to be compatible.

    # The gtfs feed covers the month of October 2023; select to Wednesday, October 11, 2023
    # gtfs_model doesn't include calendar_dates so read this ourselves
    # tableau viz of this feed: https://10ay.online.tableau.com/#/site/metropolitantransportationcommission/views/regional_feed_511_2023-10/Dashboard1?:iid=1
    calendar_dates_df = pd.read_csv(INPUT_2023GTFS / "calendar_dates.txt")
    WranglerLogger.debug(f"calendar_dates_df (len={len(calendar_dates_df):,}):\n{calendar_dates_df}")
    calendar_dates_df = calendar_dates_df.loc[ (calendar_dates_df.date == 20231011) & (calendar_dates_df.exception_type == 1) ]
    WranglerLogger.debug(f"After filtering calendar_dates_df (len={len(calendar_dates_df):,}):\n{calendar_dates_df}")
    # make service_id a string
    calendar_dates_df['service_id'] = calendar_dates_df['service_id'].astype(str)
    service_ids_df = calendar_dates_df[['service_id']].drop_duplicates().reset_index(drop=True)
    # Convert DataFrame to list for the updated load_feed_from_path function
    service_ids = service_ids_df['service_id'].tolist()
    WranglerLogger.debug(f"After filtering service_ids (len={len(service_ids):,}):\n{service_ids}")

    # Read a GTFS network (not wrangler_flavored)
    gtfs_model = load_feed_from_path(INPUT_2023GTFS, wrangler_flavored=False, service_ids_filter=service_ids)
    WranglerLogger.debug(f"gtfs_model:\n{gtfs_model}")
    # drop some columns that are not required or useful
    gtfs_model.stops.drop(columns=['stop_code','stop_desc','stop_url','tts_stop_name','platform_code','stop_timezone','wheelchair_boarding'], inplace=True)

    # drop SFO Airport rail/bus for now
    drop_transit_agency(gtfs_model, agency_id='SI')

    # filter out routes outside of Bay Area
    filter_transit_by_boundary(
      gtfs_model,
      COUNTY_SHAPEFILE, 
      partially_include_route_type_action={RouteType.RAIL:'truncate'})
    WranglerLogger.debug(f"gtfs_model:\n{gtfs_model}")

    # New stations which opened between 2015 and 2023
    ADD_STOP_IDS = {
      # BART to San Jose
      'WARM',  # Warm Springs / South Fremont (opened 2017)
      'MLPT',  # Milpitas (opened 2020)
      'BERY',  # Berryessa (opened 2020)
      # BART to Antioch
      'PCTR',  # Pittsburg Center (opened 2018)
      'ANTC',  # Antioch (opened 2018)
      # Muni Central Subway
      '17876', # Muni Chinatown - Rose Pak Station
      '17877', # Muni Union Square/Market St Station
      '17878', # Muni Yerba Buena/Moscone Station Northbound
      '17879', # Muni Fourth & Brannan Northbound
      '17872', # Muni Fourth & Brannan Southbound
      '17873', # Muni Yerba Buena/Moscone Station Southbound
      '17874', # Muni Union Square/Market St Station Southbound
      # Muni F Line
      '17283', # The Embarcadero & Ferry Building SB
      '14513', # The Embarcadero & Ferry Building NB
      # Muni J Line
      '17073', # Church & Market Inbound
      '15418', # Balboa Park BART/Mezzanine Level
      # Muni M Line
      '17164', # San Jose Ave & Geneva Ave (1 of 2)
      # Treasure Island Ferry
      'TF:1',  # Treasure Island Ferry Terminal
      # Richmond Ferry
      '7211',  # (service started 2019)
      # Vallejo Ferry Terminal
      '7212',  # (service started)
      # Mare Island Ferry Terminal
      '7213',  # (service started in 2017)
      # Alameda Seaplane Lagoon Ferry Terminal
      '7207',  # (service started 2021) 
      # Capitol Corridor
      'AM:FFV', # Fairfield-Vacaville Station (opened 2017)
      # SMART
      '71011',  # Larkspur
      '71021',  # San Rafael
      '71031',  # Marin Civic Center
      '71041',  # Novato Hamilton
      '71051',  # Novato Downtown
      '71061',  # Novato San Marin
      '71071',  # Petaluma Downtown
      '71091',  # Cotati
      '71101',  # Rohnert Park
      '71111',  # Santa Rosa Downtown
      '71121',  # Santa Rosa North
      '71131',  # Sonoma County Airport
      # VTA
      # this was open but it's not in the network
      '64747',  # San Antonio Station NB on the Green Line
      '65866',  # "Patrick Henry Pocket Track"
    }
    # create nodes for these stations
    new_station_nodes_gdf = create_nodes_for_new_stations(ADD_STOP_IDS, gtfs_model, nodes_gdf)

    # add them to road_nodes_gdf
    road_nodes_gdf = gpd.GeoDataFrame(pd.concat([
      road_nodes_gdf, 
      new_station_nodes_gdf.drop(columns=['stop_id'])], ignore_index=True))

    stop_id_to_model_node_id = new_station_nodes_gdf[['stop_id','model_node_id']].set_index('stop_id').to_dict()['model_node_id']
    WranglerLogger.debug(f"stop_id_to_model_node_id={stop_id_to_model_node_id}")

    stop_id_to_model_node_id['FRMT'] = 2625947  # BART Fremont
    stop_id_to_model_node_id['PITT'] = 3097273  # BART Pittsburg/Baypoint
    stop_id_to_model_node_id['SBRN'] = 1556366  # BART San Bruno
    stop_id_to_model_node_id['SFIA'] = 1556368  # BART SFO
    stop_id_to_model_node_id['MLBR'] = 1556367  # BART Millbrae
    stop_id_to_model_node_id['AM:SUI'] = 3547320  # Capitol Corridor Suisun-Fairfield
    stop_id_to_model_node_id['AM:DAV'] = 3547319  # Capitol Corridor Davis
    stop_id_to_model_node_id['17166'] = 1027771 # Fourth and King NB
    stop_id_to_model_node_id['17397'] = 1027891 # Fourth and King SB

    stop_id_to_model_node_id['14534'] = 1027749 # The Embarcadero & Washington St SB
    stop_id_to_model_node_id['14726'] = 1027750 # Don Chee Way/Steuart St WB
    stop_id_to_model_node_id['15682'] = 1027788 # Market St & Main St EB
    stop_id_to_model_node_id['14727'] = 1027790 # Don Chee Way/Steuart St EB
    stop_id_to_model_node_id['14532'] = 1027791 # The Embarcadero & Washington St NB
    stop_id_to_model_node_id['14006'] = 1028013 # Church St & Duboce Ave

    stop_id_to_model_node_id['13985'] = 1028012 # Church St & Market St
    stop_id_to_model_node_id['14004'] = 1027961 # Church St & Day St
    stop_id_to_model_node_id['13538'] = 1027963 # 30th St & Dolores St
    stop_id_to_model_node_id['17778'] = 1027897 # Balboa Park BART/Mezzanine Level to San Jose Ave & Geneva Ave

    stop_id_to_model_node_id['13385'] = 1027945 # 19th Ave & Randolph St NB
    stop_id_to_model_node_id['13361'] = 1027946 # 19th Ave & Junipero Serra Blvd NB
    stop_id_to_model_node_id['16262'] = 1027936 # San Jose Ave & Geneva Ave

    stop_id_to_model_node_id['72011'] = 1028039 # SF Ferry Terminal Gate E
    stop_id_to_model_node_id['72012'] = 1028039 # SF Ferry Terminal Gate G
    stop_id_to_model_node_id['72013'] = 1027623 # SF Ferry Terminal Gate F (combine with previous?)
    stop_id_to_model_node_id['7205']  = 1556391 # South San Francisco Ferry Terminal
    stop_id_to_model_node_id['7208']  = 2625971 # Main Street Alameda Ferry Terminal
    stop_id_to_model_node_id['7209']  = 2625970 # Oakland Ferry Terminal
    stop_id_to_model_node_id['TF:2']  = 1026197 # San Francisco Ferry Terminal for Treasure Island route
    stop_id_to_model_node_id['GF:43007'] = 5026530 # Tiburon Ferry Landing
    stop_id_to_model_node_id['GF:43002'] = 5026531 # Angel Island Ferry Landing

    stop_id_to_model_node_id['64806'] = 2192891 # Baypointe WB
    stop_id_to_model_node_id['64807'] = 2192908 # Champion WB
    stop_id_to_model_node_id['64800'] = 2192937 # Champion EB
    stop_id_to_model_node_id['64760'] = 2192855 # Baypointe EB

    # TODO: this is silly. Should just specify local sequence and then create these automatically...

    # Caltrain NB
    stop_id_to_model_node_id['70321'] = 2192813 # Gilroy NB 
    stop_id_to_model_node_id['70311'] = 2192812 # San Martin NB
    stop_id_to_model_node_id['70301'] = 2192811 # Morgan Hill NB
    stop_id_to_model_node_id['70291'] = 2192810 # Blossom Hill NB
    stop_id_to_model_node_id['70281'] = 2192809 # Capitol NB
    stop_id_to_model_node_id['70271'] = 2192808 # Tamien NB
    stop_id_to_model_node_id['70261'] = 2192815 # San Jose Diridon NB
    stop_id_to_model_node_id['70251'] = 2172876 # College Park Station NB
    stop_id_to_model_node_id['70231'] = 2192817 # Lawrence NB
    stop_id_to_model_node_id['70241'] = 2192816 # Santa Clara NB
    stop_id_to_model_node_id['70221'] = 2192818 # Sunnyvale NB
    stop_id_to_model_node_id['70211'] = 2192819 # Mountain View NB
    stop_id_to_model_node_id['70171'] = 2192822 # Palo Alto NB
    stop_id_to_model_node_id['70141'] = 1556381 # Redwood City NB
    stop_id_to_model_node_id['70121'] = 1556386 # Belmont NB
    stop_id_to_model_node_id['70111'] = 1556382 # Hillsdale NB
    stop_id_to_model_node_id['70131'] = 1556385 # San Carlos NB
    stop_id_to_model_node_id['70051'] = 1556390 # San Bruno NB
    stop_id_to_model_node_id['70091'] = 1556388 # San Mateo NB
    stop_id_to_model_node_id['70061'] = 1556383 # Millbrae NB
    stop_id_to_model_node_id['70041'] = 1556384 # South San Francisco NB
    stop_id_to_model_node_id['70021'] = 1027622 # 22nd Street NB
    stop_id_to_model_node_id['70011'] = 1027620 # San Francisco NB
    # Caltrain SB
    stop_id_to_model_node_id['70012'] = 1027617 # San Francisco SB
    stop_id_to_model_node_id['70022'] = 1027618 # 22nd Street SB
    stop_id_to_model_node_id['70042'] = 1556369 # South San Francisco SB
    stop_id_to_model_node_id['70052'] = 1556370 # San Bruno SB
    stop_id_to_model_node_id['70062'] = 1556371 # Millbrae SB
    stop_id_to_model_node_id['70092'] = 1556373 # San Mateo SB
    stop_id_to_model_node_id['70132'] = 1556377 # San Carlos SB
    stop_id_to_model_node_id['70112'] = 1556375 # Hillsdale SB
    stop_id_to_model_node_id['70122'] = 1556376 # Belmont
    stop_id_to_model_node_id['70142'] = 1556378 # Redwood City SB
    stop_id_to_model_node_id['70172'] = 2192799 # Palo Alto SB
    stop_id_to_model_node_id['70212'] = 2192802 # Mountain View SB
    stop_id_to_model_node_id['70222'] = 2192803 # Sunnyvale SB
    stop_id_to_model_node_id['70242'] = 2192805 # Santa Clara SB
    stop_id_to_model_node_id['70232'] = 2192804 # Lawrence SB
    stop_id_to_model_node_id['70262'] = 2192807 # San Jose SB
    # VTA
    stop_id_to_model_node_id['64746'] = 2192842 # Convention Center
    stop_id_to_model_node_id['64748'] = 2192843 # San Antonio to Santa Clara
    stop_id_to_model_node_id['64797'] = 2192934 # Old Ironsides
    stop_id_to_model_node_id['64810'] = 2192911 # Old Ironsides
    # Set the name in road_nodes_gdf to the stop_name for these nodes using dataframe joins
    WranglerLogger.info("Setting node names to stop names for mapped transit stops")

    # Create a dataframe from the stop_id to model_node_id mapping
    stop_node_mapping_df = pd.DataFrame(
      list(stop_id_to_model_node_id.items()), 
      columns=['stop_id', 'model_node_id']
    ).drop_duplicates(subset=['model_node_id'], keep='first') # don't create duplicate model_node_ids
    # Join with gtfs stops to get stop names
    stop_node_mapping_df = stop_node_mapping_df.merge(
      gtfs_model.stops[['stop_id', 'stop_name']], 
      on='stop_id', 
      how='left'
    )
    # Merge with the mapping to get stop names
    road_nodes_gdf = road_nodes_gdf.merge(
      stop_node_mapping_df[['model_node_id', 'stop_name']], 
      on='model_node_id', 
      how='left',
      indicator=True
    )
 #    WranglerLogger.debug(f"road_nodes_gdf.loc[road_nodes_gdf._merge=='both']:\n{road_nodes_gdf.loc[road_nodes_gdf._merge=='both']}")
    road_nodes_gdf.loc[ pd.isna(road_nodes_gdf['name']) & (road_nodes_gdf._merge == 'both'), 'name'] = road_nodes_gdf['stop_name']
    # CT:L5WranglerLogger.debug(f"road_nodes_gdf.loc[road_nodes_gdf._merge=='both']:\n{road_nodes_gdf.loc[road_nodes_gdf._merge=='both']}")
    # Drop temporary columns
    road_nodes_gdf.drop(columns=['stop_name','_merge'], inplace=True)

    # Define transit links to add between new stations
    # model_ids should either be mapped to model_node_ids 
    # Format: (from_stop_id, to_stop_id, oneway)
    TRANSIT_LINKS_TO_ADD = [
      # BART to San Jose
      ('FRMT', 'WARM', False),  # Fremont to Warm Springs
      ('WARM', 'MLPT', False),  # Warm Springs to Milpitas
      ('MLPT', 'BERY', False),  # Milpitas to Berryessa
      # BART - these links are missing for some reason
      ('SBRN', 'SFIA', True),   # San Bruno to SFO
      ('SFIA', 'MLBR', True),   # SFO to Millbrae
      # eBart extension
      ('PITT', 'PCTR', False),  # Pittsburg/Baypoint to Pittsburg Center
      ('PCTR', 'ANTC', False),  # Pittsburg Center to Antioch
      # Muni Central Subway Northbound
      ('17166', '17879', True), # Fourth and King St to Fouth and Brannan
      ('17879', '17878', True), # Fourth and Brannan to Yerba Buena/Moscone Station
      ('17878', '17877', True), # Yerba Buena/Moscone Station to Union Square/Market St Station
      ('17877', '17876', True), # Union Square/Market St Station to Chinatown - Rose Pak Station
      # Muni Central Subway Southbound
      ('17876', '17874', True), # Chinatown - Rose Pak Station to Union Square/Market St Station
      ('17874', '17873', True), # Union Square/Market St Station to Yerba Buena/Moscone Station
      ('17873', '17872', True), # Yerba Buena/Moscone Station to Fourth & Brannan
      ('17872', '17397', True), # Fourth & Brannan to Fourth and King
      # Muni F line
      ('14534', '17283', True), # The Embarcadero & Washington St to The Embarcadero & Ferry Building
      ('17283', '14726', True), # The Embarcadero & Ferry Building to Don Chee Way/Steuart St

      ('15682', '14727', True), # Market St & Main St to Don Chee Way/Steuart St
      ('14727', '14513', True), # Don Chee Way/Steuart St to The Embarcadero & Ferry Building
      ('14513', '14532', True), # The Embarcadero & Ferry Building to The Embarcadero & Washington St
      # Muni J line
      ('14004', '13538', True), # Church St & Day St to 30th and Dolores St
      ('15418', '17778', True), # Balboa Park BART/Mezzanine Level to San Jose Ave & Geneva Ave
      ('13985', '17073', True), # Church St & 16th St to Church St & Market St
      ('17073', '14006', True), # Church & Market St to Church St & Duboce Ave
      # Muni M line
      ('17164', '16262', True), # San Jose Ave & Geneva Ave to same
      # Treasure Island Ferry
      ('TF:1',  'TF:2',  False),
      # SF Ferry Terminal to Richmond Ferry 
      ('72011', '7211',  False),
      # SF Ferry Terminal to Oakland Ferry Terminal
      ('72012', '7209', True), # reverse already exists
       # SF Ferry Terminal to Tiburon Ferry
      ('TF:2',  'GF:43007', False),
      # SF Ferry Terminal to Vallejo to Mare Island
      ('72011', '7212',  False),
      ('7212',  '7213',  False),
      # Alameda Seaplane Lagoon to South San Francisco Ferry
      ('7207',  '7205',  False),
      # Alameda Seaplane Lagoon to San Francisco
      ('7207',  '72011', False),
      # Alameda Main Street Ferry Terminal to South San Francisco Ferry Terminal
      ('7208',  '7205',  True), # reverse link already exists
      # Alameda Main Street Ferry Terminal to San Francisco
      ('7208', '72013', False),
      # Angel Island to San Francisco
      ('GF:43002', '72011', False),
      # Angel Island to Tiburon
      ('GF:43002', 'GF:43007', False),
      # Capitol Corridor
      ('AM:SUI','AM:FFV', False), # Suisun-Fairfield to Fairfield-Vacaville
      ('AM:FFV','AM:DAV', False), # Fairfield-Vacaville to Davis
      # SMART
      ('71011','71021', False), # Larkspur to San Rafael
      ('71021','71031', False), # San Rafael to Marin Civic Center
      ('71031','71041', False), # Marin Civic Center to Novato Hamilton
      ('71041','71051', False), # Novato Hamilton to Novato Downtown
      ('71051','71061', False), # Novato Downtown to Novato San Marin
      ('71061','71071', False), # Novato San Marin to Petaluma Downtown
      ('71071','71091', False), # Petaluma Downtown to Cotati
      ('71091','71101', False), # Cotati to Rohnert Park
      ('71101','71111', False), # Rohnert Park to Santa Rosa Downtown
      ('71111','71121', False), # Santa Rosa Downtown to Santa Rosa North
      ('71121','71131', False), # Santa Rosa North to Sonoma County Airport
      # VTA Orange Line WB
      ('64806', '64807', True), # Baypointe to Champion WB
      # VTA Orange Line EB
      ('64800', '64760', True), # Champion to Baypointe EB

      # Caltrain limited links - Northbound
      ('70321','70311', True), # Gilroy to San Martin NB
      ('70311','70301', True), # San Martin to Morgan Hill NB
      ('70301','70291', True), # Morgan Hill to Blossom Hill NB
      ('70291','70281', True), # Blossom Hill to Capitol NB
      ('70281','70271', True), # Capitol to Tamien NB
      ('70271','70261', True), # Tamien to San Jose Diridon
      ('70261','70251', True), # San Jose Diridon to College Park NB
      ('70251','70241', True), # College Park to Santa Clara NB
      ('70261','70231', True), # San Jose Diridon to Lawrence NB
      ('70261','70211', True), # San Jose Diridon to Mountain View NB
      ('70241','70221', True), # Santa Clara to Sunnyvale NB
      ('70211','70171', True), # Mountain View to Palo Alto NB
      ('70171','70141', True), # Palo Alto to Redwood City NB
      ('70141','70121', True), # Redwood City to Belmont NB
      ('70111','70091', True), # Hillsdale to San Mateo NB
      ('70131','70091', True), # San Carlos to San Mateo NP
      ('70091','70061', True), # San Mateo NB to Millbrae NB
      ('70051','70021', True), # San Bruno to 22nd Street NB
      ('70041','70021', True), # South San Francisco to 22nd Street NB
      ('70061','70021', True), # Millbrae to 22nd Street NB
      ('70061','70011', True), # Millbrae to San Francisco NB
      # Caltrain limited links - Southbound
      ('70012','70042', True), # San Francisco to South San Francisco SB
      ('70042','70062', True), # South San Francisco to Millbrae SB
      ('70022','70052', True), # 22nd Street to San Bruno SB
      ('70022','70062', True), # 22nd Street to Millbrae SB
      ('70062','70092', True), # Millbrae to San Mateo SB
      ('70062','70112', True), # Millbrae to Hillsdale SB
      ('70122','70142', True), # Belmont to Redwood City SB
      ('70112','70142', True), # Hillsdale to Redwood City SB
      ('70092','70132', True), # San Mateo to San Carlos SB
      ('70142','70172', True), # Redwood City to Palo Alto SB
      ('70172','70212', True), # Palo Alto to Mountain View SB
      ('70222','70242', True), # Sunnyvale to Santa Clara SB
      ('70212','70262', True), # Mountain View to San Jose SB
      ('70232','70262', True), # Lawrence to San Jose SB

      # VTA links Green Line
      ('64746', '64747', True), # Convention Center to San Antonio
      ('64747', '64748', True), # San Antonio to Santa Clara
      ('65866', '64797', True), # PATRICK HENRY POCKET TRACK to Old Ironsides
      ('64810', '65866', True), # Old Ironsides to PATRICK HENRY POCKET TRACK
    ]

    # Create transit links for the new stations
    # Note: Use road_nodes_gdf which contains the newly created station nodes
    new_transit_links_gdf = create_transit_links_for_new_stations(
      TRANSIT_LINKS_TO_ADD, 
      stop_id_to_model_node_id,
      road_nodes_gdf,
      road_links_gdf
    )

    # Add new links to road_links_gdf
    if len(new_transit_links_gdf) > 0:
      road_links_gdf = gpd.GeoDataFrame(pd.concat([road_links_gdf, new_transit_links_gdf], ignore_index=True))
      WranglerLogger.info(f"Added {len(new_transit_links_gdf)} new transit links to roadway network")

    # remove Suisun-Fairfield to Davis and vice versa since Fairfield-Vacavaville was added in between
    len_road_links_gdf = len(road_links_gdf)
    road_links_gdf = road_links_gdf.loc[ (road_links_gdf.A != stop_id_to_model_node_id['AM:SUI']) | ((road_links_gdf.B != stop_id_to_model_node_id['AM:DAV']))]
    road_links_gdf = road_links_gdf.loc[ (road_links_gdf.B != stop_id_to_model_node_id['AM:SUI']) | ((road_links_gdf.A != stop_id_to_model_node_id['AM:DAV']))]
    WranglerLogger.debug(f"{len_road_links_gdf=:,} {len(road_links_gdf)=:,}")
    assert(len(road_links_gdf) == len_road_links_gdf-2)

    # remove VTA Convention Center to Santa Clara since San Antonio was added in between
    len_road_links_gdf = len(road_links_gdf)
    road_links_gdf = road_links_gdf.loc[ (road_links_gdf.A != stop_id_to_model_node_id['64746']) | ((road_links_gdf.B != stop_id_to_model_node_id['64748']))]
    WranglerLogger.debug(f"{len_road_links_gdf=:,} {len(road_links_gdf)=:,}")
    assert(len(road_links_gdf) == len_road_links_gdf-1)

    # TODO: There are others to remove but maybe just do it programmatically :D

    # The Hillsdale Caltrain station moved in 2021
    HILLSDALE_STOP_ID = '70112'
    hillsdale_stop_dict = gtfs_model.stops.loc[gtfs_model.stops.stop_id==HILLSDALE_STOP_ID].to_dict(orient='records')[0]
    WranglerLogger.debug(f"Hillsdale stop:{hillsdale_stop_dict}")

    # Vasco Rt Amtrak Station seems to be located slightly incorrectly
    AMTRAK_VASCO_STOP_ID = 'CE:VAS'
    amtrak_vasco_stop_dict = gtfs_model.stops.loc[gtfs_model.stops.stop_id==AMTRAK_VASCO_STOP_ID].to_dict(orient='records')[0]
    WranglerLogger.debug(f"Amtrak Vasco stop:{amtrak_vasco_stop_dict}")

    # J Church St & Market St Station seems to be located incorrectly
    J_CHURCH_MARKET_STOP_ID = '18059'
    j_church_market_stop_dict = gtfs_model.stops.loc[gtfs_model.stops.stop_id==J_CHURCH_MARKET_STOP_ID].to_dict(orient='records')[0]

    # M 19th Ave & Randolf Street NB seems to located incorrectly
    M_19TH_RANDOLF_STOP_ID = '13385'
    m_19th_randolf_stop_dict = gtfs_model.stops.loc[gtfs_model.stops.stop_id==M_19TH_RANDOLF_STOP_ID].to_dict(orient='records')[0]

    # M San Jose and Geneva move
    M_SAN_JOSE_GENEVA_STOP_ID = '16262'
    m_san_jose_geneva_stop_id = gtfs_model.stops.loc[gtfs_model.stops.stop_id==M_SAN_JOSE_GENEVA_STOP_ID].to_dict(orient='records')[0]

    # Finally, truncate the gtfs_model SolTrans Route B because it includes one stop out of region
    truncate_route_at_stop(gtfs_model, route_id="ST:B", direction_id=0, stop_id='829201', truncate="before")
    truncate_route_at_stop(gtfs_model, route_id="ST:B", direction_id=1, stop_id='829201', truncate="after")

    # TODO: What is locationReferences?  Can we drop?  Convert to string for now
    road_links_gdf['locationReferences'] = road_links_gdf['locationReferences'].astype(str)

    fix_link_access(road_links_gdf, 'access')
    fix_link_access(road_links_gdf, 'ML_access')

    # Fix numeric columns before creating the roadway network
    WranglerLogger.debug("Fixing numeric columns with empty strings...")
    fix_numeric_columns(road_links_gdf)

    # Fix columns with mixed types
    fix_mixed_type_columns(road_links_gdf)

    # create roadway network
    roadway_network =  network_wrangler.load_roadway_from_dataframes(
      links_df=road_links_gdf,
      nodes_df=road_nodes_gdf,
      shapes_df=road_links_gdf
    )
    WranglerLogger.debug(f"roadway_net:\n{roadway_network}")
    WranglerLogger.info(f"RoadwayNetwork created with {len(roadway_network.nodes_df):,} nodes and {len(roadway_network.links_df):,} links.")

    # Split Geyserville Avenue link to add transit stop 
    geyserville_stop_node_id = generate_node_ids(road_nodes_gdf, range(4_500_000 + 1, 5_000_000), n=1)[0]
    WranglerLogger.debug(f"Adding node for {geyserville_stop_node_id=}")
    # Get the A and B nodes for the Geyserville Avenue link
    roadway_network.split_link(
      A=4514838, 
      B=4540403,
      new_model_node_id=geyserville_stop_node_id,
      fraction=0.3,
      split_reverse_link=True
    )
    # Split VTA Blue Line link to add Santa Clara NB station
    vta_santa_clara_nb_stop_node_id = generate_node_ids(road_nodes_gdf, range(2_000_000 + 1, 2_500_000), n=1)[0]
    WranglerLogger.debug(f"Adding node for {vta_santa_clara_nb_stop_node_id=}")
    # Get the A and B nodes for the Geyserville Avenue link
    roadway_network.split_link(
      A=2192868, 
      B=2192869,
      new_model_node_id=vta_santa_clara_nb_stop_node_id,
      fraction=0.5,
      split_reverse_link=False
    )

    move_transit_nodes_df = pd.DataFrame([
      # update Hillsdale coordinates
      {'model_node_id':1556382, 'X':hillsdale_stop_dict['stop_lon'], 'Y':hillsdale_stop_dict['stop_lat']},
      {'model_node_id':1556375, 'X':hillsdale_stop_dict['stop_lon'], 'Y':hillsdale_stop_dict['stop_lat']},
      # update Amtrak Vasco coordinates
      {'model_node_id':2625973, 'X':amtrak_vasco_stop_dict['stop_lon'], 'Y':amtrak_vasco_stop_dict['stop_lat']},
      # update J Church and Market St coordinates
      {'model_node_id':1027951, 'X':j_church_market_stop_dict['stop_lon'], 'Y':j_church_market_stop_dict['stop_lat']},
      # update M 19th and Randolf coordinates
      {'model_node_id':1027945, 'X':m_19th_randolf_stop_dict['stop_lon'], 'Y':m_19th_randolf_stop_dict['stop_lat']},
      # Update M San Jose and Geneva coordinates
      {'model_node_id':1027936, 'X':m_san_jose_geneva_stop_id['stop_lon'], 'Y':m_san_jose_geneva_stop_id['stop_lat']},
    ])
    WranglerLogger.debug(f"move_transit_nodes_df:\n{move_transit_nodes_df}")
    # check if any model_node_ids are missing
    WranglerLogger.debug(f"roadway_network.nodes_df.tail():\n{roadway_network.nodes_df.tail()}")
    WranglerLogger.debug(f"roadway_network.nodes_df.loc[roadway_network.nodes_df['model_node_id'].isna()]:\n{roadway_network.nodes_df.loc[ roadway_network.nodes_df['model_node_id'].isna()]}")
    # use RoadwayNetwork.move_nodes()
    roadway_network.move_nodes(move_transit_nodes_df)

    # Check for any columns with lists after project application and convert them all to strings
    WranglerLogger.debug("Checking for list columns after project application...")
    for col in roadway_network.links_df.columns:
      if roadway_network.links_df[col].dtype == 'object':
        # Check entire column, not just sample
        has_lists = False
        for val in roadway_network.links_df[col]:
          if isinstance(val, list):
            has_lists = True
            break
          
        if has_lists:
          WranglerLogger.debug(f"  Column {col} still contains lists - converting all values to string")
          # Convert each value individually to handle lists properly
          roadway_network.links_df[col] = roadway_network.links_df[col].apply(
            lambda x: str(x) if isinstance(x, list) else x
          )
          # Now ensure all values are strings
          roadway_network.links_df[col] = roadway_network.links_df[col].fillna('').astype(str)

    # Write the 2023 roadway network to parquet files
    WranglerLogger.info("Writing 2023 roadway network to parquet files...")

    try:
      roadway_network_dir.mkdir(exist_ok=True)
      write_roadway(
        roadway_network, 
        out_dir=roadway_network_dir,
        prefix="road_net_2023",
        file_format="parquet",
        overwrite=True,
        true_shape=True
      )
      WranglerLogger.info(f"Roadway network saved to {roadway_network_dir}")
    except Exception as e:
      WranglerLogger.error(f"Error writing roadway network: {e}")
      raise
    
    WranglerLogger.info("Finished writing 2023 roadway network files")

    tableau_utils.write_geodataframe_as_tableau_hyper(
      roadway_network.links_df,  # drop distance==0 links because otherwise this will error
      (OUTPUT_DIR / "mtc_links.hyper").resolve(),
      "mtc_links"
    )
    tableau_utils.write_geodataframe_as_tableau_hyper(
      roadway_network.nodes_df,
      (OUTPUT_DIR / "mtc_nodes.hyper").resolve(),
      "mtc_nodes"
    )

    # write the gtfs version of the transit network now, before converting to Feed
    WranglerLogger.info(f"Writing gtfs_model to {transit_network_dir}")
    transit_gtfs_model_dir.mkdir(exist_ok=True)
    write_transit(gtfs_model, out_dir=transit_gtfs_model_dir, file_format="txt", overwrite=True)
  
  # Define time periods for frequency calculation: 3a-6a, 6a-10a, 10a-3p, 3p-7p, 7p-3a
  time_periods = [
    {'start_time': '03:00:00', 'end_time': '06:00:00'},  # 3a-6a
    {'start_time': '06:00:00', 'end_time': '10:00:00'},  # 6a-10a
    {'start_time': '10:00:00', 'end_time': '15:00:00'},  # 10a-3p
    {'start_time': '15:00:00', 'end_time': '19:00:00'},  # 3p-7p
    {'start_time': '19:00:00', 'end_time': '03:00:00'},  # 7p-3a (crosses midnight)
  ]
  
  # Create feed with default 3-hour headway for routes with only one trip in a period
  try:
    feed = create_feed_from_gtfs_model(
      gtfs_model,
      roadway_network,
      local_crs="EPSG:2227", # https://epsg.io/2227
      crs_units="feet",
      time_periods=time_periods,
      default_frequency_for_onetime_route=10800,
      skip_stop_agencies = 'CT'
    )
    WranglerLogger.debug(f"Created feed from gtfs_model: {feed}")
  except NodeNotFoundError as e:
    # catch NodeNotFoundError and write out unmached stops to tableau for investigation
    WranglerLogger.error(f"Caught NodeNotFoundError: Failed to match some GTFS stops to roadway network nodes")
    WranglerLogger.error(str(e))
    
    # Write unmatched stops to Tableau for investigation if available
    if hasattr(e, 'unmatched_stops_gdf') and len(e.unmatched_stops_gdf) > 0:
      unmatched_stops_file = (OUTPUT_DIR / "unmatched_gtfs_stops.hyper").resolve()
      WranglerLogger.info(f"Writing {len(e.unmatched_stops_gdf)} unmatched stops to {unmatched_stops_file}")
      
      # Prepare the unmatched stops data for Tableau - include ALL fields
      WranglerLogger.debug(f"e.unmatched_stops_gdf type={type(e.unmatched_stops_gdf)} rows=\n{e.unmatched_stops_gdf}")
      
      # Rename lat/lon to X/Y for the tableau utility if they exist
      if 'stop_lon' in e.unmatched_stops_gdf.columns and 'stop_lat' in e.unmatched_stops_gdf.columns:
        e.unmatched_stops_gdf.rename(columns={'stop_lon': 'X', 'stop_lat': 'Y'}, inplace=True)
      
      # Write to Tableau
      tableau_utils.write_geodataframe_as_tableau_hyper(e.unmatched_stops_gdf, unmatched_stops_file, "unmatched_stops")
      
      WranglerLogger.error(f"Unmatched stops written to {unmatched_stops_file}")
    
    # Re-raise the exception to stop processing
    raise
  except TransitValidationError as e:
    # catch TransitValidationError and write out unmached stops to tableau for investigation
    WranglerLogger.error(f"Caught TransitValidationError: {e}")
    WranglerLogger.error(f"{e.exception_source}")

    if hasattr(e, 'failed_connectivity_sequences'):
      WranglerLogger.error(f"failed_connectivity_sequences (pprinted):\n{pprint.pformat(e.failed_connectivity_sequences)}")

    if hasattr(e, 'shape_links_gdf') and len(e.shape_links_gdf) > 0:
      shape_links_file = (OUTPUT_DIR / "shape_links.hyper").resolve()
      WranglerLogger.info(f"Writing {len(e.shape_links_gdf)} shape links to {shape_links_file}")

      WranglerLogger.debug(f"e.shape_links_gdf.head():\n{e.shape_links_gdf.head()}")

      # Write to Tableau
      tableau_utils.write_geodataframe_as_tableau_hyper(e.shape_links_gdf, shape_links_file, "shape_link")
      
      WranglerLogger.error(f"Shape links written to {shape_links_file}")


    # Re-raise the exception to stop processing
    raise
  
  # Create TransitNetwork from the Feed and validate it
  WranglerLogger.info("Creating TransitNetwork from Feed")
  transit_network = TransitNetwork(feed=feed)
  WranglerLogger.info(f"TransitNetwork created with {len(transit_network.feed.stops)} stops and {len(transit_network.feed.routes)} routes")

  # Save the transit network regardless of validation issues
  WranglerLogger.info("Saving TransitNetwork to files")

  try:
    transit_network_dir.mkdir(exist_ok=True)
    write_transit(transit_network, out_dir=transit_network_dir, file_format="csv")
    WranglerLogger.info(f"Transit network saved to {transit_network_dir}")
  except Exception as e:
    WranglerLogger.error(f"Failed to save transit network: {e}")

  # debugging: check if any stops.stop_id or shapes.shape_model_node_ids are not in the roadway network
  stops_roadway_gdf = pd.merge(
    left=feed.stops,
    right=roadway_network.nodes_df,
    how='outer',
    left_on='stop_id',
    right_on='model_node_id',
    indicator=True,
    suffixes=('','_road')
  )
  stops_roadway_gdf = gpd.GeoDataFrame(stops_roadway_gdf)
  WranglerLogger.debug(f"type(stops_roadway_gdf):\n{type(stops_roadway_gdf)}")
  WranglerLogger.debug(f"stops_roadway_gdf._merge.value_counts():\n{stops_roadway_gdf._merge.value_counts()}")
  WranglerLogger.debug(f"stops_roadway_gdf.loc[ stops_roadway_gdf._merge == 'left_only']:\n{stops_roadway_gdf.loc[ stops_roadway_gdf._merge == 'left_only']}")
  WranglerLogger.debug(f"stops_roadway_gdf.loc[ stops_roadway_gdf._merge == 'both']:\n{stops_roadway_gdf.loc[ stops_roadway_gdf._merge == 'both']}")
  # write this as a hyper
  tableau_utils.write_geodataframe_as_tableau_hyper(
    stops_roadway_gdf.loc[ stops_roadway_gdf._merge == 'both'],
    (OUTPUT_DIR / "stops_roadway.hyper").resolve(),
    "stops_roadway"
  )
  shapes_roadway_gdf = pd.merge(
    left=feed.shapes,
    right=roadway_network.nodes_df,
    how='outer',
    left_on='shape_model_node_id',
    right_on='model_node_id',
    indicator=True,
    suffixes=('','_road')
  )
  shapes_roadway_gdf = gpd.GeoDataFrame(shapes_roadway_gdf)
  WranglerLogger.debug(f"shapes_roadway_gdf._merge.value_counts():\n{shapes_roadway_gdf._merge.value_counts()}")
  WranglerLogger.debug(f"shapes_roadway_gdf.loc[ stops_roadway_gdf._merge == 'left_only']:\n{shapes_roadway_gdf.loc[ shapes_roadway_gdf._merge == 'left_only']}")
  WranglerLogger.debug(f"shapes_roadway_gdf.loc[ stops_roadway_gdf._merge == 'both']:\n{shapes_roadway_gdf.loc[ shapes_roadway_gdf._merge == 'both']}")
  # write this as a hyper
  tableau_utils.write_geodataframe_as_tableau_hyper(
    shapes_roadway_gdf.loc[ shapes_roadway_gdf._merge == 'both'],
    (OUTPUT_DIR / "shapes_roadway.hyper").resolve(),
    "shapes_roadway"
  )

  # This is done by setting the road_net to roadway_network but we'll call this explicitly so
  # we can write out more useful debug data
  missing_shape_links = network_wrangler.transit.validate.shape_links_without_road_links(feed.shapes, roadway_network.links_df)
  WranglerLogger.debug(f"missing_shape_links:\n{missing_shape_links}")

  if len(missing_shape_links) > 0:
    # Write out missing_shape_links as tableau hyper
    WranglerLogger.warning(f"Found {len(missing_shape_links):,} missing shape links - writing to Tableau Hyper file")
    # check for A==B
    WranglerLogger.warning(f"Found the following missing_shape_links with A==B:\n" + \
                           f"{missing_shape_links.loc[ missing_shape_links.A==missing_shape_links.B]}")
    missing_shape_links = missing_shape_links.loc[ missing_shape_links.A != missing_shape_links.B]
    WranglerLogger.warning(f"Filtered these out; have {len(missing_shape_links):,} remaining")

    # Create LineString geometry from shape point coordinates
    
    def create_line_from_shape_pts(row):
      # Use shape_pt_lat/lon columns directly
      point_a = shapely.geometry.Point(row['shape_pt_lon_A'], row['shape_pt_lat_A'])
      point_b = shapely.geometry.Point(row['shape_pt_lon_B'], row['shape_pt_lat_B'])
      return shapely.geometry.LineString([point_a, point_b])
    
    missing_shape_links['geometry'] = missing_shape_links.apply(create_line_from_shape_pts, axis=1)
    
    # Convert to GeoDataFrame if there are valid geometries
    if len(missing_shape_links) > 0:
        missing_shape_links_gdf = gpd.GeoDataFrame(
            missing_shape_links,
            geometry='geometry',
            crs='EPSG:4326'  # lat/lon coordinates are in WGS84
        )
        
        # Write to Tableau Hyper
        tableau_utils.write_geodataframe_as_tableau_hyper(
            missing_shape_links_gdf,
            (OUTPUT_DIR / "missing_shape_links.hyper").resolve(),
            "missing_shape_links"
        )
        WranglerLogger.info(f"Wrote {len(missing_shape_links_gdf)} missing shape links to Tableau Hyper file")
    else:
        WranglerLogger.warning("No valid missing shape links to write (all had invalid coordinates)")

  missing_nodes = network_wrangler.transit.validate.transit_nodes_without_road_nodes(feed, roadway_network.nodes_df)
  WranglerLogger.debug(f"missing_nodes:\n{missing_nodes}")

  # Set the roadway network - wrap in try/catch since this can fail
  try:
    transit_network.road_net = roadway_network
    WranglerLogger.info("Successfully associated roadway network with transit network")
  except Exception as e:
    WranglerLogger.warning(f"Could not associate roadway network: {e}")
    WranglerLogger.warning("Continuing without roadway network association")

  
  try:
    write_transit(transit_network, out_dir=transit_network_dir, file_format="csv")
    WranglerLogger.info(f"Transit network saved to {transit_network_dir}")
  except Exception as e:
    WranglerLogger.error(f"Failed to save transit network: {e}")
  
  # Log summary statistics
  WranglerLogger.info("=== Transit Network Summary ===")
  WranglerLogger.info(f"Routes: {len(transit_network.feed.routes)}")
  WranglerLogger.info(f"Stops: {len(transit_network.feed.stops)}")
  WranglerLogger.info(f"Trips: {len(transit_network.feed.trips)}")
  WranglerLogger.info(f"Stop Times: {len(transit_network.feed.stop_times)}")
  WranglerLogger.info(f"Shapes: {len(transit_network.feed.shapes)}")
  WranglerLogger.info(f"Frequencies: {len(transit_network.feed.frequencies)}")
  WranglerLogger.info("===============================")

