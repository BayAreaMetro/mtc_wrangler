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
import typing
import pandas as pd
import geopandas as gpd
import shapely.geometry

import tableau_utils
import network_wrangler
from network_wrangler import WranglerLogger
from network_wrangler import write_transit
from network_wrangler.transit.network import TransitNetwork
from network_wrangler.utils.transit import \
  drop_transit_agency, filter_transit_by_boundary, create_feed_from_gtfs_model, truncate_route_at_stop
from network_wrangler.errors import NodeNotFoundError
from network_wrangler.roadway.nodes.create import generate_node_ids

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
      
  Returns:
      GeoDataFrame of new transit links with appropriate attributes
  """
  WranglerLogger.debug("=== create_transit_links_for_new_stations() ====")

  # Get the maximum model_link_id from existing links
  max_link_id = existing_links_gdf['model_link_id'].max()
  next_link_id = max_link_id + 1
  
  link_dicts = []
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
    next_link_id += 1

  new_links_gdf = gpd.GeoDataFrame(data=link_dicts, crs=node_gdf.crs)    
  WranglerLogger.debug(f"new_links_gdf:\n{new_links_gdf}")
  return new_links_gdf

if __name__ == "__main__":
  pd.options.display.max_columns = None
  pd.options.display.width = None
  pd.options.display.min_rows = 50

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
  service_ids = calendar_dates_df[['service_id']].drop_duplicates().reset_index(drop=True)
  WranglerLogger.debug(f"After filtering service_ids (len={len(service_ids):,}):\n{service_ids}")

  # Read a GTFS network (not wrangler_flavored)
  gtfs_model = network_wrangler.transit.io.load_feed_from_path(INPUT_2023GTFS, wrangler_flavored=False, service_ids_filter=service_ids)
  WranglerLogger.debug(f"gtfs_model:\n{gtfs_model}")
  # drop some columns that are not required or useful
  gtfs_model.stops.drop(columns=['stop_code','stop_desc','stop_url','tts_stop_name','platform_code','stop_timezone','wheelchair_boarding'])

  WranglerLogger.debug(f"gtfs_model.stops.loc[ gtfs_model.stops['stop_id'] == 'mtc:powell']\n{gtfs_model.stops.loc[ gtfs_model.stops['stop_id'] == 'mtc:powell']}")
  # drop SFO Airport rail/bus
  drop_transit_agency(gtfs_model, agency_id='SI')

  # filter out routes outside of Bay Area
  filter_transit_by_boundary(
    gtfs_model,
    COUNTY_SHAPEFILE, 
    partially_include_route_type_action={2:'truncate'})
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
    # Treasure Island Ferry
    'TF:1',  # Treasure Island Ferry Terminal
    'TF:2',  # San Francisco Ferry Terminal for Treasure Island route
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
  stop_id_to_model_node_id['72011'] = 1028039 # SF Ferry Terminal
  stop_id_to_model_node_id['7205']  = 1556391 # South San Francisco Ferry Terminal

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
    # Treasure Island Ferry
    ('TF:1', 'TF:2', False),
    # SF Ferry Terminal to Richmond Ferry 
    ('72011', '7211', False),
    # SF Ferry Terminal to Vallejo to Mare Island
    ('72011', '7212', False),
    ('7212',  '7213', False),
    # Alameda Seaplane Lagoon to South San Francisco Ferry
    ('7207',  '7205', False),
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

  # The Hillsdale Caltrain station moved in 2021
  HILLSDALE_STOP_ID = '70112'
  hillsdale_stop_dict = gtfs_model.stops.loc[gtfs_model.stops.stop_id==HILLSDALE_STOP_ID].to_dict(orient='records')[0]
  WranglerLogger.debug(f"Hillsdale stop:{hillsdale_stop_dict}")

  # Vasco Rt Amtrak Station seems to be located slightly incorrectly
  AMTRAK_VASCO_STOP_ID = 'CE:VAS'
  amtrak_vasco_stop_dict = gtfs_model.stops.loc[gtfs_model.stops.stop_id==AMTRAK_VASCO_STOP_ID].to_dict(orient='records')[0]
  WranglerLogger.debug(f"Amtrak Vasco stop:{amtrak_vasco_stop_dict}")

  # Finally, truncate the gtfs_model SolTrans Route B because it includes one stop out of region
  truncate_route_at_stop(gtfs_model, route_id="ST:B", direction_id=0, stop_id='829201', truncate="before")
  truncate_route_at_stop(gtfs_model, route_id="ST:B", direction_id=1, stop_id='829201', truncate="after")

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

  move_transit_nodes_df = pd.DataFrame([
    # update Hillsdale coordinates
    {'model_node_id':1556382, 'X':hillsdale_stop_dict['stop_lon'], 'Y':hillsdale_stop_dict['stop_lat']},
    {'model_node_id':1556375, 'X':hillsdale_stop_dict['stop_lon'], 'Y':hillsdale_stop_dict['stop_lat']},
    # update Amtrak Vasco coordinates
    {'model_node_id':2625973, 'X':amtrak_vasco_stop_dict['stop_lon'], 'Y':amtrak_vasco_stop_dict['stop_lat']}  
  ])
  WranglerLogger.debug(f"move_transit_nodes_df:\n{move_transit_nodes_df}")
  # check if any model_node_ids are missing
  WranglerLogger.debug(f"roadway_network.nodes_df.tail():\n{roadway_network.nodes_df.tail()}")
  WranglerLogger.debug(f"roadway_network.nodes_df.loc[roadway_network.nodes_df['model_node_id'].isna()]:\n{roadway_network.nodes_df.loc[ roadway_network.nodes_df['model_node_id'].isna()]}")
  # use RoadwayNetwork.move_nodes()
  roadway_network.move_nodes(move_transit_nodes_df)

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
      time_periods, 
      default_frequency_for_onetime_route=10800
    )
    WranglerLogger.debug(f"Created feed from gtfs_model: {feed}")
  except NodeNotFoundError as e:
    # catch NodeNotFoundError and write out unmached stops to tableau for investigation
    WranglerLogger.error(f"Failed to match some GTFS stops to roadway network nodes:")
    WranglerLogger.error(str(e))
    
    # Write unmatched stops to Tableau for investigation if available
    if hasattr(e, 'unmatched_stops_gdf') and len(e.unmatched_stops_gdf) > 0:
      unmatched_stops_file = (OUTPUT_DIR / "unmatched_gtfs_stops.hyper").resolve()
      WranglerLogger.info(f"Writing {len(e.unmatched_stops_gdf)} unmatched stops to {unmatched_stops_file}")
      
      # Prepare the unmatched stops data for Tableau - include ALL fields
      unmatched_stops_gdf = e.unmatched_stops_gdf.copy()
      WranglerLogger.debug(f"unmatched_stops_gdf type={type(unmatched_stops_gdf)} rows=\n{unmatched_stops_gdf}")
      
      # Convert any list columns to strings for Tableau
      for col in unmatched_stops_gdf.columns:
        if unmatched_stops_gdf[col].dtype == 'object':
          # Check if any values are lists
          if any(isinstance(val, list) for val in unmatched_stops_gdf[col].dropna()):
            unmatched_stops_gdf[col] = unmatched_stops_gdf[col].apply(
              lambda x: ', '.join(map(str, x)) if isinstance(x, list) else str(x) if pd.notna(x) else ''
            )
      
      # Rename lat/lon to X/Y for the tableau utility if they exist
      if 'stop_lon' in unmatched_stops_gdf.columns and 'stop_lat' in unmatched_stops_gdf.columns:
        unmatched_stops_output = unmatched_stops_gdf.rename(columns={'stop_lon': 'X', 'stop_lat': 'Y'})
      
      # Write to Tableau
      from tableau_utils import write_geodataframe_as_tableau_hyper
      write_geodataframe_as_tableau_hyper(unmatched_stops_output, unmatched_stops_file, "unmatched_stops")
      
      WranglerLogger.error(f"Unmatched stops written to {unmatched_stops_file}")
    
    # Re-raise the exception to stop processing
    raise
  
  # Create TransitNetwork from the Feed and validate it
  WranglerLogger.info("Creating TransitNetwork from Feed")
  transit_network = TransitNetwork(feed=feed)
  WranglerLogger.info(f"TransitNetwork created with {len(transit_network.feed.stops)} stops and {len(transit_network.feed.routes)} routes")

  # Save the transit network regardless of validation issues
  WranglerLogger.info("Saving TransitNetwork to files")
  transit_output_dir = OUTPUT_DIR / "transit_network"
  transit_output_dir.mkdir(exist_ok=True)
  try:
    write_transit(transit_network, out_dir=transit_output_dir, file_format="csv")
    WranglerLogger.info(f"Transit network saved to {transit_output_dir}")
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
    write_transit(transit_network, out_dir=transit_output_dir, file_format="csv")
    WranglerLogger.info(f"Transit network saved to {transit_output_dir}")
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

