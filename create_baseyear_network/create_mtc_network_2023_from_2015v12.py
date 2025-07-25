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
import pandas as pd
import geopandas as gpd
import shapely.geometry

import tableau_utils
import network_wrangler
from network_wrangler import WranglerLogger

INPUT_2015v12 = pathlib.Path("E:\\Box\\Modeling and Surveys\\Development\\Travel Model Two Conversion\\Model Inputs\\2015-tm22-dev-sprint-03\standard_network_after_project_cards")
INPUT_2023GTFS = pathlib.Path("M:\\Data\\Transit\\511\\2023-10")
OUTPUT_DIR = pathlib.Path("M:\\Development\\Travel Model Two\\Supply\\Network Creation 2025\\from_2015v12")
USERNAME = getpass.getuser()
if USERNAME=="lmz":
  INPUT_2015v12 = pathlib.Path("../../standard_network_after_project_cards")
  INPUT_2023GTFS = pathlib.Path("../../511gtfs_2023-10")
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

if __name__ == "__main__":
  pd.options.display.max_columns = None
  pd.options.display.width = None

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
  # For the rows that do not have geometry, create a simple two-point line geometry from the node locations
  # Use all nodes, not just road nodes
  WranglerLogger.debug(f"After merging with shapes_gdf, road_links_df[['_merge']].value_counts():\n{road_links_df[['_merge']].value_counts()}")
  road_links_df.drop(columns=['_merge'], inplace=True)

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
  
  # create roadway network
  roadway_network =  network_wrangler.load_roadway_from_dataframes(
    links_df=road_links_gdf,
    nodes_df=road_nodes_gdf,
    shapes_df=road_links_gdf
  )
  WranglerLogger.debug(f"roadway_net:\n{roadway_network}")
  WranglerLogger.info(f"RoadwayNetwork created with {len(roadway_network.nodes_df):,} nodes and {len(roadway_network.links_df):,} links.")

  tableau_utils.write_geodataframe_as_tableau_hyper(
    road_links_gdf.loc[ road_links_gdf['distance'] > 0],  # drop distance==0 links because otherwise this will error
    (OUTPUT_DIR / "mtc_links.hyper").resolve(),
    "mtc_links"
  )
  tableau_utils.write_geodataframe_as_tableau_hyper(
    road_nodes_gdf,
    (OUTPUT_DIR / "mtc_nodes.hyper").resolve(),
    "mtc_nodes"
  )
  # the gtfs feed covers the month of October 2023; select to Wednesday, October 11, 2023
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


  from network_wrangler.utils.transit import create_feed_from_gtfs_model
  
  # Define time periods for frequency calculation: 3a-6a, 6a-10a, 10a-3p, 3p-7p, 7p-3a
  time_periods = [
    {'start_time': '03:00:00', 'end_time': '06:00:00'},  # 3a-6a
    {'start_time': '06:00:00', 'end_time': '10:00:00'},  # 6a-10a
    {'start_time': '10:00:00', 'end_time': '15:00:00'},  # 10a-3p
    {'start_time': '15:00:00', 'end_time': '19:00:00'},  # 3p-7p
    {'start_time': '19:00:00', 'end_time': '03:00:00'},  # 7p-3a (crosses midnight)
  ]
  
  # Create feed with default 3-hour headway for routes with only one trip in a period
  feed = create_feed_from_gtfs_model(gtfs_model, roadway_network, time_periods, default_frequency_for_onetime_route=10800)
  
  # Create TransitNetwork from the Feed and validate it
  WranglerLogger.info("Creating TransitNetwork from Feed")
  from network_wrangler.transit.network import TransitNetwork
  
  transit_network = TransitNetwork(feed=feed)
  WranglerLogger.info(f"TransitNetwork created with {len(transit_network.feed.stops)} stops and {len(transit_network.feed.routes)} routes")
  
  # debugging: check if any stops.stop_id or shapes.shape_mode_node_ids are not in the roadway network
  stops_roadway_gdf = pd.merge(
    left=feed.stops,
    right=roadway_network.nodes_df,
    how='outer',
    left_on='stop_id',
    right_on='model_node_id',
    indicator=True,
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
    indicator=True
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

  # This is dont by setting the road_net to roadway_network but we'll call this explicitly so
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
  
  # Save the transit network regardless of validation issues
  WranglerLogger.info("Saving TransitNetwork to files")
  transit_output_dir = OUTPUT_DIR / "transit_network"
  transit_output_dir.mkdir(exist_ok=True)
  
  try:
    # Write transit network tables
    transit_network.write(transit_output_dir, file_format="csv")
    WranglerLogger.info(f"Transit network saved to {transit_output_dir}")
  except Exception as e:
    WranglerLogger.error(f"Failed to save transit network: {e}")
    # Try to save the feed directly
    WranglerLogger.info("Attempting to save Feed tables directly...")
    try:
      feed.write(transit_output_dir, file_format="csv")
      WranglerLogger.info(f"Feed tables saved to {transit_output_dir}")
    except Exception as e2:
      WranglerLogger.error(f"Failed to save feed tables: {e2}")
  
  # Log summary statistics
  WranglerLogger.info("=== Transit Network Summary ===")
  WranglerLogger.info(f"Routes: {len(transit_network.feed.routes)}")
  WranglerLogger.info(f"Stops: {len(transit_network.feed.stops)}")
  WranglerLogger.info(f"Trips: {len(transit_network.feed.trips)}")
  WranglerLogger.info(f"Stop Times: {len(transit_network.feed.stop_times)}")
  WranglerLogger.info(f"Shapes: {len(transit_network.feed.shapes)}")
  WranglerLogger.info(f"Frequencies: {len(transit_network.feed.frequencies)}")
  WranglerLogger.info("===============================")

