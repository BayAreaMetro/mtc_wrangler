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
import pathlib
import pandas as pd
import geopandas as gpd
import shapely.geometry

import tableau_utils
import network_wrangler
from network_wrangler import WranglerLogger

INPUT_2015v12 = pathlib.Path("E:\\Box\\Modeling and Surveys\\Development\\Travel Model Two Conversion\\Model Inputs\\2015-tm22-dev-sprint-03\standard_network_after_project_cards")
INPUT_2023GTFS = pathlib.Path("M:\\Data\\Transit\\511\\2023-10")
NODES_FILE = INPUT_2015v12 / "v12_node.geojson"
LINKS_FILE = INPUT_2015v12 / "v12_link.json"
SHAPES_FILE = INPUT_2015v12 / "v12_shape.geojson"

OUTPUT_DIR = pathlib.Path("M:\\Development\\Travel Model Two\\Supply\\Network Creation 2025\\from_2015v12")
NOW = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

def create_line(row):
  """Simple method to create shapely.geometry.LineString from coordinates in a DataFrame row
  """
  start_point = shapely.geometry.Point(row['X_A'], row['Y_A'])
  end_point = shapely.geometry.Point(row['X_B'], row['Y_B'])
  return shapely.geometry.LineString([start_point, end_point])

def fix_link_lanes(links_gdf: pd.DataFrame, lanes_col: str):
  """Makes lanes columns network_wrangler 1.0 compliant.

  Updates the given column so that it only contains integers, and scoped values are set into sc_[lanes_col]
  Args:
      links_df (pd.DataFrame): the RoadLinks DataFrame
      lanes_col (str): 'lanes' or 'ML_lanes'
  """
  lanes_dict_list = links_gdf.loc[links_gdf[lanes_col].apply(lambda x: isinstance(x, dict)), lanes_col].to_list()
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
    links_gdf.loc[ links_gdf[lanes_col]==lanes_dict, lanes_col] = lanes_dict['default']
    # since sc_lanes may be a dictionary, make copies of it for each row to set or
    # pandas will error that the length doesn't match the rows
    links_gdf.loc[ links_gdf[lanes_col]==lanes_dict, f'sc_{lanes_col}'] = [sc_lanes] * len(links_gdf[links_gdf[lanes_col] == lanes_dict])

  # Set null, blank, '0' or 'NaN' to 0
  links_gdf.loc[ links_gdf[lanes_col].isnull(), lanes_col ] = 0
  links_gdf.loc[ links_gdf[lanes_col] == '',    lanes_col ] = 0
  links_gdf.loc[ links_gdf[lanes_col] == '0',   lanes_col ] = 0
  links_gdf.loc[ links_gdf[lanes_col] == 'NaN', lanes_col ] = 0

  # reset and check
  links_gdf[f'{lanes_col}_type'] = links_gdf[lanes_col].apply(type).astype(str)
  WranglerLogger.debug(f"links_gdf[['{lanes_col}_type]']].value_counts():")
  WranglerLogger.debug(links_gdf[[f'{lanes_col}_type']].value_counts())

  WranglerLogger.debug(f"strings value_counts():")
  WranglerLogger.debug(links_gdf.loc[ links_gdf[lanes_col].apply(lambda x: isinstance(x, str)), lanes_col])

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

  # Noting that 'id','fromIntersectionId','toIntersectionId' is not unicque
  # because there are a bunch with id='walktorailN' or 'tap_N', and fromIntersectionId/toIntersectionId=None
  duplicates = links_df.loc[links_df.duplicated(subset=['id','fromIntersectionId','toIntersectionId'], keep=False)]
  WranglerLogger.debug(f"duplicated: len={len(duplicates):,}:\n{duplicates}")

  links_df = pd.merge(
    left=links_df,
    right=shapes_gdf[['id','fromIntersectionId','toIntersectionId','geometry']],
    on=['id','fromIntersectionId','toIntersectionId'],
    how='left',
    indicator=True,
  )
  # For the rows that do not have geometry, create a simple two-point line geometry from the node locations
  WranglerLogger.debug(f"After merging with shapes_gdf, links_df[['_merge']].value_counts():\n{links_df[['_merge']].value_counts()}")
  links_df.drop(columns=['_merge'], inplace=True)

  no_geometry_links = links_df.loc[ pd.isnull(links_df.geometry) ]
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
  
  # create links_gdf now that we have geometry for everything
  links_gdf = gpd.GeoDataFrame(pd.concat([
    links_df.loc[ pd.notnull(links_df.geometry) ],
    no_geometry_links]),
    crs=shapes_gdf.crs)
  WranglerLogger.debug(f"Created links_gdf with dtypes:\n{links_gdf.dtypes}")
  WranglerLogger.debug(f"links_gdf:\n{links_gdf}")

  # fill in missing managed values with 0
  WranglerLogger.debug(f"links_gdf['managed'].value_counts():\n{links_gdf['managed'].value_counts()}")
  WranglerLogger.debug(f"links_gdf['managed'].apply(type).value_counts():\n{links_gdf['managed'].apply(type).value_counts()}")
  links_gdf.loc[links_gdf.managed == '', 'managed'] = 0 # blank -> 0
  links_gdf['managed'] = links_gdf['managed'].astype(int)
  WranglerLogger.debug(f"links_gdf['managed'].value_counts():\n{links_gdf['managed'].value_counts()}")

  # The columns lanes and ML_lanes are a combination of types, including dictionaries representing time-scoped versions
  # Fix this according to network_wrangler standard
  fix_link_lanes(links_gdf, lanes_col='lanes')
  fix_link_lanes(links_gdf, lanes_col='ML_lanes')

  # network_wrangler requires distance field
  links_gdf_feet = links_gdf.to_crs(epsg=2227)
  links_gdf_feet['distance'] = links_gdf_feet.length / 5280 # distance is in miles
  # join back to links_gdf
  links_gdf = links_gdf.merge(
    right=links_gdf_feet[['A','B','distance']],
    how='left',
    on=['A','B'],
    validate='one_to_one'
  )
  # shape_id is a string
  links_gdf['shape_id'] = links_gdf.model_link_id.astype(str)

  # are there links with distance==0?
  WranglerLogger.debug(f"links_gdf.loc[ links_gdf['distance'] == 0 ]:\n{links_gdf.loc[ links_gdf['distance'] == 0 ]}")

  # create roadway network
  roadway_network =  network_wrangler.load_roadway_from_dataframes(
    links_df=links_gdf,
    nodes_df=nodes_gdf,
    shapes_df=links_gdf
  )
  WranglerLogger.debug(f"roadway_net:\n{roadway_network}")
  WranglerLogger.info(f"RoadwayNetwork created with {len(roadway_network.nodes_df):,} nodes and {len(roadway_network.links_df):,} links.")

  tableau_utils.write_geodataframe_as_tableau_hyper(
    links_gdf.loc[ links_gdf['distance'] > 0],  # drop distance==0 links because otherwise this will error
    OUTPUT_DIR / "mtc_links.hyper",
    "mtc_links"
  )
  tableau_utils.write_geodataframe_as_tableau_hyper(
    nodes_gdf,
    OUTPUT_DIR / "mtc_nodes.hyper",
    "mtc_nodes"
  )
  # Read a GTFS network (not wrangler_flavored)
  gtfs_model = network_wrangler.transit.io.load_feed_from_path(INPUT_2023GTFS, wrangler_flavored=False)
  WranglerLogger.debug(f"gtfs_model:\n{gtfs_model}")
  feed = network_wrangler.utils.transit.create_feed_from_gtfs_model(gtfs_model, roadway_network)

