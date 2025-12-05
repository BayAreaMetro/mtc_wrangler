r"""Converts an MTC network wrangler Scenario to an Emme model network.

References:
  * Asana: GMNS+ / NetworkWrangler2 > Build 2023 network from scratch
    https://app.asana.com/1/11860278793487/project/15119358130897/task/1210468893117122

 Example usage: 
 
 python convert_scenario_to_emme_network.py 
   --overwrite
   "M:\Development\Travel Model Two\Supply\Network Creation 2025\from_OSM\SanMateo\7_scenario\mtc_2023_scenario.yml" 
   "E:\GitHub\tm2\tm2py-utils\tm2py_utils\config\develop"
   "M:\Development\Travel Model Two\Supply\Network Creation 2025\from_OSM\SanMateo\7_scenario\emme"

"""

USAGE = __doc__
import argparse
import pandas as pd
import pathlib
import shutil

import network_wrangler
from network_wrangler import WranglerLogger
from network_wrangler.scenario import load_scenario
from network_wrangler.roadway.model_roadway import ModelRoadwayNetwork

import inro.emme.desktop.app as _app
import inro.emme.database.emmebank as _emmebank

from models.mtc_roadway_schema import MTCFacilityType
import models.mtc_network

# for model/scenario config
from tm2py.config import Configuration

def fix_missing_fields(model_roadway_net: ModelRoadwayNetwork):
    """ Fill in missing fields in the model_roadway_net tables

    Args:
        model_road_net (ModelRoadwayNetwork): network to modify in place
    """
    # nodes fields from mtc_roadway_schema.MTCRoadNodesTable

    # county: default to ''
    model_roadway_net.nodes_df['county'] = model_roadway_net.nodes_df['county'].replace({None:''}).fillna('')
    WranglerLogger.debug(f"model_roadway_net.nodes_df.county:\n{model_roadway_net.nodes_df['county'].value_counts(dropna=False)}")

    # taz_centroid: default to False
    model_roadway_net.nodes_df.loc[
        pd.isnull(model_roadway_net.nodes_df['taz_centroid']), 'taz_centroid'
    ] = 0
    model_roadway_net.nodes_df['taz_centroid'] = model_roadway_net.nodes_df['taz_centroid'].astype(bool)
    WranglerLogger.debug(f"model_roadway_net.nodes_df.taz_centroid:\n{model_roadway_net.nodes_df['taz_centroid'].value_counts(dropna=False)}")

    # maz_centroid: default to False
    model_roadway_net.nodes_df.loc[
        pd.isnull(model_roadway_net.nodes_df['maz_centroid']), 'maz_centroid'
    ] = 0
    model_roadway_net.nodes_df['maz_centroid'] = model_roadway_net.nodes_df['maz_centroid'].astype(bool)
    WranglerLogger.debug(f"model_roadway_net.nodes_df.maz_centroid:\n{model_roadway_net.nodes_df['maz_centroid'].value_counts(dropna=False)}")

    # create sort order -- we want to sort so that TAZ centroids are first, then MAZ centroids, then all other nodes sorted by model_node_id
    model_roadway_net.nodes_df['sort_group'] = 3 # road noads
    model_roadway_net.nodes_df.loc[ model_roadway_net.nodes_df['taz_centroid'],'sort_group'] = 1
    model_roadway_net.nodes_df.loc[ model_roadway_net.nodes_df['maz_centroid'],'sort_group'] = 2
    model_roadway_net.nodes_df.sort_values(by=['sort_group','model_node_id'], inplace=True, ignore_index=True)
    WranglerLogger.debug(f"After sorting using sort_group, model_roadway_net.nodes_df:\n{model_roadway_net.nodes_df}")

    # links fields from mtc_roadway_schema.MTCRoadLinksTable

    WranglerLogger.debug(f"model_roadway_net.links_df.rail_only:\n{model_roadway_net.links_df['rail_only'].value_counts(dropna=False)}")
    WranglerLogger.debug(f"model_roadway_net.links_df.bus_only:\n{model_roadway_net.links_df['bus_only'].value_counts(dropna=False)}")
    WranglerLogger.debug(f"model_roadway_net.links_df.ferry_only:\n{model_roadway_net.links_df['ferry_only'].value_counts(dropna=False)}")
    WranglerLogger.debug(f"model_roadway_net.links_df.drive_access:\n{model_roadway_net.links_df['drive_access'].value_counts(dropna=False)}")
    WranglerLogger.debug(f"model_roadway_net.links_df.bike_access:\n{model_roadway_net.links_df['bike_access'].value_counts(dropna=False)}")
    WranglerLogger.debug(f"model_roadway_net.links_df.walk_access:\n{model_roadway_net.links_df['walk_access'].value_counts(dropna=False)}")

    # roadway: default to ''
    model_roadway_net.links_df['roadway'] = model_roadway_net.links_df['roadway'].replace({None:''}).fillna('')
    WranglerLogger.debug(f"model_roadway_net.links_df.roadway:\n{model_roadway_net.links_df['roadway'].value_counts(dropna=False)}")

    # projects: default to ''
    model_roadway_net.links_df['projects'] = model_roadway_net.links_df['projects'].replace({None:''}).fillna('')
    WranglerLogger.debug(f"model_roadway_net.links_df.projects:\n{model_roadway_net.links_df['projects'].value_counts(dropna=False)}")

    # managed: default to 0
    model_roadway_net.links_df.loc[ pd.isnull(model_roadway_net.links_df['managed']),'managed'] = 0
    WranglerLogger.debug(f"model_roadway_net.links_df.managed:\n{model_roadway_net.links_df['managed'].value_counts(dropna=False)}")

    # ref: default to ''
    model_roadway_net.links_df['ref'] = model_roadway_net.links_df['ref'].replace({None:''}).fillna('')
    WranglerLogger.debug(f"model_roadway_net.links_df.ref:\n{model_roadway_net.links_df['ref'].value_counts(dropna=False)}")

    # county: default to ''
    model_roadway_net.links_df['county'] = model_roadway_net.links_df['county'].replace({None:''}).fillna('')
    WranglerLogger.debug(f"model_roadway_net.links_df.county:\n{model_roadway_net.links_df['county'].value_counts(dropna=False)}")

    # facility type: missing values are connectors
    model_roadway_net.links_df.loc[ 
        model_roadway_net.links_df['roadway'].isin(['ml_access_point','ml_egress_point']), 'ft'] = MTCFacilityType.CONNECTOR
    model_roadway_net.links_df['ft'] = model_roadway_net.links_df['ft'].astype(int)
    WranglerLogger.debug(f"model_roadway_net.links_df.ft:\n{model_roadway_net.links_df['ft'].value_counts(dropna=False)}")


if __name__ == "__main__":
    # Setup pandas display options
    pd.options.display.max_columns = None
    pd.options.display.width = None
    
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("--overwrite", action="store_true", help="Delete previous version (otherwise it will error)")
    parser.add_argument("input_scenario_yml", type=pathlib.Path, help="Network Wrangler scenario yaml")
    parser.add_argument("tm2_config_dir", type=pathlib.Path, help="Directory with tm2 model_config.toml and scenario_config.toml")
    parser.add_argument("output_dir", type=pathlib.Path, help="Output directory")
    args = parser.parse_args()

    # Setup logging
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    INFO_LOG  = output_dir / "convert_scenario_to_emme_network.info.log"
    DEBUG_LOG = output_dir / "convert_scenario_to_emme_network.debug.log"

    network_wrangler.setup_logging(
        info_log_filename=INFO_LOG,
        debug_log_filename=DEBUG_LOG,
        std_out_level="info",
        file_mode='w'
    )

    tm2_config = Configuration.load_toml([
        args.tm2_config_dir / "model_config.toml", 
        args.tm2_config_dir / "scenario_config.toml"
    ])

    if args.overwrite:
        prev_proj = output_dir / tm2_config.emme.project_path.parent
        if prev_proj.exists():
            shutil.rmtree(prev_proj)
            WranglerLogger.info(f"Found existing project at {prev_proj}; removed.")

    mtc_scenario = load_scenario(args.input_scenario_yml)
    mtc_scenario.road_net._shapes_df = mtc_scenario.road_net.links_df
    WranglerLogger.debug(f"mtc_scenario:\n{mtc_scenario}")

    # create ModelRoadwayNetwork instance
    mtc_scenario.road_net.config.MODEL_ROADWAY.ADDITIONAL_COPY_FROM_GP_TO_ML = [
        # these are in the mtc_roadway_schema: MTCRoadLinksTable
        "county",
        "ft",
        # useclass?
        "length", # network_wrangler uses distance
    ]

    WranglerLogger.debug(f"{mtc_scenario.road_net.links_df.crs=}")
    model_roadway_net = mtc_scenario.road_net.model_net

    # debug:
    # new parallel links have managed = 1
    WranglerLogger.debug(
        f"managed lane links:\n"
        f"{model_roadway_net.links_df.loc[model_roadway_net.links_df.managed ==1]}"
    )
    # access/egress links for managed lanes
    # new parallel links have managed = 1
    WranglerLogger.debug(
        f"managed access/egress lane links:\n"
        f"{model_roadway_net.links_df.loc[model_roadway_net.links_df['roadway'].isin(['ml_access_point','ml_egress_point'])]}"
    )

    # fill in missing fields in the model_roadway_net dataframes (and make the dtypes more reasonable)
    fix_missing_fields(model_roadway_net)

    WranglerLogger.debug(f"model_roadway_net type={type(model_roadway_net)}:\n{model_roadway_net}")
    model_roadway_net.write(output_dir, overwrite=True, true_shape=True)
    WranglerLogger.info(f"Wrote model_roadway_net to {output_dir}")
    
    # Convert to local projection, in feet
    # JSON doesn't support CRS, so we need to do this after writing the model network in that format
    model_roadway_net.nodes_df.to_crs(crs=models.mtc_network.LOCAL_CRS_FEET, inplace=True)
    model_roadway_net.links_df.to_crs(crs=models.mtc_network.LOCAL_CRS_FEET, inplace=True)
    WranglerLogger.debug(f"{model_roadway_net.nodes_df.crs=}")
    WranglerLogger.debug(f"{model_roadway_net.links_df.crs=}")

    WranglerLogger.debug(f"model nodes:\n{model_roadway_net.nodes_df}")
    # check ML link nodes
    WranglerLogger.debug(
        f"managed link nodes:\n"
        f"{model_roadway_net.nodes_df.loc[pd.notnull(model_roadway_net.nodes_df['GP_model_node_id'])]}"
    )


    # create EMME Project from scratch
    # tm2_config.emme.project_path is a relative path
    WranglerLogger.info(f"project name={str(tm2_config.emme.project_path.parent)}")
    emme_project_file = _app.create_project(output_dir, name=str(tm2_config.emme.project_path.parent))
    WranglerLogger.info(f"Created emme_project: {emme_project_file}")
    # create project spatial reference file
    proj_spatial_file = output_dir / "mtc_network.prj"
    with open(proj_spatial_file, "w") as file:
        file.write(models.mtc_network.LOCAL_PRJ)
    WranglerLogger.info(f"Created spatial file: {proj_spatial_file}")
    # open EMME application; this returns a project
    emme_app = _app.start_dedicated(
        visible=True,
        user_initials='MTC',
        project= output_dir / tm2_config.emme.project_path
    )
    WranglerLogger.debug(f"emme_app has type {type(emme_app)}")
    WranglerLogger.info(f"Started emme_app returning project: {emme_app.project}")
    # set the spatial referenve to the file we created, which matches LOCAL_CRS_FEET
    emme_app.project.spatial_reference_file = str(proj_spatial_file)
    WranglerLogger.debug(f"emme_app.project.spatial_reference_file: {emme_app.project.spatial_reference_file}")
    emme_app.project.name = mtc_scenario.name

    # we're going to create emmebank databases by mode:
    # drive, transit, active
    # drive and transit have a scenario per timeperiod
    # active mode networks have one scenario, but may need to be split into pieces due to node/link limitations

    # Per EMME Help:
    # Network fields are editable, per-scenario attributes. They are useful to store text attributes on network
    # elements such as street names, labels, region names, etc. They are also useful to store values such as numbers
    # with double precision, alternate IDs (IDs from another source), etc. Compared to data tables and DBF attributes
    # which are static, they are always in sync with the current state of the network.
    # 
    # They are available for all network domains: modes, nodes, links, turns, transit vehicles, transit lines,
    # and transit segments.
    # 
    # They play a role similar to Extra attributes, but while extra attributes can only contain numeric values and
    # are saved in the EMME database, network fields can be of several types (string, integer, real or boolean) 
    # and are saved in an external file associated with the scenario (scenario_id.db) in the Database folder of
    # the project.
    #
    # *Database network attributes*
    #
    # Standard attributes are part of the basic network data. These attributes have predefined names, and
    # most of them have a special meaning (for example, link length or node coordinates). Adding a network
    # element, for example a link, implies specifying the values of its standard attributes. These values
    # may then be modified when the element is edited, made available to EMME modelling procedures, and
    # displayed in Desktop, for instance.
    #
    # The standard attributes include three user data items for each node, link, turn and transit line
    # (and for transit segments, if this is requested at database creation time). The contents of these data
    # items are defined by the user, but must be numeric. These items are referred to by using predefined
    # names such as ul3 (link user data 3), ui2 (node user data 2), etc.
    #
    # *Extra attributes* are user-defined attributes that can be associated with nodes, links, turns, transit lines
    # and segments. When creating an extra attribute, the user gives the attribute a name (of the form @laa...a), 
    # a description and a default value. When an element is added to a scenario, all its extra attributes are
    # initialized to their respective default values. When an extra attribute is created, it is tagged with the
    # creation time. This time stamp is updated whenever the attribute is modified. (Timestamps on extra attributes
    # may be consulted in Desktop Attribute list windows.)
    # 
    # Both user data items and extra attributes are useful for storing observed values, data derived from
    # calculations, etc. They can both be used as input or output for EMME procedures or for user-specific models
    # and analyses. Extra attributes cannot be used as keywords in functions but they can be accessed in
    # volume-delay and turn penalty functions through the extra function parameters (see Set extra function
    # parameters tool).
    #
    # *Assignment result attributes* are automatically written to the network, depending on the procedure used.
    # They include auto volumes on links and turns, transit volumes on line segments, etc. Assignment result
    # attributes become available once an assignment procedure has been performed, and may be lost following
    # certain database modifications. See the traffic and transit assignment tool descriptions for specifics
    # on which assignment result attributes are used.

    # create emmebank database for project
    # TODO: these should come from the network?
    # starting with versions from E:\TM2\emme_project\Database_highway\emmebank
    emme_emmebank_dimensions = {
        'scenarios'             : 6,
        'centroids'             : 5_000,
        'regular_nodes'         : 994_999,
        'links'                 : 2_000_000,
        'transit_vehicles'      : 600,
        'transit_lines'         : 40_000,
        'transit_segments'      : 2_000_000,
        'turn_entries'          : 3_000_000,
        'full_matrices'         : 9999,
        'origin_matrices'       : 999,
        'destination_matrices'  : 999,
        'scalar_matrices'       : 999,
        'extra_attribute_values': 100_000_000,
        # don't know what these are
        'functions'             : 99,
        'operators'             : 5000,
        'sola_analyses'         : 240
    }
    DB_DIR = output_dir / tm2_config.emme.highway_database_path.parent
    DB_DIR.mkdir(parents=True, exist_ok=True)
    WranglerLogger.info(f"Creating {DB_DIR}")
    emme_emmebank = _emmebank.create(output_dir / tm2_config.emme.highway_database_path, emme_emmebank_dimensions)
    emme_emmebank.unit_of_length='mi'
    emme_emmebank.coord_unit_length=1.0/5280.0  # coord_unit = feet
    emme_emmebank.title = "drive_network"
    WranglerLogger.info(f"Created emme_emmebank {emme_emmebank}")
    WranglerLogger.info(f"{emme_emmebank.unit_of_length=}")
    WranglerLogger.info(f"{emme_emmebank.coord_unit_length=}")
    WranglerLogger.info(f"{emme_emmebank.dimensions=}")

    # create emme Scenario object
    drive_scenario = emme_emmebank.create_scenario(tm2_config.emme.all_day_scenario_id)
    drive_scenario.title = "Drive, all day"

    # add emmebank to project
    WranglerLogger.debug(f"emme_emmebank.path: {emme_emmebank.path}")
    emme_db = emme_app.data_explorer().add_database(emme_emmebank.path)
    emme_db.open()
    emme_app.refresh_data()

    # get scenario ready for network creation

    # add network_wrangler standard fields
    # TODO: this should move to emme_wrangler since it's not MTC-specific
    # from RoadNodesTable
    drive_scenario.create_network_field('NODE', '#model_node_id', 'INTEGER32')
    drive_scenario.create_network_field('NODE', '#osm_node_id',   'STRING')
    # from RoadLinksTable
    drive_scenario.create_network_field('LINK', '#a_node',       'INTEGER32') # #A was an error
    drive_scenario.create_network_field('LINK', '#b_node',       'INTEGER32')
    drive_scenario.create_network_field('LINK', '#name',         'STRING')
    drive_scenario.create_network_field('LINK', '#rail_only',    'BOOLEAN')
    drive_scenario.create_network_field('LINK', '#bus_only',     'BOOLEAN')
    drive_scenario.create_network_field('LINK', '#ferry_only',   'BOOLEAN')
    drive_scenario.create_network_field('LINK', '#drive_access', 'BOOLEAN')
    drive_scenario.create_network_field('LINK', '#bike_access',  'BOOLEAN')
    drive_scenario.create_network_field('LINK', '#walk_access',  'BOOLEAN')
    drive_scenario.create_network_field('LINK', '#roadway',      'STRING')
    drive_scenario.create_network_field('LINK', '#projects',     'STRING')
    drive_scenario.create_network_field('LINK', '#managed',      'INTEGER32')
    drive_scenario.create_network_field('LINK', '#ref',          'STRING')

    # add mtc standard fields (see mtc_roadway_schema.py)
    drive_scenario.create_network_field('NODE', '#node_county',  'STRING') # must be unique, across tables (link, node, etc)
    drive_scenario.create_network_field('NODE', '#taz_centroid', 'BOOLEAN')
    drive_scenario.create_network_field('NODE', '#maz_centroid', 'BOOLEAN')
    drive_scenario.create_network_field('LINK', '#link_county',  'STRING')
    drive_scenario.create_network_field('LINK', '#ft',           'INTEGER32')

    # get Network object from scenario
    drive_network = drive_scenario.get_network()

    # create modes
    # general drive
    drive_network.create_mode('AUTO', tm2_config.highway.generic_highway_mode_code)
    drive_network.mode(tm2_config.highway.generic_highway_mode_code).description = "car"

    for transit_mode_config in tm2_config.transit.modes:
        # WranglerLogger.debug(f"transit_mode_config: {transit_mode_config}")
        if transit_mode_config.description in ['drive_acc','knrdummy','pnrdummy']:
            drive_network.create_mode(transit_mode_config.assign_type, transit_mode_config.mode_id)
            drive_network.mode(transit_mode_config.mode_id).description = transit_mode_config.description
            WranglerLogger.debug(f"Added drive mode with type='{transit_mode_config.assign_type}' "
                                 f"id='{transit_mode_config.mode_id}' "
                                 f"description='{transit_mode_config.description}'")

    # Network.create_node() for centroids and nodes
    # TAZs and then MAZs and then all other nodes
    model_node_id_to_emme_id = {}
    for index, row in model_roadway_net.nodes_df.iterrows():
        # WranglerLogger.debug(f"Processing node {index}: row=\n{row}")
        # id is a string
        emme_node = drive_network.create_node(
            id=index+1, 
            is_centroid=row['taz_centroid'] | row['maz_centroid']
        )
        # set standard attributes
        emme_node['x'] = row['geometry'].x
        emme_node['y'] = row['geometry'].y
        # set additional attributes
        emme_node['#model_node_id'] = row['model_node_id']
        emme_node['#osm_node_id']   = row['osm_node_id']
        # set mtc-specific attributes
        emme_node['#node_county']   = row['county']
        emme_node['#taz_centroid']  = row['taz_centroid']
        emme_node['#maz_centroid']  = row['maz_centroid']

        # save mapping from model_node_id to emme id
        model_node_id_to_emme_id[row['model_node_id']] = emme_node.number
    WranglerLogger.info(f"Created {len(model_node_id_to_emme_id):,} emme nodes")

    # create dataframe for model_node_id_to_emme_id
    model_node_id_to_emme_id_df = pd.DataFrame(
        {'emme_node_id':model_node_id_to_emme_id.values()},
        index=model_node_id_to_emme_id.keys()
    )
    model_node_id_to_emme_id_df.index.name = 'model_node_id'
    WranglerLogger.debug(f"model_node_id_to_emme_id_df:\n{model_node_id_to_emme_id_df}")
    # save it
    xwalk_file = output_dir / tm2_config.highway.model_to_emme_node_id_xwalk
    model_node_id_to_emme_id_df.to_csv(xwalk_file, header=True, index=True)
    WranglerLogger.info(f"Wrote {xwalk_file}")

    # Network.create_link() for links
    default_modes = tm2_config.highway.generic_highway_mode_code
    # TODO: what about the other modes?
    for index, row in model_roadway_net.links_df.iterrows():
        emme_link = drive_network.create_link(
            i_node_id=model_node_id_to_emme_id[row['A']],
            j_node_id=model_node_id_to_emme_id[row['B']],
            modes=default_modes
        )
        # set standard attributes
        emme_link['length']        = row['distance'] # distance is in miles so use this
        emme_link['type']          = 1 # what is this?
        emme_link['num_lanes']     = row['lanes']
        # set intermediate coordinates, if there are any
        link_coords = list(row['geometry'].coords)
        if len(link_coords) > 2: 
            link_coords = link_coords[1:-1]
            emme_link.vertices = link_coords
        # set additional attributes
        emme_link['#a_node']       = row['A']
        emme_link['#b_node']       = row['B']
        emme_link['#name']         = row['name']
        emme_link['#rail_only']    = row['rail_only']
        emme_link['#bus_only']     = row['bus_only']
        emme_link['#ferry_only']   = row['ferry_only']
        emme_link['#drive_access'] = row['drive_access']
        emme_link['#bike_access']  = row['bike_access']
        emme_link['#walk_access']  = row['walk_access']
        emme_link['#roadway']      = row['roadway']
        emme_link['#projects']     = row['projects']
        emme_link['#managed']      = row['managed']
        emme_link['#ref']          = row['ref']
        # set mtc-specific attributes
        emme_link['#link_county']  = row['county']
        emme_link['#ft']           = row['ft']

    # Scenario.publish_network
    drive_scenario.publish_network(drive_network)
    WranglerLogger.info("Published drive_scenario network")

    # list project databases
    for db in emme_app.project.databases:
        WranglerLogger.info(f"database: type={type(db)} name={db.name()} title={db.title()} path={db.title()}")
        for scenario in db.scenarios():
            WranglerLogger.info(f"scenario: type={type(scenario)} number={scenario.number()} title={scenario.title()}")

    emme_app.project.save()

    # emme_app.close()