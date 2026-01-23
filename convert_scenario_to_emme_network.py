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
import pprint
import shutil

import network_wrangler
from network_wrangler import WranglerLogger
from network_wrangler.scenario import load_scenario
from network_wrangler.roadway.model_roadway import ModelRoadwayNetwork
from network_wrangler.models.gtfs.types import RouteType

import inro.emme.desktop.app as _app
import inro.emme.database.emmebank as _emmebank

from models.mtc_roadway_schema import MTCFacilityType
import models.mtc_network

# for model/scenario config
from tm2py.config import Configuration

# mapping from RouteType to Emme transit mode description
ROUTE_TYPE_TO_EMME_TRANSIT_MODE = {
    RouteType.TRAM       :'light_rail',
    RouteType.SUBWAY     :'light_rail',
    RouteType.RAIL       :'heavy_rail',
    RouteType.BUS        :'local_bus',
    RouteType.FERRY      :'ferry',
    RouteType.CABLE_TRAM :'light_rail',
    RouteType.TROLLEYBUS :'local_bus',
}

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
    WranglerLogger.debug(f"model_roadway_net.nodes_df.taz_centroid:\n{model_roadway_net.nodes_df['taz_centroid'].value_counts(dropna=False)}")
    model_roadway_net.nodes_df.loc[
        pd.isnull(model_roadway_net.nodes_df['taz_centroid']), 'taz_centroid'
    ] = 0
    model_roadway_net.nodes_df['taz_centroid'] = model_roadway_net.nodes_df['taz_centroid'].astype(bool)
    WranglerLogger.debug(f"model_roadway_net.nodes_df.taz_centroid:\n{model_roadway_net.nodes_df['taz_centroid'].value_counts(dropna=False)}")

    # maz_centroid: default to False
    WranglerLogger.debug(f"model_roadway_net.nodes_df.maz_centroid:\n{model_roadway_net.nodes_df['maz_centroid'].value_counts(dropna=False)}")
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
    WranglerLogger.debug(f"model_roadway_net.links_df.county:\n{model_roadway_net.links_df['county'].value_counts(dropna=False)}")
    model_roadway_net.links_df['county'] = model_roadway_net.links_df['county'].replace({None:''}).fillna('')
    WranglerLogger.debug(f"model_roadway_net.links_df.county:\n{model_roadway_net.links_df['county'].value_counts(dropna=False)}")

    # facility type: missing values are connectors
    WranglerLogger.debug(f"model_roadway_net.links_df.ft:\n{model_roadway_net.links_df['ft'].value_counts(dropna=False)}")
    model_roadway_net.links_df.loc[ 
        model_roadway_net.links_df['roadway'].isin(['ml_access_point','ml_egress_point']), 'ft'] = MTCFacilityType.CONNECTOR
    model_roadway_net.links_df['ft'] = model_roadway_net.links_df['ft'].astype(int)
    WranglerLogger.debug(f"model_roadway_net.links_df.ft:\n{model_roadway_net.links_df['ft'].value_counts(dropna=False)}")


def create_emmebank_network(
        network_mode: str,
        mtc_scenario: network_wrangler.Scenario,
        model_roadway_net: ModelRoadwayNetwork,
        tm2_config: Configuration,
        emme_app: _app
):
    """Creates an emmebank for the given network mode, including time-of-day scenarios with networks.

    In terrible need of refactoring but I wanted to get it all down first.

    Args:
        network_mode (str): One of 'drive', 'transit'
        mtc_scenario (network_wrangler.Scenario): The Scenario including roadway and transit networks
        model_roadway_net (ModelRoadwayNetwork): A model version of the roadway network, in case
          custom preprocessing is done
        tm2py_config (tm2py.config.Configuration): MTC TM2 configuration
        emme_app (inro.emme.desktop.app): The EMME app instance; it should have the project open
          for which we'll add the networks.
    """
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
    # create emmebank database for project
    # TODO: these should come from the network
    # starting with versions from E:\TM2\emme_project\Database_highway\emmebank
    emme_emmebank_dimensions = {
        'scenarios'             : 6 if network_mode in ['drive','transit'] else 1, # all day, plus one per time period
        'centroids'             : 20_000,
        'regular_nodes'         : 979_999,
        'links'                 : 2_000_000,
        'transit_vehicles'      : 600,
        'transit_lines'         : 40_000,
        'transit_segments'      : 2_000_000,
        'turn_entries'          : 3_000_000,
        'full_matrices'         : 9999, # what is this?
        'origin_matrices'       : 999,  # what is this?
        'destination_matrices'  : 999,  # what is this?
        'scalar_matrices'       : 999,  # what is this?
        'extra_attribute_values': 100_000_000, # what is this?
        'functions'             : 99,   # what is this?
        'operators'             : 5000, # what is this?
        'sola_analyses'         : 240   # what is this?
    }
    if network_mode == 'drive':
        DB_PATH = output_dir / tm2_config.emme.highway_database_path
    elif network_mode == 'transit':
        DB_PATH = output_dir / tm2_config.emme.transit_database_path
    elif network_mode == 'active_north':
        DB_PATH = output_dir / tm2_config.emme.active_north_database_path
    elif network_mode == 'active_south':
        DB_PATH = output_dir / tm2_config.emme.active_south_database_path
    else:
        raise ValueError(f"Invalid value for network_mode:'{network_mode}'")

    WranglerLogger.info(f"Creating {DB_PATH.parent}")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    emme_emmebank = _emmebank.create(DB_PATH, emme_emmebank_dimensions)
    emme_emmebank.unit_of_length='mi'
    emme_emmebank.coord_unit_length=1.0/5280.0  # coord_unit = feet
    emme_emmebank.title = f"{network_mode}_network"
    WranglerLogger.info(f"Created emme_emmebank {emme_emmebank}")
    WranglerLogger.debug(f"{emme_emmebank.unit_of_length=}")
    WranglerLogger.debug(f"{emme_emmebank.coord_unit_length=}")
    WranglerLogger.debug(f"{emme_emmebank.dimensions=}")

    # create emme Scenario object
    emme_scenario = emme_emmebank.create_scenario(tm2_config.emme.all_day_scenario_id)
    emme_scenario.title = f"{network_mode}, all day"

    # add emmebank to project
    WranglerLogger.debug(f"emme_emmebank.path: {emme_emmebank.path}")
    emme_db = emme_app.data_explorer().add_database(emme_emmebank.path)
    emme_db.open()
    emme_app.refresh_data()

    # get scenario ready for network creation

    # add network_wrangler standard fields
    # TODO: this should move to emme_wrangler since it's not MTC-specific
    # from RoadNodesTable
    emme_scenario.create_network_field('NODE', '#model_node_id', 'INTEGER32')
    emme_scenario.create_network_field('NODE', '#osm_node_id',   'STRING')
    # from RoadLinksTable
    emme_scenario.create_network_field('LINK', '#a_node',       'INTEGER32') # #A was an error
    emme_scenario.create_network_field('LINK', '#b_node',       'INTEGER32')
    emme_scenario.create_network_field('LINK', '#name',         'STRING')
    emme_scenario.create_network_field('LINK', '#rail_only',    'BOOLEAN')
    emme_scenario.create_network_field('LINK', '#bus_only',     'BOOLEAN')
    emme_scenario.create_network_field('LINK', '#ferry_only',   'BOOLEAN')
    emme_scenario.create_network_field('LINK', '#drive_access', 'BOOLEAN')
    emme_scenario.create_network_field('LINK', '#bike_access',  'BOOLEAN')
    emme_scenario.create_network_field('LINK', '#walk_access',  'BOOLEAN')
    emme_scenario.create_network_field('LINK', '#roadway',      'STRING')
    emme_scenario.create_network_field('LINK', '#projects',     'STRING')
    emme_scenario.create_network_field('LINK', '#managed',      'INTEGER32')
    emme_scenario.create_network_field('LINK', '#ref',          'STRING')

    # add mtc standard fields (see mtc_roadway_schema.py)
    emme_scenario.create_network_field('NODE', '#node_county',  'STRING') # must be unique, across tables (link, node, etc)
    emme_scenario.create_network_field('NODE', '#taz_centroid', 'BOOLEAN')
    emme_scenario.create_network_field('NODE', '#maz_centroid', 'BOOLEAN')
    emme_scenario.create_network_field('LINK', '#link_county',  'STRING')
    emme_scenario.create_network_field('LINK', '#ft',           'INTEGER32')

    # get Network object from scenario
    emme_network = emme_scenario.get_network()

    # save for later use in this method
    # { mode.description: {'emme_mode_id':one letter, 'type':'AUTO'|'AUX_TRANSIT'|'TRANSIT'}}
    emme_modes = {}
    # { (agency_id, route_type): emme_transit_vehicle_id number}
    emme_transit_vehicles = {}

    # create modes
    car_mode = emme_network.create_mode('AUTO', tm2_config.highway.generic_highway_mode_code)
    car_mode.description = "car"
    emme_modes['car'] = {'emme_mode_id':tm2_config.highway.generic_highway_mode_code, 'type': 'AUTO'}

    if network_mode == 'drive':
        # add other highway modes
        maz_auto_mode = emme_network.create_mode('AUX_AUTO', tm2_config.highway.maz_to_maz.mode_code)
        maz_auto_mode.description = "maz_auto"
        emme_modes['maz_auto'] = {'emme_mode_id': tm2_config.highway.maz_to_maz.mode_code, 'type':'AUX_AUTO'}

        # add codes for other highway assignment classes: drive alone, shared ride, trucks, tolled classes, etc
        for highway_class_config in tm2_config.highway.classes:
            class_auto_mode = emme_network.create_mode('AUX_AUTO', highway_class_config.mode_code)
            class_auto_mode.description = highway_class_config.name
            emme_modes[highway_class_config.name] = {'emme_mode_id':highway_class_config.mode_code, 'type':'AUX_AUTO'}


    for transit_mode_config in tm2_config.transit.modes:
        # WranglerLogger.debug(f"transit_mode_config: {transit_mode_config}")
        if network_mode == 'drive':
            # for drive, only include these
            if not transit_mode_config.description in ['knrdummy','pnrdummy']: continue

        emme_network.create_mode(transit_mode_config.assign_type, transit_mode_config.mode_id)
        emme_network.mode(transit_mode_config.mode_id).description = transit_mode_config.description
        emme_modes[transit_mode_config.description] = {
            'emme_mode_id':transit_mode_config.mode_id,
            'type': transit_mode_config.assign_type
        }
    WranglerLogger.debug(f"emme_modes: {pprint.pformat(emme_modes)}")

    # create transit vehicle types
    if network_mode == 'transit':
        # simple assumption: one per agency and mode (e.g. Muni bus)
        # for agency in mtc_scenario.transit_net.feed.agencies
        # TODO: this is overly simple; need to distinguish between rail types, local vs express bus
        WranglerLogger.info(f"Creating transit vehicles for agencies x mode")
        WranglerLogger.debug(f"agencies:\n{mtc_scenario.transit_net.feed.agencies}")
        WranglerLogger.debug(f"routes:\n{mtc_scenario.transit_net.feed.routes}")
        WranglerLogger.debug(f"trips:\n{mtc_scenario.transit_net.feed.trips}")
        WranglerLogger.debug(f"frequencies:\n{mtc_scenario.transit_net.feed.frequencies}")

        # add time period string
        if 'time_period' not in mtc_scenario.transit_net.feed.frequencies.columns:
            mtc_scenario.transit_net.feed.frequencies['start_time_str'] = \
                mtc_scenario.transit_net.feed.frequencies['start_time'].dt.strftime('%H:%M')
            mtc_scenario.transit_net.feed.frequencies['end_time_str'] = \
                mtc_scenario.transit_net.feed.frequencies['end_time'].dt.strftime('%H:%M')
            mtc_scenario.transit_net.feed.frequencies['time_str'] = \
                mtc_scenario.transit_net.feed.frequencies['start_time_str'] + '-' + \
                mtc_scenario.transit_net.feed.frequencies['end_time_str']
            mtc_scenario.transit_net.feed.frequencies['time_period'] = \
                mtc_scenario.transit_net.feed.frequencies['time_str'].map(models.TIME_PERIOD_TO_LABEL)
        WranglerLogger.debug(f"frequencies:\n{mtc_scenario.transit_net.feed.frequencies}")

        for agency_index, agency_row in mtc_scenario.transit_net.feed.agencies.iterrows():
            agency_id = agency_row['agency_id']
            agency_route_types = mtc_scenario.transit_net.feed.routes.loc[ 
                mtc_scenario.transit_net.feed.routes['agency_id'] == agency_id,
                'route_type'
            ].drop_duplicates().tolist()
            agency_route_types.sort() # sort in place
            WranglerLogger.debug(f"agency {agency_id} serves route_type {agency_route_types}")
            for agency_route_type in agency_route_types:
                # create unique id for this transit vehicle
                transit_vehicle_id = len(emme_transit_vehicles) + 1
                emme_mode_description = ROUTE_TYPE_TO_EMME_TRANSIT_MODE[agency_route_type]
                emme_mode_id = emme_modes[emme_mode_description]['emme_mode_id']
                emme_transit_vehicle = emme_network.create_transit_vehicle(id=transit_vehicle_id, mode_id=emme_mode_id)
                # set description
                emme_transit_vehicle.description = f"{agency_id} {RouteType(agency_route_type).name}"
                # record
                emme_transit_vehicles[(agency_id, RouteType(agency_route_type))] = transit_vehicle_id
        WranglerLogger.debug(f"emme_transit_vehicles: {pprint.pformat(emme_transit_vehicles)}")

    # Network.create_node() for centroids and nodes
    # TAZs and then MAZs and then all other nodes
    model_node_id_to_emme_id = {}
    for index, row in model_roadway_net.nodes_df.iterrows():
        # WranglerLogger.debug(f"Processing node {index}: row=\n{row}")
        # id is a string
        emme_node = emme_network.create_node(
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
    if network_mode == 'drive':
        xwalk_file = output_dir / tm2_config.highway.model_to_emme_node_id_xwalk
        model_node_id_to_emme_id_df.to_csv(xwalk_file, header=True, index=True)
        WranglerLogger.info(f"Wrote {xwalk_file}")

    # Network.create_link() for links
    rail_modes = ''
    ferry_modes = ''
    if network_mode == 'drive':
        default_modes = tm2_config.highway.generic_highway_mode_code
    elif network_mode == 'transit':
        default_modes = \
            emme_modes['car']['emme_mode_id'] + \
            emme_modes['local_bus']['emme_mode_id'] + \
            emme_modes['exp_bus']['emme_mode_id']
        rail_modes = \
            emme_modes['comm_rail']['emme_mode_id'] + \
            emme_modes['heavy_rail']['emme_mode_id'] + \
            emme_modes['light_rail']['emme_mode_id']
        ferry_modes = \
            emme_modes['ferry']['emme_mode_id']
    else:
        default_modes = \
            emme_modes['walk']['emme_mode_id']        

    num_links_created = 0
    for index, row in model_roadway_net.links_df.iterrows():
        link_modes = default_modes

        # don't include irrelevant links
        if network_mode == 'drive':
            if not row['drive_access']: continue
        if network_mode == 'transit':
            if not row['drive_access'] and not row['rail_only'] and not row['bus_only'] and not row['ferry_only']: continue
            if row['rail_only']: link_modes = rail_modes
            if row['ferry_only']: link_modes = ferry_modes
        if network_mode.startswith('active'):
            if not row['bike_access'] and not row['walk_access']: continue
        
        
        emme_link = emme_network.create_link(
            i_node_id=model_node_id_to_emme_id[row['A']],
            j_node_id=model_node_id_to_emme_id[row['B']],
            modes=link_modes
        )
        # set standard attributes
        emme_link['length']        = row['distance'] # distance is in miles so use this
        emme_link['type']          = 1 # what is this?
        # this is an emme requirement
        if row['lanes'] > 9.9:
            WranglerLogger.warning(f"The following link has lanes>9.9: setting to 9:\n{row}")
            emme_link['num_lanes'] = 9
        else:
            emme_link['num_lanes'] = row['lanes']

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

        num_links_created += 1

    WranglerLogger.info(f"Created {num_links_created:,} emme links")

    # create transit lines
    if network_mode == "transit":
        num_transit_lines_created = 0

        # we're going to need to map nodes to emme nodes so let's just do it now
        mtc_scenario.transit_net.feed.shapes['shape_emme_node_id'] = \
            mtc_scenario.transit_net.feed.shapes['shape_model_node_id'].map(model_node_id_to_emme_id)

        # we could join tables and just iterate through trips but this is readable enough...
        for _, agency_row in mtc_scenario.transit_net.feed.agencies.iterrows():
            agency_id = agency_row['agency_id']

            agency_routes_df = mtc_scenario.transit_net.feed.routes.loc[
                mtc_scenario.transit_net.feed.routes['agency_id'] == agency_id
            ]
            # iterate through routes for this agency
            for _, route_row in agency_routes_df.iterrows():
                route_id = route_row['route_id']
                route_type = RouteType(route_row['route_type'])

                # iterate through trips for this agency
                route_trips_df = mtc_scenario.transit_net.feed.trips.loc[
                    mtc_scenario.transit_net.feed.trips['route_id'] == route_id
                ]

                for _, trip_row in route_trips_df.iterrows():
                    trip_id = trip_row['trip_id']
                    shape_id = trip_row['shape_id']

                    WranglerLogger.debug(f"Processing {agency_id} {route_id} {trip_id}")
                    trip_stoptimes_df = mtc_scenario.transit_net.feed.stop_times.loc[
                        mtc_scenario.transit_net.feed.stop_times['trip_id'] == trip_id
                    ]
                    trip_shapes_df = mtc_scenario.transit_net.feed.shapes.loc[
                        mtc_scenario.transit_net.feed.shapes['shape_id'] == shape_id
                    ]
                    # iterate over shapes and stops
                    WranglerLogger.debug(f"trip_shapes_df:\n{trip_shapes_df}")
                    WranglerLogger.debug(f"trip_stoptimes_df:\n{trip_stoptimes_df}")

                    # stop_id is still set in the shapes so we don't need to worry about stoptimes
                    # Emme transit line documentation:
                    # - Space (space), comma (,) and colon (:) are reserved characters which are used
                    #   as delimiters to separate fields in transaction file formats.
                    # - To use a reserved character inside the transit line name field, enclose
                    #   the entire string in single-quotes (‘).
                    # - Changed in version 4.4: Character limit of ID increased from six to 40 characters.
                    emme_transit_line_id = f"'{route_id} {shape_id}'"
                    # since we do run into the 40 character limit, dispance with ':20230930'
                    emme_transit_line_id = emme_transit_line_id.replace(':20230930','')

                    emme_transit_vehicle_id = emme_transit_vehicles[(agency_id,route_type)]
                    emme_shape_itinerary = trip_shapes_df['shape_emme_node_id'].tolist()
                    stop_id_itinerary = trip_shapes_df['stop_id'].tolist()
                    assert(len(emme_shape_itinerary) == len(stop_id_itinerary))
                    WranglerLogger.debug(f"stop_id_itinerary: {stop_id_itinerary}")

                    try:
                        emme_transit_line = emme_network.create_transit_line(
                            emme_transit_line_id, emme_transit_vehicle_id, emme_shape_itinerary
                        )
                        emme_transit_line.description = f"{route_id} {route_row['route_long_name']}"

                        # each node defaults to being a stop
                        # disallow alightings and boardings for non-stop nodes
                        stop_id_idx = 0
                        for emme_transit_segment in emme_transit_line.segments():
                            # WranglerLogger.debug(
                            #     f"emme_transit_segment "
                            #     f"id={emme_transit_segment.id} "
                            #     f"i_node={emme_transit_segment.i_node} "
                            #     f"j_node={emme_transit_segment.j_node} "
                            #     f"loop_index={emme_transit_segment.loop_index}"
                            # )
                            # if it's not a stop, disallow alightings, boardings
                            if stop_id_itinerary[stop_id_idx] is None:
                                emme_transit_segment.allow_alightings = False
                                emme_transit_segment.allow_boardings = False
                                # WranglerLogger.debug(f"Disallowing alightings and boardings")
                            stop_id_idx += 1

                        num_transit_lines_created += 1
                    except Exception as e:
                        WranglerLogger.warning(f"Failed to create line [{emme_transit_line_id}]: {e}")

        WranglerLogger.info(f"Created {num_transit_lines_created:,} emme transit lines")

    # Scenario.publish_network
    emme_scenario.publish_network(emme_network)
    WranglerLogger.info(f"Published scenario network for {network_mode}")

    # for checking for timeperiod scoped columns
    scoped_lanes_mask  = model_roadway_net.links_df['sc_lanes'].apply(lambda x: isinstance(x, list))
    scoped_access_mask = model_roadway_net.links_df['sc_access'].apply(lambda x: isinstance(x, list))
    scoped_price_mask  = model_roadway_net.links_df['sc_price'].apply(lambda x: isinstance(x, list))

    # update roadway links with scoped managed lanes
    scoped_links_df = model_roadway_net.links_df.loc[ 
        (model_roadway_net.links_df['managed'] == 1) &
        (scoped_lanes_mask | scoped_access_mask | scoped_price_mask)
    ].copy()
    # map to emme node ids
    scoped_links_df['A_emme'] = scoped_links_df['A'].map(model_node_id_to_emme_id)
    scoped_links_df['B_emme'] = scoped_links_df['B'].map(model_node_id_to_emme_id)
    scoped_links_df['GP_A_emme'] = scoped_links_df['GP_A'].map(model_node_id_to_emme_id)
    scoped_links_df['GP_B_emme'] = scoped_links_df['GP_B'].map(model_node_id_to_emme_id)

    WranglerLogger.debug(f"TIME_PERIOD_TO_LABEL:{models.TIME_PERIOD_TO_LABEL}")
    WranglerLogger.debug(f"Scoped roadway links:\n{scoped_links_df}")
    # default to 0
    scoped_columns = []
    for time_period in models.MTC_TIME_PERIODS.keys():
        scoped_links_df[f'lanes {time_period}'] = 0
        scoped_links_df[f'price {time_period}'] = 0
        scoped_links_df[f'access {time_period}'] = 'all'
        scoped_columns = scoped_columns + [f'lanes {time_period}',f'price {time_period}',f'access {time_period}']

        # scoped_links_df[f'access {time_period}'] = 0
    # this is slow but we don't typically have that many scoped links
    for _, scoped_link in scoped_links_df.iterrows():
        model_link_id = scoped_link.model_link_id
        # WranglerLogger.debug(f"model_link_id {model_link_id} scoped_link:\n{scoped_link}")

        # if there are no lanes, nothing to do
        if not isinstance(scoped_link.sc_lanes, list): continue

        # convert to { time_period_str: price_val }
        scoped_price_dict = {}
        if isinstance(scoped_link.sc_price, list):
            for scoped_price in scoped_link.sc_price:
                time_period_str = f"{scoped_price.timespan[0]}-{scoped_price.timespan[1]}"
                if time_period_str not in models.TIME_PERIOD_TO_LABEL: continue
                scoped_price_dict[models.TIME_PERIOD_TO_LABEL[time_period_str]] = \
                    scoped_price.value

        # convert to { time_period_str: access_val }
        scoped_access_dict = {}
        if isinstance(scoped_link.sc_access, list):
            for scoped_access in scoped_link.sc_access:
                time_period_str = f"{scoped_access.timespan[0]}-{scoped_access.timespan[1]}"
                if time_period_str not in models.TIME_PERIOD_TO_LABEL: continue
                scoped_access_dict[models.TIME_PERIOD_TO_LABEL[time_period_str]] = \
                    scoped_access.value

        # go through the scoped lanes dicts
        for scoped_lanes in scoped_link.sc_lanes:
            # WranglerLogger.debug(f"scoped_lanes: {scoped_lanes}")
            # set 'lanes TIMEPERIOD'
            time_period_str = f"{scoped_lanes.timespan[0]}-{scoped_lanes.timespan[1]}"
            if time_period_str not in models.TIME_PERIOD_TO_LABEL: continue

            time_period = models.TIME_PERIOD_TO_LABEL[time_period_str]
            scoped_links_df.loc[ scoped_links_df['model_link_id']== model_link_id,
                                f'lanes {time_period}'] = scoped_lanes.value

            # set price
            # TODO: update for TM2 toll lookup
            if time_period in scoped_price_dict:
                scoped_links_df.loc[ scoped_links_df['model_link_id']== model_link_id,
                                    f'price {time_period}'] = scoped_price_dict[time_period]                

            # express lanes access only applies for free -- so don't set if price is set...
            elif time_period in scoped_access_dict:
                scoped_links_df.loc[ scoped_links_df['model_link_id']== model_link_id,
                                    f'access {time_period}'] = scoped_access_dict[time_period]

    WranglerLogger.debug(
        f"After preprocessing, Scoped roadway links:\n"
        f"{scoped_links_df[['A','B','A_emme','B_emme','GP_A','GP_B','GP_A_emme','GP_B_emme','access','price','lanes']+scoped_columns]}"
    )

    # create time of day scenarios
    for time_period_config in tm2_config.time_periods:
        time_period = time_period_config.name.upper()
        WranglerLogger.info(f"Creating emme_scenario for timeperiod='{time_period}' {type(time_period)}")
        emme_period_scenario = emme_emmebank.copy_scenario(
            source_id = emme_scenario.id,
            destination_id = time_period_config.emme_scenario_id
        )
        emme_period_scenario.title = f"{network_mode}, {time_period}"
        emme_period_network = emme_period_scenario.get_network()

        # make updates to roadway based on scoped links
        links_modified = 0
        for _, scoped_link in scoped_links_df.iterrows():
            # nothing to do
            if scoped_link[f'lanes {time_period}'] == 0: continue

            # otherwise, set lanes and remove from GP_lanes
            managed_link = emme_period_network.link(scoped_link.A_emme, scoped_link.B_emme)
            gp_link = emme_period_network.link(scoped_link.GP_A_emme, scoped_link.GP_B_emme)

            managed_link.num_lanes = scoped_link[f'lanes {time_period}']
            gp_link.num_lanes = max(gp_link.num_lanes - scoped_link[f'lanes {time_period}'], 0)
            links_modified += 2

            if scoped_link[f'price {time_period}'] > 0:
                # TODO: price -> toll mapping, access?
                pass
            elif scoped_link[f'access {time_period}'] != 'all':
                # TODO: update access via modes
                pass

        WranglerLogger.info(f"  Updated {links_modified} roadway links for time-period specific modifications")
        # for transit, we need to handle time periods and set frequencies
        if network_mode != "transit": continue

        deleted_lines = 0
        modified_lines = 0
        for _, trip in mtc_scenario.transit_net.feed.trips.iterrows():
            emme_transit_line_id = f"'{trip.route_id} {trip.shape_id}'"
            emme_transit_line_id = emme_transit_line_id.replace(':20230930','')

            # get frequencies for this trip
            trip_freqs_df = mtc_scenario.transit_net.feed.frequencies.loc[ mtc_scenario.transit_net.feed.frequencies['trip_id'] == trip.trip_id]
            trip_freqs_df.set_index('time_period', inplace=True)
            WranglerLogger.debug(f"trip_freqs_df:\n{trip_freqs_df}")
            headway_secs = trip_freqs_df['headway_secs'].to_dict()
            WranglerLogger.debug(f"headway_secs:{headway_secs} keys={headway_secs.keys()} types={[type(key) for key in headway_secs.keys()]}")
            WranglerLogger.debug(f"{time_period} in headway_secs.keys()? {time_period in headway_secs.keys()}")

            # delete the transit line if it's not running in this time period
            if time_period in headway_secs.keys(): 
                # set headway, which is in minutes
                headway_min = headway_secs[time_period]/60.0
                if headway_min > 1000:
                    WranglerLogger.warning(
                        f"Headway for {trip.trip_id} in {time_period} is "
                        f"{headway_min} mins, which is higher than EMME max; setting to 999"
                    )
                    headway_min = 999
                emme_period_network.transit_line(emme_transit_line_id).headway = headway_min
                modified_lines += 1
            else:
                emme_period_network.delete_transit_line(emme_transit_line_id)
                deleted_lines += 1

        WranglerLogger.info(f"  Deleted {deleted_lines:,} transit lines and set headways for {modified_lines:,} for this time period")
        # Scenario.publish_network
        emme_period_scenario.publish_network(emme_period_network)
    
    return

if __name__ == "__main__":
    # Setup pandas display options
    pd.options.display.max_columns = None
    pd.options.display.width = None
    pd.options.display.max_rows = 300 # number of rows to show before truncating
    
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
    mtc_scenario.road_net.config.MODEL_ROADWAY.ADDITIONAL_COPY_FROM_GP_LINK_TO_ML = [
        # these are in the mtc_roadway_schema: MTCRoadLinksTable
        "county",
        "ft",
        # useclass?
        "length", # network_wrangler uses distance
        "tolltype",
        "tollbooth",
        # "tollseg" # To be implemented
    ]
    mtc_scenario.config.ADDITIONAL_COPY_TO_ACCESS_EGRESS = [
        "county"
    ]
    mtc_scenario.road_net.config.MODEL_ROADWAY.ADDITIONAL_COPY_FROM_GP_NODE_TO_ML = [
        "county",
        "taz_centroid",
        "maz_centroid",
        "is_ctrl_acc_hwy",
        "is_interchange"
    ]   
    # Managed lane offset is 4_500_000: https://bayareametro.github.io/tm2py/inputs/#county-node-numbering-system
    # but we can't use that because there are some managed lanes for two-way links
    mtc_scenario.road_net.config.IDS.ML_NODE_ID_METHOD = 'range'
    # TODO: This is for alameda...
    mtc_scenario.road_net.config.IDS.ML_NODE_ID_RANGE = (7_000_000, 7_500_000 - 1)

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
    # drive, transit
    # TODO: WAIT WAIT WAIT ARE WE?
    for network_mode in ['drive','transit']:
        create_emmebank_network(network_mode, mtc_scenario, model_roadway_net, tm2_config, emme_app)

    # list project databases
    for db in emme_app.project.databases:
        WranglerLogger.info(f"database: type={type(db)} name={db.name()} title={db.title()} path={db.title()}")
        for scenario in db.scenarios():
            WranglerLogger.info(f"scenario: type={type(scenario)} number={scenario.number()} title={scenario.title()}")

    emme_app.project.save()

    # emme_app.close()