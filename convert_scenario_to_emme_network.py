r"""Converts an MTC network wrangler Scenario to an Emme model network.

References:
  * Asana: GMNS+ / NetworkWrangler2 > Build 2023 network from scratch
    https://app.asana.com/1/11860278793487/project/15119358130897/task/1210468893117122

 Example usage: 
 
 python convert_scenario_to_emme_network.py 
   --ovewrite
   "M:\Development\Travel Model Two\Supply\Network Creation 2025\from_OSM\BayArea\7_scenario\mtc_2023_scenario.yml" 
   "E:\GitHub\tm2\tm2py-utils\tm2py_utils\config\develop"
   "M:\Development\Travel Model Two\Supply\Network Creation 2025\from_OSM\BayArea\7_scenario\emme"

"""

USAGE = __doc__
import argparse
import pathlib
import shutil

import network_wrangler
from network_wrangler import WranglerLogger
from network_wrangler.scenario import load_scenario

import inro.emme.desktop.app as _app
import inro.emme.database.emmebank as _emmebank

# for model/scenario config
from tm2py.config import Configuration

if __name__ == "__main__":
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
        prev_proj = output_dir / "wrangler_project"
        if prev_proj.exists():
            shutil.rmtree(prev_proj)
            WranglerLogger.info(f"Found existing project at {prev_proj}; removed.")

    emme_project = _app.create_project(output_dir, name="wrangler_project")
    WranglerLogger.info(f"Created emme_project: {emme_project}")
    emme_app = _app.start_dedicated(
        visible=True,
        user_initials='MTC',
        project= output_dir / "wrangler_project" / "wrangler_project.emp"
    )
    WranglerLogger.info(f"Started emme_app: {emme_app}")
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
    DB_DIR = output_dir / "wrangler_project" / "Database"
    DB_DIR.mkdir(parents=True, exist_ok=True)
    emme_emmebank = _emmebank.create(DB_DIR / "emmebank", emme_emmebank_dimensions)
    emme_emmebank.unit_of_length='mi'
    emme_emmebank.coord_unit_length=1.0/5280.0  # coord_unit = feet
    WranglerLogger.info(f"Created emme_emmebank {emme_emmebank}")
    WranglerLogger.info(f"{emme_emmebank.unit_of_length=}")
    WranglerLogger.info(f"{emme_emmebank.coord_unit_length=}")
    WranglerLogger.info(f"{emme_emmebank.dimensions=}")

    mtc_scenario = load_scenario(args.input_scenario_yml)
    WranglerLogger.debug(f"mtc_scenario:\n{mtc_scenario}")

    # test
    managed_links_df = mtc_scenario.road_net.links_df.of_type.managed
    WranglerLogger.debug(f"managed_links_df:\n{managed_links_df}")
    WranglerLogger.debug(f"managed\n{mtc_scenario.road_net.links_df['managed'].value_counts(dropna=False)}")

    # create ModelRoadwayNetwork instance
    model_roadway_net = mtc_scenario.road_net.model_net
    WranglerLogger.debug(f"model_roadway_net:\n{model_roadway_net}")
    model_roadway_net.write(output_dir, overwrite=True, true_shape=True)
    WranglerLogger.info(f"Wrote model_roadway_net to {output_dir}")

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