r"""Converts an MTC network wrangler Scenario to a Cube-readable travel model network.

Leverages the `cube_wrangler` library (https://github.com/network-wrangler/cube_wrangler, 
to write out:
  * a fixed-width roadway network plus the Cube `NETWORK` build script that reads it
    (`cube_wrangler.roadway.write_roadway_as_fixedwidth`)
  * an optional roadway shapefile/csv for GIS QA (`cube_wrangler.roadway.write_roadway_as_shp`)
  * a Cube PT line file (.lin) with one LINE per GTFS trip, built directly from the
    scenario's TransitNetwork feed (see write_cube_transit_lines() below)

Unlike Emme -- which models each time period as a separate Scenario/network copy -- a single
Cube network carries every time-of-day value as a suffixed column (e.g. `lanes_AM`,
`price_sov_PM`) and a single .lin file carries `FREQ[1..5]` per line, so there is only ever
one roadway network and one transit line file written per scenario.

Known limitations / TODOs (flagged inline below):
  * cube_wrangler.roadway.write_roadway_as_fixedwidth() references `parameters.string_col`,
    which is not actually defined on cube_wrangler.parameters.Parameters (library bug as of
    cube-wrangler 0.2.1) -- worked around here by setting it after construction.
  * cube_wrangler.transit.StandardTransit.write_as_cube_lin() depends on a `trip_cube_df`
    attribute that is never populated anywhere in the library, so it cannot be used as-is;
    this script writes the .lin file directly instead (see write_cube_transit_lines()).
  * Managed-lane (ML) time-of-day "steal a GP lane" logic (handled per-time-period-scenario
    in the Emme script) is not replicated here. ML_lanes_* columns are written, but the
    general-purpose lanes_* columns are not reduced to account for them.
  * Cube transit MODE numbers are looked up from GTFS agency_name (+ route_long_name/
    short_name/desc keywords for operators that run both local and express/BRT/LRT service)
    against MTC's documented operator-coded scheme:
    https://github.com/BayAreaMetro/modeling-website/wiki/TransitModes
    Unrecognized agencies fall back to a generic mode by route_type (logged as a warning).
    OWNER numbers below are still a sequential placeholder and need to be reconciled with
    the project's actual Cube operator numbering before use in assignment/fares.

Example usage:

python convert_scenario_to_cube_network.py
  --overwrite
  "M:\Development\Travel Model Two\Supply\Network Creation 2025\from_OSM\SanMateo\7_scenario\mtc_2023_scenario.yml"
  "M:\Development\Travel Model Two\Supply\Network Creation 2025\from_OSM\SanMateo\7_scenario\cube"

Requires `cube_wrangler` to be installed in the active environment, e.g.:
  pip install -e E:\GitHub\tm2\cube_wrangler
"""

USAGE = __doc__
import argparse
import pathlib
import re
import shutil

import pandas as pd

import network_wrangler
from network_wrangler import WranglerLogger
from network_wrangler.scenario import load_scenario
from network_wrangler.roadway.model_roadway import ModelRoadwayNetwork
from network_wrangler.models.gtfs.types import RouteType

from cube_wrangler.parameters import Parameters, TimePeriodsConfig
from cube_wrangler.roadway import (
    split_properties_by_time_period_and_category,
    convert_types,
    write_roadway_as_fixedwidth,
    write_roadway_as_shp,
)

from models.mtc_roadway_schema import MTCFacilityType, MTCUseClass, COUNTY_NAME_TO_NUM
import models.mtc_network

# Official MTC line-haul mode numbering, coded by operator (and local vs. express/BRT where
# the operator runs both): https://github.com/BayAreaMetro/modeling-website/wiki/TransitModes
# The local/express/BRT/LRT distinction for a given operator is a property of the *route*
# (GTFS route_long_name/short_name/desc), not of agency_name, so each rule optionally requires
# a route-text keyword too. Rules are checked in order; the first full match wins, so
# operator-specific variants (e.g. ("ac transit", "brt", 85)) must precede that operator's
# plain fallback (e.g. ("ac transit", None, 30)).
AGENCY_ROUTE_TO_CUBE_MODE: list[tuple[str, "str | None", int]] = [
    ("ac transit", "brt", 85),
    ("ac transit", "transbay", 83),
    ("ac transit", None, 30),
    ("samtrans", "express", 80),
    ("samtrans", None, 24),
    ("vta", "community", 27),
    ("vta", "express", 81),
    ("vta", "lrt", 111),
    ("vta", "light rail", 111),
    ("vta", None, 28),
    ("county connection", "express", 86),
    ("county connection", None, 42),
    ("cccta", "express", 86),
    ("cccta", None, 42),
    ("tri delta", "brt", 96),
    ("tri delta", None, 44),
    ("tri-delta", "brt", 96),
    ("tri-delta", None, 44),
    ("westcat", "express", 90),
    ("westcat", None, 46),
    ("soltrans", None, 91),
    ("fairfield", "express", 92),
    ("fairfield", None, 52),
    ("vine", "express", 93),
    ("vine", None, 60),
    ("golden gate transit", "express", 87),  # doesn't distinguish SF (87) vs Richmond (88)
    ("golden gate transit", None, 70),
    ("golden gate ferry", "larkspur", 101),
    ("golden gate ferry", None, 102),
    ("smart", "express", 94),
    ("smart", None, 135),
    ("muni", "cable", 20),
    ("muni", "metro", 110),
    ("muni", "brt", 89),
    ("muni", None, 21),
    ("regional express rex", "express", 98),
    ("regional express rex", None, 78),
    ("caltrain", "shuttle", 14),
    ("caltrain", None, 130),
    ("wheels", "shuttle", 17),
    ("wheels", None, 33),
    ("amtrak", "shuttle", 18),
    ("amtrak", "capitol corridor", 131),
    ("amtrak", "san joaquin", 132),
    ("west berkeley", None, 10),
    ("broadway shuttle", None, 11),
    ("emery go", None, 12),  # "Emery Go-Round"
    ("stanford", None, 13),
    ("menlo park", None, 16),
    ("palo alto", None, 16),
    ("san leandro links", None, 19),
    ("union city transit", None, 38),
    ("vallejo transit", None, 49),
    ("american canyon", None, 55),
    ("vacaville city coach", None, 56),
    ("benicia breeze", None, 58),
    ("sonoma county transit", None, 63),
    ("santa rosa", None, 66),
    ("petaluma transit", None, 68),
    ("north bay", None, 69),
    ("contra costa av", None, 75),
    ("dumbarton express", None, 82),
    ("dumbarton group rapid transit", None, 112),
    ("dumbarton rail", None, 134),
    ("east bay ferries", None, 100),
    ("vallejo baylink", None, 104),
    ("south san francisco ferry", None, 105),
    ("regional hovercraft", None, 106),
    ("treasure island ferry", None, 107),
    ("blue & gold", None, 103),
    ("angel island", None, 103),
    ("oakland/alameda gondola", None, 113),
    ("maglev", None, 114),
    ("sr-85", None, 115),
    ("mountain view avn", None, 116),
    ("contra costa gondola", None, 117),
    ("e-bart", None, 120),
    ("bart", None, 120),
    ("oakland airport connector", None, 121),
    ("valley link", None, 136),
    ("high-speed rail", None, 137),
    ("high speed rail", None, 137),
]
_ACE_AGENCY_RE = re.compile(r"\bace\b", re.I)  # Altamont Corridor Express; avoid substring false-positives

# fallback when no AGENCY_ROUTE_TO_CUBE_MODE rule matches
ROUTE_TYPE_TO_CUBE_MODE_DEFAULT = {
    RouteType.TRAM       : 110,  # light rail
    RouteType.SUBWAY     : 120,  # heavy rail
    RouteType.RAIL       : 130,  # commuter rail
    RouteType.BUS        : 21,   # local bus
    RouteType.FERRY      : 100,  # ferry
    RouteType.CABLE_TRAM : 20,   # cable car
    RouteType.TROLLEYBUS : 21,   # local bus
}


def get_cube_transit_mode(agency_name: str, route_row: pd.Series, route_type: RouteType) -> int:
    """Look up the Cube transit MODE number for a GTFS route, per MTC's operator-coded scheme.

    See https://github.com/BayAreaMetro/modeling-website/wiki/TransitModes

    Args:
        agency_name: GTFS agency_name for the route's operator
        route_row: row from feed.routes, used to detect express/BRT/LRT variants by keyword
            in route_long_name / route_short_name / route_desc
        route_type: GTFS route_type, used only as a fallback if nothing else matches

    Returns:
        Cube MODE number
    """
    agency_name_lower = (agency_name or "").lower()
    route_text_lower = " ".join(str(route_row.get(c, "") or "") for c in
        ("route_long_name", "route_short_name", "route_desc")).lower()

    if _ACE_AGENCY_RE.search(agency_name_lower):
        return 133  # Altamont Corridor Express

    for agency_keyword, route_keyword, mode in AGENCY_ROUTE_TO_CUBE_MODE:
        if agency_keyword not in agency_name_lower:
            continue
        if route_keyword is not None and route_keyword not in route_text_lower:
            continue
        return mode

    default_mode = ROUTE_TYPE_TO_CUBE_MODE_DEFAULT.get(route_type, 21)
    WranglerLogger.warning(
        f"No Cube mode mapping for agency_name='{agency_name}'; "
        f"defaulting to {default_mode} based on route_type={route_type}"
    )
    return default_mode




def fix_missing_fields(model_roadway_net: ModelRoadwayNetwork):
    """Fill in missing fields in the model_roadway_net tables and establish node sort order.

    Identical in spirit to the function of the same name in convert_scenario_to_emme_network.py;
    duplicated here so this script has no dependency on the Emme API.

    Args:
        model_roadway_net (ModelRoadwayNetwork): network to modify in place
    """
    # county: default to ''
    model_roadway_net.nodes_df['county'] = model_roadway_net.nodes_df['county'].replace({None:''}).fillna('')

    # taz_centroid / maz_centroid: default to False
    model_roadway_net.nodes_df.loc[pd.isnull(model_roadway_net.nodes_df['taz_centroid']), 'taz_centroid'] = 0
    model_roadway_net.nodes_df['taz_centroid'] = model_roadway_net.nodes_df['taz_centroid'].astype(bool)
    model_roadway_net.nodes_df.loc[pd.isnull(model_roadway_net.nodes_df['maz_centroid']), 'maz_centroid'] = 0
    model_roadway_net.nodes_df['maz_centroid'] = model_roadway_net.nodes_df['maz_centroid'].astype(bool)

    # sort order: TAZ centroids first, then MAZ centroids, then all other nodes by model_node_id
    # Cube requires that zone/centroid node numbers occupy 1..ZONES contiguously, so this sort
    # order is what makes renumber_nodes_for_cube() below produce a valid numbering.
    model_roadway_net.nodes_df['sort_group'] = 3
    model_roadway_net.nodes_df.loc[model_roadway_net.nodes_df['taz_centroid'], 'sort_group'] = 1
    model_roadway_net.nodes_df.loc[model_roadway_net.nodes_df['maz_centroid'], 'sort_group'] = 2
    model_roadway_net.nodes_df.sort_values(by=['sort_group', 'model_node_id'], inplace=True, ignore_index=True)

    # links fields from mtc_roadway_schema.MTCRoadLinksTable
    model_roadway_net.links_df['roadway'] = model_roadway_net.links_df['roadway'].replace({None:''}).fillna('')
    model_roadway_net.links_df['projects'] = model_roadway_net.links_df['projects'].replace({None:''}).fillna('')
    model_roadway_net.links_df.loc[pd.isnull(model_roadway_net.links_df['managed']), 'managed'] = 0
    model_roadway_net.links_df['ref'] = model_roadway_net.links_df['ref'].replace({None:''}).fillna('')
    model_roadway_net.links_df['county'] = model_roadway_net.links_df['county'].replace({None:''}).fillna('')

    # facility type: missing values are connectors
    model_roadway_net.links_df.loc[
        model_roadway_net.links_df['roadway'].isin(['ml_access_point', 'ml_egress_point']), 'ft'] = MTCFacilityType.CONNECTOR
    model_roadway_net.links_df['ft'] = model_roadway_net.links_df['ft'].astype(int)


def renumber_nodes_for_cube(model_roadway_net: ModelRoadwayNetwork) -> tuple[dict, int]:
    """Build a contiguous Cube node numbering (`N`) from model_node_id.

    Cube's `ZONES = <n>` directive requires that zone/centroid nodes occupy node numbers
    1..ZONES contiguously. MTC's model_node_id county-numbering scheme does not satisfy that,
    so (like the Emme script's model_node_id_to_emme_id mapping) we assign a fresh sequential
    id, relying on the TAZ-centroids-first sort order established in fix_missing_fields().

    Args:
        model_roadway_net: network whose nodes_df must already be sorted by
          ['sort_group', 'model_node_id'] (see fix_missing_fields)

    Returns:
        (model_node_id_to_cube_id, num_tazs)
    """
    model_roadway_net.nodes_df['N'] = range(1, len(model_roadway_net.nodes_df) + 1)
    node_id_map = dict(zip(model_roadway_net.nodes_df['model_node_id'], model_roadway_net.nodes_df['N']))
    num_tazs = int(model_roadway_net.nodes_df['taz_centroid'].sum())
    WranglerLogger.info(f"Renumbered {len(node_id_map):,} nodes for Cube; {num_tazs:,} are TAZ centroids (N=1..{num_tazs})")
    return node_id_map, num_tazs


def fix_cube_fields(model_roadway_net: ModelRoadwayNetwork, node_id_map: dict):
    """Map MTC/network_wrangler fields to the column names/types cube_wrangler expects.

    Modifies model_roadway_net.nodes_df and .links_df in place.

    Args:
        model_roadway_net: network to modify in place
        node_id_map: model_node_id -> Cube N, from renumber_nodes_for_cube()
    """
    links_df = model_roadway_net.links_df
    nodes_df = model_roadway_net.nodes_df

    # retain the original model node ids for traceability, then remap A/B to Cube node numbers
    links_df['model_a_node_id'] = links_df['A']
    links_df['model_b_node_id'] = links_df['B']
    links_df['A'] = links_df['A'].map(node_id_map)
    links_df['B'] = links_df['B'].map(node_id_map)
    if 'GP_A' in links_df.columns:
        links_df['GP_A'] = links_df['GP_A'].map(node_id_map)
        links_df['GP_B'] = links_df['GP_B'].map(node_id_map)

    # roadway_class <- ft; centroidconnect <- ft==CONNECTOR
    links_df['roadway_class'] = links_df['ft'].astype(int)
    links_df['centroidconnect'] = (links_df['ft'] == MTCFacilityType.CONNECTOR).astype(int)
    # TODO: assign_group is used by Cube VDFs/toll lookups; defaulting to roadway_class until
    # the project defines a real assignment-group scheme for the rebuilt network
    links_df['assign_group'] = links_df['roadway_class']

    # county name -> int code (1-9 Bay Area counties; 0 for External/unmapped)
    links_df['county'] = links_df['county'].map(COUNTY_NAME_TO_NUM).fillna(0).astype(int)

    # truck_access: excluded only when useclass marks the link as NO_TRUCKS
    if 'useclass' in links_df.columns:
        links_df['truck_access'] = links_df['drive_access'] & (links_df['useclass'] != MTCUseClass.NO_TRUCKS)
    else:
        links_df['truck_access'] = links_df['drive_access']

    # bike/walk indicator columns (as plain ints, matching cube_wrangler conventions)
    links_df['bike'] = links_df['bike_access'].astype(int)
    links_df['walk'] = links_df['walk_access'].astype(int)

    # node-level mode-presence flags, derived from the links incident on each node
    for flag_col, access_col in [
        ('drive_node', 'drive_access'),
        ('walk_node', 'walk_access'),
        ('bike_node', 'bike_access'),
    ]:
        access_links = links_df.loc[links_df[access_col]]
        node_ids_with_access = pd.concat([access_links['A'], access_links['B']]).unique()
        nodes_df[flag_col] = nodes_df['N'].isin(node_ids_with_access).astype(int)

    transit_links = links_df.loc[links_df['rail_only'] | links_df['bus_only'] | links_df['ferry_only']]
    transit_node_ids = pd.concat([transit_links['A'], transit_links['B']]).unique()
    nodes_df['transit_node'] = nodes_df['N'].isin(transit_node_ids).astype(int)

    # Cube wants projected coordinates (feet); geometry should already be reprojected by caller
    nodes_df['X'] = nodes_df['geometry'].x
    nodes_df['Y'] = nodes_df['geometry'].y


def build_cube_parameters(output_dir: pathlib.Path, num_tazs: int) -> Parameters:
    """Build a cube_wrangler Parameters instance for this run.

    Args:
        output_dir: directory to write Cube network files into
        num_tazs: number of TAZ centroids (-> Parameters.zones)

    Returns:
        Parameters instance
    """
    cube_params = Parameters(
        scratch_location=output_dir,
        settings_location=output_dir / "cube_wrangler_settings",
        zones=num_tazs,
        # NT stands in for MTC's EV (evening) period; cube_wrangler only has 5 period slots
        time_periods=TimePeriodsConfig(
            EA=tuple(models.mtc_network.MTC_TIME_PERIODS['EA']),
            AM=tuple(models.mtc_network.MTC_TIME_PERIODS['AM']),
            MD=tuple(models.mtc_network.MTC_TIME_PERIODS['MD']),
            PM=tuple(models.mtc_network.MTC_TIME_PERIODS['PM']),
            NT=tuple(models.mtc_network.MTC_TIME_PERIODS['EV']),
        ),
    )
    # work around cube_wrangler bugs: write_roadway_as_fixedwidth()/project.py read
    # parameters.string_col and parameters.output_epsg, but Parameters never defines either
    # (as of cube-wrangler 0.2.1)
    cube_params.string_col = ['roadway', 'name', 'county']
    cube_params.output_epsg = models.mtc_network.LOCAL_CRS_FEET

    # work around cube_wrangler bug: rename_variables_for_dbf() (used by write_roadway_as_shp())
    # requires parameters.net_to_dbf_crosswalk to exist, but the library ships no such file;
    # an empty crosswalk is fine -- unmapped columns just keep their original (network) name
    cube_params.net_to_dbf_crosswalk.parent.mkdir(parents=True, exist_ok=True)
    cube_params.net_to_dbf_crosswalk.write_text("net,dbf\n")
    return cube_params


def write_cube_roadway_network(model_roadway_net: ModelRoadwayNetwork, cube_params: Parameters, output_dir: pathlib.Path):
    """Write the Cube roadway network (fixed-width + build script, plus a QA shapefile).

    Args:
        model_roadway_net: network with Cube-ready columns (see fix_cube_fields)
        cube_params: Parameters from build_cube_parameters()
        output_dir: directory to write into
    """
    # split scoped lanes/ML_lanes/price/access/trn_priority/ttime_assert properties into
    # per-time-period columns (e.g. lanes_AM, price_PM, access_EA).
    # Build our own properties_to_split rather than using cube_params.properties_to_split
    # (which splits "price" by category too): cube_wrangler passes the whole fallback list
    # (e.g. ["sov", "default"]) as prop_for_scope's `category` arg, but prop_for_scope only
    # accepts a single str/int category, so the categories codepath raises a pydantic
    # ValidationError. MTC's network doesn't vary price/access by vehicle category here, so
    # time-period-only splitting (no "categories" key) sidesteps that cube_wrangler bug.
    time_period_to_time = cube_params.time_period_to_time
    properties_to_split = {
        prop: {"v": prop, "time_periods": time_period_to_time}
        for prop in ["trn_priority", "ttime_assert", "lanes", "ML_lanes", "price", "access"]
    }
    split_properties_by_time_period_and_category(model_roadway_net, properties_to_split=properties_to_split)

    # coerce column types to the CubeLinksTable / CubeNodesTable schemas
    convert_types(model_roadway_net)

    time_suffixes = list(cube_params.time_period_to_time.keys())
    link_output_variables = ['model_link_id', 'A', 'B', 'model_a_node_id', 'model_b_node_id',
        'distance', 'roadway', 'name', 'roadway_class', 'assign_group', 'county', 'centroidconnect',
        'rail_only', 'bus_only', 'drive_access', 'bike_access', 'walk_access', 'truck_access', 'bike', 'walk',
        'managed', 'geometry',
    ]
    link_output_variables += [f'lanes_{tp}' for tp in time_suffixes]
    link_output_variables += [f'ML_lanes_{tp}' for tp in time_suffixes]
    link_output_variables += [f'price_{tp}' for tp in time_suffixes]
    link_output_variables += [f'access_{tp}' for tp in time_suffixes]
    link_output_variables = [c for c in link_output_variables if c in model_roadway_net.links_df.columns]

    node_output_variables = ['model_node_id', 'N', 'osm_node_id', 'drive_node', 'walk_node',
        'bike_node', 'transit_node', 'X', 'Y', 'geometry',
    ]
    node_output_variables = [c for c in node_output_variables if c in model_roadway_net.nodes_df.columns]

    write_roadway_as_fixedwidth(
        roadway_net=model_roadway_net,
        parameters=cube_params,
        zones=cube_params.zones,
        link_output_variables=link_output_variables,
        node_output_variables=node_output_variables,
    )
    WranglerLogger.info(f"Wrote Cube fixed-width network + build script to {output_dir}")

    write_roadway_as_shp(
        roadway_net=model_roadway_net,
        parameters=cube_params,
        link_output_variables=link_output_variables,
        node_output_variables=node_output_variables,
        data_to_csv=True,
        data_to_dbf=False,
    )
    WranglerLogger.info(f"Wrote Cube QA shapefiles/csvs to {output_dir}")


def write_cube_transit_lines(mtc_scenario: network_wrangler.Scenario, node_id_map: dict, output_lin_file: pathlib.Path):
    """Write the scenario's GTFS transit feed out as a Cube PT line (.lin) file.

    cube_wrangler.transit.StandardTransit.write_as_cube_lin() is not usable as-is (see module
    docstring), so this builds the Cube `LINE` blocks directly, one per GTFS trip, following the
    format used in TM1's trn/transitLines.lin (LINE NAME=..., FREQ[1..5]=..., MODE=..., N=...).

    Args:
        mtc_scenario: Scenario with a populated transit_net.feed
        node_id_map: model_node_id -> Cube N, from renumber_nodes_for_cube()
        output_lin_file: path to write the .lin file to
    """
    feed = mtc_scenario.transit_net.feed
    time_period_codes = list(models.mtc_network.MTC_TIME_PERIODS.keys())  # EA, AM, MD, PM, EV order -> FREQ[1..5]

    # add time_period label to frequencies, same approach as convert_scenario_to_emme_network.py
    if 'time_period' not in feed.frequencies.columns:
        start_str = feed.frequencies['start_time'].dt.strftime('%H:%M')
        end_str = feed.frequencies['end_time'].dt.strftime('%H:%M')
        feed.frequencies['time_period'] = (start_str + '-' + end_str).map(models.mtc_network.TIME_PERIOD_TO_LABEL)

    feed.shapes['shape_cube_node_id'] = feed.shapes['shape_model_node_id'].map(node_id_map)

    agency_id_to_name = dict(zip(feed.agencies['agency_id'], feed.agencies['agency_name']))
    agency_id_to_owner = {agency_id: idx + 1 for idx, agency_id in enumerate(feed.agencies['agency_id'])}

    lines_written = 0
    lines_skipped = 0
    with output_lin_file.open('w') as lin_file:
        lin_file.write(';;<<Trnbuild>>;;\n\n')

        for _, route_row in feed.routes.iterrows():
            route_id = route_row['route_id']
            route_type = RouteType(route_row['route_type'])
            agency_name = agency_id_to_name.get(route_row['agency_id'], '')
            cube_mode = get_cube_transit_mode(agency_name, route_row, route_type)

            route_trips_df = feed.trips.loc[feed.trips['route_id'] == route_id]
            for _, trip_row in route_trips_df.iterrows():
                trip_id = trip_row['trip_id']
                shape_id = trip_row['shape_id']

                trip_shapes_df = feed.shapes.loc[feed.shapes['shape_id'] == shape_id]
                if len(trip_shapes_df) < 2:
                    WranglerLogger.warning(f"Trip {trip_id} (route {route_id}) has <2 shape points; skipping")
                    lines_skipped += 1
                    continue

                trip_freqs_df = feed.frequencies.loc[feed.frequencies['trip_id'] == trip_id].set_index('time_period')
                headway_secs = trip_freqs_df['headway_secs'].to_dict()
                # Cube FREQ is a headway in minutes; 0 means the line doesn't run that period
                freqs = []
                for tp_code in time_period_codes:
                    if tp_code in headway_secs:
                        freqs.append(min(headway_secs[tp_code] / 60.0, 999.0))
                    else:
                        freqs.append(0.0)
                if not any(f > 0 for f in freqs):
                    lines_skipped += 1
                    continue

                # Cube LINE NAME must be unique and reasonably short; quote it per Cube convention
                line_name = f"{route_id}_{shape_id}".replace(' ', '_').replace(',', '_')
                if len(line_name) > 38:
                    line_name = line_name[:38]
                long_name = str(route_row.get('route_long_name', '') or '').replace('"', "'")

                node_itinerary = []
                for _, shape_row in trip_shapes_df.iterrows():
                    cube_node_id = shape_row['shape_cube_node_id']
                    is_stop = pd.notnull(shape_row.get('stop_id'))
                    node_itinerary.append(cube_node_id if is_stop else -cube_node_id)

                lin_file.write(f'LINE NAME="{line_name}",\n')
                lin_file.write('    COLOR=1,\n')
                for i, freq in enumerate(freqs, start=1):
                    lin_file.write(f'    FREQ[{i}]={freq:.5g},\n')
                lin_file.write(f'    LONGNAME="{long_name}",\n')
                lin_file.write(f'    MODE={cube_mode},\n')
                lin_file.write('    ONEWAY=T,\n')
                lin_file.write(f'    OWNER="{agency_id_to_owner.get(route_row["agency_id"], 1)}",\n')
                lin_file.write(' N=' + ',\n    '.join(str(n) for n in node_itinerary) + '\n\n')
                lines_written += 1

    WranglerLogger.info(f"Wrote {lines_written:,} Cube transit lines to {output_lin_file} ({lines_skipped:,} skipped)")


if __name__ == "__main__":
    pd.options.display.max_columns = None
    pd.options.display.width = None
    pd.options.display.max_rows = 300

    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--overwrite", action="store_true", help="Delete previous version (otherwise it will error)")
    parser.add_argument("input_scenario_yml", type=pathlib.Path, help="Network Wrangler scenario yaml")
    parser.add_argument("output_dir", type=pathlib.Path, help="Output directory")
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.overwrite:
        for pattern in ["complete_network.*", "links.*", "nodes.*", "*.lin", "*.s", "*.log"]:
            for existing_file in output_dir.glob(pattern):
                existing_file.unlink()

    network_wrangler.setup_logging(
        info_log_filename=output_dir / "convert_scenario_to_cube_network.info.log",
        debug_log_filename=output_dir / "convert_scenario_to_cube_network.debug.log",
        std_out_level="info",
        file_mode='w'
    )

    mtc_scenario = load_scenario(args.input_scenario_yml)
    mtc_scenario.road_net._shapes_df = mtc_scenario.road_net.links_df
    WranglerLogger.debug(f"mtc_scenario:\n{mtc_scenario}")

    # same ModelRoadwayNetwork setup as convert_scenario_to_emme_network.py
    mtc_scenario.road_net.config.MODEL_ROADWAY.ADDITIONAL_COPY_FROM_GP_LINK_TO_ML = [
        "county", "ft", "length", "tolltype", "tollbooth",
    ]
    mtc_scenario.config.ADDITIONAL_COPY_TO_ACCESS_EGRESS = ["county"]
    mtc_scenario.road_net.config.MODEL_ROADWAY.ADDITIONAL_COPY_FROM_GP_NODE_TO_ML = [
        "county", "taz_centroid", "maz_centroid", "is_ctrl_acc_hwy", "is_interchange",
    ]
    # Size ML/access/egress link ids off the network's actual max link id (rather than a
    # hardcoded guess) so none of the four id "bands" -- GP, ML, access, egress -- can ever
    # collide, regardless of whether this is a single-county or full Bay Area extent.
    # access_df/egress_df model_link_id = <offset> + <ML_LINK_ID_SCALAR> + GP_model_link_id
    # (see network_wrangler.roadway.model_roadway._create_dummy_connector_links), so each band
    # must be a multiple of link_id_band, which itself exceeds every real GP link id.
    max_node_id = int(mtc_scenario.road_net.nodes_df['model_node_id'].max())
    max_link_id = int(mtc_scenario.road_net.links_df['model_link_id'].max())
    ml_node_start = 10 ** len(str(max_node_id))
    link_id_band = 10 ** len(str(max_link_id))

    mtc_scenario.road_net.config.IDS.ML_NODE_ID_METHOD = 'range'
    mtc_scenario.road_net.config.IDS.ML_NODE_ID_RANGE = (ml_node_start, ml_node_start + 500_000 - 1)
    mtc_scenario.road_net.config.IDS.ML_LINK_ID_SCALAR = link_id_band                # ML:      [1*band, 2*band)
    mtc_scenario.road_net.config.MODEL_ROADWAY.ACCESS_LINK_ID_OFFSET = link_id_band  # access:  [2*band, 3*band)
    mtc_scenario.road_net.config.MODEL_ROADWAY.EGRESS_LINK_ID_OFFSET = 2 * link_id_band  # egress: [3*band, 4*band)

    model_roadway_net = mtc_scenario.road_net.model_net

    fix_missing_fields(model_roadway_net)

    # Cube wants projected coordinates, in feet
    model_roadway_net.nodes_df.to_crs(crs=models.mtc_network.LOCAL_CRS_FEET, inplace=True)
    model_roadway_net.links_df.to_crs(crs=models.mtc_network.LOCAL_CRS_FEET, inplace=True)

    node_id_map, num_tazs = renumber_nodes_for_cube(model_roadway_net)
    fix_cube_fields(model_roadway_net, node_id_map)

    cube_params = build_cube_parameters(output_dir, num_tazs)
    write_cube_roadway_network(model_roadway_net, cube_params, output_dir)

    transit_lin_file = output_dir / "transitLines.lin"
    write_cube_transit_lines(mtc_scenario, node_id_map, transit_lin_file)

    WranglerLogger.info("Done.")
