import pathlib
import datetime
import getpass

COUNTY_SHAPEFILE = pathlib.Path("M:\\Data\\Census\\Geography\\tl_2010_06_county10\\tl_2010_06_county10_9CountyBayArea.shp")

INPUT_2023GTFS = pathlib.Path("M:\\Data\\Transit\\511\\2023-09")
# OUTPUT_DIR = pathlib.Path("M:\\Development\\Travel Model Two\\Supply\\Network Creation 2025\\from_OSM")
OUTPUT_DIR = pathlib.Path("data/processed")
NOW = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
USERNAME = getpass.getuser()
if USERNAME=="lmz":
    COUNTY_SHAPEFILE = pathlib.Path("../../tl_2010_06_county10/tl_2010_06_county10_9CountyBayArea.shp").resolve()
    INPUT_2023GTFS = pathlib.Path("../../511gtfs_2023-09").resolve()
    OUTPUT_DIR = pathlib.Path("../../output_from_OSM").resolve()

ROADWAY_OUTPUT_FORMATS = ['parquet','geojson']

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

NETWORK_SIMPLIFY_TOLERANCE = 20 # feet

LOCAL_CRS_FEET = "EPSG:2227"
""" NAD83 / California zone 3 (ftUS) https://epsg.io/2227 """

COUNTY_NAME_TO_GTFS_AGENCIES = {
    'San Francisco': [
        'SF', # SF Muni
        'BA', # BART
        # 'GG', # Golden Gate Transit
        'CT', # Caltrain
    ],
    'San Mateo': [
        'SM', # SamTrans
        'BA', # BART
        'CT', # Caltrain
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
    'unclassified',   # minor public roads
    'residential',    # residential street
    'living_street',  # pedestrian-focused residential
    'service',        # vehicle access to building, parking lot, etc.
    'track',          # minor land-access roads
]