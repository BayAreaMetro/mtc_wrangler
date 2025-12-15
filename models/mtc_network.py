"""MTC-specific network classes.

Provides MTC-customized versions of Network Wrangler network classes with
additional validation for MTC-required fields.
"""
import pathlib
from typing import Optional, Union

import geopandas as gpd
import pygris
import us

import pandas as pd

from network_wrangler import WranglerLogger
from network_wrangler.params import LAT_LON_CRS
from network_wrangler.roadway.network import RoadwayNetwork
from network_wrangler.utils.models import validate_df_to_model

from .mtc_roadway_schema import MTC_COUNTIES, MTCRoadLinksTable, MTCRoadNodesTable

FEET_PER_MILE = 5280.0

LOCAL_CRS_FEET = "EPSG:2227"
""" NAD83 / California zone 3 (ftUS) https://epsg.io/2227 """

LOCAL_PRJ = 'PROJCS["NAD83 / California zone 3 (ftUS)",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4269"]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["standard_parallel_1",38.43333333333333],PARAMETER["standard_parallel_2",37.06666666666667],PARAMETER["latitude_of_origin",36.5],PARAMETER["central_meridian",-120.5],PARAMETER["false_easting",6561666.667],PARAMETER["false_northing",1640416.667],UNIT["US survey foot",0.3048006096012192,AUTHORITY["EPSG","9003"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","2227"]]'

MTC_TIME_PERIODS = {
    'EA': ['03:00','06:00'],  # 3a-6a
    'AM': ['06:00','10:00'],  # 6a-10a
    'MD': ['10:00','15:00'],  # 10a-3p
    'PM': ['15:00','19:00'],  # 3p-7p
    'EV': ['19:00','03:00'],  # 7p-3a (crosses midnight)
}
""" Used for TM1 and TM2: https://bayareametro.github.io/tm2py/inputs/?h=time+period#time-periods """

TIME_PERIOD_TO_LABEL = {'-'.join(value): key for key, value in MTC_TIME_PERIODS.items()}
""" For lookup up from scoped links """

def get_county_geodataframe(
        output_dir: pathlib.Path,
        state: str
) -> gpd.GeoDataFrame:
    """
    Fetch the US Census TIGER shapefile for 2010 county shapes using pygris,
    or uses cached version if available.

    Saves to output_dir / tl_2010_us_county10 / tl_2010_us_county10.shp
    """
    county_shapefile = output_dir / "tl_2010_us_county10" / "tl_2010_us_county10.shp"
    if county_shapefile.exists():
        county_gdf = gpd.read_file(county_shapefile)
        WranglerLogger.info(f"Read {county_shapefile}")
    else:
        WranglerLogger.info(f"Fetching California 2010 county shapes using pygris")
        county_gdf = pygris.counties(state = 'CA', cache = True, year = 2010)
        # save it to the cache dir
        county_shapefile.parent.mkdir(exist_ok=True)
        county_gdf.to_file(county_shapefile)

    my_state = us.states.lookup(state)
    county_gdf = county_gdf.loc[ county_gdf["STATEFP10"] == my_state.fips]
    WranglerLogger.debug(f"county_gdf:\n{county_gdf}")
    return county_gdf

def get_county_bbox(
        counties: list[str],
        base_output_dir: pathlib.Path,
) -> tuple[float, float, float, float]:
    """
    The coordinates are converted to WGS84 (EPSG:4326) if needed.

    Args:
        counties: list of California counties to include.
        base_output_dir: Base directory for shared resources (county shapefiles)

    Returns:
        tuple: Bounding box as (west, south, east, north) in decimal degrees.
               These are longitude/latitude coordinates in WGS84 projection.

    Note:
        The returned tuple order (west, south, east, north) matches the format
        expected by osmnx.graph_from_bbox() function.
    """
    county_gdf = get_county_geodataframe(base_output_dir, "CA")
    county_gdf = county_gdf[county_gdf['NAME10'].isin(counties)].copy()

    # Get the total bounds (bounding box) of all counties
    # Returns (minx, miny, maxx, maxy)
    bbox = county_gdf.total_bounds
    WranglerLogger.info(f"Bounding box for Bay Area counties: minx={bbox[0]:.6f}, miny={bbox[1]:.6f}, maxx={bbox[2]:.6f}, maxy={bbox[3]:.6f}")
    
    # Convert to WGS84 (EPSG:4326) if not already
    if county_gdf.crs != LAT_LON_CRS:
        WranglerLogger.info(f"Converting from {county_gdf.crs} to {LAT_LON_CRS}")
        county_gdf_wgs84 = county_gdf.to_crs(LAT_LON_CRS)
        bbox = county_gdf_wgs84.total_bounds
        WranglerLogger.info(f"Bounding box in WGS84: minx={bbox[0]:.6f}, miny={bbox[1]:.6f}, maxx={bbox[2]:.6f}, maxy={bbox[3]:.6f}")
    
    # OSMnx expects (left, bottom, right, top) which is (west, south, east, north)
    # bbox is currently (minx, miny, maxx, maxy) which is (west, south, east, north)
    west = bbox[0]
    south = bbox[1]
    east = bbox[2]
    north = bbox[3]
    
    return (west, south, east, north)


def assign_county_to_geodataframes(
    links_gdf: gpd.GeoDataFrame,
    nodes_gdf: gpd.GeoDataFrame,
    base_output_dir: pathlib.Path,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Assign county attribute to links and nodes geodataframes via spatial join.

    Performs spatial joins to assign county based on geometry.
    Links that span multiple counties are assigned to the county with the longest
    intersection length.

    If a 'county' column already exists, only null or empty values will be overwritten.

    Args:
        links_gdf: GeoDataFrame of links with geometry
        nodes_gdf: GeoDataFrame of nodes with geometry
        base_output_dir: path for get_county_geodataframe()
    Returns:
        Tuple of (links_gdf, nodes_gdf) with 'county' column assigned.
    """
    WranglerLogger.info("Performing spatial join to assign counties...")

    # Read the county shapefile for spatial joins
    county_gdf = get_county_geodataframe(base_output_dir, "CA")
    county_gdf = county_gdf.rename(columns={'NAME10': 'county'})
    WranglerLogger.debug(f"county_gdf:\n{county_gdf}")
    
    # Check if county column already exists and preserve non-null/non-empty values
    links_has_county = 'county' in links_gdf.columns
    nodes_has_county = 'county' in nodes_gdf.columns

    if links_has_county:
        links_gdf = links_gdf.rename(columns={'county': 'county_existing'})
    if nodes_has_county:
        nodes_gdf = nodes_gdf.rename(columns={'county': 'county_existing'})

    # Ensure all are in the same CRS (LOCAL_CRS_FEET)
    county_gdf = county_gdf.to_crs(LOCAL_CRS_FEET)
    links_gdf = links_gdf.to_crs(LOCAL_CRS_FEET)
    nodes_gdf = nodes_gdf.to_crs(LOCAL_CRS_FEET)

    # Dissolve counties to one region shape and create convex hull
    region_shape = county_gdf.loc[ county_gdf['county'].isin(MTC_COUNTIES)].dissolve().convex_hull.iloc[0]

    # Filter to links that intersect with region
    links_gdf = links_gdf[links_gdf.intersects(region_shape)].copy()
    WranglerLogger.info(f"Filtered to {len(links_gdf):,} links intersecting region")

    # Filter nodes to only those referenced in the filtered links
    used_nodes = pd.concat([links_gdf['A'], links_gdf['B']]).unique()
    nodes_gdf = nodes_gdf[nodes_gdf['osmid'].isin(used_nodes)]
    WranglerLogger.info(f"Filtered to {len(nodes_gdf):,} nodes that are referenced in links")

    # Store expected counts after filtering (spatial joins should not change these)
    expected_links_count = len(links_gdf)
    expected_nodes_count = len(nodes_gdf)

    # Spatial join for nodes - use point geometry
    nodes_gdf = gpd.sjoin(
        nodes_gdf,
        county_gdf[['geometry', 'county']],
        how='left',
        predicate='within'
    )
    # Use "External" for nodes outside counties
    nodes_gdf['county'] = nodes_gdf['county'].fillna('External')

    # Merge back existing county values (only overwrite null/empty)
    if nodes_has_county:
        # Use existing value if it's not null and not empty string
        mask = nodes_gdf['county_existing'].notna() & (nodes_gdf['county_existing'] != '')
        nodes_gdf.loc[mask, 'county'] = nodes_gdf.loc[mask, 'county_existing']
        nodes_gdf = nodes_gdf.drop(columns=['county_existing'])

    # First, do a spatial join to find all intersecting counties
    WranglerLogger.info(f"Before joining links to counties, {len(links_gdf)=:,}")
    links_gdf = gpd.sjoin(
        links_gdf,
        county_gdf[['geometry', 'county']],
        how='left',
        predicate='intersects'
    )
    WranglerLogger.debug(f"{len(links_gdf)=:,}")
    WranglerLogger.debug(f"links_gdf:\n{links_gdf}")

    # Use "External" for links outside counties
    links_gdf['county'] = links_gdf['county'].fillna('External')
    WranglerLogger.debug(f"links_gdf:\n{links_gdf}")

    # The only links to adjust are those that matched to multiple counties
    multicounty_links_gdf = links_gdf[links_gdf.duplicated(subset=['A','B','key'], keep=False)].copy()
    WranglerLogger.debug(f"multicounty_links_gdf:\n{multicounty_links_gdf}")

    if len(multicounty_links_gdf) > 0:
        # Calculate intersection lengths for multi-county links
        WranglerLogger.info(f"Found {len(multicounty_links_gdf):,} links in multicounty_links_gdf")

        # Calculate intersection length for each link-county pair
        multicounty_links_gdf['intersection_length'] = multicounty_links_gdf.apply(
            lambda row: row.geometry.intersection(
                county_gdf[county_gdf['county'] == row['county']].iloc[0].geometry
            ).length if not pd.isna(row['county']) else 0,
            axis=1
        )

        # Sorting by index (ascending), intersection_length (descending)
        multicounty_links_gdf.sort_values(
            by=['A','B','key','intersection_length'],
            ascending=[True, True, True, False],
            inplace=True)
        WranglerLogger.debug(f"multicounty_links_gdf:\n{multicounty_links_gdf}")
        # drop duplicates now, keeping first
        multicounty_links_gdf.drop_duplicates(subset=['A','B','key'], keep='first', inplace=True)
        WranglerLogger.debug(f"After dropping duplicates: multicounty_links_gdf:\n{multicounty_links_gdf}")

        # put them back together
        links_gdf = pd.concat([
            links_gdf.drop_duplicates(subset=['A','B','key'], keep=False), # single-county links
            multicounty_links_gdf
        ])
        # verify that each link is only represented once
        multicounty_links_gdf = links_gdf[links_gdf.duplicated(subset=['A','B','key'], keep=False)]
        assert(len(multicounty_links_gdf)==0)

        # drop temporary columns
        links_gdf.drop(columns=['index_right','intersection_length'], inplace=True)
        links_gdf.reset_index(drop=True, inplace=True)

    # Drop the extra columns from spatial join
    links_gdf = links_gdf.drop(columns=['index_right'], errors='ignore')

    # Merge back existing county values (only overwrite null/empty)
    if links_has_county:
        # Use existing value if it's not null and not empty string
        mask = links_gdf['county_existing'].notna() & (links_gdf['county_existing'] != '')
        links_gdf.loc[mask, 'county'] = links_gdf.loc[mask, 'county_existing']
        links_gdf = links_gdf.drop(columns=['county_existing'])

    WranglerLogger.debug(f"links_gdf with one county per link:\n{links_gdf}")

    # Sort nodes by county for consistent numbering
    nodes_gdf = nodes_gdf.sort_values('county').reset_index(drop=True)
    WranglerLogger.debug(f"nodes_gdf:\n{nodes_gdf}")

    # revert to LAT_LON_CRS
    links_gdf = links_gdf.to_crs(LAT_LON_CRS)
    nodes_gdf = nodes_gdf.to_crs(LAT_LON_CRS)

    # Verify no duplicates were created
    assert len(links_gdf) == expected_links_count, \
        f"Links count changed: expected {expected_links_count}, got {len(links_gdf)}"
    assert len(nodes_gdf) == expected_nodes_count, \
        f"Nodes count changed: expected {expected_nodes_count}, got {len(nodes_gdf)}"

    return links_gdf, nodes_gdf


class MTCRoadwayNetwork(RoadwayNetwork):
    """MTC-specific roadway network with additional validation.

    Extends RoadwayNetwork to enforce MTC-specific schema requirements including
    county, jurisdiction, and mtc_facility_type fields.

    Args:
        nodes_df: GeoDataFrame of roadway nodes
        links_df: GeoDataFrame of roadway links
        shapes_df: GeoDataFrame of roadway shapes (optional)
        validate_mtc: If True, validates against MTC schemas (default: True)
        **kwargs: Additional arguments passed to RoadwayNetwork

    Example:
        ```python
        from mtc_wrangler.models.mtc_network import MTCRoadwayNetwork

        # Load a network with MTC validation
        net = MTCRoadwayNetwork.read(
            link_file="links.geojson",
            node_file="nodes.geojson",
            validate_mtc=True
        )

        # Access network data
        print(net.links_df[['model_link_id', 'county', 'jurisdiction']])
        ```
    """

    def __init__(
        self,
        nodes_df=None,
        links_df=None,
        shapes_df=None,
        validate_mtc: bool = True,
        **kwargs
    ):
        """Initialize MTC Roadway Network with optional MTC-specific validation."""
        # Initialize parent RoadwayNetwork
        super().__init__(
            nodes_df=nodes_df,
            links_df=links_df,
            shapes_df=shapes_df,
            **kwargs
        )

        # Apply MTC-specific validation if requested
        if validate_mtc:
            self.validate()

    def validate(self):
        """Validate network against MTC-specific schemas.

        This method can be called explicitly to validate the network after
        modifications have been made to the dataframes.

        Example:
            ```python
            # Modify network
            net.links_df['county'] = 'Alameda'

            # Validate changes
            net.validate()
            ```
        """
        WranglerLogger.debug("MTCRoadwayNetwork.validate() called")
        self.links_df = validate_df_to_model(self.links_df, MTCRoadLinksTable)
        self.nodes_df = validate_df_to_model(self.nodes_df, MTCRoadNodesTable)

    @classmethod
    def read(
        cls,
        link_file: Union[str, pathlib.Path],
        node_file: Union[str, pathlib.Path],
        shape_file: Optional[Union[str, pathlib.Path]] = None,
        validate_mtc: bool = True,
        **kwargs
    ) -> "MTCRoadwayNetwork":
        """Read network from files with MTC validation.

        Args:
            link_file: pathlib.Path to links file (GeoJSON, shapefile, etc.)
            node_file: pathlib.Path to nodes file (GeoJSON, shapefile, etc.)
            shape_file: Optional pathlib.path to shapes file
            validate_mtc: If True, validates against MTC schemas (default: True)
            **kwargs: Additional arguments passed to parent read method

        Returns:
            MTCRoadwayNetwork instance

        Example:
            ```python
            net = MTCRoadwayNetwork.read(
                link_file="data/links.geojson",
                node_file="data/nodes.geojson",
                validate_mtc=True
            )
            ```
        """
        # Use parent class read method
        network = RoadwayNetwork.read(
            link_file=link_file,
            node_file=node_file,
            shape_file=shape_file,
            **kwargs
        )

        # Convert to MTCRoadwayNetwork
        mtc_network = cls(
            nodes_df=network.nodes_df,
            links_df=network.links_df,
            shapes_df=network.shapes_df,
            validate_mtc=validate_mtc,
            **{k: v for k, v in network.__dict__.items()
               if k not in ['nodes_df', 'links_df', 'shapes_df']}
        )

        return mtc_network

    def write(
        self,
        out_dir: Union[str, pathlib.Path],
        validate_mtc: bool = True,
        **kwargs
    ):
        """Write network to files with optional MTC validation.

        Args:
            out_dir: Output directory for network files
            validate_mtc: If True, validates against MTC schemas before writing
            **kwargs: Additional arguments passed to parent write method

        Example:
            ```python
            net.write("output/network", validate_mtc=True)
            ```
        """
        # Validate before writing if requested
        if validate_mtc:
            self.validate()

        # Use parent write method
        super().write(out_dir=out_dir, **kwargs)
