USAGE = """
Create MTC base year networks (2023) from Overture.

References:
"""
import datetime
import getpass
import pathlib
import geopandas as gpd
import pandas as pd
import overturemaps

import network_wrangler
from network_wrangler import WranglerLogger
import tableau_utils

COUNTY_SHAPEFILE = pathlib.Path("M:\\Data\\Census\\Geography\\tl_2010_06_county10\\tl_2010_06_county10_9CountyBayArea.shp")
OUTPUT_DIR = pathlib.Path("M:\\Development\\Travel Model Two\\Supply\\Network Creation 2025\\from_OvertureM")
NOW = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
USERNAME = getpass.getuser()

if USERNAME=="lmz":
    COUNTY_SHAPEFILE = pathlib.Path("../../tl_2010_06_county10/tl_2010_06_county10_9CountyBayArea.shp")
    OUTPUT_DIR = pathlib.Path("../../output_from_Overture")

def get_county_bbox(county_shapefile):
  """
  Read county shapefile and return bounding box in WGS84 coordinates.
  
  Args:
    county_shapefile: Path to the county shapefile
    
  Returns:
    tuple: Bounding box as (minx, miny, maxx, maxy) in WGS84
  """
  WranglerLogger.info(f"Reading county shapefile from {county_shapefile}")
  county_gdf = gpd.read_file(county_shapefile)
  
  # Get the total bounds (bounding box) of all counties
  # Returns (minx, miny, maxx, maxy)
  bbox = county_gdf.total_bounds
  WranglerLogger.info(f"Bounding box for Bay Area counties: minx={bbox[0]:.6f}, miny={bbox[1]:.6f}, maxx={bbox[2]:.6f}, maxy={bbox[3]:.6f}")
  
  # Convert to WGS84 (EPSG:4326) if not already
  if county_gdf.crs != "EPSG:4326":
    WranglerLogger.info(f"Converting from {county_gdf.crs} to EPSG:4326")
    county_gdf_wgs84 = county_gdf.to_crs("EPSG:4326")
    bbox = county_gdf_wgs84.total_bounds
    WranglerLogger.info(f"Bounding box in WGS84: minx={bbox[0]:.6f}, miny={bbox[1]:.6f}, maxx={bbox[2]:.6f}, maxy={bbox[3]:.6f}")
  
  # Convert bbox to tuple (overturemaps expects tuple, not numpy array)
  return tuple(bbox)

if __name__ == "__main__":
  pd.options.display.max_columns = None
  pd.options.display.width = None
  pd.options.display.min_rows = 20
  
  # Create output directory if it doesn't exist
  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
  
  INFO_LOG  = OUTPUT_DIR / f"create_mtc_network_from_overture.info.log"
  DEBUG_LOG = OUTPUT_DIR / f"create_mtc_network_from_overture.debug.log"

  network_wrangler.setup_logging(
    info_log_filename=INFO_LOG,
    debug_log_filename=DEBUG_LOG,
    std_out_level="info",
    file_mode='w'
  )
  WranglerLogger.info(f"Created by {__file__}")

  OVERTURE_MAP_DIR = OUTPUT_DIR / "overture_map"
  
  # Create output directory if it doesn't exist
  OVERTURE_MAP_DIR.mkdir(parents=True, exist_ok=True)
  
  # Define which Overture types to download for transportation network
  # 'segment' contains road segments, 'connector' contains intersections/nodes
  overture_types = ['segment', 'connector']
  
  # Check if files already exist and skip download if they do
  existing_files = {}
  for overture_type in overture_types:
    parquet_file = OVERTURE_MAP_DIR / f"overture_{overture_type}.parquet"
    if parquet_file.exists():
      existing_files[overture_type] = parquet_file
      WranglerLogger.info(f"Found existing file for '{overture_type}': {parquet_file}")
  
  # Download missing data
  if len(existing_files) < len(overture_types):
    # Get bounding box from county shapefile (only when we need to download)
    bbox = get_county_bbox(COUNTY_SHAPEFILE)
    
    try:
      for overture_type in overture_types:
        if overture_type in existing_files:
          WranglerLogger.info(f"Skipping download for '{overture_type}' - file already exists")
          continue
          
        WranglerLogger.info(f"Downloading Overture Maps '{overture_type}' type for bounding box...")
        
        # Download data using overturemaps-py
        # Note: This downloads the latest release by default
        gdf = overturemaps.core.geodataframe(
          overture_type=overture_type,
          bbox=bbox  # (minx, miny, maxx, maxy)
        )
        
        # Save to geoparquet format
        output_file = OVERTURE_MAP_DIR / f"overture_{overture_type}.parquet"
        WranglerLogger.info(f"Saving {len(gdf)} features to {output_file}")
        gdf.to_parquet(output_file)
        
        # Debug log the head of the data
        WranglerLogger.debug(f"First 5 rows of {overture_type} data:")
        WranglerLogger.debug(f"\n{gdf.head()}")
        WranglerLogger.debug(f"Columns: {list(gdf.columns)}")
        WranglerLogger.debug(f"Data types:\n{gdf.dtypes}")
        
    except ImportError:
      WranglerLogger.error("overturemaps package not installed. Install it with: pip install overturemaps")
      WranglerLogger.error("See https://docs.overturemaps.org/getting-data/overturemaps-py/ for more information")
      raise
  else:
    WranglerLogger.info("All Overture data files already exist, skipping download")
  
  # Read the county shapefile for spatial join
  WranglerLogger.info(f"Reading county shapefile for spatial join from {COUNTY_SHAPEFILE}")
  county_gdf = gpd.read_file(COUNTY_SHAPEFILE)
  
  # Ensure county shapefile is in WGS84 for consistent spatial join
  if county_gdf.crs != "EPSG:4326":
    WranglerLogger.info(f"Converting county shapefile from {county_gdf.crs} to EPSG:4326")
    county_gdf = county_gdf.to_crs("EPSG:4326")
  
  # Get the county name column (adjust if different in your shapefile)
  # Common column names are 'NAME10', 'NAME', 'COUNTY', etc.
  county_name_col = None
  for col in ['NAME10', 'NAME', 'COUNTY', 'County']:
    if col in county_gdf.columns:
      county_name_col = col
      break
  
  if county_name_col is None:
    WranglerLogger.warning(f"Could not find county name column. Available columns: {list(county_gdf.columns)}")
    county_name_col = county_gdf.columns[0]  # Use first column as fallback
  
  WranglerLogger.info(f"Using '{county_name_col}' as county name column")
  
  # Read the saved files and convert to Tableau Hyper format
  WranglerLogger.info("Converting geoparquet files to Tableau Hyper format...")
  for overture_type in overture_types:
    parquet_file = OVERTURE_MAP_DIR / f"overture_{overture_type}.parquet"
    hyper_file = OUTPUT_DIR / f"overture_{overture_type}.hyper"
    
    if parquet_file.exists():
      WranglerLogger.info(f"Reading {parquet_file}")
      gdf = gpd.read_parquet(parquet_file)
      orig_count = len(gdf)
      WranglerLogger.info(f"  - {orig_count:,} features")
      WranglerLogger.debug(f"  - First 3 rows:\n{gdf.head(3)}")
      
      # Set CRS to WGS84 if not already set (Overture data is in WGS84)
      if gdf.crs is None:
        WranglerLogger.info(f"  - Setting CRS to EPSG:4326 (WGS84)")
        gdf = gdf.set_crs("EPSG:4326")
      
      # Perform spatial join with counties
      WranglerLogger.info(f"  - Performing spatial join with counties...")
      
      # For segments (lines), use the representative point (midpoint)
      # For connectors (points), use the geometry directly
      if overture_type == 'segment':
        # Create a temporary point geometry for spatial join
        gdf['join_geometry'] = gdf.geometry.representative_point()
        gdf_for_join = gdf.set_geometry('join_geometry')
      else:
        gdf_for_join = gdf
      
      # Perform spatial join to add county information
      gdf_with_county = gpd.sjoin(
        gdf_for_join,
        county_gdf[[county_name_col, 'geometry']],
        how='left',
        predicate='within'
      )
      
      # Remove the temporary join geometry column if it exists
      if 'join_geometry' in gdf_with_county.columns:
        gdf_with_county = gdf_with_county.drop(columns=['join_geometry'])
        # Reset geometry to original
        gdf_with_county = gdf_with_county.set_geometry('geometry')
      
      # Rename the county column to 'county'
      gdf_with_county = gdf_with_county.rename(columns={county_name_col: 'county'})
      
      # Drop the index_right column from spatial join
      if 'index_right' in gdf_with_county.columns:
        gdf_with_county = gdf_with_county.drop(columns=['index_right'])
      
      # Check if row count changed
      new_count = len(gdf_with_county)
      if new_count != orig_count:
        WranglerLogger.warning(f"  - Row count changed after spatial join! Original: {orig_count:,}, New: {new_count:,}")
      else:
        WranglerLogger.info(f"  - Row count preserved: {new_count:,}")
      
      # Log how many features were matched to counties
      matched = gdf_with_county['county'].notna().sum()
      WranglerLogger.info(f"  - {matched:,} features ({matched/new_count*100:.1f}%) matched to a county")
      
      # Log county distribution
      county_counts = gdf_with_county['county'].value_counts(dropna=False)
      WranglerLogger.info(f"  - County distribution:\n{county_counts}")
      
      # Convert to Tableau Hyper format (use absolute path for Tableau)
      hyper_file_abs = hyper_file.resolve()
      WranglerLogger.info(f"Writing to Tableau Hyper file: {hyper_file_abs}")
      tableau_utils.write_geodataframe_as_tableau_hyper(
        gdf_with_county, 
        hyper_file_abs, 
        f"overture_{overture_type}"
      )
      WranglerLogger.info(f"  - Successfully wrote {hyper_file}")
    else:
      WranglerLogger.warning(f"Parquet file not found: {parquet_file}")