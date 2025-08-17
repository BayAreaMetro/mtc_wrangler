USAGE = """
  Utility to save geodataframes as Tableau Hyper files.
  This is useful for quickly exporting geospatial data to Tableau for visualization.
"""
import shapely
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from network_wrangler import WranglerLogger

def write_geodataframe_as_tableau_hyper(in_gdf, filename, tablename):
    """
    Write a GeoDataFrame or DataFrame with X,Y columns to a Tableau Hyper file.
    See https://tableau.github.io/hyper-db/docs/guides/hyper_file/geodata

    This is kind of a bummer because it would be preferrable to write to something more standard, like
    geofeather or geoparquet, but Tableau doesn't support those formats yet.
    
    Args:
        in_gdf: A GeoDataFrame or a DataFrame with X,Y columns
        filename: Output filename for the Hyper file
        tablename: Name of the table within the Hyper file
    """
    WranglerLogger.info(f"write_geodataframe_as_tableau_hyper: {filename=}, {tablename=}")
    
    # Handle regular DataFrame with X,Y columns
    if isinstance(in_gdf, pd.DataFrame) and not isinstance(in_gdf, gpd.GeoDataFrame):
        if 'X' in in_gdf.columns and 'Y' in in_gdf.columns:
            WranglerLogger.info("Converting DataFrame with X,Y columns to GeoDataFrame")
            # Create Point geometries from X,Y coordinates
            in_gdf['geometry'] = [Point(xy) for xy in zip(in_gdf.X, in_gdf.Y)]
            gdf = gpd.GeoDataFrame(in_gdf, crs='EPSG:4326')
        else:
            raise ValueError("Input DataFrame must have 'X' and 'Y' columns or be a GeoDataFrame")
    else:
        # make a copy since we'll be messing with the columns
        # make sure it's in WSG84
        gdf = in_gdf.to_crs(crs='EPSG:4326')

    # Convert any list values to strings for Tableau
    cols_to_str = set()
    for col in gdf.columns:
        if gdf[col].dtype == 'object':
            # Check if any values are lists
            if any(isinstance(val, list) for val in gdf[col].dropna()):
                gdf[col] = gdf[col].apply(
                    lambda x: ', '.join(map(str, x)) if isinstance(x, list) else str(x) if pd.notna(x) else ''
                )
                cols_to_str.add(col)
        if gdf[col].dtype == 'int64':
            # Check if any values are outside of int32 range (-2147483648 to 2147483647)
            # Tableau Hyper uses int32, so we need to convert int64 values that exceed this range
            min_val = gdf[col].min()
            max_val = gdf[col].max()
            int32_min = -2147483648
            int32_max = 2147483647
            
            if pd.notna(min_val) and pd.notna(max_val):
                if min_val < int32_min or max_val > int32_max:
                    WranglerLogger.warning(
                        f"Column '{col}' has int64 values outside int32 range "
                        f"[{int32_min}, {int32_max}]: min={min_val}, max={max_val}. "
                        f"Converting to string."
                    )
                    cols_to_str.add(col)
    
    # Convert columns in cols_to_str to string types
    for col in cols_to_str:
        if col in gdf.columns:
            gdf[col] = gdf[col].astype(str)


    # Validate geometries - check for invalid lat/lon values and insufficient coordinates
    def validate_geometry(geom):
        """
        Check if geometry is valid for Tableau Hyper.
        Returns tuple (is_valid, reason).
        """
        if geom is None or geom.is_empty:
            return False, "Empty or null geometry"
        
        # Check if geometry has enough coordinates
        # Points need 1 coordinate, lines need at least 2
        try:
            geom_type = geom.geom_type
            coords = list(geom.coords) if hasattr(geom, 'coords') else []
            
            if geom_type == 'Point':
                if len(coords) < 1:
                    return False, "Point has no coordinates"
            elif geom_type in ['LineString', 'LinearRing']:
                if len(coords) < 2:
                    return False, f"{geom_type} has less than 2 coordinates"
                # Check if all coordinates are the same (zero-length line)
                if len(coords) >= 2:
                    first_coord = coords[0]
                    if all(coord == first_coord for coord in coords):
                        return False, f"{geom_type} has all coordinates at the same location"
            elif geom_type == 'Polygon':
                # Check exterior ring
                if len(geom.exterior.coords) < 3:
                    return False, "Polygon exterior has less than 3 coordinates"
            elif geom_type in ['MultiPoint', 'MultiLineString', 'MultiPolygon']:
                if len(geom.geoms) == 0:
                    return False, f"Empty {geom_type}"
                # Check each sub-geometry
                for sub_geom in geom.geoms:
                    is_valid, reason = validate_geometry(sub_geom)
                    if not is_valid:
                        return False, f"{geom_type} contains invalid geometry: {reason}"
        except Exception as e:
            return False, f"Error checking geometry: {e}"
        
        # Check bounds for valid lat/lon values
        try:
            bounds = geom.bounds
            min_lon, min_lat, max_lon, max_lat = bounds[0], bounds[1], bounds[2], bounds[3]
            
            # Check if latitude or longitude is outside valid range
            lat_valid = -90 <= min_lat <= 90 and -90 <= max_lat <= 90
            lon_valid = -180 <= min_lon <= 180 and -180 <= max_lon <= 180
            
            if not lat_valid:
                return False, f"Invalid latitude: min={min_lat}, max={max_lat}"
            if not lon_valid:
                return False, f"Invalid longitude: min={min_lon}, max={max_lon}"
        except Exception as e:
            return False, f"Error checking bounds: {e}"
        
        return True, "Valid"
    
    # Validate all geometries
    gdf['geometry_validation'] = gdf['geometry'].apply(validate_geometry)
    gdf['is_valid'] = gdf['geometry_validation'].apply(lambda x: x[0])
    gdf['validation_reason'] = gdf['geometry_validation'].apply(lambda x: x[1])
    
    # Filter out invalid geometries
    invalid_rows = gdf[~gdf['is_valid']]
    
    if len(invalid_rows) > 0:
        WranglerLogger.warning(f"Found {len(invalid_rows)} rows with invalid geometries. These will be filtered out.")
        WranglerLogger.warning("Invalid geometry reasons:")
        reason_counts = invalid_rows['validation_reason'].value_counts()
        for reason, count in reason_counts.items():
            WranglerLogger.warning(f"  - {reason}: {count} rows")
        
        # Log sample of invalid rows (first 5)
        WranglerLogger.debug(f"Invalid rows:\n{invalid_rows}")

        # Filter to keep only valid rows
        gdf = gdf[gdf['is_valid']].copy()
        WranglerLogger.info(f"Keeping {len(gdf)} valid rows for Tableau export")
    
    # Remove the temporary validation columns
    gdf = gdf.drop(columns=['geometry_validation', 'is_valid', 'validation_reason'])

    import tableauhyperapi

    # Convert geometry to WKT format
    gdf['geometry_wkt'] = gdf['geometry'].apply(lambda geom: geom.wkt)
    # drop this column, we don't need it any longer
    gdf.drop(columns='geometry', inplace=True)

    table_def = tableauhyperapi.TableDefinition(tablename)
    # Inserter definition contains the column definition for the values that are inserted
    # The data input has two text values Name and Location_as_text
    inserter_definition = []

    # Specify the conversion of SqlType.text() to SqlType.tabgeography() using CAST expression in Inserter.ColumnMapping.
    # Specify all columns into which data is inserter in Inserter.ColumnMapping list. For columns that do not require any
    # transformations provide only the names
    column_mappings = []

    for col in gdf.columns:
        # geometry_wkt to be converted from WKT to geometry via column_mapping
        if col == 'geometry_wkt':
            table_def.add_column('geometry', tableauhyperapi.SqlType.tabgeography())
            # insert as geometry_wkt
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name='geometry_wkt', type=tableauhyperapi.SqlType.text(), nullability=tableauhyperapi.NOT_NULLABLE))
            # convert to geometry
            column_mappings.append(tableauhyperapi.Inserter.ColumnMapping(
                'geometry', f'CAST({tableauhyperapi.escape_name("geometry_wkt")} AS TABLEAU.TABGEOGRAPHY)'))
            continue

        if gdf[col].dtype == bool:
            table_def.add_column(col, tableauhyperapi.SqlType.bool())
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name=col, type=tableauhyperapi.SqlType.bool()))  
        elif gdf[col].dtype == int:
            table_def.add_column(col, tableauhyperapi.SqlType.int())
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name=col, type=tableauhyperapi.SqlType.int()))
        elif gdf[col].dtype == float:
            table_def.add_column(col, tableauhyperapi.SqlType.double())
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name=col, type=tableauhyperapi.SqlType.double()))
        else:
            table_def.add_column(col, tableauhyperapi.SqlType.text())
            inserter_definition.append(tableauhyperapi.TableDefinition.Column(
                name=col, type=tableauhyperapi.SqlType.text()))
            # convert others to text
            gdf[col] = gdf[col].astype(str)
        column_mappings.append(col)

    WranglerLogger.debug(f"table_def={table_def}")
    WranglerLogger.debug(f"column_mappings={column_mappings}")

    table_name = tableauhyperapi.TableName("Extract", tablename)
    with tableauhyperapi.HyperProcess(telemetry=tableauhyperapi.Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with tableauhyperapi.Connection(endpoint=hyper.endpoint, database=filename, 
                                        create_mode=tableauhyperapi.CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_schema("Extract")
            connection.catalog.create_table(table_def)

            with tableauhyperapi.Inserter(connection, table_def, columns=column_mappings, inserter_definition=inserter_definition) as inserter:

                inserter.add_rows(rows=gdf.itertuples(index=False, name=None))
                inserter.execute()


    WranglerLogger.info(f"GeoDataFrame written to {filename} as Tableau Hyper file.")