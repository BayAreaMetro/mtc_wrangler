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

    # Convert any list columns to strings for Tableau
    for col in gdf.columns:
        if gdf[col].dtype == 'object':
            # Check if any values are lists
            if any(isinstance(val, list) for val in gdf[col].dropna()):
                gdf[col] = gdf[col].apply(
                    lambda x: ', '.join(map(str, x)) if isinstance(x, list) else str(x) if pd.notna(x) else ''
                )

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