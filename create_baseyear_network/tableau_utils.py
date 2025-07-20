USAGE = """
  Utility to save geodataframes as Tableau Hyper files.
  This is useful for quickly exporting geospatial data to Tableau for visualization.
"""
import shapely
import geopandas as gpd
from network_wrangler import WranglerLogger

def write_geodataframe_as_tableau_hyper(in_gdf, filename, tablename):
    """
    Write a GeoDataFrame to a Tableau Hyper file.
    See https://tableau.github.io/hyper-db/docs/guides/hyper_file/geodata

    This is kind of a bummer because it would be preferrable to write to something more standard, like
    geofeather or geoparquet, but Tableau doesn't support those formats yet.
    """
    WranglerLogger.info(f"write_geodataframe_as_tableau_hyper: {filename=}, {tablename=}")
    # make a copy since we'll be messing with the columns
    gdf = in_gdf.copy()

    import tableauhyperapi

    # Check if all entries in the geometry column are valid Shapely geometries
    is_valid_geometry = gdf['geometry'].apply(lambda x: isinstance(x, shapely.geometry.base.BaseGeometry))
    WranglerLogger.debug(f"is_valid_geometry: \n{is_valid_geometry.value_counts()}")

    # count coordinates per geometry
    gdf['coord_count'] = gdf.geometry.count_coordinates()
    WranglerLogger.debug(f"gdf.coord_count.value_counts(): \n{gdf.coord_count.value_counts()}")
    WranglerLogger.debug(f"gdf.coord_count==1: \n{gdf.loc[gdf.coord_count == 1]}")

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