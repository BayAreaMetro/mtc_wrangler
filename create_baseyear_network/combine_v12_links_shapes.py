# 
# combine v12_shapes.geojson with v12_links.json and output v12_links.geojson
#
import json
import pathlib
import geopandas as gpd
import pandas as pd

v12_dir = pathlib.Path("../../standard_network_after_project_cards")
links_file = v12_dir / "v12_link.json"
shapes_file = v12_dir / "v12_shape.geojson"
combined_file = v12_dir / "v12_shape_link.geojson"

with open(links_file, 'r') as file:
    link_data = json.load(file)
link_df = pd.DataFrame(link_data)
print(f"Read {len(link_df)} rows from {links_file}")
print(link_df.head())
print(link_df.dtypes)

shape_gdf = gpd.read_file(shapes_file)
print(f"Read {len(shape_gdf)} rows from {shapes_file}")
print(shape_gdf.head())
print(shape_gdf.dtypes)

# duplicate links 
key = ['id','fromIntersectionId','toIntersectionId']
links_with_dupe_keys = link_df.loc[ link_df.duplicated(subset=key, keep=False) ]
print(f"links with duplicate keys:\n{links_with_dupe_keys}")

shape_gdf = pd.merge(
    left=shape_gdf,
    right=link_df,
    on=['id','fromIntersectionId','toIntersectionId'],
    how='outer',
    validate='one_to_many',
    indicator=True
)
print(f"shape_gdf['_merge'].value_counts():\n{shape_gdf['_merge'].value_counts()}")

print(f"shape_gdf has {len(shape_gdf.columns)} columns")
shape_gdf.to_file(combined_file, driver="GeoJSON")
print(f"Wrote {len(shape_gdf)} lines to {combined_file}")
