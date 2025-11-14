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

# Per Sijia (https://app.asana.com/1/11860278793487/task/1209256117977561?focus=true)
# "For a two-way street, there are two link records and just one shape record.
# The two link records share the shape id and have opposite fromIntersectionId and 
# toIntersectionId values. The one shape record has shape id and only has
# fromIntersectionId and toIntersectionId for one direction. This is how
# SharedStreet coded things. We don't really use or update fromIntersectionId
# and toIntersectionId fields in Network Wrangler. When new links are added in
# Network Wrangler, we do generate a new shape id, but we do not bother with new
# fromIntersectionId and toIntersectionId. Therefore, there are nulls values in the
# fromIntersectionId and toIntersectionId fields, which is why the joining has 
# duplicates."
shape_gdf = pd.merge(
    left=shape_gdf,
    right=link_df,
    on=['id'],
    how='outer',
    validate='one_to_many',
    indicator=True
)
print(f"shape_gdf['_merge'].value_counts():\n{shape_gdf['_merge'].value_counts()}")

# project to crs in feet and add length
LOCAL_CRS_FEET = "EPSG:2227"
FEET_PER_MILE = 5280.0

shape_gdf.to_crs(LOCAL_CRS_FEET, inplace=True)
shape_gdf['length'] = shape_gdf.length
shape_gdf['distance'] = shape_gdf['length']/FEET_PER_MILE

print(f"shape_gdf has {len(shape_gdf.columns)} columns")
shape_gdf.to_file(combined_file, driver="GeoJSON")
print(f"Wrote {len(shape_gdf)} lines to {combined_file}")
