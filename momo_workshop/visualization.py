import osmnx as ox
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import geopandas as gpd
import seaborn as sns
import folium
from folium import plugins
import seaborn as sns

def create_osmnx_plot(osm_network):
    
    # Create an interactive plot using OSMnx
    fig, ax = ox.plot_graph(
        osm_network,
        figsize=(15, 15),
        node_size=0,  # Hide nodes for cleaner view
        edge_color='grey',
        edge_alpha=0.5,
        bgcolor='white',
        show=False,
        close=False
    )
    
    # Make it interactive with matplotlib
    plt.ion()  # Supposed to turn interactive mode but not working in notebook
    plt.show()
    
    return fig, ax



def compare_original_and_simplified_networks(original_nw, simplified_nw, network_names=("Original OSM", "Simplified OSM")):
    
    print(f"=== Network Comparison: {network_names[0]} vs {network_names[1]} ===\n")
    
    # Basic counts
    orig_nodes = len(original_nw.nodes())
    orig_edges = len(original_nw.edges())
    simp_nodes = len(simplified_nw.nodes())
    simp_edges = len(simplified_nw.edges())
    
    print("📊 BASIC STATISTICS")
    print(f"Nodes: {orig_nodes:,} → {simp_nodes:,} ({simp_nodes/orig_nodes:.1%} remaining)")
    print(f"Edges: {orig_edges:,} → {simp_edges:,} ({simp_edges/orig_edges:.1%} remaining)")
    print(f"Nodes removed: {orig_nodes - simp_nodes:,} ({(orig_nodes - simp_nodes)/orig_nodes:.1%})")
    print(f"Edges removed: {orig_edges - simp_edges:,} ({(orig_edges - simp_edges)/orig_edges:.1%})")
    
    # Degree analysis
    print(f"\n🔗 CONNECTIVITY ANALYSIS")
    orig_degrees = [d for n, d in original_nw.degree()]
    simp_degrees = [d for n, d in simplified_nw.degree()]
    
    print(f"Average degree: {np.mean(orig_degrees):.2f} → {np.mean(simp_degrees):.2f}")
    print(f"Max degree: {max(orig_degrees)} → {max(simp_degrees)}")
    
    # Degree distribution
    orig_degree_counts = pd.Series(orig_degrees).value_counts().sort_index()
    simp_degree_counts = pd.Series(simp_degrees).value_counts().sort_index()
    
    print(f"\nDegree 2 nodes (typical street continuation): {orig_degree_counts.get(2, 0):,} → {simp_degree_counts.get(2, 0):,}")
    print(f"Degree 3 nodes (T-intersections): {orig_degree_counts.get(3, 0):,} → {simp_degree_counts.get(3, 0):,}")
    print(f"Degree 4+ nodes (complex intersections): {sum(orig_degree_counts[orig_degree_counts.index >= 4]):,} → {sum(simp_degree_counts[simp_degree_counts.index >= 4]):,}")
    
    # Edge length analysis (if geometry exists)
    if 'length' in list(original_nw.edges(data=True))[0][2]:
        print(f"\n📏 EDGE LENGTH ANALYSIS")
        orig_lengths = [data['length'] for u, v, data in original_nw.edges(data=True)]
        simp_lengths = [data['length'] for u, v, data in simplified_nw.edges(data=True)]
        
        print(f"Average edge length: {np.mean(orig_lengths):.1f}m → {np.mean(simp_lengths):.1f}m")
        print(f"Total network length: {sum(orig_lengths)/1000:.1f}km → {sum(simp_lengths)/1000:.1f}km")
        
        print(f"Shortest edge: {min(orig_lengths):.1f}m → {min(simp_lengths):.1f}m")
        print(f"Longest edge: {max(orig_lengths):.1f}m → {max(simp_lengths):.1f}m")


def plot_node_degree_changes(original_nw, simplified_nw):
    """Analyze how node degrees changed during simplification - plot only"""
    
    orig_degrees = pd.Series([d for n, d in original_nw.degree()], name='Original')
    simp_degrees = pd.Series([d for n, d in simplified_nw.degree()], name='Simplified')
    
    # Create comparison DataFrame
    degree_comparison = pd.DataFrame({
        'Original': orig_degrees.value_counts().sort_index(),
        'Simplified': simp_degrees.value_counts().sort_index()
    }).fillna(0).astype(int)
    
    # Plot degree distribution
    fig, ax = plt.subplots(figsize=(12, 6))
    degree_comparison[['Original', 'Simplified']].plot(kind='bar', ax=ax)
    ax.set_title('Node Degree Distribution: Before vs After Simplification')
    ax.set_xlabel('Node Degree')
    ax.set_ylabel('Number of Nodes')
    ax.legend()
    plt.xticks(rotation=0)
    plt.show()


def create_downtown_network_map(nw_gdf):

    # Define downtown bounding box
    downtown_sf_bbox = [-122.42, 37.77, -122.39, 37.80]

    # Filter network to bbox
    subset_gdf = nw_gdf.cx[downtown_sf_bbox[0]:downtown_sf_bbox[2], 
                            downtown_sf_bbox[1]:downtown_sf_bbox[3]]

    print(f"Original network: {len(nw_gdf):,} links")
    print(f"Subset network: {len(subset_gdf):,} links")

    

    # Create A + B column for tooltip
    subset_gdf["A & B (Combined)"] = subset_gdf["A"].astype(str) + ", " + subset_gdf["B"].astype(str) 

    # Create palette for 23 cats
    palette1 = sns.color_palette("Set1", 9).as_hex()
    palette2 = sns.color_palette("Set2", 8).as_hex() 
    palette3 = sns.color_palette("Dark2", 6).as_hex()
    highway_palette = palette1 + palette2 + palette3

    # Define the tooltip columns - missing osm link id, ML columns
    tooltip_cols = ["A & B (Combined)", "highway", "name", "oneway", "reversed", "lanes", "bike_access", "truck_access", "walk_access", "bus_only", "ferry_only", "rail_only"] 

    # This drags render speed so not using for now
    # esri_satellite = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'

    m = subset_gdf.explore(
        column='highway',
        categorical=True,
        cmap=highway_palette,
        legend=True,
        style_kwds={'weight': 3, 'opacity': 1},
        tooltip=tooltip_cols,
        popup=True,
        tiles='CartoDB dark_matter',
        # tiles=esri_satellite,
        # attr='Esri',
        zoom_start=15,
        location=[37.787589, -122.403542] # Hard coded for downtown links but could make dynamic to create for other areas of the city
    )
    m.save('downtown_sf_network_map.html')
    # return m


def clip_original_and_simplified_links(orig_links, links_gdf, taz_gdf):

    # For taz mask
    taz_list = [360, 293, 292, 406, 562, 561, 565]
    taz_gdf_subs = taz_gdf[taz_gdf["TAZ"].isin(taz_list)]

    # Clip
    orig_links_gdf_clip = gpd.clip(orig_links, taz_gdf_subs)
    links_gdf_clip = gpd.clip(links_gdf, taz_gdf_subs)

    return orig_links_gdf_clip, links_gdf_clip


def map_original_and_simplified_links(orig_links_gdf_clip, links_gdf_clip, output_dir):

    # Create color palette
    palette1 = sns.color_palette("Set1", 9).as_hex()
    palette2 = sns.color_palette("Set2", 8).as_hex() 
    palette3 = sns.color_palette("Dark2", 6).as_hex()
    highway_palette = palette1 + palette2 + palette3

    # Get unique highway types and create color mapping
    highway_types = sorted(orig_links_gdf_clip['highway'].unique())
    highway_colors = {highway_type: highway_palette[i % len(highway_palette)] 
                    for i, highway_type in enumerate(highway_types)}
    
    # Create A + B col for tooltip
    orig_links_gdf_clip["A & B (Combined)"] = orig_links_gdf_clip["A"].astype(str) + ", " + orig_links_gdf_clip["B"].astype(str)
    links_gdf_clip["A & B (Combined)"] = links_gdf_clip["A"].astype(str) + ", " + links_gdf_clip["B"].astype(str)

    # Define tooltip fields
    tooltip_fields = ["A & B (Combined)", "highway", "name", "oneway", "reversed", "lanes", "bike_access", "truck_access", "walk_access", "bus_only"]
    tooltip_aliases = ["A & B:", "Highway:", "Name:", "Oneway:", "Reversed:", "Lanes:", "Bike Access:", "Truck Access:", "Walk Access:", "Bus Only:"]

    # Get bounds for the map instead of centroid
    bounds = orig_links_gdf_clip.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

    # Create dual map with CartoDB light base maps
    m = plugins.DualMap(location=[center_lat, center_lon], zoom_start=17, tiles=None)
    print(f"Created map {type(m)}")

    folium.TileLayer("cartodbpositron").add_to(m.m1)
    folium.TileLayer("cartodbpositron").add_to(m.m2)

    # Add titles to each map
    title_left = '''
    <div style="position: fixed; 
                top: 10px; left: 25%; transform: translateX(-50%); width: 200px; height: 40px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:16px; font-weight:bold; text-align:center; padding: 8px;
                white-space: nowrap;">
    <p>Original OSM Network</p>
    </div>
    '''
    
    title_right = '''
    <div style="position: fixed; 
                top: 10px; left: 75%; transform: translateX(-50%); width: 200px; height: 40px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:16px; font-weight:bold; text-align:center; padding: 8px;
                white-space: nowrap;">
    <p>Simplified OSM Network</p>
    </div>
    '''

    # Add original links to left map with highway-based colors
    for highway_type in highway_types:
        highway_subset = orig_links_gdf_clip[orig_links_gdf_clip['highway'] == highway_type]
        if not highway_subset.empty:
            folium.GeoJson(
                highway_subset,
                style_function=lambda x, color=highway_colors[highway_type]: {
                    'color': color,
                    'weight': 2,
                    'opacity': 0.8
                },
                tooltip=folium.GeoJsonTooltip(
                    fields=tooltip_fields,
                    aliases=tooltip_aliases
                    ),
                    marker=folium.Circle(
                        radius=0, 
                        # opacity=0,
                        stroke=False
                        )
            ).add_to(m.m1)

    # Add simplified links to right map with highway-based colors
    links_gdf_clip_with_highway = links_gdf_clip.copy()
    # Map simplified links to highway types if not already present
    if 'highway' not in links_gdf_clip_with_highway.columns:
        # You may need to add logic here to map highway types to simplified links
        # For now, using a default style
        folium.GeoJson(
            links_gdf_clip,
            style_function=lambda x: {
                'color': 'blue', 
                'weight': 2,
                'opacity': 0.8
            },
            tooltip=folium.GeoJsonTooltip(
                fields=tooltip_fields,
                aliases=tooltip_aliases
            ),
        ).add_to(m.m2)
    else:
        for highway_type in highway_types:
            highway_subset = links_gdf_clip_with_highway[links_gdf_clip_with_highway['highway'] == highway_type]
            if not highway_subset.empty:
                folium.GeoJson(
                    highway_subset,
                    style_function=lambda x, color=highway_colors[highway_type]: {
                        'color': color,
                        'weight': 2,
                        'opacity': 0.8
                    },
            tooltip=folium.GeoJsonTooltip(
                fields=tooltip_fields,
                aliases=tooltip_aliases
            )
                ).add_to(m.m2)

    # Create legend using Folium's native legend functionality
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 200px; height: auto; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:14px; padding: 10px">
    <p><b>Highway Types</b></p>
    '''

    for highway_type, color in highway_colors.items():
        legend_html += f'''
        <p><i class="fa fa-minus" style="color:{color}; font-size: 20px"></i> {highway_type}</p>
        '''

    legend_html += '</div>'

    # Add titles and legend to both maps
    m.m1.get_root().html.add_child(folium.Element(title_left))
    m.m1.get_root().html.add_child(folium.Element(legend_html))
    
    m.m2.get_root().html.add_child(folium.Element(title_right))
    m.m2.get_root().html.add_child(folium.Element(legend_html))

    m.save(output_dir / "split_map.html")
    return m
