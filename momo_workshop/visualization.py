from typing import Optional, Tuple, Dict
from pathlib import Path
import networkx as nx
import osmnx as ox
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import geopandas as gpd
import seaborn as sns
import folium
from folium import plugins

# Dictionary mapping highway types to display categories
HIGHWAY_CATEGORY_MAP = {
    # Transit links
    'transit': 'Transit',
    'busway': 'Transit',
    'rail': 'Transit',
    'light_rail': 'Transit',
    
    # MAZ/TAZ centroids
    'MAZ': 'MAZ connector',
    'TAZ': 'TAZ connector',

    # Footway and cycle
    'footway': 'Footway/Cycle',
    'path': 'Footway/Cycle',
    'cycleway': 'Footway/Cycle',
    'pedestrian': 'Footway/Cycle',
    'steps': 'Footway/Cycle',
    'living_street': 'Footway/Cycle',
    
    # Auto links by hierarchy
    'motorway': 'Motorway/Trunk',
    'motorway_link': 'Motorway/Trunk',
    'trunk': 'Motorway/Trunk',
    'trunk_link': 'Motorway/Trunk',
    'primary': 'Primary/Secondary',
    'primary_link': 'Primary/Secondary',
    'secondary': 'Primary/Secondary',
    'secondary_link': 'Primary/Secondary',
    'tertiary': 'Tertiary/Local',
    'tertiary_link': 'Tertiary/Local',
    'residential': 'Tertiary/Local',
    'unclassified': 'Tertiary/Local',
    'service': 'Tertiary/Local',
}

# Dictionary for category styles (color, line width)
CATEGORY_STYLES = {
    'Transit': ('#1f77b4', 4),           # Medium blue
    'MAZ connector': ('#9467bd', 2),     # Purple, thin
    'TAZ connector': ('#6a3d9a', 3),     # Darker purple, medium
    'Footway/Cycle': ('#2ca02c', 2),     # Green
    'Motorway/Trunk': ('#d62728', 6),    # Dark red-orange
    'Primary/Secondary': ('#ff7f0e', 5), # Dark orange
    'Tertiary/Local': ('#ff9933', 4),    # Medium orange
    'Other': ('#808080', 2)              # Gray
}

def create_osmnx_plot(osm_network: nx.MultiDiGraph) -> Tuple[plt.Figure, plt.Axes]:
    """Create an interactive plot of an OSMnx network graph.
    
    Args:
        osm_network: A networkx MultiDiGraph created by OSMnx representing the street network.
    
    Returns:
        tuple: A tuple containing:
            - fig (matplotlib.figure.Figure): The matplotlib figure object.
            - ax (matplotlib.axes.Axes): The matplotlib axes object.
    """
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



def compare_original_and_simplified_networks(
    original_nw: nx.Graph, 
    simplified_nw: nx.Graph, 
    network_names: Tuple[str, str] = ("Original OSM", "Simplified OSM")
) -> None:
    """Compare statistics between original and simplified network graphs.
    
    Prints comprehensive statistics comparing node counts, edge counts, degree distributions,
    and edge lengths (if available) between two network graphs.
    
    Args:
        original_nw: A networkx graph representing the original network.
        simplified_nw: A networkx graph representing the simplified network.
        network_names (tuple, optional): A tuple of two strings naming the networks for display.
            Defaults to ("Original OSM", "Simplified OSM").
    
    Returns:
        None: Prints comparison statistics to console.
    """
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


def plot_node_degree_changes(original_nw: nx.Graph, simplified_nw: nx.Graph) -> None:
    """Plot bar chart comparing node degree distributions between original and simplified networks.
    
    Creates a bar chart showing the distribution of node degrees (number of connections per node)
    for both the original and simplified networks side by side.
    
    Args:
        original_nw: A networkx graph representing the original network.
        simplified_nw: A networkx graph representing the simplified network.
    
    Returns:
        None: Displays a matplotlib bar chart.
    """
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


def create_downtown_network_map(nw_gdf: gpd.GeoDataFrame, output_html_file: Optional[Path | str] = None) -> folium.Map:
    """Create an interactive Folium map of downtown San Francisco network links.
    
    Filters the network to downtown SF bounding box and creates an interactive map
    with color-coded highway types and tooltips showing link attributes.
    
    Args:
        nw_gdf (gpd.GeoDataFrame): A GeoDataFrame containing network links with columns including
            'A', 'B', 'highway', 'name', 'oneway', 'reversed', 'lanes', 'bike_access',
            'truck_access', 'walk_access', 'bus_only', 'ferry_only', 'rail_only'.
        output_html_file (Optional[Path | str]): If provided, saves the map to this HTML file path.
            Can be either a string path or pathlib.Path object. If None, no file is saved. Defaults to None.
    
    Returns:
        folium.Map: The interactive Folium map object that can be displayed or further modified.
    """
    # Define downtown bounding box
    downtown_sf_bbox = [-122.42, 37.77, -122.39, 37.80]

    # Filter network to bbox
    subset_gdf = nw_gdf.cx[downtown_sf_bbox[0]:downtown_sf_bbox[2], 
                            downtown_sf_bbox[1]:downtown_sf_bbox[3]]

    print(f"Original network: {len(nw_gdf):,} links")
    print(f"Subset network: {len(subset_gdf):,} links")

    # Create A + B column for tooltip
    subset_gdf["A & B (Combined)"] = subset_gdf["A"].astype(str) + ", " + subset_gdf["B"].astype(str) 
    
    # Create highway_display column with aggregate categories
    # Check for unknown highway types
    unknown_highways = set(subset_gdf['highway'].unique()) - set(HIGHWAY_CATEGORY_MAP.keys())
    if unknown_highways:
        print(f"ERROR: Unknown highway types found: {unknown_highways}")
        raise ValueError(f"Unknown highway types: {unknown_highways}. Please add them to HIGHWAY_CATEGORY_MAP.")
    
    subset_gdf['highway_display'] = subset_gdf['highway'].map(HIGHWAY_CATEGORY_MAP)

    # Define the tooltip columns
    tooltip_cols = ["A & B (Combined)", "highway", "highway_display", "name", "oneway", "reversed", "lanes", "ML_lanes", "access", "ML_access", "bike_access", "truck_access", "walk_access", "bus_only", "ferry_only", "rail_only"] 

    # Create base map with CartoDB light background
    center_lat, center_lon = 37.787589, -122.403542
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=15,
        tiles='CartoDB positron'  # Use CartoDB light tiles with proper name
    )
    
    # Add each display category with custom styling
    display_categories = subset_gdf['highway_display'].unique()
    
    for display_cat in display_categories:
        cat_subset = subset_gdf[subset_gdf['highway_display'] == display_cat]
        color, width = CATEGORY_STYLES.get(display_cat, ('#808080', 2))
        
        if not cat_subset.empty:
            folium.GeoJson(
                cat_subset,
                style_function=lambda x, color=color, width=width: {
                    'color': color,
                    'weight': width,
                    'opacity': 0.8
                },
                tooltip=folium.GeoJsonTooltip(
                    fields=tooltip_cols,
                    aliases=["Nodes (A,B)", "Highway Type", "Category", "Name", "One-way", "Reversed", "Lanes", "ML Lanes", "Access", "ML Access", "Bike Access", "Truck Access", "Walk Access", "Bus Only", "Ferry Only", "Rail Only"],
                    style="background-color: white; color: #333333; font-family: arial; font-size: 11px; padding: 7px;"
                ),
                popup=folium.GeoJsonPopup(
                    fields=tooltip_cols,
                    aliases=["Nodes (A,B)", "Highway Type", "Category", "Name", "One-way", "Reversed", "Lanes", "ML Lanes", "Access", "ML Access", "Bike Access", "Truck Access", "Walk Access", "Bus Only", "Ferry Only", "Rail Only"]
                )
            ).add_to(m)
    
    # Add simplified legend based on display categories actually present
    legend_html = '<div style="position: fixed; top: 10px; right: 10px; width: 200px; height: auto; background-color: white; border:2px solid grey; z-index:9999; font-size:11px; padding: 7px">'
    legend_html += '<p style="margin: 2px 0;"><b>Road Categories</b></p>'
    
    # Only add legend items for categories present in the data
    category_order = ['Transit', 'MAZ connector', 'TAZ connector', 'Footway/Cycle', 'Motorway/Trunk', 'Primary/Secondary', 'Tertiary/Local', 'Other']
    for category in category_order:
        if category in display_categories:
            color, width = CATEGORY_STYLES.get(category, ('#808080', 2))
            # Scale font size based on line width
            font_size = 10 + width * 2
            legend_html += f'<p style="margin: 2px 0;"><i class="fa fa-minus" style="color:{color}; font-size: {font_size}px"></i> {category}</p>'
    
    legend_html += '</div>'
    m.get_root().html.add_child(folium.Element(legend_html))
    
    if output_html_file:
        m.save(output_html_file)
    return m


def clip_original_and_simplified_links(orig_links: gpd.GeoDataFrame, links_gdf: gpd.GeoDataFrame, taz_gdf: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Clip original and simplified link networks to specific TAZ zones.
    
    Filters both original and simplified networks to only include links within
    specified Traffic Analysis Zones (TAZs).
    
    Args:
        orig_links (gpd.GeoDataFrame): GeoDataFrame of original network links.
        links_gdf (gpd.GeoDataFrame): GeoDataFrame of simplified network links.
        taz_gdf (gpd.GeoDataFrame): GeoDataFrame containing TAZ boundaries with a 'TAZ' column.
    
    Returns:
        tuple: A tuple containing:
            - orig_links_gdf_clip (gpd.GeoDataFrame): Original links clipped to TAZ boundaries.
            - links_gdf_clip (gpd.GeoDataFrame): Simplified links clipped to TAZ boundaries.
    """
    # For taz mask
    taz_list = [360, 293, 292, 406, 562, 561, 565]
    taz_gdf_subs = taz_gdf[taz_gdf["TAZ"].isin(taz_list)]

    # Clip
    orig_links_gdf_clip = gpd.clip(orig_links, taz_gdf_subs)
    links_gdf_clip = gpd.clip(links_gdf, taz_gdf_subs)

    return orig_links_gdf_clip, links_gdf_clip


def map_original_and_simplified_links(orig_links_gdf_clip: gpd.GeoDataFrame, links_gdf_clip: gpd.GeoDataFrame, output_file: Optional[Path | str] = None) -> folium.plugins.DualMap:
    """Create a dual-pane Folium map comparing original and simplified network links.
    
    Creates an interactive side-by-side map with original links on the left and simplified
    links on the right, color-coded by highway type with a shared legend.
    
    Args:
        orig_links_gdf_clip (gpd.GeoDataFrame): Clipped GeoDataFrame of original network links
            with a 'highway' column indicating road type.
        links_gdf_clip (gpd.GeoDataFrame): Clipped GeoDataFrame of simplified network links,
            optionally with a 'highway' column.
        output_file (Optional[Path | str]): Full file path where the output HTML map will be saved.
            Can be either a string path or pathlib.Path object. If None, no file is saved. Defaults to None.
    
    Returns:
        folium.plugins.DualMap: The dual map object showing both networks side by side.
            Optionally saves the map to the specified output file path if provided.
    """
    # Check for unknown highway types
    orig_links_gdf_clip = orig_links_gdf_clip.copy()
    unknown_highways = set(orig_links_gdf_clip['highway'].unique()) - set(HIGHWAY_CATEGORY_MAP.keys())
    if unknown_highways:
        print(f"ERROR: Unknown highway types in original links: {unknown_highways}")
        raise ValueError(f"Unknown highway types: {unknown_highways}. Please add them to HIGHWAY_CATEGORY_MAP.")
    
    orig_links_gdf_clip['highway_display'] = orig_links_gdf_clip['highway'].map(HIGHWAY_CATEGORY_MAP)
    
    links_gdf_clip = links_gdf_clip.copy()
    if 'highway' in links_gdf_clip.columns:
        unknown_highways = set(links_gdf_clip['highway'].unique()) - set(HIGHWAY_CATEGORY_MAP.keys())
        if unknown_highways:
            print(f"ERROR: Unknown highway types in simplified links: {unknown_highways}")
            raise ValueError(f"Unknown highway types: {unknown_highways}. Please add them to HIGHWAY_CATEGORY_MAP.")
        links_gdf_clip['highway_display'] = links_gdf_clip['highway'].map(HIGHWAY_CATEGORY_MAP)
    
    # Create A + B columns for tooltip
    orig_links_gdf_clip["A & B (Combined)"] = orig_links_gdf_clip["A"].astype(str) + ", " + orig_links_gdf_clip["B"].astype(str)
    links_gdf_clip["A & B (Combined)"] = links_gdf_clip["A"].astype(str) + ", " + links_gdf_clip["B"].astype(str)

    # Define tooltip fields
    tooltip_fields = ["A & B (Combined)", "highway", "highway_display", "name", "oneway", "reversed", "lanes", "ML_lanes", "access", "ML_access", "bike_access", "truck_access", "walk_access", "bus_only"]
    tooltip_aliases = ["A & B:", "Highway:", "Category:", "Name:", "Oneway:", "Reversed:", "Lanes:", "ML Lanes:", "Access:", "ML Access:", "Bike Access:", "Truck Access:", "Walk Access:", "Bus Only:"]
    
    # Get bounds for the map
    bounds = orig_links_gdf_clip.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

    # Create dual map without default tiles
    m = plugins.DualMap(location=[center_lat, center_lon], zoom_start=17, tiles=None)
    print(f"Created map {type(m)}")

    # Add CartoDB light background to both maps
    folium.TileLayer("CartoDB positron").add_to(m.m1)
    folium.TileLayer("CartoDB positron").add_to(m.m2)

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

    # Get unique display categories from original links
    display_categories = sorted(orig_links_gdf_clip['highway_display'].unique())

    # Add original links to left map with category-based colors and widths
    for display_cat in display_categories:
        cat_subset = orig_links_gdf_clip[orig_links_gdf_clip['highway_display'] == display_cat]
        color, width = CATEGORY_STYLES.get(display_cat, ('#808080', 2))
        
        if not cat_subset.empty:
            folium.GeoJson(
                cat_subset,
                style_function=lambda x, color=color, width=width: {
                    'color': color,
                    'weight': width,
                    'opacity': 0.8
                },
                tooltip=folium.GeoJsonTooltip(
                    fields=tooltip_fields,
                    aliases=tooltip_aliases
                )
            ).add_to(m.m1)

    # Handle simplified links styling
    if 'highway_display' not in links_gdf_clip.columns:
        # If no highway column, use default blue styling
        folium.GeoJson(
            links_gdf_clip,
            style_function=lambda x: {
                'color': '#1f77b4', 
                'weight': 3,
                'opacity': 0.8
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["A & B (Combined)"],
                aliases=["A & B:"]
            )
        ).add_to(m.m2)
    else:
        # Use category-based styling for simplified links
        simplified_categories = sorted(links_gdf_clip['highway_display'].unique())
        for display_cat in simplified_categories:
            cat_subset = links_gdf_clip[links_gdf_clip['highway_display'] == display_cat]
            color, width = CATEGORY_STYLES.get(display_cat, ('#808080', 2))
            
            if not cat_subset.empty:
                folium.GeoJson(
                    cat_subset,
                    style_function=lambda x, color=color, width=width: {
                        'color': color,
                        'weight': width,
                        'opacity': 0.8
                    },
                    tooltip=folium.GeoJsonTooltip(
                        fields=tooltip_fields,
                        aliases=tooltip_aliases
                    )
                ).add_to(m.m2)

    # Create dynamic simplified legend based on categories present
    all_categories = set(display_categories)
    if 'highway_display' in links_gdf_clip.columns:
        all_categories.update(links_gdf_clip['highway_display'].unique())
    
    legend_html = '<div style="position: fixed; bottom: 50px; left: 50px; width: 160px; height: auto; background-color: white; border:2px solid grey; z-index:9999; font-size:11px; padding: 7px">'
    legend_html += '<p style="margin: 2px 0;"><b>Link Categories</b></p>'
    
    # Add legend items in a logical order
    category_order = ['Transit', 'MAZ connector', 'TAZ connector', 'Footway/Cycle', 'Motorway/Trunk', 'Primary/Secondary', 'Tertiary/Local', 'Other']
    for category in category_order:
        if category in all_categories:
            color, width = CATEGORY_STYLES.get(category, ('#808080', 2))
            # Scale font size based on line width to show visual hierarchy
            font_size = 10 + width * 2
            legend_html += f'<p style="margin: 2px 0;"><i class="fa fa-minus" style="color:{color}; font-size: {font_size}px"></i> {category}</p>'
    
    legend_html += '</div>'

    # Add titles and legend to both maps
    m.m1.get_root().html.add_child(folium.Element(title_left))
    m.m1.get_root().html.add_child(folium.Element(legend_html))
    
    m.m2.get_root().html.add_child(folium.Element(title_right))
    m.m2.get_root().html.add_child(folium.Element(legend_html))

    if output_file:
        m.save(output_file)
    return m