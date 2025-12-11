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

# Dictionary of common bounding boxes [min_lon, min_lat, max_lon, max_lat]
BOUNDING_BOXES = {
    'SF_downtown': [-122.42, 37.77, -122.39, 37.80],
    'SF_financial_district': [-122.41, 37.79, -122.39, 37.80],
    'SF_mission': [-122.43, 37.75, -122.40, 37.77],
    'SF': [-122.52, 37.70, -122.35, 37.82],  # All of San Francisco
    'Oakland_downtown': [-122.28, 37.79, -122.26, 37.82],
    'Berkeley_downtown': [-122.27, 37.86, -122.25, 37.88],
    'San_Jose_downtown': [-121.90, 37.32, -121.87, 37.35],
    'Bay_Area': [-123.0, 36.9, -121.0, 38.9]
}

# Dictionary mapping roadway types to display categories
ROADWAY_CATEGORY_MAP = {
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
    'track': 'Footway/Cycle',
    'bridleway': 'Footway/Cycle',
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
    'residential_link': 'Tertiary/Local',
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
    'Primary/Secondary': ('#e67300', 5), # Muted dark orange
    'Tertiary/Local': ('#cc7a00', 4),    # Muted medium orange
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


def create_roadway_network_map(
    nw_gdf: gpd.GeoDataFrame, 
    output_html_file: Optional[Path | str] = None,
    bbox_name: Optional[str] = None,
) -> folium.Map:
    """Create an interactive Folium map of roadway network links.
    
    Creates an interactive map with color-coded roadway types and tooltips showing link attributes.
    Can optionally filter to a specific bounding box area.
    
    Args:
        nw_gdf (gpd.GeoDataFrame): A GeoDataFrame containing network links with columns including
            'A', 'B', 'roadway', 'name', 'oneway', 'reversed', 'lanes', 'bike_access',
            'truck_access', 'walk_access', 'bus_only', 'ferry_only', 'rail_only'.
        output_html_file (Optional[Path | str]): If provided, saves the map to this HTML file path.
            Can be either a string path or pathlib.Path object. If None, no file is saved. Defaults to None.
        bbox_name (Optional[str]): Name of bounding box from BOUNDING_BOXES dictionary to filter the map.
            If None, no spatial filtering is applied. Defaults to 'SF_downtown'.
    
    Returns:
        folium.Map: The interactive Folium map object that can be displayed or further modified.
    """
    # Apply bounding box filter if specified
    if bbox_name:
        if bbox_name not in BOUNDING_BOXES:
            raise ValueError(f"Unknown bounding box: {bbox_name}. Available options: {list(BOUNDING_BOXES.keys())}")
        bbox = BOUNDING_BOXES[bbox_name]
        subset_gdf = nw_gdf.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]
        print(f"Original network: {len(nw_gdf):,} links")
        print(f"Filtered to {bbox_name}: {len(subset_gdf):,} links")
    else:
        subset_gdf = nw_gdf
        print(f"Network: {len(nw_gdf):,} links (no spatial filtering)")

    # Create A + B column for tooltip
    subset_gdf["A & B (Combined)"] = subset_gdf["A"].astype(str) + ", " + subset_gdf["B"].astype(str) 
    
    # Create roadway_display column with aggregate categories
    # Check for unknown roadway types
    unknown_roadways = set(subset_gdf['roadway'].unique()) - set(ROADWAY_CATEGORY_MAP.keys())
    if unknown_roadways:
        print(f"ERROR: Unknown roadway types found: {unknown_roadways}")
        raise ValueError(f"Unknown roadway types: {unknown_roadways}. Please add them to ROADWAY_CATEGORY_MAP.")
    
    subset_gdf['roadway_display'] = subset_gdf['roadway'].map(ROADWAY_CATEGORY_MAP)

    # Define the tooltip columns
    tooltip_cols = ["A & B (Combined)", "roadway", "roadway_display", "name", "oneway", "reversed", "lanes", "ML_lanes", "access", "ML_access", "bike_access", "truck_access", "walk_access", "bus_only", "ferry_only", "rail_only"] 

    # Calculate map center and zoom
    if bbox_name:
        # Use the filtered data bounds for center
        bounds = subset_gdf.total_bounds
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lon = (bounds[0] + bounds[2]) / 2
        zoom_start = 15
    else:
        # Use SF bbox for center/zoom when no filtering
        sf_bbox = BOUNDING_BOXES['SF']
        center_lat = (sf_bbox[1] + sf_bbox[3]) / 2
        center_lon = (sf_bbox[0] + sf_bbox[2]) / 2
        zoom_start = 12
    
    # Create base map with CartoDB light background
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom_start,
        tiles='CartoDB positron'  # Use CartoDB light tiles with proper name
    )
    
    # Add each display category with custom styling
    display_categories = subset_gdf['roadway_display'].unique()
    
    for display_cat in display_categories:
        cat_subset = subset_gdf[subset_gdf['roadway_display'] == display_cat]
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
                    aliases=["Nodes (A,B)", "Roadway Type", "Category", "Name", "One-way", "Reversed", "Lanes", "ML Lanes", "Access", "ML Access", "Bike Access", "Truck Access", "Walk Access", "Bus Only", "Ferry Only", "Rail Only"],
                    style="background-color: white; color: #333333; font-family: arial; font-size: 11px; padding: 7px;"
                ),
                popup=folium.GeoJsonPopup(
                    fields=tooltip_cols,
                    aliases=["Nodes (A,B)", "Roadway Type", "Category", "Name", "One-way", "Reversed", "Lanes", "ML Lanes", "Access", "ML Access", "Bike Access", "Truck Access", "Walk Access", "Bus Only", "Ferry Only", "Rail Only"]
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
        taz_gdf (gpd.GeoDataFrame): GeoDataFrame containing TAZ boundaries with a 'TAZ_NODE' column.
    
    Returns:
        tuple: A tuple containing:
            - orig_links_gdf_clip (gpd.GeoDataFrame): Original links clipped to TAZ boundaries.
            - links_gdf_clip (gpd.GeoDataFrame): Simplified links clipped to TAZ boundaries.
    """
    # For taz mask
    taz_list = [360, 293, 292, 406, 562, 561, 565]
    taz_gdf_subs = taz_gdf[taz_gdf["TAZ_NODE"].isin(taz_list)]

    # Clip
    orig_links_gdf_clip = gpd.clip(orig_links, taz_gdf_subs)
    links_gdf_clip = gpd.clip(links_gdf, taz_gdf_subs)

    return orig_links_gdf_clip, links_gdf_clip


def map_original_and_simplified_links(orig_links_gdf_clip: gpd.GeoDataFrame, links_gdf_clip: gpd.GeoDataFrame, output_file: Optional[Path | str] = None) -> folium.plugins.DualMap:
    """Create a dual-pane Folium map comparing original and simplified network links.
    
    Creates an interactive side-by-side map with original links on the left and simplified
    links on the right, color-coded by roadway type with a shared legend.
    
    Args:
        orig_links_gdf_clip (gpd.GeoDataFrame): Clipped GeoDataFrame of original network links
            with a 'roadway' column indicating road type.
        links_gdf_clip (gpd.GeoDataFrame): Clipped GeoDataFrame of simplified network links,
            optionally with a 'roadway' column.
        output_file (Optional[Path | str]): Full file path where the output HTML map will be saved.
            Can be either a string path or pathlib.Path object. If None, no file is saved. Defaults to None.
    
    Returns:
        folium.plugins.DualMap: The dual map object showing both networks side by side.
            Optionally saves the map to the specified output file path if provided.
    """
    # Check for unknown roadway types
    orig_links_gdf_clip = orig_links_gdf_clip.copy()
    unknown_roadways = set(orig_links_gdf_clip['roadway'].unique()) - set(ROADWAY_CATEGORY_MAP.keys())
    if unknown_roadways:
        print(f"ERROR: Unknown roadway types in original links: {unknown_roadways}")
        raise ValueError(f"Unknown roadway types: {unknown_roadways}. Please add them to ROADWAY_CATEGORY_MAP.")
    
    orig_links_gdf_clip['roadway_display'] = orig_links_gdf_clip['roadway'].map(ROADWAY_CATEGORY_MAP)
    
    links_gdf_clip = links_gdf_clip.copy()
    if 'roadway' in links_gdf_clip.columns:
        unknown_roadways = set(links_gdf_clip['roadway'].unique()) - set(ROADWAY_CATEGORY_MAP.keys())
        if unknown_roadways:
            print(f"ERROR: Unknown roadway types in simplified links: {unknown_roadways}")
            raise ValueError(f"Unknown roadway types: {unknown_roadways}. Please add them to ROADWAY_CATEGORY_MAP.")
        links_gdf_clip['roadway_display'] = links_gdf_clip['roadway'].map(ROADWAY_CATEGORY_MAP)
    
    # Create A + B columns for tooltip
    orig_links_gdf_clip["A & B (Combined)"] = orig_links_gdf_clip["A"].astype(str) + ", " + orig_links_gdf_clip["B"].astype(str)
    links_gdf_clip["A & B (Combined)"] = links_gdf_clip["A"].astype(str) + ", " + links_gdf_clip["B"].astype(str)

    # Define tooltip fields
    tooltip_fields = ["A & B (Combined)", "roadway", "roadway_display", "name", "oneway", "reversed", "lanes", "access", "bike_access", "truck_access", "walk_access", "bus_only"]
    tooltip_aliases = ["A & B:", "Roadway:", "Category:", "Name:", "Oneway:", "Reversed:", "Lanes:", "Access:", "Bike Access:", "Truck Access:", "Walk Access:", "Bus Only:"]
    
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
    display_categories = sorted(orig_links_gdf_clip['roadway_display'].unique())

    # Add original links to left map with category-based colors and widths
    for display_cat in display_categories:
        cat_subset = orig_links_gdf_clip[orig_links_gdf_clip['roadway_display'] == display_cat]
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
    if 'roadway_display' not in links_gdf_clip.columns:
        # If no roadway column, use default blue styling
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
        simplified_categories = sorted(links_gdf_clip['roadway_display'].unique())
        for display_cat in simplified_categories:
            cat_subset = links_gdf_clip[links_gdf_clip['roadway_display'] == display_cat]
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
    if 'roadway_display' in links_gdf_clip.columns:
        all_categories.update(links_gdf_clip['roadway_display'].unique())
    
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


def create_roadway_transit_map(
    roadway_gdf: gpd.GeoDataFrame,
    transit_gdf: gpd.GeoDataFrame,
    output_html_file: Optional[Path | str] = None,
    bbox_name: Optional[str] = None,
    route_ids: Optional[list[str]] = None
) -> folium.Map:
    """Create an interactive Folium map showing both roadway and transit networks.
    
    Creates a map with roadway links (excluding centroid connectors) and overlays transit
    network links as a separate layer. Transit links are shown in a distinct blue color.
    
    Args:
        roadway_gdf (gpd.GeoDataFrame): GeoDataFrame containing roadway network links with
            columns including 'A', 'B', 'roadway', 'name', 'lanes', etc.
        transit_gdf (gpd.GeoDataFrame): GeoDataFrame containing transit network links.
            Should have geometry and optionally route/service information.
        output_html_file (Optional[Path | str]): If provided, saves the map to this HTML file path.
            Can be either a string path or pathlib.Path object. If None, no file is saved. Defaults to None.
        bbox_name (Optional[str]): Name of bounding box from BOUNDING_BOXES dictionary to filter the map.
            If None, no spatial filtering is applied. Defaults to None.
        route_ids (Optional[list[str]]): List of route_id strings to display. If None, displays all routes.
            Defaults to None.
    
    Returns:
        folium.Map: The interactive Folium map object showing both networks.
    """
    # Apply bounding box filter if specified
    if bbox_name:
        if bbox_name not in BOUNDING_BOXES:
            raise ValueError(f"Unknown bounding box: {bbox_name}. Available options: {list(BOUNDING_BOXES.keys())}")
        bbox = BOUNDING_BOXES[bbox_name]
        
        # Filter networks to bbox
        roadway_subset = roadway_gdf.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]].copy()
        transit_subset = transit_gdf.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]].copy()
        
        print(f"Original roadway network: {len(roadway_gdf):,} links")
        print(f"Filtered to {bbox_name}: {len(roadway_subset):,} roadway links")
        print(f"Transit network in {bbox_name}: {len(transit_subset):,} links")
    else:
        roadway_subset = roadway_gdf.copy()
        transit_subset = transit_gdf.copy()
        print(f"Roadway network: {len(roadway_gdf):,} links (no spatial filtering)")
        print(f"Transit network: {len(transit_gdf):,} links (no spatial filtering)")
    
    # Exclude centroid connectors (MAZ and TAZ) from roadway
    roadway_subset = roadway_subset[~roadway_subset['roadway'].isin(['MAZ', 'TAZ'])]
    print(f"Roadway network after removing centroids: {len(roadway_subset):,} links")
    
    # Filter transit by route_ids if specified
    if route_ids is not None and 'route_id' in transit_subset.columns:
        transit_subset = transit_subset[transit_subset['route_id'].isin(route_ids)].copy()
        print(f"Transit network filtered to {len(route_ids)} routes: {len(transit_subset):,} links")
        if transit_subset.empty:
            print(f"WARNING: No transit links found for route_ids: {route_ids}")
    
    # Create A + B column for roadway tooltips
    roadway_subset["A & B (Combined)"] = roadway_subset["A"].astype(str) + ", " + roadway_subset["B"].astype(str)
    
    # Create roadway_display column for roadway
    unknown_roadways = set(roadway_subset['roadway'].unique()) - set(ROADWAY_CATEGORY_MAP.keys())
    if unknown_roadways:
        print(f"WARNING: Unknown roadway types found: {unknown_roadways}. Treating as 'Other'.")
        for unk in unknown_roadways:
            ROADWAY_CATEGORY_MAP[unk] = 'Other'
    
    roadway_subset['roadway_display'] = roadway_subset['roadway'].map(ROADWAY_CATEGORY_MAP)
    
    # Calculate map center and zoom
    if bbox_name:
        # Use the filtered data bounds for center
        bounds = roadway_subset.total_bounds
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lon = (bounds[0] + bounds[2]) / 2
        zoom_start = 15
    else:
        # Use SF bbox for center/zoom when no filtering
        sf_bbox = BOUNDING_BOXES['SF']
        center_lat = (sf_bbox[1] + sf_bbox[3]) / 2
        center_lon = (sf_bbox[0] + sf_bbox[2]) / 2
        zoom_start = 12
    
    # Create base map with CartoDB light background
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom_start,
        tiles='CartoDB positron'
    )
    
    # Add roadway network layers by category (excluding centroids)
    display_categories = roadway_subset['roadway_display'].unique()
    
    for display_cat in display_categories:
        cat_subset = roadway_subset[roadway_subset['roadway_display'] == display_cat]
        color, width = CATEGORY_STYLES.get(display_cat, ('#808080', 2))
        
        if not cat_subset.empty:
            folium.GeoJson(
                cat_subset,
                style_function=lambda x, color=color, width=width: {
                    'color': color,
                    'weight': width,
                    'opacity': 0.7  # Slightly transparent for roadway
                },
                tooltip=folium.GeoJsonTooltip(
                    fields=["A & B (Combined)", "roadway", "roadway_display", "name", "lanes"],
                    aliases=["Nodes (A,B)", "roadway Type", "Category", "Name", "Lanes"],
                    style="background-color: white; color: #333333; font-family: arial; font-size: 11px; padding: 7px;"
                ),
                name=f"Roadway: {display_cat}"
            ).add_to(m)
    
    # Add transit network as overlay
    if not transit_subset.empty:
        # Convert to GeoJSON to handle data type issues
        import json
        
        # Create a clean copy for conversion
        transit_clean = transit_subset.copy()
        
        # Convert problematic data types before GeoJSON conversion
        for col in transit_clean.columns:
            if col == 'geometry':
                continue
            try:
                # Convert int64 and float64 to native Python types
                if pd.api.types.is_integer_dtype(transit_clean[col]):
                    transit_clean[col] = transit_clean[col].fillna(0).astype(int)
                elif pd.api.types.is_float_dtype(transit_clean[col]):
                    transit_clean[col] = transit_clean[col].fillna(0.0).astype(float)
                elif pd.api.types.is_object_dtype(transit_clean[col]):
                    transit_clean[col] = transit_clean[col].fillna('').astype(str)
            except:
                # If conversion fails, convert to string
                transit_clean[col] = transit_clean[col].astype(str)
        
        # Create A + B column for transit tooltips if columns exist
        if 'A' in transit_clean.columns and 'B' in transit_clean.columns:
            transit_clean["A & B (Combined)"] = transit_clean["A"].astype(str) + ", " + transit_clean["B"].astype(str)
        
        # Convert to GeoJSON string and back to ensure JSON compatibility
        transit_geojson = json.loads(transit_clean.to_json())
        
        # Prepare transit tooltip fields based on available columns
        transit_tooltip_fields = []
        transit_tooltip_aliases = []
        
        # Check for common transit fields and add if present
        if 'A & B (Combined)' in transit_clean.columns:
            transit_tooltip_fields.append('A & B (Combined)')
            transit_tooltip_aliases.append('Nodes (A,B)')
        if 'trip_id' in transit_clean.columns:
            transit_tooltip_fields.append('trip_id')
            transit_tooltip_aliases.append('Trip ID')
        if 'route_id' in transit_clean.columns:
            transit_tooltip_fields.append('route_id')
            transit_tooltip_aliases.append('Route ID')
        if 'route_short_name' in transit_clean.columns:
            transit_tooltip_fields.append('route_short_name')
            transit_tooltip_aliases.append('Route')
        if 'direction_id' in transit_clean.columns:
            transit_tooltip_fields.append('direction_id')
            transit_tooltip_aliases.append('Direction')
        if 'name' in transit_clean.columns:
            transit_tooltip_fields.append('name')
            transit_tooltip_aliases.append('Name')
        if 'shape_id' in transit_clean.columns:
            transit_tooltip_fields.append('shape_id')
            transit_tooltip_aliases.append('Shape ID')
        
        # Create transit layer with distinct styling
        if transit_tooltip_fields:
            transit_layer = folium.GeoJson(
                transit_geojson,
                style_function=lambda x: {
                    'color': '#87CEEB',  # Light sky blue for transit
                    'weight': 4,
                    'opacity': 0.9
                },
                tooltip=folium.GeoJsonTooltip(
                    fields=transit_tooltip_fields,
                    aliases=transit_tooltip_aliases,
                    style="background-color: lightyellow; color: #333333; font-family: arial; font-size: 11px; padding: 7px;"
                ),
                name="Transit Network"
            )
        else:
            transit_layer = folium.GeoJson(
                transit_geojson,
                style_function=lambda x: {
                    'color': '#87CEEB',  # Light sky blue for transit
                    'weight': 4,
                    'opacity': 0.9
                },
                name="Transit Network"
            )
        
        transit_layer.add_to(m)
    
    # Add layer control to toggle layers
    folium.LayerControl().add_to(m)
    
    # Create legend
    legend_html = '<div style="position: fixed; top: 10px; right: 10px; width: 200px; height: auto; background-color: white; border:2px solid grey; z-index:9999; font-size:11px; padding: 7px">'
    legend_html += '<p style="margin: 2px 0;"><b>Network Layers</b></p>'
    
    # Add transit to legend
    legend_html += '<p style="margin: 2px 0;"><i class="fa fa-minus" style="color:#87CEEB; font-size: 14px"></i> Transit Network</p>'
    
    # Add separator
    legend_html += '<hr style="margin: 5px 0;">'
    legend_html += '<p style="margin: 2px 0;"><b>Road Categories</b></p>'
    
    # Add roadway categories
    category_order = ['Transit', 'Footway/Cycle', 'Motorway/Trunk', 'Primary/Secondary', 'Tertiary/Local', 'Other']
    for category in category_order:
        if category in display_categories:
            color, width = CATEGORY_STYLES.get(category, ('#808080', 2))
            font_size = 10 + width * 2
            legend_html += f'<p style="margin: 2px 0;"><i class="fa fa-minus" style="color:{color}; font-size: {font_size}px"></i> {category}</p>'
    
    legend_html += '</div>'
    m.get_root().html.add_child(folium.Element(legend_html))
    
    if output_html_file:
        m.save(output_html_file)
    
    return m