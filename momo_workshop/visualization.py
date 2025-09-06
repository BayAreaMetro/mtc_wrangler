import osmnx as ox
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def create_osmnx_plot(osm_network):
    
    # Create an interactive plot using OSMnx
    fig, ax = ox.plot_graph(
        osm_network,
        figsize=(15, 15),
        node_size=0,  # Hide nodes for cleaner view
        edge_color='blue',
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