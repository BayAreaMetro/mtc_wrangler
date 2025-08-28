# Network Wrangler: Creating Transportation Networks from OpenStreetMap & GTFS

## MoMo Workshop 2025

### Overview

This guide demonstrates how to create travel demand model networks from OpenStreetMap (OSM) data using Network Wrangler. The process integrates road network data from OSM with transit data from GTFS feeds to create comprehensive multimodal transportation networks suitable for modeling.

### Table of Contents
1. [Prerequisites](#prerequisites)
2. [Architecture](#architecture)
3. [Run the Script](#run-the-script)
4. [What does the script do?](#what-does-the-script-do)
5. [Output Files](#output-files)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Software
- **Python 3.9+** with conda/miniconda
- **Network Wrangler v1.0-beta.3** or later
- **OSMnx v1.9+** for OpenStreetMap data retrieval
- **GTFS feed** (e.g., from 511 Bay Area)

### Installation
```bash
# Create and activate conda environment
conda create -n network_wrangler python=3.10
conda activate network_wrangler

# TODO: For now, we need to use BayAreaMetro version of network_wrangler
# pending PR merge/release: 
# https://github.com/network-wrangler/network_wrangler/pull/408

# Install network-wrangler with visualization support
# pip install network-wrangler[viz]
# Ensure your current working directory is where you want the cloned directory
git clone https://github.com/BayAreaMetro/network_wrangler.git network_wrangler
# Install network_wrangler in editable mode
cd network_wrangler
pip install -e .

# Install additional dependencies
pip install scikit-learn tableauhyperapi pygris

# Clone mtc_wrangler with script to run
cd ..
git clone https://github.com/BayAreaMetro/mtc_wrangler.git mtc_wrangler
```

### Required Data Files
1. **County Shapefile**: Boundary definitions for your region
   - Example: `tl_2010_06_county10_9CountyBayArea.shp`
2. **GTFS Transit Feed**: Public transit schedules and routes
   - Example: `511gtfs_2023-09/` directory, download and unzip from [BayArea_511gtfs_2023-09.zip](https://drive.google.com/file/d/1wu-echoNNi5NzQh3BT4RwfnHYlUg8ZK5/view?usp=sharing)
3. **Output Directory**: Location for generated network files
   - Example: `output_from_OSM/`

---

## Architecture

### Data Flow
```
OpenStreetMap → OSMnx → NetworkX Graph → Standardization → Network Wrangler
                                                  ↓
                                           GTFS Integration
                                                  ↓
                                          Final Network Files
```

### Key Components

1. **OSM Data Extraction**: Downloads road network from OpenStreetMap
2. **Network Simplification**: Consolidates intersections and removes redundancies
3. **Attribute Standardization**: Normalizes highway types, lanes, and access
4. **GTFS Integration**: Adds transit stops and routes to road network
5. **Output Generation**: Creates files in multiple formats

---

## Run the Script

```bash
# Navigate to the script directory
cd mtc_wrangler/create_baseyear_network/

# Run for a single county
python create_mtc_network_from_OSM.py "San Francisco"

```

## What does the script do?

### Step 1: OSM Network Extraction

The script first downloads road network data from OpenStreetMap:

```python
# For a single county
g = osmnx.graph_from_place(f'{county}, California, USA', network_type='all')

# For Bay Area (using bounding box)
bbox = get_county_bbox(COUNTY_SHAPEFILE)
g = osmnx.graph_from_bbox(bbox, network_type='all')
```

**What happens:**
- Connects to OSM Overpass API
- Downloads all roads, paths, and transit ways
- Creates NetworkX MultiDiGraph structure
- Caches data locally for faster re-runs

**Output:** `0_graph_OSM_{county}.pkl` (cached raw network)

### Step 2: Network Simplification

The network is simplified to reduce complexity while preserving connectivity:

```python
g = osmnx.simplification.consolidate_intersections(
    g, 
    tolerance=20,  # feet
    rebuild_graph=True,
    dead_ends=True,
    reconnect_edges=True
)
```

**What happens:**
- Merges nearby intersections (within 20 feet)
- Preserves dead-ends and cul-de-sacs
- Maintains network topology
- Reduces node count by ~50-70%

**Output:** `1_graph_OSM_{county}_simplified20.pkl`

### Step 3: Attribute Standardization

The script standardizes OSM attributes for consistency:

#### Highway Classification
```python
standardize_highway_value(links_gdf)
```
- Maps OSM highway tags to standard categories
- Sets access permissions by mode (drive, walk, bike, bus, truck)
- Handles special cases (busways, pedestrian streets)

#### Lane Processing
```python
links_gdf = standardize_lanes_value(links_gdf)
```
- Resolves directional lanes (forward/backward)
- Separates bus lanes from general traffic
- Fills missing values using highway type statistics
- Handles bidirectional streets

### Step 4: County Assignment (Bay Area only)

For multi-county networks, performs spatial join:

```python
# Assign counties to nodes and links
nodes_gdf = gpd.sjoin(nodes_gdf, county_gdf, how='left', predicate='within')
links_gdf = gpd.sjoin(links_gdf, county_gdf, how='left', predicate='intersects')
```

### Step 5: ID Assignment

Creates model-specific identifiers based on county:

| County | Node Range | Link Range |
|--------|------------|------------|
| San Francisco | 1,000,000+ | 1,000,000+ |
| San Mateo | 1,500,000+ | 2,000,000+ |
| Santa Clara | 2,000,000+ | 3,000,000+ |
| Alameda | 2,500,000+ | 4,000,000+ |
| Contra Costa | 3,000,000+ | 5,000,000+ |
| Solano | 3,500,000+ | 6,000,000+ |
| Napa | 4,000,000+ | 7,000,000+ |
| Sonoma | 4,500,000+ | 8,000,000+ |
| Marin | 5,000,000+ | 9,000,000+ |
| External | 900,001+ | 0+ |

### Step 6: GTFS Integration

Loads and processes transit data:

```python
# Load GTFS feed
gtfs_model = load_feed_from_path(INPUT_2023GTFS, service_ids_filter=service_ids)

# Filter to geography
filter_transit_by_boundary(gtfs_model, county_gdf)

# Create transit network on roadway
feed = create_feed_from_gtfs_model(
    gtfs_model,
    roadway_network,
    local_crs=LOCAL_CRS_FEET,
    timeperiods=TIME_PERIODS,
    add_stations_and_links=True
)
```

**What happens:**
- Filters transit routes to specified geography
- Creates transit stops as network nodes
- Adds access/egress links between stops and roads
- Calculates service frequencies by time period

### Step 7: Output Generation

Creates final network files in multiple formats:

```python
# Write roadway network
write_roadway(roadway_network, out_dir=OUTPUT_DIR, 
              file_format='parquet', true_shape=True)

# Write transit feed
write_transit(feed, feed_dir, overwrite=True)
```

## Output Files

The script generates multiple file formats for different use cases:

### File Structure
```
output_from_OSM/
├── 0_graph_OSM_{county}.pkl                    # Raw OSM graph
├── 1_graph_OSM_{county}_simplified20.pkl       # Simplified graph
├── 3_simplified_{county}_links.hyper           # Tableau visualization
├── 3_simplified_{county}_nodes.hyper           # Tableau visualization
├── 4_roadway_network_{county}_link.parquet     # Road network
├── 4_roadway_network_{county}_node.parquet     # Road nodes
├── 4_gtfs_model_{county}/                      # Filtered GTFS
├── 5_roadway_network_inc_transit_{county}_*    # Network with transit
└── 6_feed_{county}/                            # Final transit feed
```

### File Formats

| Format | Extension | Use Case |
|--------|-----------|----------|
| Parquet | `.parquet` | Fast data analysis, Python/R |
| GeoPackage | `.gpkg` | GIS software (QGIS, ArcGIS) |
| GeoJSON | `.geojson` | Web mapping, interchange |
| Tableau Hyper | `.hyper` | Tableau visualization |

### Key Output Columns

**Links (Roads)**
- `A`, `B`: Start and end node IDs
- `highway`: Road type classification
- `lanes`: Number of general traffic lanes
- `buslanes`: Number of bus-only lanes
- `*_access`: Modal access permissions
- `model_link_id`: Unique identifier
- `geometry`: Link shape

**Nodes (Intersections)**
- `model_node_id`: Unique identifier
- `X`, `Y`: Coordinates (longitude, latitude)
- `county`: Assigned county
- `street_count`: Connectivity measure

---

## Troubleshooting

### Common Issues

#### 1. OSM Download Timeout
**Problem:** Network request times out when downloading large areas.
```
ReadTimeout: HTTPSConnectionPool(host='overpass-api.de', port=443)
```

**Solution:** The script caches downloads. Re-run to use cached data, or download smaller areas individually.

#### 2. Memory Issues
**Problem:** Out of memory when processing Bay Area network.

**Solution:** 
- Process counties individually first
- Increase system swap space
- Use a machine with more RAM (16GB+ recommended)

#### 3. Missing Lane Data
**Problem:** Many links have missing lane counts.

**Solution:** The script automatically fills missing values using:
1. Highway type statistics (mode of lanes per highway type)
2. Default of 1 lane if no data available

#### 4. Transit Stop Matching
**Problem:** Transit stops don't match to road network.
```
No path found between stops
```

**Solution:** 
- Check that road network includes transit-accessible streets
- Verify coordinate systems match
- Increase search radius for stop-to-road matching

### Performance Tips

1. **Use Caching**: The script caches intermediate files. Don't delete `.pkl` files unless necessary.

2. **Start Small**: Test with a single county before processing entire region.

3. **Monitor Progress**: Enable debug logging to track processing:
   ```python
   network_wrangler.setup_logging(std_out_level="debug")
   ```

4. **Parallel Processing**: Process multiple counties in parallel using separate terminal sessions.

---

## References

- [Network Wrangler Documentation](https://network-wrangler.github.io/network_wrangler/)
- [OSMnx Documentation](https://osmnx.readthedocs.io/)
- [OpenStreetMap Wiki](https://wiki.openstreetmap.org/)
- [GTFS Specification](https://gtfs.org/specification/gtfs/)
- [MTC Network Creation Steps](https://docs.google.com/document/d/1TU0nsUHmyKfYZDbwjeCFiW09w53fyWu7X3XcRlNyf2o/edit)

---

## Contact & Support

For questions about this workflow:
- GitHub Issues: [network-wrangler/network_wrangler](https://github.com/network-wrangler/network_wrangler/issues)
- MTC Contact: [Your contact information]

---

*Last Updated: January 2025*
*Script Version: 1.0*
*Network Wrangler Version: 1.0-beta.3*
