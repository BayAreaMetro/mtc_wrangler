#!/bin/zsh
# Run create_mtc_network_from_OSM.py for all 9 Bay Area counties

# Array of Bay Area county names
counties=(
    "Alameda"
    "Contra Costa"
    "Marin"
    "Napa"
    "San Francisco"
    "San Mateo"
    "Santa Clara"
    "Solano"
    "Sonoma"
)

# Loop through each county and run the command
for county in "${counties[@]}"; do
    echo "========================================="
    echo "Processing county: $county"
    echo "========================================="

    python create_mtc_network_from_OSM.py "$county" ../../511gtfs_2023-09 ../../output_from_OSM parquet hyper

    # Check exit status
    if [ $? -eq 0 ]; then
        echo "✓ Successfully completed: $county"
    else
        echo "✗ Failed: $county"
        # Uncomment the line below to stop on first error
        # exit 1
    fi

    echo ""
done

echo "========================================="
echo "All counties processed"
echo "========================================="
