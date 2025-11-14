"""MTC-specific network classes.

Provides MTC-customized versions of Network Wrangler network classes with
additional validation for MTC-required fields.
"""
from pathlib import Path
from typing import Optional, Union

from network_wrangler import WranglerLogger
from network_wrangler.roadway.network import RoadwayNetwork
from network_wrangler.utils.models import validate_df_to_model

from .mtc_roadway_schema import MTCRoadLinksTable, MTCRoadNodesTable


class MTCRoadwayNetwork(RoadwayNetwork):
    """MTC-specific roadway network with additional validation.

    Extends RoadwayNetwork to enforce MTC-specific schema requirements including
    county, jurisdiction, and mtc_facility_type fields.

    Args:
        nodes_df: GeoDataFrame of roadway nodes
        links_df: GeoDataFrame of roadway links
        shapes_df: GeoDataFrame of roadway shapes (optional)
        validate_mtc: If True, validates against MTC schemas (default: True)
        **kwargs: Additional arguments passed to RoadwayNetwork

    Example:
        ```python
        from mtc_wrangler.models.mtc_network import MTCRoadwayNetwork

        # Load a network with MTC validation
        net = MTCRoadwayNetwork.read(
            link_file="links.geojson",
            node_file="nodes.geojson",
            validate_mtc=True
        )

        # Access network data
        print(net.links_df[['model_link_id', 'county', 'jurisdiction']])
        ```
    """

    def __init__(
        self,
        nodes_df=None,
        links_df=None,
        shapes_df=None,
        validate_mtc: bool = True,
        **kwargs
    ):
        """Initialize MTC Roadway Network with optional MTC-specific validation."""
        # Initialize parent RoadwayNetwork
        super().__init__(
            nodes_df=nodes_df,
            links_df=links_df,
            shapes_df=shapes_df,
            **kwargs
        )

        # Apply MTC-specific validation if requested
        if validate_mtc:
            self.validate()

    def validate(self):
        """Validate network against MTC-specific schemas.

        This method can be called explicitly to validate the network after
        modifications have been made to the dataframes.

        Example:
            ```python
            # Modify network
            net.links_df['county'] = 'Alameda'

            # Validate changes
            net.validate()
            ```
        """
        WranglerLogger.debug("MTCRoadwayNetwork.validate() called")
        self.links_df = validate_df_to_model(self.links_df, MTCRoadLinksTable)
        self.nodes_df = validate_df_to_model(self.nodes_df, MTCRoadNodesTable)

    @classmethod
    def read(
        cls,
        link_file: Union[str, Path],
        node_file: Union[str, Path],
        shape_file: Optional[Union[str, Path]] = None,
        validate_mtc: bool = True,
        **kwargs
    ) -> "MTCRoadwayNetwork":
        """Read network from files with MTC validation.

        Args:
            link_file: Path to links file (GeoJSON, shapefile, etc.)
            node_file: Path to nodes file (GeoJSON, shapefile, etc.)
            shape_file: Optional path to shapes file
            validate_mtc: If True, validates against MTC schemas (default: True)
            **kwargs: Additional arguments passed to parent read method

        Returns:
            MTCRoadwayNetwork instance

        Example:
            ```python
            net = MTCRoadwayNetwork.read(
                link_file="data/links.geojson",
                node_file="data/nodes.geojson",
                validate_mtc=True
            )
            ```
        """
        # Use parent class read method
        network = RoadwayNetwork.read(
            link_file=link_file,
            node_file=node_file,
            shape_file=shape_file,
            **kwargs
        )

        # Convert to MTCRoadwayNetwork
        mtc_network = cls(
            nodes_df=network.nodes_df,
            links_df=network.links_df,
            shapes_df=network.shapes_df,
            validate_mtc=validate_mtc,
            **{k: v for k, v in network.__dict__.items()
               if k not in ['nodes_df', 'links_df', 'shapes_df']}
        )

        return mtc_network

    def write(
        self,
        out_dir: Union[str, Path],
        validate_mtc: bool = True,
        **kwargs
    ):
        """Write network to files with optional MTC validation.

        Args:
            out_dir: Output directory for network files
            validate_mtc: If True, validates against MTC schemas before writing
            **kwargs: Additional arguments passed to parent write method

        Example:
            ```python
            net.write("output/network", validate_mtc=True)
            ```
        """
        # Validate before writing if requested
        if validate_mtc:
            self.validate()

        # Use parent write method
        super().write(out_dir=out_dir, **kwargs)
