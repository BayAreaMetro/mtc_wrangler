"""MTC-specific roadway network schemas.

Extends Network Wrangler base schemas with MTC-required fields and validation rules.
"""
from enum import IntEnum, Enum
from typing import Optional

import pandera as pa
from pandera import Field
from pandera.typing import Series

from network_wrangler.models.roadway.tables import RoadLinksTable, RoadNodesTable

class MTCCounty(str, Enum):
    """Nine Bay Area counties in the MTC region."""
    ALAMEDA = "Alameda"
    CONTRA_COSTA = "Contra Costa"
    MARIN = "Marin"
    NAPA = "Napa"
    SAN_FRANCISCO = "San Francisco"
    SAN_MATEO = "San Mateo"
    SANTA_CLARA = "Santa Clara"
    SOLANO = "Solano"
    SONOMA = "Sonoma"
    EXTERNAL = "External"


COUNTY_NAME_TO_CENTROID_START_NUM = {
    MTCCounty.SAN_FRANCISCO.value: 0,
    MTCCounty.SAN_MATEO.value    : 100_000,
    MTCCounty.SANTA_CLARA.value  : 200_000,
    MTCCounty.ALAMEDA.value      : 300_000,
    MTCCounty.CONTRA_COSTA.value : 400_000,
    MTCCounty.SOLANO.value       : 500_000,
    MTCCounty.NAPA.value         : 600_000,
    MTCCounty.SONOMA.value       : 700_000,
    MTCCounty.MARIN.value        : 800_000,
}
"""Mapping of county names to centroid ID starting ranges.

https://bayareametro.github.io/tm2py/input/network/#county-node-numbering-system
"""

COUNTY_NAME_TO_NODE_START_NUM = {
    MTCCounty.SAN_FRANCISCO.value: 1_000_000,
    MTCCounty.SAN_MATEO.value    : 1_500_000,
    MTCCounty.SANTA_CLARA.value  : 2_000_000,
    MTCCounty.ALAMEDA.value      : 2_500_000,
    MTCCounty.CONTRA_COSTA.value : 3_000_000,
    MTCCounty.SOLANO.value       : 3_500_000,
    MTCCounty.NAPA.value         : 4_000_000,
    MTCCounty.SONOMA.value       : 4_500_000,
    MTCCounty.MARIN.value        : 5_000_000,
    MTCCounty.EXTERNAL.value     : 900_001,
}
"""Mapping of county names to node ID starting ranges.

Each county is assigned a range of node IDs to ensure unique, non-overlapping
identification across the MTC network. External nodes start at 900,001.

https://bayareametro.github.io/tm2py/input/network/#county-node-numbering-system
"""


MTC_COUNTIES = tuple(
    county for county in COUNTY_NAME_TO_NODE_START_NUM.keys()
    if county != MTCCounty.EXTERNAL.value
)
"""Tuple of MTC county names in node ID range order (excludes External).

Contains the nine Bay Area counties ordered by their node ID ranges:
('San Francisco', 'San Mateo', 'Santa Clara', 'Alameda', 'Contra Costa',
'Solano', 'Napa', 'Sonoma', 'Marin')
"""


COUNTY_NAME_TO_NUM = {county: i + 1 for i, county in enumerate(MTC_COUNTIES)}
"""Mapping of county names to sequential numbers (1-9).

Counties are numbered based on their node ID range order:
1=San Francisco, 2=San Mateo, 3=Santa Clara, 4=Alameda, 5=Contra Costa,
6=Solano, 7=Napa, 8=Sonoma, 9=Marin
"""


class MTCFacilityType(IntEnum):
    """Functional class (ft) codes for highway assignment.

    These codes are used to assign volume delay functions (VDF) in tm2py.

    Reference: [MTC Network Rebuild Requirements](https://docs.google.com/document/d/17OeXT8jxIst-vmGLl6eZVXx5b20cmct1p1WOpAuhi0M/edit?usp=sharing)

    | Code | Facility Type       | Intention                                                      | Speed Limit      | Example                  |
    |------|---------------------|----------------------------------------------------------------|------------------|--------------------------|
    | 1    | Freeway             | Move vehicles across counties                                  | 50+ mph          | I-80                     |
    | 2    | Expressway          | Connect freeways to other freeways or business districts       | 40-60 mph        | San Tomas Expressway     |
    | 3    | Ramp                | Connect arterials to freeways or expressways                   | 20-45 mph        | Tassajara Road to I-580  |
    | 4    | Divided Arterial    | Move vehicles across cities                                    | 35-50 mph        | El Camino Real           |
    | 5    | Undivided Arterial  | Move vehicles across cities                                    | 35-45 mph        | Ashby Ave                |
    | 6    | Collector           | Collect traffic from local roads and deliver to arterials      | 25-40 mph        | Fruitvale Road           |
    | 7    | Local               | Connect roads to homes                                         | 20-35 mph        | Lance Drive              |
    | 8    | Connector           | Connects centroids; access/egress links for managed lanes      | ??               |                          |
    | 99   | Not Assigned        | Service Road                                                   | 20 mph           | Parking lot              |

    **Key Characteristics:**

    - **Controlled Access**: Freeway (always), Expressway (sometimes), others (no)
    - **Turn Pockets/Lanes**: Freeway/Expressway/Ramp (always/N/A), Arterials (nearly always), Collectors (sometimes), Local/Dummy (never)
    - **Physical Separation**: Freeway/Expressway (always), Ramp/Divided Arterial (usually/nearly always), others (rarely/never)
    - **On-street Parking**: Never for Freeway/Expressway/Ramp, sometimes for Arterials, usually/always for Collector/Local/Dummy
    - **Walkable**: Never for Freeway/Ramp, rarely for Expressway, nearly always to always for Arterials/Collectors/Local/Dummy
    """
    FREEWAY = 1
    EXPRESSWAY = 2
    RAMP = 3
    DIVIDED_ARTERIAL = 4
    UNDIVIDED_ARTERIAL = 5
    COLLECTOR = 6
    LOCAL = 7
    CONNECTOR = 8
    NOT_ASSIGNED = 99


class MTCUseClass(IntEnum):
    """Vehicle-class restrictions classification codes.

    Used to define link access restrictions (auto-only, HOV only, etc.)
    in highway assignment.
    """
    GENERAL_PURPOSE = 0
    HOV2 = 2
    HOV3 = 3
    NO_TRUCKS = 4


class MTCRoadLinksTable(RoadLinksTable):
    """MTC-specific roadway links table with additional required fields.

    Extends the base RoadLinksTable from Network Wrangler with MTC-specific
    attributes required for Bay Area transportation modeling and highway assignment.

    Additional Required Fields:
        county: County name (must be one of the 9 Bay Area counties)
        ft: Functional class (used to assign volume delay functions)
        useclass: Vehicle-class restrictions classification (auto-only, HOV only, etc.)
        tollbooth: Toll booth location indicator (bridge vs value toll)
        tollseg: Toll segment index for toll value lookups
    """

    # Required MTC fields
    county: Series[str] = Field(coerce=True, nullable=False)
    ft: Optional[Series[int]] = Field(coerce=True, nullable=True, default=None)
    # TODO: Should this be automatically created from the access attribute in the RoadLinksTable?
    useclass: Optional[Series[int]] = Field(coerce=True, nullable=True, default=None)
    tollbooth: Optional[Series[int]] = Field(coerce=True, nullable=True, default=None)
    tollseg: Optional[Series[int]] = Field(coerce=True, nullable=True, default=None)

    @pa.check("county")
    def check_valid_county(cls, county: Series) -> Series[bool]:
        """Validate that county values are valid MTCCounty enum values."""
        valid_counties = {e.value for e in MTCCounty}
        return county.isin(valid_counties)

    @pa.check("ft")
    def check_valid_ft(cls, ft: Series) -> Series[bool]:
        """Validate that ft values are valid MTCFacilityType enum values."""
        valid_fts = {e.value for e in MTCFacilityType}
        # Allow NaN for optional field
        return ft.isna() | ft.isin(valid_fts)

    @pa.check("useclass")
    def check_valid_useclass(cls, useclass: Series) -> Series[bool]:
        """Validate that useclass values are valid MTCUseClass enum values."""
        valid_useclasses = {e.value for e in MTCUseClass}
        # Allow NaN for optional field
        return useclass.isna() | useclass.isin(valid_useclasses)

    class Config(RoadLinksTable.Config):
        """Inherit parent configuration settings."""
        pass


class MTCRoadNodesTable(RoadNodesTable):
    """MTC-specific roadway nodes table with additional required fields.

    Extends the base RoadNodesTable from Network Wrangler with MTC-specific
    attributes required for Bay Area transportation modeling.

    Additional Required Fields:
        county: County name (must be one of the 9 Bay Area counties)
        taz_centroid: Indicates if node is a TAZ (Traffic Analysis Zone) centroid
        maz_centroid: Indicates if node is a MAZ (Micro-zone) centroid
    """

    # Required MTC fields
    county: Series[str] = Field(coerce=True, nullable=False)
    taz_centroid: Series[bool] = Field(coerce=True, nullable=False)
    maz_centroid: Series[bool] = Field(coerce=True, nullable=False)

    @pa.check("county")
    def check_valid_county(cls, county: Series) -> Series[bool]:
        """Validate that county values are valid MTCCounty enum values."""
        valid_counties = {e.value for e in MTCCounty}
        return county.isin(valid_counties)

    class Config(RoadNodesTable.Config):
        """Inherit parent configuration settings."""
        pass
