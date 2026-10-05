"""MTC-specific roadway network schemas.

Extends Network Wrangler base schemas with MTC-required fields and validation rules.
"""
from enum import IntEnum, Enum
from typing import Optional

import pandera as pa
from pandera import Field
from pandas import Int64Dtype as Int64
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


COUNTY_NAME_TO_CENTROID_START_NUM_TM2 = {
    MTCCounty.SAN_FRANCISCO.value: 1,
    MTCCounty.SAN_MATEO.value    : 100_001,
    MTCCounty.SANTA_CLARA.value  : 200_001,
    MTCCounty.ALAMEDA.value      : 300_001,
    MTCCounty.CONTRA_COSTA.value : 400_001,
    MTCCounty.SOLANO.value       : 500_001,
    MTCCounty.NAPA.value         : 600_001,
    MTCCounty.SONOMA.value       : 700_001,
    MTCCounty.MARIN.value        : 800_001,
}
"""Mapping of county names to centroid ID starting ranges 
planned for TM2.

https://bayareametro.github.io/tm2py/input/network/#county-node-numbering-system
"""

COUNTY_NAME_TO_CENTROID_START_NUM = {
    MTCCounty.SAN_FRANCISCO.value: 1,
    MTCCounty.SAN_MATEO.value    : 191,
    MTCCounty.SANTA_CLARA.value  : 347,
    MTCCounty.ALAMEDA.value      : 715,
    MTCCounty.CONTRA_COSTA.value : 1040,
    MTCCounty.SOLANO.value       : 1211,
    MTCCounty.NAPA.value         : 1291,
    MTCCounty.SONOMA.value       : 1318,
    MTCCounty.MARIN.value        : 1404,
}
"""Mapping of county names to centroid ID startaring ranges
for TM1 1454 zone system

https://github.com/BayAreaMetro/modeling-website/wiki/TazData#taz1454-by-county
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

GANTRY_NAME_TO_TOLLBOOTH = {
    'Benicia-Martinez Bridge Toll'           : 1,
    'Carquinez Bridge Toll'                  : 2,
    'Richmond-San Rafael Bridge Toll'        : 3,
    'Golden Gate Bridge Automated Toll Plaza': 4,
    'San Francisco-Oakland Bay Bridge Toll'  : 5,
    'San Mateo-Hayward Bridge Toll'          : 6,
    'Dumbarton Bridge Toll'                  : 7,
    'Antioch Bridge Toll'                    : 8,
}
"""Numbering consistent with TM1 TOLLCLASS:

See https://github.com/BayAreaMetro/modeling-website/wiki/MasterNetworkLookupTables#toll-code-tollclass"""

class MTCFacilityType(IntEnum):
    """Functional class (ft) codes for highway assignment.

    These codes are used to assign volume delay functions (VDF) in tm2py.

    Reference: [TM1 Master Network Lookup Tables - Facility Type (FT)](https://github.com/BayAreaMetro/modeling-website/wiki/MasterNetworkLookupTables#facility-type-ft)

    | Code | Facility Type                | Notes                                                          |
    |------|-------------------------------|-----------------------------------------------------------------|
    | 1    | Freeway-to-freeway connector  |                                                                   |
    | 2    | Freeway                       |                                                                   |
    | 3    | Expressway                    |                                                                   |
    | 4    | Collector                     |                                                                   |
    | 5    | Freeway ramp                  |                                                                   |
    | 6    | Dummy link                    | Used for centroid connectors and managed-lane access/egress links |
    | 7    | Major arterial                |                                                                   |
    | 8    | ITS-managed Freeway            | Not relevant to pricing or tolling                               |
    | 9    | Special facility              |                                                                   |
    | 10   | Toll plaza                    |                                                                   |
    | 11   | Local road                    | No TM1-equivalent; added for MTC use                             |
    | 99   | Not Assigned                  | No TM1-equivalent                                                |
    """
    FREEWAY_TO_FREEWAY_CONNECTOR = 1
    FREEWAY = 2
    EXPRESSWAY = 3
    COLLECTOR = 4
    RAMP = 5
    DUMMY_LINK = 6
    ARTERIAL = 7
    MANAGED_FREEWAY = 8
    SPECIAL_FACILITY = 9
    TOLL_PLAZA = 10
    LOCAL = 11
    NOT_ASSIGNED = 99

class MTCTollType(str, Enum):
    """Type of toll"""
    NO_TOLL = "no_toll"
    BRIDGE = "bridge"
    EXPRESS_LANE = "express_lane"
    CORDON = "cordon"
    ALL_LANE_TOLLING = "all_lane_tolling"

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
    ft: Optional[Series[Int64]] = Field(coerce=True, nullable=True, default=None)
    # TODO: Should this be automatically created from the access attribute in the RoadLinksTable?
    useclass: Optional[Series[Int64]] = Field(coerce=True, nullable=True, default=None)
    tolltype: Series[str] = Field(coerce=True, nullable=False, default=MTCTollType.NO_TOLL.value)
    tollbooth: Optional[Series[Int64]] = Field(coerce=True, nullable=True, default=None)
    tollseg: Optional[Series[Int64]] = Field(coerce=True, nullable=True, default=None)

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

    @pa.check("tolltype")
    def check_valid_tolltype(cls, tolltype: Series) -> Series[bool]:
        """Validate that tolltype values are valid MTCTollType enum values."""
        valid_tolltypes = {e.value for e in MTCTollType}
        return tolltype.isin(valid_tolltypes)
    
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
        is_ctrl_acc_hwy: Indicates if node is on a controlled access highway
          (freeway or expressway)
        is_interchange: For nodes with is_ctrl_acc_hwy==True, indicates if
          this node is an interchange
    """

    # Required MTC fields
    county: Series[str] = Field(coerce=True, nullable=False)
    taz_centroid: Series[bool] = Field(coerce=True, nullable=False)
    maz_centroid: Series[bool] = Field(coerce=True, nullable=False)
    # for highway/expressway reliability
    is_ctrl_acc_hwy: Series[bool] = Field(coerce=True, nullable=False)
    is_interchange: Series[bool] = Field(coerce=True, nullable=False)

    @pa.check("county")
    def check_valid_county(cls, county: Series) -> Series[bool]:
        """Validate that county values are valid MTCCounty enum values."""
        valid_counties = {e.value for e in MTCCounty}
        return county.isin(valid_counties)

    class Config(RoadNodesTable.Config):
        """Inherit parent configuration settings."""
        pass
