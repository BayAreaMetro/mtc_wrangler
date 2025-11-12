"""MTC-specific roadway network schemas.

Extends Network Wrangler base schemas with MTC-required fields and validation rules.
"""
from typing import Optional

import pandera as pa
from pandera import Field
from pandera.typing import Series

from network_wrangler.models.roadway.tables import RoadLinksTable, RoadNodesTable


class MTCRoadLinksTable(RoadLinksTable):
    """MTC-specific roadway links table with additional required fields.

    Extends the base RoadLinksTable from Network Wrangler with MTC-specific
    attributes required for Bay Area transportation modeling.

    Additional Required Fields:
        county: County name (must be one of the 9 Bay Area counties)
        jurisdiction: Jurisdiction/agency responsible for the facility
        mtc_facility_type: MTC facility type classification

    Additional Optional Fields:
        hov_eligibility: HOV lane eligibility requirements
        toll_facility: Toll facility identifier
        express_lane_facility: Express lane facility identifier
    """

    # Required MTC fields
    county: Series[str] = Field(coerce=True, nullable=False)
    jurisdiction: Series[str] = Field(coerce=True, nullable=False)
    mtc_facility_type: Series[str] = Field(coerce=True, nullable=False)

    # Optional MTC fields
    hov_eligibility: Optional[Series[str]] = Field(coerce=True, nullable=True, default=None)
    toll_facility: Optional[Series[str]] = Field(coerce=True, nullable=True, default=None)
    express_lane_facility: Optional[Series[str]] = Field(coerce=True, nullable=True, default=None)

    @pa.check("county")
    def check_valid_county(cls, county: Series) -> Series[bool]:
        """Validate county is one of the 9 Bay Area counties."""
        valid_counties = {
            'Alameda',
            'Contra Costa',
            'Marin',
            'Napa',
            'San Francisco',
            'San Mateo',
            'Santa Clara',
            'Solano',
            'Sonoma'
        }
        return county.isin(valid_counties)

    @pa.check("mtc_facility_type")
    def check_valid_facility_type(cls, mtc_facility_type: Series) -> Series[bool]:
        """Validate facility type is a recognized MTC classification."""
        valid_types = {
            'freeway',
            'expressway',
            'major_arterial',
            'minor_arterial',
            'collector',
            'local',
            'ramp',
            'connector',
            'centroid_connector'
        }
        return mtc_facility_type.isin(valid_types)

    class Config(RoadLinksTable.Config):
        """Inherit parent configuration settings."""
        pass


class MTCRoadNodesTable(RoadNodesTable):
    """MTC-specific roadway nodes table with additional required fields.

    Extends the base RoadNodesTable from Network Wrangler with MTC-specific
    attributes required for Bay Area transportation modeling.

    Additional Required Fields:
        county: County name (must be one of the 9 Bay Area counties)

    Additional Optional Fields:
        taz: Traffic Analysis Zone identifier
        maz: Micro-zone/MAZ identifier
    """

    # Required MTC fields
    county: Series[str] = Field(coerce=True, nullable=False)

    # Optional MTC fields
    taz: Optional[Series[int]] = Field(coerce=True, nullable=True, default=None)
    maz: Optional[Series[int]] = Field(coerce=True, nullable=True, default=None)

    @pa.check("county")
    def check_valid_county(cls, county: Series) -> Series[bool]:
        """Validate county is one of the 9 Bay Area counties."""
        valid_counties = {
            'Alameda',
            'Contra Costa',
            'Marin',
            'Napa',
            'San Francisco',
            'San Mateo',
            'Santa Clara',
            'Solano',
            'Sonoma'
        }
        return county.isin(valid_counties)

    class Config(RoadNodesTable.Config):
        """Inherit parent configuration settings."""
        pass
