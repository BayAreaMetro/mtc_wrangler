
This directory contains scripts for building a baseyear MTC network.

Files:

* [`create_mtc_network_2023_from_2015v12.py`](create_mtc_network_2023_from_2015v12.py) - Creates the 2023 network from the 2015 v12 network created by WSP around 2018.  Uses a [511 GTFS regional transit dataset](https://511.org/open-data/transit) for transit. Not fully functional.
 
* [`create_mtc_network_from_OSM.py`](create_mtc_network_from_OSM.py) - Creates the 2023 network from [Open Street Map (OSM)](https://www.openstreetmap.org) and  Uses a [511 GTFS regional transit dataset](https://511.org/open-data/transit) for transit.. Mostly functional.  Also used for the [Modeling Mobility GMNS Workshop](NetworkWrangler_MoMoWorkshop_2025.md)

* [`create_mtc_network_from_overture.py`](create_mtc_network_from_overture.py) - Quick exploration to create a network using the [Overture Maps Transportation theme](https://docs.overturemaps.org/guides/transportation/) but development has been paused since this source has no information on lane counts.

* [`tableau_utils.py`](tableau_utils.py) - Utilities to export networks to Tableau hyper files, which is a quick and useful viewing tool if you have a Tableau license.
