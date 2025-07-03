USAGE = """

Create MTC base year networks (2023) from the previously created 2015 networks.

Tested in July 2025 with:
  * network_wrangler, https://github.com/network-wrangler/network_wrangler/tree/main

References:
  * Asana: Year 2023 Model Run and Calibration Tasks > Network in Network Wrangler Format (https://app.asana.com/1/11860278793487/project/15119358130897/task/1209256117977561?focus=true)
  * MTC Year 2023 Network Creation Steps Google Doc (https://docs.google.com/document/d/1TU0nsUHmyKfYZDbwjeCFiW09w53fyWu7X3XcRlNyf2o/edit?tab=t.0#heading=h.kt1d1r2i57ei)
  * https://github.com/Metropolitan-Council/met_council_wrangler/blob/main/notebooks
"""
import datetime
import pathlib
import pandas as pd

import network_wrangler
from network_wrangler import WranglerLogger

INPUT_2015v12 = pathlib.Path("E:\\Box\\Modeling and Surveys\\Development\\Travel Model Two Conversion\\Model Inputs\\2015-tm22-dev-sprint-03\standard_network_after_project_cards")
INPUT_2023GTFS = pathlib.Path("M:\\Data\\Transit\\511\\2023-10")

OUTPUT_DIR = pathlib.Path("M:\\Development\\Travel Model Two\\Supply\\Network Creation 2025\\from_2015v12")
NOW = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

if __name__ == "__main__":
    INFO_LOG  = OUTPUT_DIR / f"create_mtc_network_{NOW}.info.log"
    DEBUG_LOG = OUTPUT_DIR / f"create_mtc_network_{NOW}.debug.log"

    network_wrangler.setup_logging(
        info_log_filename=INFO_LOG,
        debug_log_filename=DEBUG_LOG,
        std_out_level="info",
    )
    WranglerLogger.info(f"Created by {__file__}")

    # Read a GTFS network (not wrangler_flavored)
    gtfs_model = network_wrangler.transit.io.load_feed_from_path(INPUT_2023GTFS, wrangler_flavored=False)
    WranglerLogger.debug(f"gtfs_model:\n{gtfs_model}")
    
