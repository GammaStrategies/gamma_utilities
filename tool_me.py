import os
import sys
import logging
from datetime import datetime, timezone

from bins.log import log_helper

# set working directory the script's
os.chdir(os.path.dirname(os.path.realpath(__file__)))


from bins.configuration import CONFIGURATION
from bins.general.general_utilities import log_time_passed, convert_string_datetime
from apps import (
    database_feeder,
    database_feeder_service,
    database_reScrape,
    database_reports,
    save_config,
)
from apps.checks import general as general_checks
from apps.repair import general as general_repairs

from tests import test
from bins.cache.files_manager import reset_cache_files


# START ####################################################################################################################
if __name__ == "__main__":
    print(f" Python version: {sys.version}")

    __module_name = " Gamma tools"

    ##### main ######
    logging.getLogger(__name__).info(
        f" Start {__module_name}   ----------------------> "
    )

    # start time log
    _startime = datetime.now(timezone.utc)

    # set telegram token and chat id if passed from command line
    if CONFIGURATION["_custom_"]["cml_parameters"].telegram_token:
        CONFIGURATION["logs"]["telegram"]["token"] = CONFIGURATION["_custom_"][
            "cml_parameters"
        ].telegram_token
        CONFIGURATION["logs"]["telegram"]["enabled"] = True
    if CONFIGURATION["_custom_"]["cml_parameters"].telegram_chat_id:
        CONFIGURATION["logs"]["telegram"]["chat_id"] = CONFIGURATION["_custom_"][
            "cml_parameters"
        ].telegram_chat_id
        CONFIGURATION["logs"]["telegram"]["enabled"] = True

    # cml debug mode ?
    if CONFIGURATION["_custom_"]["cml_parameters"].debug:
        try:
            # set log level
            CONFIGURATION["logs"]["level"] = "DEBUG"
            # reload configuration
            log_helper.setup_logging(customconf=CONFIGURATION)
        except Exception as e:
            logging.getLogger(__name__).error(f" Can't set cml debug mode. Error: {e} ")

    # convert datetimes if exist
    if CONFIGURATION["_custom_"]["cml_parameters"].ini_datetime:
        # convert to datetime UTC
        try:
            CONFIGURATION["_custom_"]["cml_parameters"].ini_datetime = (
                convert_string_datetime(
                    string=CONFIGURATION["_custom_"]["cml_parameters"].ini_datetime
                )
            )
        except Exception:
            logging.getLogger(__name__).error(
                f" Can't convert command line passed ini datetime-> {CONFIGURATION['_custom_']['cml_parameters'].ini_datetime}"
            )
    if CONFIGURATION["_custom_"]["cml_parameters"].end_datetime:
        # convert to datetime
        try:
            CONFIGURATION["_custom_"]["cml_parameters"].end_datetime = (
                convert_string_datetime(
                    string=CONFIGURATION["_custom_"]["cml_parameters"].end_datetime
                )
            )
        except Exception:
            logging.getLogger(__name__).error(
                f" Can't convert command line passed end datetime-> {CONFIGURATION['_custom_']['cml_parameters'].end_datetime}"
            )

    # reset cache files
    reset_cache_files()

    # choose the first of the  parsed options
    if CONFIGURATION["_custom_"]["cml_parameters"].db_feed:
        # database feeder:  --db_feed
        database_feeder.main(option=CONFIGURATION["_custom_"]["cml_parameters"].db_feed)
    elif CONFIGURATION["_custom_"]["cml_parameters"].service:
        # service loop  --service
        database_feeder_service.main(
            option=CONFIGURATION["_custom_"]["cml_parameters"].service
        )
    elif CONFIGURATION["_custom_"]["cml_parameters"].service_network:
        # service loop specific  --service_network
        database_feeder_service.main(
            option="network",
            network=CONFIGURATION["_custom_"]["cml_parameters"].service_network,
            protocol="gamma",
        )
    elif CONFIGURATION["_custom_"]["cml_parameters"].check:
        # checks   --check
        general_checks.main(option=CONFIGURATION["_custom_"]["cml_parameters"].check)

    elif CONFIGURATION["_custom_"]["cml_parameters"].repair:
        # repairs   --repair
        general_repairs.main(option=CONFIGURATION["_custom_"]["cml_parameters"].repair)

    elif CONFIGURATION["_custom_"]["cml_parameters"].analysis:
        # analysis   --analysis
        database_reports.main(
            option=CONFIGURATION["_custom_"]["cml_parameters"].analysis
        )

    elif CONFIGURATION["_custom_"]["cml_parameters"].rescrape:
        # rescrape   --rescrape
        database_reScrape.main(
            option=CONFIGURATION["_custom_"]["cml_parameters"].rescrape
        )

    elif CONFIGURATION["_custom_"]["cml_parameters"].save_config:
        # save config   --save_config="file to save"
        save_config.main(
            cfg_name=CONFIGURATION["_custom_"]["cml_parameters"].save_config
        )
    elif CONFIGURATION["_custom_"]["cml_parameters"].test:
        test.main(option=CONFIGURATION["_custom_"]["cml_parameters"].test)

    else:
        # nothin todo
        logging.getLogger(__name__).info(" Nothing to do. How u doin? ")

    logging.getLogger(__name__).info(
        f" took {log_time_passed.get_timepassed_string(start_time=_startime)} to complete"
    )
    logging.getLogger(__name__).info(
        f" Exit {__module_name}    <----------------------"
    )
