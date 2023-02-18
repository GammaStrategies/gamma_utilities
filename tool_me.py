import os
import sys
import logging
from datetime import datetime

# set working directory the script's
os.chdir(os.path.dirname(os.path.realpath(__file__)))


from bins.configuration import CONFIGURATION
from bins.general.general_utilities import log_time_passed
from apps import database_feeder, database_feeder_service


# START ####################################################################################################################
if __name__ == "__main__":

    print(f" Python version: {sys.version}")

    __module_name = " Gamma tools"

    ##### main ######
    logging.getLogger(__name__).info(
        " Start {}   ----------------------> ".format(__module_name)
    )
    # start time log
    _startime = datetime.utcnow()

    # choose the first of the  parsed options
    if CONFIGURATION["_custom_"]["cml_parameters"].db_feed:
        # database feeder:  -db_feed operations
        database_feeder.main(option=CONFIGURATION["_custom_"]["cml_parameters"].db_feed)
    elif CONFIGURATION["_custom_"]["cml_parameters"].service:
        # service loop
        database_feeder_service.main(
            option=CONFIGURATION["_custom_"]["cml_parameters"].service
        )
    elif CONFIGURATION["_custom_"]["cml_parameters"].service_network:
        database_feeder_service.main(
            option="network",
            network=CONFIGURATION["_custom_"]["cml_parameters"].service_network,
            protocol="gamma",
        )
    else:
        logging.getLogger(__name__).info(" Nothing to do. How u doin? ")

    logging.getLogger(__name__).info(
        " took {} to complete".format(
            log_time_passed.get_timepassed_string(start_time=_startime)
        )
    )
    logging.getLogger(__name__).info(
        " Exit {}    <----------------------".format(__module_name)
    )
