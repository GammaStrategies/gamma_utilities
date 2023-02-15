#
#  periodic data action
#
import os
import sys
from pathlib import Path
import logging

# import signal
import multiprocessing as mp
from datetime import datetime, timezone


if __name__ == "__main__":
    # append parent directory pth
    CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
    PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
    sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION

from apps.database_feeder import (
    feed_hypervisor_static,
    feed_operations,
    feed_hypervisor_status,
    feed_prices,
)


def local_db_sequence():
    while True:
        for protocol in CONFIGURATION["script"]["protocols"].keys():
            for network in CONFIGURATION["script"]["protocols"][protocol][
                "networks"
            ].keys():

                # feed static operations
                for dex in CONFIGURATION["script"]["protocols"][protocol]["networks"][
                    network
                ].keys():
                    feed_hypervisor_static(
                        protocol=protocol,
                        network=network,
                        dex=dex,
                        rewrite=False,
                        threaded=True,
                    )

                # feed database with all operations from static hyprervisor addresses
                feed_operations(protocol=protocol, network=network)

                # feed database with status from all operations
                feed_hypervisor_status(
                    protocol=protocol, network=network, threaded=True
                )


def global_db_sequence():
    while True:
        for protocol in CONFIGURATION["script"]["protocols"].keys():
            for network in CONFIGURATION["script"]["protocols"][protocol][
                "networks"
            ].keys():
                # feed database with prices from all status
                feed_prices(
                    protocol=protocol, network=network, rewrite=False, threaded=False
                )


# START ####################################################################################################################
if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        " Start {}   ----------------------> ".format(__module_name)
    )

    global_db_sequence()

    logging.getLogger(__name__).info(
        " Exit {}    <----------------------".format(__module_name)
    )
