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

from bins.configuration import CONFIGURATION

from apps.database_feeder import (
    feed_hypervisor_static,
    feed_operations,
    feed_hypervisor_status,
    feed_prices,
)


def local_db_sequence():
    """feed all local database collections in an infinite loop"""
    try:
        while True:
            for protocol in CONFIGURATION["script"]["protocols"].keys():
                for network in CONFIGURATION["script"]["protocols"][protocol][
                    "networks"
                ].keys():

                    # feed static operations
                    for dex in CONFIGURATION["script"]["protocols"][protocol][
                        "networks"
                    ][network].keys():
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
    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(" Local database feeding loop stoped by user")
    except:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-feeding local database data. error {sys.exc_info()[0]}"
        )
    # send eveyone not updating anymore
    logging.getLogger("telegram").info(" Local database feeding loop stoped")


def global_db_sequence():
    """feed global database collections in an infinite loop"""
    try:
        while True:
            for protocol in CONFIGURATION["script"]["protocols"].keys():
                for network in CONFIGURATION["script"]["protocols"][protocol][
                    "networks"
                ].keys():
                    # feed database with prices from all status
                    feed_prices(protocol=protocol, network=network)
    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(
            " Global database feeding loop stoped by user"
        )
    except:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-feeding global database data. error {sys.exc_info()[0]}"
        )
    # send eveyone not updating anymore
    logging.getLogger("telegram").info(" Global database feeding loop stoped")


def main(option: str):

    if option == "local":
        local_db_sequence()
    elif option == "global":
        global_db_sequence()
    else:
        raise NotImplementedError(
            f" Can't find any action to be taken from {option} service option"
        )
