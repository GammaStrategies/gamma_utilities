#
#  periodic data action
#

import os
import sys
import logging

# import signal
import multiprocessing as mp
from datetime import datetime, timedelta, timezone
import time
from apps.feeds.latest.mutifeedistribution.currents import (
    feed_latest_multifeedistribution_snapshot,
)
from apps.feeds.price_paths import create_price_paths_json
from apps.feeds.revenue_operations import (
    create_revenue_addresses,
    feed_revenue_operations,
)
from bins.general.enums import text_to_chain

from bins.general.general_utilities import identify_me

from .parallel_feed import process_queues, select_process_queues

from bins.configuration import CONFIGURATION

from .feeds.operations import feed_operations
from .database_feeder import (
    feed_timestamp_blocks,
    feed_blocks_timestamp,
)

from .feeds.status.hypervisors.general import feed_hypervisor_status
from .feeds.status.rewards.general import feed_rewards_status

from .feeds.prices import (
    feed_prices,
    create_tokenBlocks_all,
)

from .feeds.latest.price.latest import (
    create_latest_usd_prices_address_json,
    feed_latest_usd_prices,
)

from .database_checker import repair_all


def network_sequence_loop(
    protocol: str,
    network: str,
    do_prices: bool = False,
    do_userStatus: bool = False,
    do_repairs: bool = False,
):
    """local database feeding loop.
        it will also feed the 'blocks' global collection

    Args:
        protocol (str):
        network (str):
    """

    # feed database with all operations from static hypervisor addresses
    feed_operations(protocol=protocol, network=network, force_back_time=True)

    # feed database with status
    feed_hypervisor_status(protocol=protocol, network=network, threaded=True)

    # feed global blocks data with status
    feed_timestamp_blocks(network=network, protocol=protocol)

    # feed global blocks data with daily
    feed_blocks_timestamp(network=network)

    if do_prices:
        # feed network prices ( before user status to avoid price related errors)
        price_sequence_loop(network=network)

    if do_userStatus:
        # feed user_status data
        raise NotImplementedError("user status data is not implemented yet")

    if do_repairs:
        # try to repair all errors found in logs
        repair_all()

    # feed rewards status ( needs prices and blocks)
    feed_rewards_status(protocol=protocol, network=network)


def price_sequence_loop(network: str):
    # feed most used token proces
    limit_prices = 100

    feed_prices(
        network=network,
        price_ids=create_tokenBlocks_all(network=network, limit=limit_prices),
        coingecko=True,
        use_not_to_process_prices=True,
        limit_not_to_process_prices=limit_prices * 2,
        max_prices=limit_prices,
    )

    # force feed prices from already known using conversion
    # logging.getLogger(__name__).info(f">   all token prices from already known/top")
    # feed_prices_force_sqrtPriceX96(protocol=protocol, network=network)

    # # feed all token prices left but weth
    # logging.getLogger(__name__).info(f">   all token prices left but weth")
    # feed_prices(
    #     protocol=protocol,
    #     network=network,
    #     price_ids=create_tokenBlocks_allTokensButWeth(
    #         protocol=protocol, network=network
    #     ),
    #     coingecko=False,
    # )
    # # feed all token prices left
    # logging.getLogger(__name__).info(f">   all token prices left")
    # feed_prices(
    #     protocol=protocol,
    #     network=network,
    #     price_ids=create_tokenBlocks_allTokens(protocol=protocol, network=network),
    #     coingecko=True,
    # )

    # # feed rewards token prices
    # logging.getLogger(__name__).info(f">   rewards token prices")
    # feed_prices(
    #     protocol=protocol,
    #     network=network,
    #     price_ids=create_tokenBlocks_rewards(protocol=protocol, network=network),
    #     coingecko=True,
    # )


# services
def local_db_service():
    """feed all local database collections in an infinite loop"""
    identity = identify_me()
    # send eveyone service ON
    logging.getLogger("telegram").info(
        f" Local database feeding loop started at {identity}"
    )
    try:
        while True:
            for protocol in CONFIGURATION["script"]["protocols"]:
                # override networks if specified in cml
                networks = (
                    CONFIGURATION["_custom_"]["cml_parameters"].networks
                    or CONFIGURATION["script"]["protocols"][protocol]["networks"]
                )
                for network in networks:
                    network_sequence_loop(
                        protocol=protocol,
                        network=network,
                        do_prices=CONFIGURATION["_custom_"]["cml_parameters"].do_prices
                        or False,
                        do_repairs=CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].do_repairs
                        or False,
                    )

    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(" Local database feeding loop stoped by user")
    except Exception:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-feeding local database data. error {sys.exc_info()[0]}"
        )
    # send eveyone not updating anymore
    logging.getLogger("telegram").info(
        f" Local database feeding loop stoped at {identity}"
    )


def global_db_service():
    """feed global database collections in an infinite loop"""
    identity = identify_me()
    # send eveyone service ON
    logging.getLogger("telegram").info(
        f" Global database feeding loop started at {identity}"
    )
    try:
        while True:
            for protocol in CONFIGURATION["script"]["protocols"]:
                # override networks if specified in cml
                networks = (
                    CONFIGURATION["_custom_"]["cml_parameters"].networks
                    or CONFIGURATION["script"]["protocols"][protocol]["networks"]
                )
                for network in networks:
                    price_sequence_loop(network=network)

    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(
            " Global database feeding loop stoped by user"
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-feeding global database data. error {e}"
        )
    # send eveyone not updating anymore
    logging.getLogger("telegram").info(
        f" Global database feeding loop stoped at {identity}"
    )


def network_db_service(
    protocol: str,
    network: str,
    do_prices: bool = False,
    do_userStatus: bool = False,
    do_repairs: bool = False,
):
    """feed one local database collection in an infinite loop"""
    identity = identify_me()
    logging.getLogger("telegram").info(
        f" {protocol}'s {network} database feeding loop started at {identity}"
    )
    # get minimum time between loops ( defaults to 5 minutes)
    min_loop_time = 60 * (
        CONFIGURATION["_custom_"]["cml_parameters"].min_loop_time
        or CONFIGURATION["script"].get("min_loop_time", 1)
    )
    try:
        while True:
            _startime = datetime.now(timezone.utc)
            network_sequence_loop(
                protocol=protocol,
                network=network,
                do_prices=do_prices,
                do_userStatus=do_userStatus,
                do_repairs=do_repairs,
            )
            _endtime = datetime.now(timezone.utc)
            if (_endtime - _startime).total_seconds() < min_loop_time:
                sleep_time = min_loop_time - (_endtime - _startime).total_seconds()
                logging.getLogger(__name__).debug(
                    f" {protocol}'s {network} sleeping for {sleep_time} seconds to loop again"
                )
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(
            f" {protocol}'s {network} database feeding loop stoped by user"
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-feeding {protocol}'s {network} database data. error {e}"
        )

    # telegram messaging
    logging.getLogger("telegram").info(
        f" {protocol}'s {network} database feeding loop stoped at {identity}"
    )


def queue_db_service():
    """Process all database queue in an infinite loop"""
    # identify srvr
    identity = identify_me()
    # send eveyone service ON
    logging.getLogger("telegram").info(
        f" Database queue processing loop started at {identity}"
    )
    logging.getLogger(__name__).info(" Database queue processing loop started")
    try:
        select_process_queues(
            maximum_tasks=CONFIGURATION["script"].get("queue_maximum_tasks", 10),
            queue_level=CONFIGURATION["_custom_"]["cml_parameters"].queue_level or 0,
        )
    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(" Database queue loop stoped by user")
    except Exception:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-processing database queue. error {sys.exc_info()[0]}"
        )
    # send eveyone not updating anymore
    logging.getLogger("telegram").info(f" Database queue loop stoped at {identity}")


def operations_db_service():
    """feed all database collections with operations in an infinite loop"""
    identity = identify_me()
    # send eveyone service ON
    logging.getLogger("telegram").info(
        f" Operations database feeding loop started at {identity}"
    )

    # get minimum time between loops ( defaults to 5 minutes)
    min_loop_time = 60 * (
        CONFIGURATION["_custom_"]["cml_parameters"].min_loop_time
        or CONFIGURATION["script"].get("min_loop_time", 1)
    )

    try:
        while True:
            _startime = datetime.now(timezone.utc)
            for protocol in CONFIGURATION["script"]["protocols"]:
                # override networks if specified in cml
                networks = (
                    CONFIGURATION["_custom_"]["cml_parameters"].networks
                    or CONFIGURATION["script"]["protocols"][protocol]["networks"]
                )
                for network in networks:
                    # ops
                    feed_operations(protocol=protocol, network=network)

            # nforce a min time between loops
            _endtime = datetime.now(timezone.utc)
            if (_endtime - _startime).total_seconds() < min_loop_time:
                sleep_time = min_loop_time - (_endtime - _startime).total_seconds()
                logging.getLogger(__name__).debug(
                    f" Operations database feeding service is sleeping for {sleep_time} seconds to loop again"
                )
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(
            " Operations database feeding loop stoped by user"
        )
    except Exception:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-feeding database with operations. error {sys.exc_info()[0]}"
        )
    # send eveyone not updating anymore
    logging.getLogger("telegram").info(
        f" Operations database feeding loop stoped at {identity}"
    )


def current_prices_db_service():
    identity = identify_me()
    # send eveyone service ON
    logging.getLogger("telegram").info(
        f" Current prices database feeding loop started at {identity}"
    )

    # control var to recreate the price list json file used as source for this service
    _create_file_startime = datetime.now(timezone.utc)
    _create_file_every = 60 * 60 * 2  # in seconds
    create_json_process: mp.Process | None = None

    def create_json_file(create_json_process):
        if not create_json_process or not create_json_process.is_alive():
            create_json_process = mp.Process(
                target=create_latest_usd_prices_address_json,
                name="create_json",
            )
        elif create_json_process.exitcode < 0:
            logging.getLogger(__name__).error(
                "   create_json_file process ended with an error or a terminate"
            )
            create_json_process = mp.Process(
                target=create_latest_usd_prices_address_json,
                name="create_json",
            )
        else:
            # try join n reset
            create_json_process.join()
            create_json_process = mp.Process(
                target=create_latest_usd_prices_address_json,
                name="create_json",
            )

        logging.getLogger(__name__).debug("   Starting json file creation process")
        create_json_process.start()

    # check if the json file exists, if not create it
    if not os.path.isfile(os.path.join("data", "current_usd_prices.json")):
        create_json_file(create_json_process)

    try:
        while True:
            _starttime = datetime.now(timezone.utc)
            feed_latest_usd_prices(threaded=True)
            # recreate the price json file
            _endtime = datetime.now(timezone.utc)

            if (_endtime - _create_file_startime).total_seconds() > _create_file_every:
                _create_file_startime = _endtime
                create_json_file(create_json_process)

            if (datetime.now(timezone.utc) - _starttime).total_seconds() < 20:
                sleep_time = abs(
                    20 - (datetime.now(timezone.utc) - _starttime).total_seconds()
                )
                logging.getLogger(__name__).debug(
                    f"   Sleeping for {sleep_time} seconds to loop again"
                )
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(
            " Current prices database feeding loop stoped by user"
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-feeding database with current prices. error {e}"
        )
    # send eveyone not updating anymore
    logging.getLogger("telegram").info(
        f" Current prices database feeding loop stoped at {identity}"
    )


def latest_db_service():
    """loop feeding all latest_<name> collections"""
    identity = identify_me()
    # send eveyone service ON
    logging.getLogger("telegram").info(
        f" Latests collections database feeding loop started at {identity}"
    )

    def process_manager(process_to_manage, target: callable, name: str, args=()):
        start = True
        recreate = False
        join = False

        if not process_to_manage or not process_to_manage.is_alive():
            logging.getLogger(__name__).debug(f"   {name} process is not alive.")
            recreate = True
        elif process_to_manage.exitcode < 0:
            logging.getLogger(__name__).error(
                f"   {name} process ended with an error or a terminate."
            )
            recreate = True
        else:
            logging.getLogger(__name__).warning(
                f"  {name} process is still alive and we r trying to execute it. "
            )
            # set process not to start
            start = False

        # process operations
        if join:
            logging.getLogger(__name__).debug(f"   Joining {name} process.")
            process_to_manage.join()
        if recreate:
            logging.getLogger(__name__).debug(f"   Creating {name} process.")
            process_to_manage = mp.Process(
                target=target,
                name=name,
                args=args,
            )
        if start:
            logging.getLogger(__name__).debug(f"   Starting {name} process.")
            process_to_manage.start()

    # time control var to fire callables
    time_control_loop = {
        "latest_prices": {
            "every": 120,  # 2 minute
            "last": time.time(),
            "callable": feed_latest_usd_prices,
            "args": [(True)],
            "process": None,
        },
        "create_json_prices": {
            "every": 60 * 60 * 2,  # 2 hours
            "last": time.time(),
            "callable": create_latest_usd_prices_address_json,
            "args": (),
            "process": None,
        },
        "latest_multifeedistributor": {
            "every": 60 * 10,  # 10 minutes
            "last": time.time(),
            "callable": feed_latest_multifeedistribution_snapshot,
            "args": (),
            "process": None,
        },
        "create_price_paths_json": {
            "every": 60 * 60 * 5,  # 5 hours
            "last": time.time(),
            "callable": create_price_paths_json,
            "args": (),
            "process": None,
        },
    }

    try:
        while True:
            for key, value in time_control_loop.items():
                if (time.time() - value["last"]) > value["every"]:
                    value["last"] = time.time()
                    logging.getLogger(__name__).debug(f"   Starting {key} ")
                    process_manager(
                        process_to_manage=value["process"],
                        target=value["callable"],
                        args=value["args"],
                        name=key,
                    )

                    # value["callable"](*value["args"])
                    logging.getLogger(__name__).debug(
                        f"   {key} finished in {time.time() - value['last']} seconds"
                    )
            # wait 10 seconds to loop again
            logging.getLogger(__name__).debug(
                f"   Sleeping for 10 seconds to loop again"
            )
            time.sleep(10)

    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(
            " Latests collections database feeding loop stoped by user"
        )
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-feeding database with latests collections. error {e}"
        )
    # send eveyone not updating anymore
    logging.getLogger("telegram").info(
        f" Latests collections database feeding loop stoped at {identity}"
    )
    # kill any process still alive
    for key, value in time_control_loop.items():
        if value["process"] and value["process"].is_alive():
            logging.getLogger(__name__).debug(f"   joining {key} process.")
            value["process"].join()


def revenue_operations_db_service():
    """feed all database collections with revenue operations in an infinite loop"""
    identity = identify_me()
    # send eveyone service ON
    logging.getLogger("telegram").info(
        f" Revenue operations database feeding loop started at {identity}"
    )

    # minimum time between loops, in seconds
    min_loop_time = 3600

    try:
        while True:
            _startime = datetime.now(timezone.utc)
            for protocol in CONFIGURATION["script"]["protocols"]:
                # override networks if specified in cml
                networks = (
                    CONFIGURATION["_custom_"]["cml_parameters"].networks
                    or CONFIGURATION["script"]["protocols"][protocol]["networks"]
                )
                for network in networks:
                    # find out addresses and block range to scrape

                    feed_revenue_operations(
                        chain=text_to_chain(network),
                        block_ini=CONFIGURATION["_custom_"]["cml_parameters"].ini_block,
                        block_end=CONFIGURATION["_custom_"]["cml_parameters"].end_block,
                        max_blocks_step=5000,
                        rewrite=CONFIGURATION["_custom_"]["cml_parameters"].rewrite,
                    )

            # nforce a min time between loops
            _endtime = datetime.now(timezone.utc)
            if (_endtime - _startime).total_seconds() < min_loop_time:
                sleep_time = min_loop_time - (_endtime - _startime).total_seconds()
                logging.getLogger(__name__).debug(
                    f" Revenue operations database feeding service is sleeping for {sleep_time} seconds to loop again"
                )
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logging.getLogger(__name__).debug(
            " Operations database feeding loop stoped by user"
        )
    except Exception:
        logging.getLogger(__name__).exception(
            f" Unexpected error while loop-feeding database with operations. error {sys.exc_info()[0]}"
        )
    # send eveyone not updating anymore
    logging.getLogger("telegram").info(
        f" Operations database feeding loop stoped at {identity}"
    )


def main(option: str, **kwargs):
    if option == "local":
        local_db_service()
    elif option == "global":
        global_db_service()
    elif option == "network":
        network_db_service(
            protocol=kwargs["protocol"],
            network=kwargs["network"],
            do_prices=CONFIGURATION["_custom_"]["cml_parameters"].do_prices or False,
            do_userStatus=CONFIGURATION["_custom_"]["cml_parameters"].do_userStatus
            or False,
            do_repairs=CONFIGURATION["_custom_"]["cml_parameters"].do_repairs or False,
        )
    elif option == "queue":
        queue_db_service()
    elif option == "operations":
        operations_db_service()
    elif option == "current_prices":
        current_prices_db_service()
    elif option == "latest":
        latest_db_service()
    elif option == "revenue_operations":
        revenue_operations_db_service()
    else:
        raise NotImplementedError(
            f" Can't find any action to be taken from {option} service option"
        )
