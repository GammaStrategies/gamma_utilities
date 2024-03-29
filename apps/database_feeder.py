import sys
import logging
import tqdm
import concurrent.futures
import contextlib
from datetime import datetime, timezone
from apps.feeds.frontend.revenue_stats_daily import feed_revenue_stats
from apps.feeds.latest.hypervisor_return.latest import feed_latest_hypervisor_returns

from apps.feeds.latest.mutifeedistribution.currents import (
    feed_latest_multifeedistribution_snapshot,
)
from apps.feeds.latest.price.latest import (
    create_latest_usd_prices_address_json,
    feed_latest_usd_prices,
)
from apps.feeds.price_paths import create_price_paths_json
from apps.feeds.queue.pulls.common import pull_from_queue
from apps.feeds.reports.execution import feed_global_reports
from apps.feeds.returns.builds import feed_hypervisor_returns
from bins.general.enums import Chain, text_to_chain, text_to_protocol
from .feeds.operations import feed_operations

from bins.configuration import CONFIGURATION

from bins.w3.protocols.general import erc20_cached

from bins.database.common.db_collections_common import (
    database_local,
    database_global,
)

from .feeds.static import (
    feed_hypervisor_static,
    feed_rewards_static,
    update_static_feeRecipients,
)

from .feeds.status.hypervisors.general import feed_hypervisor_status
from .feeds.status.rewards.general import feed_rewards_status

from .feeds.prices import feed_all_prices


### Blocks Timestamp #####################
def feed_blocks_timestamp(network: str):
    """ """

    logging.getLogger(__name__).info(
        f">Feeding {network} block <-> timestamp information"
    )
    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    # get a list of timestamps already in the database
    timestamps_indb = [
        x["timestamp"]
        for x in global_db_manager.get_all_block_timestamp(network=network)
    ]

    # set initial  a list of timestamps to process
    from_date = datetime.timestamp(datetime(year=2021, month=3, day=1))
    with contextlib.suppress(Exception):
        from_date = max(timestamps_indb)
    # define daily parameters
    day_in_seconds = 60 * 60 * 24
    total_days = int(
        (datetime.now(timezone.utc).timestamp() - from_date) / day_in_seconds
    )

    # create a list of timestamps to process  (daily)
    timestamps = [from_date + day_in_seconds * idx for idx in range(total_days)]

    # create a dummy erc20 obj as helper ( use only web3wrap functions)
    dummy_helper = erc20_cached(
        address="0x0000000000000000000000000000000000000000", network=network
    )
    for timestamp in timestamps:
        # brute force search closest block numbers from datetime
        block = dummy_helper.blockNumberFromTimestamp(
            timestamp=timestamp,
            inexact_mode="after",
            eq_timestamp_position="first",
        )


def feed_timestamp_blocks(network: str, protocol: str, threaded: bool = True):
    """fill global blocks data using blocks from the status collection

    Args:
        network (str):
        protocol (str):
    """
    logging.getLogger(__name__).info(
        f">Feeding {protocol}'s {network} timestamp <-> block information"
    )

    # setup database managers
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    global_db_manager = database_global(mongo_url=mongo_url)

    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    # create a dummy object to use inherited func
    dummy_helper = erc20_cached(
        address="0x0000000000000000000000000000000000000000", network=network
    )

    # get a list of blocks already in the database
    blocks_indb = [
        x["block"] for x in global_db_manager.get_all_block_timestamp(network=network)
    ]
    # create a list of items to process
    items_to_process = []
    for block in local_db_manager.get_distinct_items_from_database(
        collection_name="status", field="block"
    ):
        if block not in blocks_indb:
            items_to_process.append(block)

    _errors = 0

    # beguin processing
    with tqdm.tqdm(total=len(items_to_process)) as progress_bar:

        def _get_timestamp(block):
            try:
                # get timestamp
                return dummy_helper.timestampFromBlockNumber(block=block), block

            except Exception:
                logging.getLogger(__name__).exception(
                    f"Unexpected error while geting timestamp of block {block}"
                )
            return None, block

        if threaded:
            # threaded
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for timestamp, block in ex.map(_get_timestamp, items_to_process):
                    if timestamp:
                        # progress
                        progress_bar.set_description(
                            f" Retrieved timestamp of block {block}"
                        )
                        progress_bar.refresh()
                        # save to database
                        global_db_manager.set_block(
                            network=network, block=block, timestamp=timestamp
                        )
                    else:
                        # error found
                        _errors += 1

                    # update progress
                    progress_bar.update(1)
        else:
            # loop blocks to gather info
            for item in items_to_process:
                progress_bar.set_description(f" Retrieving timestamp of block {item}")
                progress_bar.refresh()
                try:
                    # get price
                    timestamp = _get_timestamp(item)
                    if timestamp:
                        # save to database
                        global_db_manager.set_block(
                            network=network, block=item, timestamp=timestamp
                        )
                    else:
                        # error found
                        _errors += 1
                except Exception:
                    logging.getLogger(__name__).exception(
                        f"Unexpected error while geting timestamp of block {item}"
                    )
                # add one
                progress_bar.update(1)

    with contextlib.suppress(Exception):
        if items_to_process:
            logging.getLogger(__name__).info(
                "   {} of {} ({:,.1%}) blocks could not be scraped due to errors".format(
                    _errors,
                    len(items_to_process),
                    (_errors / len(items_to_process)) if items_to_process else 0,
                )
            )


### Rewards  #######################


####### main ###########


def main(option="operations"):
    # NON CHAIN/PROTOCOL FEEDS
    if option == "global_reports":
        feed_global_reports()
    else:
        # CHAIN/PROTOCOL FEEDS
        for protocol in CONFIGURATION["script"]["protocols"]:
            # override networks if specified in cml
            networks = (
                CONFIGURATION["_custom_"]["cml_parameters"].networks
                or CONFIGURATION["script"]["protocols"][protocol]["networks"]
            )

            for network in networks:
                if option == "static":
                    for dex in CONFIGURATION["script"]["protocols"][protocol][
                        "networks"
                    ].get(network, []):
                        # filter if dex not in cml ( when cml is used )
                        if CONFIGURATION["_custom_"]["cml_parameters"].protocols:
                            if (
                                dex
                                not in CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].protocols
                            ):
                                continue

                        try:
                            # feed database with static hypervisor info
                            feed_hypervisor_static(
                                protocol=protocol,
                                network=network,
                                dex=dex,
                                rewrite=CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].rewrite,
                            )

                            # feed rewarders static
                            feed_rewards_static(
                                network=network,
                                dex=dex,
                                protocol=protocol,
                                rewrite=CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].rewrite,
                            )
                        except Exception as e:
                            logging.getLogger(__name__).exception(
                                f" Error processing {option} data from {network} {dex}  )-:  {e} "
                            )

                elif option == "static_feeRecipients":
                    for dex in CONFIGURATION["script"]["protocols"][protocol][
                        "networks"
                    ].get(network, []):
                        # filter if dex not in cml ( when cml is used )
                        if CONFIGURATION["_custom_"]["cml_parameters"].protocols:
                            if (
                                dex
                                not in CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].protocols
                            ):
                                continue

                        try:
                            # feed database
                            update_static_feeRecipients(
                                chain=text_to_chain(network),
                                dex=text_to_protocol(dex),
                                multiprocess=True,
                            )

                        except Exception as e:
                            logging.getLogger(__name__).exception(
                                f" Error processing {option} data from {network} {dex}  )-:  {e} "
                            )

                elif option == "operations":
                    for dex in CONFIGURATION["script"]["protocols"][protocol][
                        "networks"
                    ][network]:
                        # filter if dex not in cml ( when cml is used )
                        if CONFIGURATION["_custom_"]["cml_parameters"].protocols:
                            if (
                                dex
                                not in CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].protocols
                            ):
                                continue

                        # first feed static information
                        feed_hypervisor_static(
                            protocol=protocol, network=network, dex=dex
                        )

                    # feed database with all operations from static hyprervisor addresses
                    feed_operations(
                        protocol=protocol,
                        network=network,
                        date_ini=CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].ini_datetime,
                        date_end=CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].end_datetime,
                        block_ini=CONFIGURATION["_custom_"]["cml_parameters"].ini_block,
                        block_end=CONFIGURATION["_custom_"]["cml_parameters"].end_block,
                        force_back_time=False,
                    )

                elif option == "status":
                    # feed status
                    feed_hypervisor_status(
                        protocol=protocol, network=network, threaded=True
                    )

                    # feed rewards status
                    feed_rewards_status(protocol=protocol, network=network)

                elif option == "user_status":
                    # feed database with user status
                    raise NotImplementedError(f" {option} not implemented yet")

                elif option == "prices":
                    # feed database with prices from all status
                    feed_all_prices(network=network)

                elif option == "rewards":
                    for dex in CONFIGURATION["script"]["protocols"][protocol][
                        "networks"
                    ][network]:
                        # filter if dex not in cml ( when cml is used )
                        if CONFIGURATION["_custom_"]["cml_parameters"].protocols:
                            if (
                                dex
                                not in CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].protocols
                            ):
                                continue

                        feed_rewards_static(protocol=protocol, network=network, dex=dex)

                elif option == "queue":
                    pull_from_queue(
                        network=network,
                        types=CONFIGURATION["_custom_"]["cml_parameters"].queue_types,
                    )

                # elif option == "report_ramses":
                #     feed_report_ramses_gross_fees(chain=Chain.ARBITRUM, periods_back=1)

                elif option == "static_hypervisors":
                    for dex in CONFIGURATION["script"]["protocols"][protocol][
                        "networks"
                    ].get(network, []):
                        # filter if dex not in cml ( when cml is used )
                        if CONFIGURATION["_custom_"]["cml_parameters"].protocols:
                            if (
                                dex
                                not in CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].protocols
                            ):
                                continue

                        try:
                            # feed database with static hypervisor info
                            feed_hypervisor_static(
                                protocol=protocol,
                                network=network,
                                dex=dex,
                                rewrite=CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].rewrite,
                            )
                        except Exception as e:
                            logging.getLogger(__name__).exception(
                                f" Error processing {option} data from {network} {dex}  )-:  {e} "
                            )
                elif option == "static_rewards":
                    for dex in CONFIGURATION["script"]["protocols"][protocol][
                        "networks"
                    ].get(network, []):
                        # filter if dex not in cml ( when cml is used )
                        if CONFIGURATION["_custom_"]["cml_parameters"].protocols:
                            if (
                                dex
                                not in CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].protocols
                            ):
                                continue
                        try:
                            # feed rewarders static
                            feed_rewards_static(
                                network=network,
                                dex=dex,
                                protocol=protocol,
                                rewrite=CONFIGURATION["_custom_"][
                                    "cml_parameters"
                                ].rewrite,
                            )
                        except Exception as e:
                            logging.getLogger(__name__).exception(
                                f" Error processing {option} data from {network} {dex}  )-:  {e} "
                            )

                elif option == "frontend_revenue_stats":
                    feed_revenue_stats(
                        chains=[text_to_chain(network)],
                        rewrite=CONFIGURATION["_custom_"]["cml_parameters"].rewrite,
                    )

                elif option == "returns":
                    feed_hypervisor_returns(
                        chain=text_to_chain(network),
                        rewrite=CONFIGURATION["_custom_"]["cml_parameters"].rewrite,
                        hypervisor_addresses=CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].hypervisor_addresses,
                    )
                #### LATEST ####
                elif option == "latest_returns":
                    feed_latest_hypervisor_returns(
                        chain=text_to_chain(network),
                        hypervisor_addresses=CONFIGURATION["_custom_"][
                            "cml_parameters"
                        ].hypervisor_addresses,
                    )
                elif option == "latest_prices":
                    feed_latest_usd_prices()
                elif option == "create_json_prices":
                    create_latest_usd_prices_address_json()
                elif option == "latest_multifeedistributor":
                    feed_latest_multifeedistribution_snapshot()
                elif option == "create_price_paths_json":
                    create_price_paths_json()

                else:
                    raise NotImplementedError(
                        f" Can't find an operation match for {option} "
                    )
