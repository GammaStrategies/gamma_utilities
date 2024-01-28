import logging
from apps.feeds.operations import feed_operations
from apps.feeds.queue.push import build_and_save_queue_from_operation
from apps.feeds.queue.queue_item import QueueItem
from bins.database.helpers import get_default_localdb, get_from_localdb
from bins.errors.general import ProcessingError
from bins.general.enums import queueItemType, text_to_protocol
from bins.w3.builders import build_db_hypervisor_multicall


def actions_on_no_hypervisor_period_end(error: ProcessingError):
    rescrape_block_ini = error.item["ini_block"]
    rescrape_block_end = error.item["end_block"]

    # Missing operations ?  rescrape operations for this chain between defined blocks

    logging.getLogger(__name__).info(
        f" Rescraping operations for {error.chain.database_name} between blocks {rescrape_block_ini} and {rescrape_block_end}. Reason: no hypervisor period end error"
    )
    feed_operations(
        protocol="gamma",
        network=error.chain.database_name,
        block_ini=rescrape_block_ini,
        block_end=rescrape_block_end,
    )

    # control var
    # end block -1 must exist bc its an initial block
    _block_must_exist = error.item["end_block"] - 1

    # check if  error.item["end_block"] hypervisor is in queue or already in status database
    if queued_items := get_from_localdb(
        network=error.chain.database_name,
        collection="queue",
        find={
            "block": {"$in": [error.item["end_block"], _block_must_exist]},
            "address": error.item["hypervisor_address"],
            "queue_type": {"$in": ["hypervisor_status", "operation"]},
        },
    ):
        # item arlready in queue: do nothing
        logging.getLogger(__name__).info(
            f" Found {len(queued_items)} in queue with {error.item['hypervisor_address']} hype address and block {error.item['end_block']} after rescraping. No need to do anything"
        )
    elif hype_status := get_from_localdb(
        network=error.chain.database_name,
        collection="status",
        find={"block": _block_must_exist, "address": error.item["hypervisor_address"]},
    ):
        # item already in status: do nothing
        logging.getLogger(__name__).info(
            f" Found {len(hype_status)} in hypervisors status with {error.item['hypervisor_address']} address and block {_block_must_exist} after rescraping. No need to do anything"
        )
    else:
        # item not in queue or status: add hypervisor status at end block -1 to queue
        logging.getLogger(__name__).info(
            f" Adding hypervisor status {error.item['hypervisor_address']} at block {_block_must_exist} to queue ( and prices )"
        )
        # get end block operation
        if end_operations := get_from_localdb(
            network=error.chain.database_name,
            collection="operations",
            find={
                "blockNumber": error.item["end_block"],
                "address": error.item["hypervisor_address"],
                "topic": {"$in": ["deposit", "withdraw", "rebalance", "zeroBurn"]},
            },
        ):
            logging.getLogger(__name__).info(
                f" Found end block operation {end_operations[0]['topic']} for hypervisor {error.item['hypervisor_address']} at block {error.item['end_block']}. Readding it to queue to create hype status"
            )
            build_and_save_queue_from_operation(
                operation=end_operations[0], network=error.chain.database_name
            )
        else:
            logging.getLogger(__name__).error(
                f" No operation found for hypervisor {error.item['hypervisor_address']} at block {error.item['end_block']}. Creating a manual hypervisor status queued item"
            )

            hype_static = get_from_localdb(
                network=error.chain.database_name,
                collection="static",
                find={"address": error.item["hypervisor_address"]},
            )

            # create a manual item hype status
            db_result = get_default_localdb(
                network=error.chain.database_name
            ).set_queue_item(
                data=QueueItem(
                    type=queueItemType.HYPERVISOR_STATUS,
                    block=_block_must_exist,
                    address=error.item["hypervisor_address"],
                    data={
                        "transactionHash": "dummy",
                        "blockHash": "",
                        "blockNumber": 0,
                        "address": hype_static[0]["address"],
                        "timestamp": 0,
                        "decimals_token0": hype_static[0]["pool"]["token0"]["decimals"],
                        "decimals_token1": hype_static[0]["pool"]["token1"]["decimals"],
                        "decimals_contract": hype_static[0]["decimals"],
                        "tick": 0,
                        "totalAmount0": "0",
                        "totalAmount1": "0",
                        "qtty_token0": "0",
                        "qtty_token1": "0",
                        "topic": "",
                        "logIndex": 0,
                        "id": "",
                    },
                ).as_dict
            )
            logging.getLogger(__name__).info(
                f" database result match:{db_result.matched_count} mod:{db_result.modified_count} ups: {db_result.upserted_id}, "
            )
