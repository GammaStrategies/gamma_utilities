import logging
from apps.feeds.queue.queue_item import QueueItem


def pull_from_queue_user_operation(network: str, queue_item: QueueItem) -> bool:
    """User operations represent the deposit and withdrawal of tokens from a hypervisor. This function processes the user operations queue items.
        A deposit operation can be a transfer in and a withdrawal operation can be a transfer out.

        --> user address, hype address, block and timestamp, LP token qtty, token0 qtty and token1 qtty ( when depositing or withdrawing).

    Args:
        network (str):
        queue_item (QueueItem):

    """
    return False
    # 2 methods: API and web3 calls.
    # identify if operation is proxied or not

    try:
        # the operation dict is in the 'data' field.
        operation = queue_item.data

        # topic filter deposit, withdraw and transfer.
        # when TRANSFER:
        #       from/to value should not be 0x000 (burn/mint)
        #       from/to should not be the same address
        #       value should not be 0
        #       from/to value should be not staking contracts
        #       Will not have value??

        # lower case address ( to ease comparison )
        operation["address"] = operation["address"].lower()

        # log
        logging.getLogger(__name__).debug(
            f"  -> Processing {network}'s operation {operation['id']}"
        )

        # set timestamp
        operation["timestamp"] = dumb_erc20.timestampFromBlockNumber(
            block=int(operation["blockNumber"])
        )

        # get hype from db
        if hypervisor := get_from_localdb(
            network=network,
            collection="static",
            find={
                "id": create_id_hypervisor_static(
                    hypervisor_address=operation["address"]
                )
            },
        ):
            hypervisor = hypervisor[0]

        else:
            raise ValueError(
                f" No static hypervisor found for {operation['address']} while processing operation {operation['id']}"
            )

        # set tokens data
        operation["decimals_token0"] = hypervisor["pool"]["token0"]["decimals"]
        operation["decimals_token1"] = hypervisor["pool"]["token1"]["decimals"]
        operation["decimals_contract"] = hypervisor["decimals"]

        # save operation to database
        if db_return := get_default_localdb(network).set_operation(data=operation):
            logging.getLogger(__name__).debug(f" Saved operation {operation['id']}")

        # make sure hype is not in status collection already
        if not get_from_localdb(
            network=network,
            collection="status",
            find={
                "id": create_id_hypervisor_status(
                    hypervisor_address=operation["address"],
                    block=operation["blockNumber"],
                )
            },
            projection={"id": 1},
        ):
            # fire scrape event on block regarding hypervisor and rewarders snapshots (status) and token prices
            # build queue events from operation
            build_and_save_queue_from_operation(operation=operation, network=network)

        else:
            logging.getLogger(__name__).debug(
                f"  Not pushing {operation['address']} hypervisor status queue item bcause its already in the database"
            )

        # log
        logging.getLogger(__name__).debug(
            f"  <- Done processing {network}'s operation {operation['id']}"
        )

        return True

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error processing {network}'s user operation queue item: {e}"
        )

    # return result
    return False
