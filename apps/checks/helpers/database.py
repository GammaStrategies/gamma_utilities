from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, queueItemType


def get_hypervisor_last_status(chain: Chain, hypervisor_address: str) -> dict:
    """Get the last status of a hypervisor"""
    hypervisor_status = get_from_localdb(
        network=chain.database_name,
        collection="status",
        find={"address": hypervisor_address},
        sort=[("block", -1)],
        limit=1,
    )
    return hypervisor_status[0] if hypervisor_status else {}


def get_hypervisor_status_list(
    chain: Chain, hypervisor_address: str, block_ini: int, block_end: int
) -> list[dict]:
    return get_from_localdb(
        network=chain.database_name,
        collection="status",
        find={
            "address": hypervisor_address,
            "block": {"$gte": block_ini, "$lte": block_end},
        },
        sort=[("block", 1)],
    )


def get_rebalances_list(
    chain: Chain, hypervisor_address: str, block_ini: int, block_end: int
) -> list[dict]:
    return get_from_localdb(
        network=chain.database_name,
        collection="operations",
        find={
            "address": hypervisor_address,
            "blockNumber": {"$gte": block_ini, "$lte": block_end},
            "topic": "rebalance",
        },
        sort=[("blockNumber", 1)],
    )


def get_last_operation(chain: Chain, hypervisor_address: str) -> dict:
    operations = get_from_localdb(
        network=chain.database_name,
        collection="operations",
        find={
            "address": hypervisor_address,
            "topic": {"$in": ["deposit", "withdraw", "rebalance", "zeroBurn"]},
        },
        sort=[("blockNumber", -1)],
        limit=1,
    )
    return operations[0] if operations else {}


def get_hypervisor_related_operations(
    chain: Chain, hypervisor_address: str
) -> list[dict]:
    return get_from_localdb(
        network=chain.database_name,
        collection="queue",
        find={
            "$or": [
                {"address": hypervisor_address},
                {
                    "$and": [
                        {"data.reward_static.hypervisor_address": hypervisor_address},
                        {"type": queueItemType.REWARD_STATUS},
                    ]
                },
                # {"$and":[{"data.address":hypervisor_address},{"type":queueItemType.LATEST_MULTIFEEDISTRIBUTION}]},
            ]
        },
        sort=[("block", 1)],
    )
