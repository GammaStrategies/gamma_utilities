# database ids

### QUEUE id is build in the class itself
from ...general.enums import queueItemType


def combine_ids(*args) -> str:
    return "_".join(args)


def create_id_operation(logIndex, transactionHash) -> str:
    return f"{logIndex}_{transactionHash}"


def create_id_queue(
    type: queueItemType,
    block: int,
    hypervisor_address: str,
    rewarder_address: str | None = None,
    rewardToken_address: str | None = None,
) -> str:
    # base id
    result_id = f"{type}_{block}_{hypervisor_address}"

    # rewarder specific
    if rewarder_address:
        result_id += f"_{rewarder_address}"
    # reward status should have rewardToken as id
    if rewardToken_address:
        result_id += f"_{rewardToken_address}"

    # return result
    return result_id


def create_id_hypervisor_static(hypervisor_address: str) -> str:
    return f"{hypervisor_address}"


def create_id_hypervisor_status(hypervisor_address: str, block: int) -> str:
    return f"{hypervisor_address}_{block}"


def create_id_rewards_static(
    hypervisor_address: str, rewarder_address: str, rewardToken_address: str
) -> str:
    return f"{hypervisor_address}_{rewarder_address}_{rewardToken_address}"


def create_id_rewards_status(
    hypervisor_address: str, rewarder_address: str, rewardToken_address: str, block: int
) -> str:
    return f"{hypervisor_address}_{rewarder_address}_{rewardToken_address}_{block}"


def create_id_price(network: str, block: int, token_address: str) -> str:
    return f"{network}_{block}_{token_address}"


def create_id_current_price(network: str, token_address: str) -> str:
    return f"{network}_{token_address}"


def create_id_block(network: str, block: int) -> str:
    return f"{network}_{block}"


def create_id_hypervisor_returns(
    hypervisor_address: str, ini_block: int, end_block: int
) -> str:
    return f"{hypervisor_address}_{ini_block}_{end_block}"


def create_id_user_status(
    user_address: str, block: int, logIndex: int, hypervisor_address: str
) -> str:
    return f"{user_address}_{block}_{logIndex}_{hypervisor_address}"


def create_id_user_operation(
    user_address: str, block: int, logIndex: int, hypervisor_address: str
) -> str:
    return f"{user_address}_{block}_{logIndex}_{hypervisor_address}"
