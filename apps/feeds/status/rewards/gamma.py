import logging
from apps.feeds.utils import add_apr_process01

from bins.general.enums import rewarderType, text_to_chain
from bins.w3.builders import build_erc20_helper
from bins.w3.protocols.gamma.rewarder import gamma_masterchef_rewarder


def create_rewards_status_gamma(
    network: str, rewarder_static: dict, hypervisor_status: dict
) -> list:
    # build rewarder & get info
    gamma_rewarder = gamma_masterchef_rewarder(
        address=rewarder_static["rewarder_address"],
        network=network,
        block=hypervisor_status["block"],
    )
    rewards = gamma_rewarder.get_rewards(
        hypervisors_and_pids={
            rewarder_static["hypervisor_address"]: rewarder_static["rewarder_refIds"]
        }
    )
    if not rewards:
        logging.getLogger(__name__).info(
            f"           No active rewards found for {network} hype {rewarder_static['hypervisor_address']} rewarder {rewarder_static['rewarder_address']} at block {hypervisor_status['block']} pids {rewarder_static['rewarder_refIds']}"
        )
        return

    if len(rewards) > 1:
        logging.getLogger(__name__).error(
            f"           More than 1 active rewards found for {network} hype {rewarder_static['hypervisor_address']} rewarder {rewarder_static['rewarder_address']} at block {hypervisor_status['block']} pids {rewarder_static['rewarder_refIds']}"
        )

    reward_data = rewards[0]
    reward_data["rewardToken_symbol"] = rewarder_static["rewardToken_symbol"]
    reward_data["rewardToken_decimals"] = rewarder_static["rewardToken_decimals"]

    # get staked qtty
    ercHelper = build_erc20_helper(
        chain=text_to_chain(network),
        address=reward_data["hypervisor_address"],
        block=reward_data["block"],
    )
    if totalLP := ercHelper.balanceOf(reward_data["rewarder_registry"]):
        reward_data["total_hypervisorToken_qtty"] = str(totalLP)
    else:
        logging.getLogger(__name__).debug(
            f"           No total LP found for {network} hype {reward_data['hypervisor_address']} at rewarder {reward_data['rewarder_address']} pid {reward_data['rewarder_refIds']}"
        )

    # build result and return it
    result = []
    try:
        # add prices and APR to onchain status
        reward_data = add_apr_process01(
            network=network,
            hypervisor_status=hypervisor_status,
            reward_data=reward_data,
        )
        result.append(reward_data)
    except Exception as e:
        logging.getLogger(__name__).error(
            f" Gamma Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
        )
        logging.getLogger(__name__).debug(
            f" Gamma Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
        )

    return result
