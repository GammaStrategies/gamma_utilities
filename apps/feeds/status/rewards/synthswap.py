import logging
from apps.feeds.utils import add_apr_process01

from bins.general.enums import rewarderType
from bins.w3.protocols.synthswap.rewarder import synthswap_masterchef_v1


def create_rewards_status_synthswap(
    network: str, rewarder_static: dict, hypervisor_status: dict
) -> list:
    if rewarder_static["rewarder_type"] == rewarderType.SYNTHSWAP_masterchef_v1:
        # do nothing because this is the reward given by the masterchef and will be processed once we get to the rewarder ( thru the masterchef itself..)
        pass
        # return None

    # create masterchef object:  USE address == rewarder_registry on masterchef for this type of rewarder
    _masterchef = synthswap_masterchef_v1(
        address=rewarder_static["rewarder_registry"],
        network=network,
        block=hypervisor_status["block"],
        timestamp=hypervisor_status["timestamp"],
    )

    result = []
    # get rewards onchain status
    for reward_data in _masterchef.get_rewards(
        hypervisor_addresses=[rewarder_static["hypervisor_address"]],
        pids=rewarder_static["rewarder_refIds"],
        convert_bint=True,
    ):
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
                f" Synthswap Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
            )
            logging.getLogger(__name__).debug(
                f" Synthswap Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
            )

    return result
