import logging

from apps.errors.actions import process_error
from apps.feeds.status.rewards.helpers import _create_dummy_reward_status_zero
from apps.feeds.utils import add_apr_process01
from bins.errors.general import ProcessingError
from bins.general.enums import rewarderType
from bins.w3.protocols.beamswap.rewarder import beamswap_masterchef_v2


def create_rewards_status_beamswap(
    network: str, rewarder_static: dict, hypervisor_status: dict
) -> list:
    if rewarder_static["rewarder_type"] == rewarderType.BEAMSWAP_masterchef_v2:
        # do nothing because this is the reward given by the masterchef and will be processed once we get to the rewarder ( thru the masterchef itself..)
        pass
        # return None

    # create masterchef object:   USE address == rewarder_registry on masterchef for this type of rewarder
    masterchef = beamswap_masterchef_v2(
        address=rewarder_static["rewarder_registry"],
        network=network,
        block=hypervisor_status["block"],
        timestamp=hypervisor_status["timestamp"],
    )

    result = []
    # get rewards onchain status
    reward_data_list = masterchef.get_rewards(
        hypervisor_addresses=[rewarder_static["hypervisor_address"]],
        pids=rewarder_static["rewarder_refIds"],
        convert_bint=True,
    )

    # Return REWARDSxSEC=0 when no rewards are found
    if not reward_data_list:
        logging.getLogger(__name__).debug(
            f" There are no {rewarder_static['rewardToken_symbol']} rewards for {network} {hypervisor_status['address']} at block {hypervisor_status['block']}"
        )
        return [_create_dummy_reward_status_zero(hypervisor_status, rewarder_static)]

    for reward_data in reward_data_list:
        try:
            # add prices and APR to onchain status
            reward_data = add_apr_process01(
                network=network,
                hypervisor_status=hypervisor_status,
                reward_data=reward_data,
            )
            result.append(reward_data)
        except ProcessingError as e:
            logging.getLogger(__name__).error(
                f" Beamswap Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. {e.identity}. Trying to address it."
            )
            process_error(e)
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Beamswap Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
            )
            logging.getLogger(__name__).debug(
                f" Beamswap Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
            )

    return result
