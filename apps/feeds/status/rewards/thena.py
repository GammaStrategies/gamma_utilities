import logging

from apps.errors.actions import process_error
from apps.feeds.status.rewards.helpers import _create_dummy_reward_status_zero
from apps.feeds.utils import add_apr_process01
from bins.errors.general import ProcessingError
from bins.w3.protocols.thena.rewarder import thena_gauge_v2


def create_rewards_status_thena(
    network: str, rewarder_static: dict, hypervisor_status: dict
) -> list:
    result = []

    thena_gauge = thena_gauge_v2(
        address=rewarder_static["rewarder_address"],
        network=network,
        block=hypervisor_status["block"],
        timestamp=hypervisor_status["timestamp"],
    )

    # get rewards directly from gauge ( rewarder ).  Warning-> will not contain rewarder_registry field!!
    if rewards_from_gauge := thena_gauge.get_rewards(convert_bint=True):
        # rewards_from_gauge is a list
        for reward in rewards_from_gauge:
            try:
                # add rewarder registry address
                reward["rewarder_registry"] = rewarder_static["rewarder_registry"]
                # add prices and APR to onchain status
                reward = add_apr_process01(
                    network=network,
                    hypervisor_status=hypervisor_status,
                    reward_data=reward,
                )
                # add to result
                result.append(reward)
            except ProcessingError as e:
                logging.getLogger(__name__).error(
                    f" Thena Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. {e.identity}. Trying to address it."
                )
                process_error(e)
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Thena Rewards-> {network}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
                )
                logging.getLogger(__name__).debug(
                    f" Thena Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
                )
    else:
        logging.getLogger(__name__).debug(
            f" There are no {rewarder_static['rewardToken_symbol']} rewards for {network} {hypervisor_status['address']} at block {hypervisor_status['block']}"
        )
        return [_create_dummy_reward_status_zero(hypervisor_status, rewarder_static)]

    return result
