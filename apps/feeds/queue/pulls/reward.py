import logging
from apps.feeds.queue.queue_item import QueueItem
from apps.feeds.status.rewards.general import create_reward_status_from_hype_status

from bins.database.helpers import get_default_localdb


def pull_from_queue_reward_status(network: str, queue_item: QueueItem) -> bool:
    # control var
    _result = False

    # check if item block is higher than static rewarder block
    if queue_item.block < queue_item.data["reward_static"]["block"]:
        logging.getLogger(__name__).error(
            f" {network} queue item {queue_item.id} block {queue_item.block} is lower than reward creation block {queue_item.data['reward_static']['block']}.Skipping and removing from queue"
        )
        return True
    else:
        try:
            if reward_status_list := create_reward_status_from_hype_status(
                hypervisor_status=queue_item.data["hypervisor_status"],
                rewarder_static=queue_item.data["reward_static"],
                network=network,
            ):
                for idx, reward_status in enumerate(reward_status_list):
                    # only save status if rewards per second are greater than 0
                    tmp = 0
                    try:
                        tmp = int(reward_status["rewards_perSecond"])
                    except Exception as e:
                        logging.getLogger(__name__).warning(
                            f"  rewards per second are float not int at reward status id: {reward_status['dex']}'s hype {reward_status['hypervisor_address']} rewarder address {reward_status['rewarder_address']}  block {reward_status['block']}  rewardsXsec {reward_status['rewards_perSecond']}"
                        )
                        tmp = float(reward_status["rewards_perSecond"])

                    if tmp > 0:
                        if db_return := get_default_localdb(
                            network=network
                        ).set_rewards_status(data=reward_status):
                            # evaluate if price has been saved
                            if (
                                db_return.upserted_id
                                or db_return.modified_count
                                or db_return.matched_count
                            ):
                                logging.getLogger(__name__).debug(
                                    f" {network} queue item {queue_item.id} reward status saved to database -- reward status num. {idx} of {len(reward_status_list)}"
                                )
                                _result = True
                            else:
                                logging.getLogger(__name__).error(
                                    f" {network} queue item {queue_item.id} reward status not saved to database. database returned: {db_return.raw_result} -- reward status num. {idx} of {len(reward_status_list)}"
                                )
                        else:
                            logging.getLogger(__name__).error(
                                f" No database return received while trying to save results for {network} queue item {queue_item.id} -- reward status num. {idx} of {len(reward_status_list)}"
                            )
                    else:
                        logging.getLogger(__name__).debug(
                            f" {queue_item.id} has 0 rewards per second. Not saving it to database -- reward status num. {idx} of {len(reward_status_list)}"
                        )
                        _result = True

            else:
                logging.getLogger(__name__).debug(
                    f" Cant get any reward status data for {network}'s {queue_item.address} rewarder->  dex: {queue_item.data['hypervisor_status'].get('dex', 'unknown')} hype address: {queue_item.data['hypervisor_status'].get('address', 'unknown')}  block: {queue_item.block}."
                )
                # cases log count:
                #   ( count 20 )  rewarder has no status rewards at/before this block ( either there are none or the database is not updated)
        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Error processing {network}'s rewards status queue item: {e}"
            )

    return _result
