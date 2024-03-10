from apps.feeds.price_paths import create_price_paths_json
from apps.repair.blocks.general import repair_blocks
from apps.repair.hypervisor.status import repair_hypervisor_status
from apps.repair.operations.general import repair_operations
from apps.repair.prices.general import repair_prices
from apps.repair.queue.general import repair_queue
from apps.repair.queue.locked import repair_queue_locked_items
from apps.repair.rewards.general import repair_rewards_status


# repair apps
def repair_all():
    """Repair all errors found in logs"""

    # repair queue
    repair_queue_locked_items()

    # repair blocks
    repair_blocks()

    # repair hypervisors status
    repair_hypervisor_status()

    # repair prices not found in logs
    repair_prices()

    # repair missing rewards status
    # TODO: this is too time intensive right now. Need to find a better way to do it
    # repair_rewards_status()


def main(option: str, **kwargs):
    if option == "prices":
        repair_prices()
    if option == "database":
        raise NotImplementedError("Database repair not implemented")
    if option == "hypervisor_status":
        repair_hypervisor_status()
    if option == "all":
        repair_all()
    if option == "queue":
        repair_queue()
    if option == "reward_status":
        repair_rewards_status()
    if option == "operations":
        repair_operations()
    if option == "special":
        # used to check for special cases
        # reScrape_database_prices(
        #    network_limit=CONFIGURATION["_custom_"]["cml_parameters"].maximum
        # )
        create_price_paths_json()
    # else:
    #     raise NotImplementedError(
    #         f" Can't find any action to be taken from {option} checks option"
    #     )
