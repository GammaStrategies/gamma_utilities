def _create_dummy_reward_status_zero(
    hypervisor_status: dict, rewarder_static: dict
) -> dict:
    return {
        "rewards_perSecond": 0,
        "rewardToken": rewarder_static["rewardToken"],
        "rewarder_address": rewarder_static["rewarder_address"],
        "block": hypervisor_status["block"],
        "timestamp": hypervisor_status["timestamp"],
    }
