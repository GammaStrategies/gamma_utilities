import logging
from apps.feeds.utils import add_apr_process01

from bins.general.enums import Chain, rewarderType, text_to_chain
from bins.w3.protocols.camelot.rewarder import (
    camelot_rewards_nft_pool,
    camelot_rewards_nft_pool_master,
)
from bins.w3.helpers.multicaller import build_call_with_abi_part, execute_parse_calls


def create_rewards_status_camelot_spnft(
    chain: Chain, rewarder_static: dict, hypervisor_status: dict
) -> list:
    result = []
    # get rewards onchain status
    if reward_data := get_camelot_rewards_nftpool(
        chain=chain,
        rewarder_static=rewarder_static,
        hypervisor_status=hypervisor_status,
        # convert_bint=True,
    ):
        # standardize and split grail/xGrail
        for reward_data_converted in convert_parsed_multicall_result_to_reward_standard(
            reward_data
        ):
            try:
                # add prices and APR to onchain status
                reward_data_converted = add_apr_process01(
                    network=chain.database_name,
                    hypervisor_status=hypervisor_status,
                    reward_data=reward_data_converted,
                )
                result.append(reward_data_converted)
            except Exception as e:
                logging.getLogger(__name__).error(
                    f" Camelot Rewards-> {chain.database_name}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
                )
                logging.getLogger(__name__).debug(
                    f" Synthswap Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
                )

    return result


# get camelot reward information
def get_camelot_rewards_nftpool(
    chain: Chain,
    rewarder_static: dict,
    hypervisor_status: dict,
) -> dict:
    """_summary_

    Args:
        chain (Chain): _description_
        rewarder_static (dict): _description_
        hypervisor_status (dict): _description_

    Returns:
        dict: {<address>: lpToken, grailToken, xGrailToken, lastRewardTime,accRewardsPerShare, lpSupply, lpSupplyWithMultiplier, allocPoint, xGrailRewardsShare, reserve, poolEmissionRate}
    """
    # create a camelot nft pool master object
    _calls = []
    nft_pool_master = camelot_rewards_nft_pool_master(
        address=rewarder_static["rewarder_registry"],
        network=chain.database_name,
        block=hypervisor_status["block"],
        timestamp=hypervisor_status["timestamp"],
    )
    # add getPoolInfo call from master
    _calls.append(
        build_call_with_abi_part(
            abi_part=nft_pool_master.get_abi_function("getPoolInfo"),
            inputs_values=[rewarder_static["rewarder_address"]],
            address=rewarder_static["rewarder_registry"],
            object="nft_pool_master",
        )
    )
    nft_pool = camelot_rewards_nft_pool(
        address=rewarder_static["rewarder_address"],
        network=chain.database_name,
        block=hypervisor_status["block"],
        timestamp=hypervisor_status["timestamp"],
    )
    # add getPoolInfo call from pool
    _calls.append(
        build_call_with_abi_part(
            abi_part=nft_pool.get_abi_function("getPoolInfo"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nft_pool",
        )
    )
    # add xGrailRewardsShare call
    _calls.append(
        build_call_with_abi_part(
            abi_part=nft_pool.get_abi_function("xGrailRewardsShare"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nft_pool",
        )
    )

    # execute multicalls for both blocks
    multicall_result = parse_multicall_result(
        execute_parse_calls(
            network=chain.database_name,
            block=hypervisor_status["block"],
            calls=_calls,
            convert_bint=False,
        )
    )

    # add general data
    multicall_result["network"] = chain.database_name
    multicall_result["block"] = hypervisor_status["block"]
    multicall_result["timestamp"] = hypervisor_status["timestamp"]
    multicall_result["hypervisor_address"] = hypervisor_status["address"]
    multicall_result["rewarder_address"] = rewarder_static["rewarder_address"]

    # return result
    return multicall_result


def parse_multicall_result(multicall_result) -> dict:
    """

    Args:
        multicall_result (_type_): _description_

    Returns:
        dict: {lpToken, grailToken, xGrailToken, lastRewardTime,accRewardsPerShare, lpSupply, lpSupplyWithMultiplier, allocPoint, xGrailRewardsShare, reserve, poolEmissionRate}

    """
    result = {}
    for pool_info in multicall_result:
        if pool_info["object"] == "nft_pool":
            # decide what to parse
            if pool_info["name"] == "getPoolInfo":
                # process getPoolInfo
                result["lpToken"] = pool_info["outputs"][0]["value"]
                result["grailToken"] = pool_info["outputs"][1]["value"]
                result["xGrailToken"] = pool_info["outputs"][2]["value"]
                result["lastRewardTime"] = pool_info["outputs"][3]["value"]
                result["accRewardsPerShare"] = pool_info["outputs"][4]["value"]
                result["lpSupply"] = pool_info["outputs"][5]["value"]
                result["lpSupplyWithMultiplier"] = pool_info["outputs"][6]["value"]
                result["allocPoint"] = pool_info["outputs"][7]["value"]
            elif pool_info["name"] == "xGrailRewardsShare":
                # process xGrailRewardsShare
                result["xGrailRewardsShare"] = pool_info["outputs"][0]["value"]
            else:
                raise ValueError(f" Object not recognized {pool_info['object']}")

        elif pool_info["object"] == "nft_pool_master":
            # decide what to parse
            if pool_info["name"] == "getPoolInfo":
                # set vars
                result["nft_pool_address"] = pool_info["outputs"][0]["value"]
                result["allocPoint"] = pool_info["outputs"][1]["value"]
                result["lastRewardTime"] = pool_info["outputs"][2]["value"]
                result["reserve"] = pool_info["outputs"][3]["value"]
                result["poolEmissionRate"] = pool_info["outputs"][4]["value"]
            else:
                raise ValueError(f" Object not recognized {pool_info['object']}")

        else:
            raise ValueError(f" Object not recognized {pool_info['object']}")

    return result


def convert_parsed_multicall_result_to_reward_standard(
    parsed_result: dict,
) -> list[dict]:
    """Convert parsed multicall result to reward standard

    Args:
        parsed_result (dict): result from parse_multicall_result() function

    Returns:
        dict:        {
            network: str
            block: int
            timestamp: int
            hypervisor_address: str
            rewarder_address: str
            rewarder_type: str
            rewarder_refIds: list[str]
            rewardToken: str
            rewardToken_symbol: str
            rewardToken_decimals: int
            rewards_perSecond: int
            total_hypervisorToken_qtty: int
            }
    """
    result = []
    # avoiding the use of Decimal... (but u know u should..)
    xgrail_percentage = parsed_result["xGrailRewardsShare"] / 10000
    grail_percentage = (10000 - parsed_result["xGrailRewardsShare"]) / 10000

    result.append(
        {
            "network": parsed_result["network"],
            "block": parsed_result["block"],
            "timestamp": parsed_result["timestamp"],
            "hypervisor_address": parsed_result["hypervisor_address"],
            "rewarder_address": parsed_result["rewarder_address"],
            "rewarder_type": rewarderType.CAMELOT_spNFT,
            "rewarder_refIds": [],
            "rewardToken": parsed_result["grailToken"],
            "rewardToken_symbol": "GRAIL",
            "rewardToken_decimals": 18,
            "rewards_perSecond": parsed_result["poolEmissionRate"] * grail_percentage,
            "total_hypervisorToken_qtty": parsed_result["lpSupplyWithMultiplier"],
        }
    )
    result.append(
        {
            "network": parsed_result["network"],
            "block": parsed_result["block"],
            "timestamp": parsed_result["timestamp"],
            "hypervisor_address": parsed_result["hypervisor_address"],
            "rewarder_address": parsed_result["rewarder_address"],
            "rewarder_type": rewarderType.CAMELOT_spNFT,
            "rewarder_refIds": [],
            "rewardToken": parsed_result["xGrailToken"],
            "rewardToken_symbol": "xGRAIL",
            "rewardToken_decimals": 18,
            "rewards_perSecond": parsed_result["poolEmissionRate"] * xgrail_percentage,
            "total_hypervisorToken_qtty": parsed_result["lpSupplyWithMultiplier"],
        }
    )

    return result
