import logging
from apps.errors.actions import process_error
from apps.feeds.utils import add_apr_process01

from bins.errors.general import ProcessingError
from bins.general.enums import Chain, rewarderType, text_to_chain
from bins.w3.builders import build_erc20_helper
from bins.w3.protocols.camelot.rewarder import (
    camelot_rewards_nft_pool,
    camelot_rewards_nft_pool_master,
    camelot_rewards_nitro_pool,
    camelot_rewards_nitro_pool_factory,
)
from bins.w3.helpers.multicaller import build_call_with_abi_part, execute_parse_calls


# SPNFT
def create_rewards_status_camelot_spnft(
    chain: Chain, rewarder_static: dict, hypervisor_status: dict
) -> list:
    """This creates the rewards status for GRAIL and xGRAIL for the rewarder_static supplied ( does not matter if its GRAIL or xGRAIL)

    Args:
        chain (Chain):
        rewarder_static (dict):
        hypervisor_status (dict):

    Returns:
        list: GRAIL and xGRAIL independently
    """
    result = []
    # get rewards onchain status
    if reward_data := get_camelot_rewards_spnftpool(
        chain=chain,
        rewarder_static=rewarder_static,
        hypervisor_status=hypervisor_status,
        # convert_bint=True,
    ):
        # standardize and split grail/xGrail
        for (
            reward_data_converted
        ) in convert_parsed_rewards_nftpool_multicall_result_to_reward_standard(
            reward_data
        ):
            try:
                # add prices and APR to onchain status
                reward_data_converted = add_apr_process01(
                    network=chain.database_name,
                    hypervisor_status=hypervisor_status,
                    reward_data=reward_data_converted,
                )

                # modify extra field with apr
                _base_rewards = (
                    reward_data_converted["extra"]["baseRewards_per_second"]
                    / 10 ** reward_data_converted["rewardToken_decimals"]
                ) * reward_data_converted["rewardToken_price_usd"]
                _boosted_rewards = (
                    reward_data_converted["extra"]["boostedRewards_per_second"]
                    / 10 ** reward_data_converted["rewardToken_decimals"]
                ) * reward_data_converted["rewardToken_price_usd"]
                _total_tvl = (
                    reward_data_converted["total_hypervisorToken_qtty"] / 10**18
                ) * reward_data_converted["hypervisor_share_price_usd"]

                reward_data_converted["extra"]["boostedRewards_apr"] = (
                    _boosted_rewards * 60 * 60 * 24 * 365
                ) / _total_tvl
                reward_data_converted["extra"]["baseRewards_apr"] = (
                    _base_rewards * 60 * 60 * 24 * 365
                ) / _total_tvl

                # set apy to apr
                reward_data_converted["extra"]["boostedRewards_apy"] = (
                    reward_data_converted["extra"]["boostedRewards_apr"]
                )
                reward_data_converted["extra"]["baseRewards_apy"] = (
                    reward_data_converted["extra"]["baseRewards_apr"]
                )

                # convert all >8bit int to str
                reward_data_converted["rewards_perSecond"] = str(
                    reward_data_converted["rewards_perSecond"]
                )
                reward_data_converted["total_hypervisorToken_qtty"] = str(
                    reward_data_converted["total_hypervisorToken_qtty"]
                )
                reward_data_converted["extra"]["baseRewards_per_second"] = str(
                    reward_data_converted["extra"]["baseRewards_per_second"]
                )
                reward_data_converted["extra"]["boostedRewards_per_second"] = str(
                    reward_data_converted["extra"]["boostedRewards_per_second"]
                )
                reward_data_converted["extra"]["baseRewards"] = str(
                    reward_data_converted["extra"]["baseRewards"]
                )
                reward_data_converted["extra"]["boostedRewards"] = str(
                    reward_data_converted["extra"]["boostedRewards"]
                )

                result.append(reward_data_converted)
            except ProcessingError as e:
                logging.getLogger(__name__).error(
                    f" Camelot spNFT Rewards-> {chain.database_name}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. {e.identity}. Trying to address it."
                )
                process_error(e)
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Camelot spNFT Rewards-> {chain.database_name}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
                )
                logging.getLogger(__name__).debug(
                    f" Camelot spNFT Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
                )

    return result


# get camelot nftPool reward information
def get_camelot_rewards_spnftpool(
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

    # MULTIPLIER will be savcet into boostedRewards: 1 week after start time
    # calc time from start to now, to get multiplier
    seconds_passed = (
        hypervisor_status["timestamp"] - rewarder_static["start_rewards_timestamp"]
    )
    # subtract 1 week to seconds passed ( as if stakers waited 1 week to stake )
    seconds_passed -= 604800
    # if seconds passed is negative, set to 0
    if seconds_passed < 0:
        seconds_passed = 0

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
    # add max multipliers and current multiplier calls
    # getMultiplierSettings -> maxGlobalMultiplier uint256, maxLockDuration uint256, maxLockMultiplier uint256, maxBoostMultiplier uint256
    _calls.append(
        build_call_with_abi_part(
            abi_part=nft_pool.get_abi_function("getMultiplierSettings"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nft_pool",
        )
    )
    # getMultiplierByLockDuration -> uint256 in *1e4
    _calls.append(
        build_call_with_abi_part(
            abi_part=nft_pool.get_abi_function("getMultiplierByLockDuration"),
            inputs_values=[seconds_passed],
            address=rewarder_static["rewarder_address"],
            object="nft_pool",
        )
    )

    # execute multicalls for both blocks
    multicall_result = parse_camelot_rewards_nftpool_multicall_result(
        execute_parse_calls(
            network=chain.database_name,
            block=hypervisor_status["block"],
            calls=_calls,
            convert_bint=False,
            timestamp=hypervisor_status["timestamp"],
        )
    )

    # add general data
    multicall_result["network"] = chain.database_name
    multicall_result["block"] = hypervisor_status["block"]
    multicall_result["timestamp"] = hypervisor_status["timestamp"]
    multicall_result["hypervisor_address"] = hypervisor_status["address"]
    multicall_result["rewarder_address"] = rewarder_static["rewarder_address"]
    multicall_result["rewarder_registry"] = rewarder_static["rewarder_registry"]
    multicall_result["seconds_passed"] = seconds_passed

    # return result
    return multicall_result


def parse_camelot_rewards_nftpool_multicall_result(multicall_result) -> dict:
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
            elif pool_info["name"] == "getMultiplierSettings":
                # process getMultiplierSettings
                result["maxGlobalMultiplier"] = pool_info["outputs"][0]["value"]
                result["maxLockDuration"] = pool_info["outputs"][1]["value"]
                result["maxLockMultiplier"] = pool_info["outputs"][2]["value"]
                result["maxBoostMultiplier"] = pool_info["outputs"][3]["value"]
            elif pool_info["name"] == "getMultiplierByLockDuration":
                # process getMultiplierByLockDuration
                result["currentMultiplier"] = pool_info["outputs"][0]["value"]
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


def convert_parsed_rewards_nftpool_multicall_result_to_reward_standard(
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

    seconds_passed = parsed_result["seconds_passed"]
    # choosen fixed multiplier = 50% of maxGlobalMultiplier
    multiplier = (parsed_result["maxGlobalMultiplier"] * 0.5) / 1e4

    grail_baseRewards_per_second = parsed_result["poolEmissionRate"] * grail_percentage
    grail_boostedRewards_per_second = (
        parsed_result["poolEmissionRate"] * grail_percentage
    ) * multiplier
    xgrail_baseRewards_per_second = (
        parsed_result["poolEmissionRate"] * xgrail_percentage
    )
    xgrail_boostedRewards_per_second = (
        parsed_result["poolEmissionRate"] * xgrail_percentage
    ) * multiplier

    result.append(
        {
            "network": parsed_result["network"],
            "block": parsed_result["block"],
            "timestamp": parsed_result["timestamp"],
            "hypervisor_address": parsed_result["hypervisor_address"],
            "rewarder_address": parsed_result["rewarder_address"],
            "rewarder_registry": parsed_result["rewarder_registry"],
            "rewarder_type": rewarderType.CAMELOT_spNFT,
            "rewarder_refIds": [],
            "rewardToken": parsed_result["grailToken"],
            "rewardToken_symbol": "GRAIL",
            "rewardToken_decimals": 18,
            "rewards_perSecond": grail_baseRewards_per_second
            + grail_boostedRewards_per_second,
            "total_hypervisorToken_qtty": parsed_result["lpSupplyWithMultiplier"],
            "extra": {
                "baseRewards": seconds_passed * grail_baseRewards_per_second,
                "boostedRewards": seconds_passed * grail_boostedRewards_per_second,
                "baseRewards_apr": 0,
                "baseRewards_apy": 0,
                "boostedRewards_apr": 0,
                "boostedRewards_apy": 0,
                "baseRewards_per_second": grail_baseRewards_per_second,
                "boostedRewards_per_second": grail_boostedRewards_per_second,
            },
        }
    )
    result.append(
        {
            "network": parsed_result["network"],
            "block": parsed_result["block"],
            "timestamp": parsed_result["timestamp"],
            "hypervisor_address": parsed_result["hypervisor_address"],
            "rewarder_address": parsed_result["rewarder_address"],
            "rewarder_registry": parsed_result["rewarder_registry"],
            "rewarder_type": rewarderType.CAMELOT_spNFT,
            "rewarder_refIds": [],
            "rewardToken": parsed_result["xGrailToken"],
            "rewardToken_symbol": "xGRAIL",
            "rewardToken_decimals": 18,
            "rewards_perSecond": xgrail_baseRewards_per_second
            + xgrail_boostedRewards_per_second,
            "total_hypervisorToken_qtty": parsed_result["lpSupplyWithMultiplier"],
            "extra": {
                "baseRewards": seconds_passed * xgrail_baseRewards_per_second,
                "boostedRewards": seconds_passed * xgrail_boostedRewards_per_second,
                "baseRewards_apr": 0,
                "baseRewards_apy": 0,
                "boostedRewards_apr": 0,
                "boostedRewards_apy": 0,
                "baseRewards_per_second": xgrail_baseRewards_per_second,
                "boostedRewards_per_second": xgrail_boostedRewards_per_second,
            },
        }
    )

    return result


# NITRO
def create_rewards_status_camelot_nitro(
    chain: Chain, rewarder_static: dict, hypervisor_status: dict
) -> list:
    result = []
    # get rewards onchain status
    if reward_data := get_camelot_rewards_nitro_pool(
        chain=chain,
        rewarder_static=rewarder_static,
        hypervisor_status=hypervisor_status,
    ):
        # standardize and split grail/xGrail
        for (
            reward_data_converted
        ) in convert_parsed_rewards_nitro_pool_multicall_result_to_reward_standard(
            reward_data
        ):
            try:
                # add prices and APR to onchain status
                reward_data_converted = add_apr_process01(
                    network=chain.database_name,
                    hypervisor_status=hypervisor_status,
                    reward_data=reward_data_converted,
                )

                # convert all >8bit int to str
                reward_data_converted["rewards_perSecond"] = str(
                    reward_data_converted["rewards_perSecond"]
                )
                reward_data_converted["total_hypervisorToken_qtty"] = str(
                    reward_data_converted["total_hypervisorToken_qtty"]
                )

                result.append(reward_data_converted)
            except ProcessingError as e:
                logging.getLogger(__name__).error(
                    f" Camelot nitro Rewards-> {chain.database_name}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. {e.identity}. Trying to address it."
                )
                process_error(e)
            except Exception as e:
                logging.getLogger(__name__).exception(
                    f" Camelot nitro Rewards-> {chain.database_name}'s {rewarder_static['rewardToken']} price at block {hypervisor_status['block']} could not be calculated. Error: {e}"
                )
                logging.getLogger(__name__).debug(
                    f" Camelot nitro Rewards last err debug data -> rewarder_static {rewarder_static}           hype status {hypervisor_status}"
                )

    return result


def get_camelot_rewards_nitro_pool(
    chain: Chain, rewarder_static: dict, hypervisor_status: dict
) -> list:
    _max_calls_atOnce = 1000
    # From the Nitro Pool Factory:
    #   using rewarder_static["rewarder_address"] as the nitro pool address
    #   ...
    nitro_pool_helper = camelot_rewards_nitro_pool(
        address="0x0000000000000000000000000000000000000000",
        network=chain.database_name,
        block=hypervisor_status["block"],
        timestamp=hypervisor_status["timestamp"],
    )
    # create nitro calls
    nitro_pool_calls = []
    # nftPool
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("nftPool"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )
    # creationTime
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("creationTime"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )
    # publishTime
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("publishTime"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )
    # lastRewardTime
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("lastRewardTime"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )
    # rewardsToken1 ->   token: <address>, amount: <int>, remainingAmount: <int>, accRewardsPerShare: <int>
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("rewardsToken1"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )
    # rewardsToken1PerSecond ->  int
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("rewardsToken1PerSecond"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )
    # rewardsToken2 ->   token: <address>, amount: <int>, remainingAmount: <int>, accRewardsPerShare: <int>
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("rewardsToken2"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )
    # rewardsToken2PerSecond ->  int
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("rewardsToken2PerSecond"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )
    # settings ->   startTime, endTime, harvestStartTime, depositEndTime, lockDurationReq, lockEndReq, depositAmountReq, whitelist:bool, description:str
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("settings"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )
    # totalDepositAmount -> int
    nitro_pool_calls.append(
        build_call_with_abi_part(
            abi_part=nitro_pool_helper.get_abi_function("totalDepositAmount"),
            inputs_values=[],
            address=rewarder_static["rewarder_address"],
            object="nitro_pool",
        )
    )

    # place calls
    multicall_result = []
    for i in range(0, len(nitro_pool_calls), _max_calls_atOnce):
        # execute calls
        multicall_result += execute_parse_calls(
            network=chain.database_name,
            block=hypervisor_status["block"],
            calls=nitro_pool_calls[i : i + _max_calls_atOnce],
            convert_bint=False,
            timestamp=hypervisor_status["timestamp"],
        )

    # Build result
    result = {}
    for _info in multicall_result:
        nitro_pool_address = _info["address"].lower()
        if not nitro_pool_address in result:
            # add general data
            result[nitro_pool_address] = {
                "network": chain.database_name,
                "block": hypervisor_status["block"],
                "timestamp": hypervisor_status["timestamp"],
                "hypervisor_address": hypervisor_status["address"],
                "rewarder_address": rewarder_static["rewarder_address"],
                "rewarder_registry": rewarder_static["rewarder_registry"],
            }

        if _info["name"] in [
            "nftPool",
            "creationTime",
            "publishTime",
            "lastRewardTime",
            "rewardsToken1PerSecond",
            "rewardsToken2PerSecond",
            "totalDepositAmount",
        ]:
            result[nitro_pool_address][_info["name"]] = _info["outputs"][0]["value"]
        elif _info["name"] in ["rewardsToken1", "rewardsToken2"]:
            result[nitro_pool_address][_info["name"]] = {
                "token": _info["outputs"][0]["value"],
                "amount": _info["outputs"][1]["value"],
                "remainingAmount": _info["outputs"][2]["value"],
                "accRewardsPerShare": _info["outputs"][3]["value"],
            }
        elif _info["name"] == "settings":
            #
            result[nitro_pool_address][_info["name"]] = {
                "startTime": _info["outputs"][0]["value"],
                "endTime": _info["outputs"][1]["value"],
                "harvestStartTime": _info["outputs"][2]["value"],
                "depositEndTime": _info["outputs"][3]["value"],
                "lockDurationReq": _info["outputs"][4]["value"],
                "lockEndReq": _info["outputs"][5]["value"],
                "depositAmountReq": _info["outputs"][6]["value"],
                "whitelist": _info["outputs"][7]["value"],
                "description": _info["outputs"][8]["value"],
            }
        else:
            raise ValueError(f" Function name not recognized {_info['name']}")

    return result


def convert_parsed_rewards_nitro_pool_multicall_result_to_reward_standard(
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
    """
    result = []

    for nitro_pool_address, pool_info in parsed_result.items():
        seconds_passed = pool_info["timestamp"] - pool_info["settings"]["startTime"]
        # check seconds passed is positive
        if seconds_passed < 0:
            logging.getLogger(__name__).error(
                f" Nitro Pool {nitro_pool_address} has negative seconds passed at block {pool_info['block']}. Setting to 0"
            )
            seconds_passed = 0

        # TOKEN 1
        if (
            pool_info["rewardsToken1"]["token"].lower()
            != "0x0000000000000000000000000000000000000000"
        ):
            # build erc20 token helper
            token1_helper = build_erc20_helper(
                chain=text_to_chain(pool_info["network"]),
                address=pool_info["rewardsToken1"]["token"].lower(),
                block=pool_info["block"],
                timestamp=pool_info["timestamp"],
            )
            result.append(
                {
                    "network": pool_info["network"],
                    "block": pool_info["block"],
                    "timestamp": pool_info["timestamp"],
                    "hypervisor_address": pool_info["hypervisor_address"],
                    "rewarder_address": pool_info["rewarder_address"],
                    "rewarder_registry": pool_info["rewarder_registry"],
                    "rewarder_type": rewarderType.CAMELOT_nitro,
                    "rewarder_refIds": [],
                    "rewardToken": pool_info["rewardsToken1"]["token"].lower(),
                    "rewardToken_symbol": token1_helper.symbol,
                    "rewardToken_decimals": token1_helper.decimals,
                    "rewards_perSecond": pool_info["rewardsToken1PerSecond"],
                    "total_hypervisorToken_qtty": pool_info["totalDepositAmount"],
                }
            )
        # TOKEN 2
        if (
            pool_info["rewardsToken2"]["token"].lower()
            != "0x0000000000000000000000000000000000000000"
        ):
            # build erc20 token helper
            token2_helper = build_erc20_helper(
                chain=text_to_chain(pool_info["network"]),
                address=pool_info["rewardsToken2"]["token"].lower(),
                block=pool_info["block"],
                timestamp=pool_info["timestamp"],
            )
            result.append(
                {
                    "network": pool_info["network"],
                    "block": pool_info["block"],
                    "timestamp": pool_info["timestamp"],
                    "hypervisor_address": pool_info["hypervisor_address"],
                    "rewarder_address": pool_info["rewarder_address"],
                    "rewarder_registry": pool_info["rewarder_registry"],
                    "rewarder_type": rewarderType.CAMELOT_nitro,
                    "rewarder_refIds": [],
                    "rewardToken": pool_info["rewardsToken2"]["token"].lower(),
                    "rewardToken_symbol": token2_helper.symbol,
                    "rewardToken_decimals": token2_helper.decimals,
                    "rewards_perSecond": pool_info["rewardsToken2PerSecond"],
                    "total_hypervisorToken_qtty": pool_info["totalDepositAmount"],
                }
            )

    return result
