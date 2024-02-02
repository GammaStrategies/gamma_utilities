import logging
from apps.feeds.utils import (
    get_hypervisor_price_per_share,
    get_reward_pool_prices,
    get_token_prices_db,
)
from apps.hypervisor_periods.base import hypervisor_periods_base
from bins.general.enums import Chain, Protocol, rewarderType
from bins.w3.builders import build_db_hypervisor, build_db_hypervisor_multicall
from bins.w3.helpers.multicaller import build_call_with_abi_part, execute_parse_calls
from bins.w3.protocols.camelot.rewarder import (
    camelot_rewards_nft_pool,
    camelot_rewards_nft_pool_master,
)


# TODO: not used. to be removed or refactored
class hypervisor_periods_camelot(hypervisor_periods_base):
    def __init__(self, chain: Chain, hypervisor_status: dict, rewarder_static: dict):
        """Creates camelot rewards data for the specified hypervisor status

        Args:
            chain (Chain):
            hypervisor_status (dict): hypervisor status ( database structure )
            rewarder_static (dict): rewarder static ( only rewarder_registry will be used ) ( database structure )

        """
        # set main vars
        self.chain = chain
        self.hypervisor_status = hypervisor_status
        # Camelor spNFT is a dual token rewarder ( grail and xGrail), but database rewarder_static only points to grail. xGrail will be processed also.
        self.rewarder_static = rewarder_static

        self.type = rewarderType.CAMELOT_spNFT

        super().__init__()

    def reset(self):
        # reset all vars
        self.total_baseRewards = 0
        self.total_boostedRewards = 0
        self.total_time_passed = 0
        self.items_to_calc_apr = []
        self._ini_cache()
        super().reset()

    def _ini_cache(self):
        self.cache = {}

    # MAIN FUNCTIONS
    def _execute_preLoop(self, hypervisor_data: dict):
        pass

    def _execute_inLoop(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        pass

    def _execute_inLoop_startItem(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        # self.rewarder_static["rewarder_address"] = spNFT pool address
        # self.rewarder_static["rewarder_registry"] = master rewarder address
        pass

    def _execute_inLoop_endItem(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        # self.rewarder_static["rewarder_address"] = spNFT pool address
        # self.rewarder_static["rewarder_registry"] = master rewarder address

        # calculate time passed between this and last item
        time_passed = current_item["timestamp"] - last_item["timestamp"]

        # get at blocks at last_item and current_item:
        #   from master getPoolInfo -> rewardsRate
        #   from nft pool getPoolInfo and xGrailRewardsShare ( % of xgrail rewards vs grail )
        last_item_rewards, current_item_rewards = self._get_rewards(
            chain=chain,
            last_item=last_item,
            current_item=current_item,
        )
        # save xgrail token address for later use
        self.grail_token_address = last_item_rewards["grailToken"]
        self.xGrail_token_address = last_item_rewards["xGrailToken"]

        # get prices and calculate period rewards
        rewards_data = self._build_rewards_from_twoPoints(
            network=chain.database_name,
            hypervisor_status=self.hypervisor_status,
            lastItem=last_item,
            currenItem=current_item,
            lastItem_rewards=last_item_rewards,
            currenItem_rewards=current_item_rewards,
        )

        # add to items to calc apr: transform to float
        self.items_to_calc_apr.append(rewards_data)

        # add totals
        self.total_baseRewards += rewards_data["rewards"]["period_yield_usd"]
        self.total_boostedRewards += 0
        self.total_time_passed += time_passed

    def _execute_postLoop(self, hypervisor_data: dict):
        if not self.items_to_calc_apr:
            logging.getLogger(__name__).error(
                f" No items to calculate apr found for {self.chain.fantasy_name}'s {self.type} rewards hypervisor {self.hypervisor_status['address']} at block {self.hypervisor_status['block']}"
            )
            return

        # calculate per second rewards
        baseRewards_per_second = (
            self.total_baseRewards / self.total_time_passed
            if self.total_time_passed
            else 0
        )
        boostedRewards_per_second = (
            self.total_boostedRewards / self.total_time_passed
            if self.total_time_passed
            else 0
        )
        totalRewards_per_second = baseRewards_per_second + boostedRewards_per_second

        if (
            not isinstance(totalRewards_per_second, int)
            and totalRewards_per_second.is_integer()
        ):
            totalRewards_per_second = int(totalRewards_per_second)

        # calculate apr
        if reward_apr_data_to_save := self._create_rewards_status_calculate_apr(
            hypervisor_address=self.hypervisor_status["address"],
            network=self.chain.database_name,
            block=self.hypervisor_status["block"],
            rewardToken_address=self.rewarder_static["rewardToken"],
            items_to_calc_apr=self.items_to_calc_apr,
        ):
            # save reward data for Grail and xGrail tokens
            # self.rewarder_static["rewardToken"] is Grail token
            _xgrail_percentage = reward_apr_data_to_save["xRewardsToken_percentage"]
            _grail_percentage = 1 - _xgrail_percentage

            # build reward data for GRAIL token
            reward_data = {
                "apr": reward_apr_data_to_save["apr"] * _grail_percentage,
                "apy": reward_apr_data_to_save["apy"] * _grail_percentage,
                "block": self.hypervisor_status["block"],
                "timestamp": self.hypervisor_status["timestamp"],
                "hypervisor_address": self.hypervisor_status["address"],
                "hypervisor_symbol": self.hypervisor_status["symbol"],
                "dex": self.hypervisor_status["dex"],
                "rewarder_address": self.rewarder_static["rewarder_address"].lower(),
                "rewarder_type": rewarderType.CAMELOT_spNFT,
                "rewarder_refIds": [],
                "rewarder_registry": self.rewarder_static["rewarder_registry"].lower(),
                "rewardToken": self.grail_token_address.lower(),
                "rewardToken_symbol": self.rewarder_static["rewardToken_symbol"],
                "rewardToken_decimals": self.rewarder_static["rewardToken_decimals"],
                "rewardToken_price_usd": reward_apr_data_to_save[
                    "rewardToken_price_usd"
                ],
                "token0_price_usd": reward_apr_data_to_save["token0_price_usd"],
                "token1_price_usd": reward_apr_data_to_save["token1_price_usd"],
                "rewards_perSecond": str(totalRewards_per_second) * _grail_percentage,
                "total_hypervisorToken_qtty": str(
                    self.hypervisor_status["totalSupply"]
                ),
                "hypervisor_share_price_usd": reward_apr_data_to_save[
                    "hypervisor_share_price_usd"
                ],
                # extra fields
                "extra": {
                    "baseRewards": str(self.total_baseRewards) * _grail_percentage,
                    "boostedRewards": str(self.total_boostedRewards)
                    * _grail_percentage,
                    "baseRewards_apr": reward_apr_data_to_save["extra"][
                        "baseRewards_apr"
                    ]
                    * _grail_percentage,
                    "baseRewards_apy": reward_apr_data_to_save["extra"][
                        "baseRewards_apy"
                    ]
                    * _grail_percentage,
                    "boostedRewards_apr": reward_apr_data_to_save["extra"][
                        "boostedRewards_apr"
                    ]
                    * _grail_percentage,
                    "boostedRewards_apy": reward_apr_data_to_save["extra"][
                        "boostedRewards_apy"
                    ]
                    * _grail_percentage,
                    "baseRewards_per_second": str(baseRewards_per_second)
                    * _grail_percentage,
                    "boostedRewards_per_second": str(boostedRewards_per_second)
                    * _grail_percentage,
                },
            }
            self.result.append(reward_data)

            # build reward data for xGRAIL token
            reward_data = {
                "apr": reward_apr_data_to_save["apr"] * _xgrail_percentage,
                "apy": reward_apr_data_to_save["apy"] * _xgrail_percentage,
                "block": self.hypervisor_status["block"],
                "timestamp": self.hypervisor_status["timestamp"],
                "hypervisor_address": self.hypervisor_status["address"],
                "hypervisor_symbol": self.hypervisor_status["symbol"],
                "dex": self.hypervisor_status["dex"],
                "rewarder_address": self.rewarder_static["rewarder_address"].lower(),
                "rewarder_type": rewarderType.CAMELOT_spNFT,
                "rewarder_refIds": [],
                "rewarder_registry": self.rewarder_static["rewarder_registry"].lower(),
                "rewardToken": self.xGrail_token_address.lower(),
                "rewardToken_symbol": "x" + self.rewarder_static["rewardToken_symbol"],
                "rewardToken_decimals": self.rewarder_static["rewardToken_decimals"],
                "rewardToken_price_usd": reward_apr_data_to_save[
                    "xRewardToken_price_usd"
                ],
                "token0_price_usd": reward_apr_data_to_save["token0_price_usd"],
                "token1_price_usd": reward_apr_data_to_save["token1_price_usd"],
                "rewards_perSecond": str(totalRewards_per_second) * _xgrail_percentage,
                "total_hypervisorToken_qtty": str(
                    self.hypervisor_status["totalSupply"]
                ),
                "hypervisor_share_price_usd": reward_apr_data_to_save[
                    "hypervisor_share_price_usd"
                ],
                # extra fields
                "extra": {
                    "baseRewards": str(self.total_baseRewards) * _xgrail_percentage,
                    "boostedRewards": str(self.total_boostedRewards)
                    * _xgrail_percentage,
                    "baseRewards_apr": reward_apr_data_to_save["extra"][
                        "baseRewards_apr"
                    ]
                    * _xgrail_percentage,
                    "baseRewards_apy": reward_apr_data_to_save["extra"][
                        "baseRewards_apy"
                    ]
                    * _xgrail_percentage,
                    "boostedRewards_apr": reward_apr_data_to_save["extra"][
                        "boostedRewards_apr"
                    ]
                    * _xgrail_percentage,
                    "boostedRewards_apy": reward_apr_data_to_save["extra"][
                        "boostedRewards_apy"
                    ]
                    * _xgrail_percentage,
                    "baseRewards_per_second": str(baseRewards_per_second)
                    * _xgrail_percentage,
                    "boostedRewards_per_second": str(boostedRewards_per_second)
                    * _xgrail_percentage,
                },
            }

    def _scrape_last_item(
        self,
        chain: Chain,
        hypervisor_address: str,
        block: int,
        protocol: Protocol,
        hypervisor_status: dict | None = None,
    ) -> dict:
        return None
        # to be able to return , multiple issues must be addressed:
        # last item must not have supply changed
        # prices must be scraped on the fly ( bc there will be no prices in database for that block )
        return build_db_hypervisor(
            address=hypervisor_address,
            network=chain.database_name,
            block=block,
            dex=protocol.database_name,
            cached=True,
        )

    def execute_processes_within_hypervisor_periods(
        self,
        timestamp_ini: int = None,
        timestamp_end: int = None,
        block_ini: int = None,
        block_end: int = None,
        try_solve_errors: bool = False,
    ) -> list:
        logging.getLogger(__name__).debug(
            f" Getting rewards of {self.chain.database_name} {self.hypervisor_status['address']} timestamp_ini: {timestamp_ini} timestamp_end: {timestamp_end or int(self.hypervisor_status['timestamp'])}  block_ini: {block_ini}  block_end: {block_end or int(self.hypervisor_status['block'])}"
        )
        return super().execute_processes_within_hypervisor_periods(
            chain=self.chain,
            hypervisor_address=self.hypervisor_status["address"],
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
            block_ini=block_ini,
            block_end=block_end or int(self.hypervisor_status["block"]),
            try_solve_errors=try_solve_errors,
            only_use_last_items=6,
        )

    # get camelot reward information
    def _get_rewards(
        self,
        chain: Chain,
        last_item: dict,
        current_item: dict,
    ) -> tuple[dict, dict]:
        # create a camelot nft pool master object
        _calls = []
        nft_pool_master = camelot_rewards_nft_pool_master(
            address=self.rewarder_static["rewarder_registry"],
            network=chain.database_name,
            block=last_item["block"],
            timestamp=last_item["timestamp"],
        )
        # add getPoolInfo call from master
        _calls.append(
            build_call_with_abi_part(
                abi_part=nft_pool_master.get_abi_function("getPoolInfo"),
                inputs_values=[self.rewarder_static["rewarder_address"]],
                address=self.rewarder_static["rewarder_registry"],
                object="nft_pool_master",
            )
        )
        nft_pool = camelot_rewards_nft_pool(
            address=self.rewarder_static["rewarder_address"],
            network=chain.database_name,
            block=last_item["block"],
            timestamp=last_item["timestamp"],
        )
        # add getPoolInfo call from pool
        _calls.append(
            build_call_with_abi_part(
                abi_part=nft_pool.get_abi_function("getPoolInfo"),
                inputs_values=[],
                address=self.rewarder_static["rewarder_address"],
                object="nft_pool",
            )
        )
        # add xGrailRewardsShare call
        _calls.append(
            build_call_with_abi_part(
                abi_part=nft_pool.get_abi_function("xGrailRewardsShare"),
                inputs_values=[],
                address=self.rewarder_static["rewarder_address"],
                object="nft_pool",
            )
        )

        # execute multicalls for both blocks
        multicall_result_lastItem = self._parse_multicall_result(
            execute_parse_calls(
                network=chain.database_name,
                block=last_item["block"],
                calls=_calls,
                convert_bint=False,
            )
        )
        multicall_result_currentItem = self._parse_multicall_result(
            execute_parse_calls(
                network=chain.database_name,
                block=current_item["block"],
                calls=_calls,
                convert_bint=False,
            )
        )
        # return result
        return multicall_result_lastItem, multicall_result_currentItem

    def _parse_multicall_result(self, multicall_result) -> dict:
        """_summary_

        Args:
            multicall_result (_type_): _description_

        Returns:
            dict: {<address>: lpToken, grailToken, xGrailToken, lastRewardTime,accRewardsPerShare, lpSupply, lpSupplyWithMultiplier, allocPoint, xGrailRewardsShare, reserve, poolEmissionRate}
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

    def _build_rewards_from_twoPoints(
        self,
        network: str,
        hypervisor_status: dict,
        lastItem: dict,
        currenItem: dict,
        lastItem_rewards: dict,
        currenItem_rewards: dict,
    ) -> dict:
        # get current token prices
        (
            grailToken_price,
            xGrailToken_price,
            token0_price,
            token1_price,
        ) = get_token_prices_db(
            network=network,
            block=hypervisor_status["block"],
            tokens=[
                lastItem_rewards["grailToken"],
                lastItem_rewards["xGrailToken"],
                hypervisor_status["pool"]["token0"]["address"],
                hypervisor_status["pool"]["token1"]["address"],
            ],
            within_timeframe=10,
        )

        # hypervisor data
        hype_price_per_share = get_hypervisor_price_per_share(
            hypervisor_status=hypervisor_status,
            token0_price=token0_price,
            token1_price=token1_price,
        )
        hype_tvl_usd = (
            int(hypervisor_status["totalSupply"])
            / (10 ** hypervisor_status["decimals"])
        ) * hype_price_per_share

        # total liquidity
        hypervisor_liquidity = int(
            hypervisor_status["basePosition"]["liquidity"]
        ) + int(hypervisor_status["limitPosition"]["liquidity"])
        # inRange liquidity
        gamma_liquidity_in_range = 0
        currentTick = int(hypervisor_status["currentTick"])
        if (
            int(hypervisor_status["baseUpper"]) >= currentTick
            and int(hypervisor_status["baseLower"]) <= currentTick
        ):
            gamma_liquidity_in_range += int(
                hypervisor_status["basePosition"]["liquidity"]
            )
        if (
            int(hypervisor_status["limitUpper"]) >= currentTick
            and int(hypervisor_status["limitLower"]) <= currentTick
        ):
            gamma_liquidity_in_range += int(
                hypervisor_status["limitPosition"]["liquidity"]
            )

        # # choose which liquidity is rewarded ( inRange or total or staked)
        # rewarded_liquidity = gamma_liquidity_in_range

        hypervisor_total0 = int(hypervisor_status["totalAmounts"]["total0"]) / (
            10 ** hypervisor_status["pool"]["token0"]["decimals"]
        )
        hypervisor_total1 = int(hypervisor_status["totalAmounts"]["total1"]) / (
            10 ** hypervisor_status["pool"]["token1"]["decimals"]
        )

        # pool data
        pool_liquidity = int(hypervisor_status["pool"]["liquidity"])
        # pool_total0 = int(distribution_data["token0_balance_in_pool"]) / (
        #     10 ** int(distribution_data["token0_decimals"])
        # )
        # pool_total1 = int(distribution_data["token1_balance_in_pool"]) / (
        #     10 ** int(distribution_data["token1_decimals"])
        # )
        # pool_tvl_usd = pool_total0 * token0_price + pool_total1 * token1_price

        xgrail_percentage = currenItem_rewards["xGrailRewardsShare"] / 10000
        grail_percentage = 1 - xgrail_percentage

        reward_x_second_grail_usd = (
            (currenItem_rewards["poolEmissionRate"] * grail_percentage)
            / (10 ** self.rewarder_static["rewardToken_decimals"])
        ) * grailToken_price
        reward_x_second_xgrail_usd = (
            (currenItem_rewards["poolEmissionRate"] * xgrail_percentage)
            / (10 ** self.rewarder_static["rewardToken_decimals"])
        ) * xGrailToken_price

        # calculate period yield
        period_seconds = currenItem["timestamp"] - lastItem["timestamp"]
        period_yield_qtty = currenItem_rewards["poolEmissionRate"] * period_seconds
        period_yield_grail_usd = (
            (period_yield_qtty * grail_percentage)
            / (10 ** self.rewarder_static["rewardToken_decimals"])
            * grailToken_price
        )
        period_yield_xgrail_usd = (
            (period_yield_qtty * xgrail_percentage)
            / (10 ** self.rewarder_static["rewardToken_decimals"])
            * xGrailToken_price
        )

        # calculate rewards APR
        lpSupplyWithMultiplier_usd_value = (
            currenItem_rewards["lpSupplyWithMultiplier"]
            / 10 ** hypervisor_status["decimals"]
        ) * hype_price_per_share
        reward_apr = (
            (reward_x_second_grail_usd + reward_x_second_xgrail_usd)
            * 365
            * 24
            * 60
            * 60
        ) / lpSupplyWithMultiplier_usd_value

        ### CHECKS
        if hype_tvl_usd > 10**18:
            raise ValueError(
                f" hype tvl is too high: {hype_tvl_usd} for {hypervisor_status['symbol']} {hypervisor_status['address']} at block {hypervisor_status['block']}"
            )

        return {
            "block": hypervisor_status["block"],
            "timestamp": hypervisor_status["timestamp"],
            "rewards": {
                "period_time": period_seconds,
                "apr": reward_apr,
                "period_yield_qtty": period_yield_qtty,
                "period_yield_usd": period_yield_grail_usd + period_yield_xgrail_usd,
                "period_yield_grail_usd": period_yield_grail_usd,
                "period_yield_xgrail_usd": period_yield_xgrail_usd,
                "reward_x_second": currenItem_rewards["poolEmissionRate"],
                "reward_x_second_grail": currenItem_rewards["poolEmissionRate"]
                * grail_percentage,
                "reward_x_second_xgrail": currenItem_rewards["poolEmissionRate"]
                * xgrail_percentage,
                "reward_x_second_usd": reward_x_second_grail_usd
                + reward_x_second_xgrail_usd,
                "reward_x_second_grail_usd": reward_x_second_grail_usd,
                "reward_x_second_xgrail_usd": reward_x_second_xgrail_usd,
            },
            "pool": {
                "address": hypervisor_status["pool"]["address"],
                # "symbol": hypervisor_status["pool"]["symbol"],
                "liquidity_inRange": pool_liquidity,
                # "total0": pool_total0,
                # "total1": pool_total1,
                # "tvl_usd": pool_tvl_usd,
            },
            "hypervisor": {
                "address": hypervisor_status["address"],
                "symbol": hypervisor_status["symbol"],
                "liquidity_total": hypervisor_liquidity,
                "liquidity_inRange": gamma_liquidity_in_range,
                # "rewarded_liquidity": rewarded_liquidity,
                "total0": hypervisor_total0,
                "total1": hypervisor_total1,
                "tvl_usd": hype_tvl_usd,
                "hypervisor_share": hype_price_per_share,
                "staked_qtty": currenItem_rewards["lpSupplyWithMultiplier"],
                "staked_qtty_usd": lpSupplyWithMultiplier_usd_value,
            },
            "prices": {
                "rewardToken": grailToken_price,
                "xRewardToken": xGrailToken_price,
                "token0": token0_price,
                "token1": token1_price,
            },
        }

    def _create_rewards_status_calculate_apr(
        self,
        hypervisor_address: str,
        network: str,
        block: int,
        rewardToken_address: str,
        items_to_calc_apr: list,
    ) -> dict:
        """

        Args:
            hypervisor_address (str):
            network (str):
            block (int):
            rewardToken_address (str):
            token0_address (str):
            token1_address (str):
            items_to_calc_apr (list): {
                                "base_rewards":
                                "boosted_rewards":
                                "time_passed":
                                "timestamp_ini":
                                "timestamp_end":
                                "pool": {
                                        "address":
                                        "liquidity":
                                    },
                                "hypervisor": {
                                    "block
                                    base_liquidity
                                    limit_liquidity
                                    "totalSupply":
                                    "underlying_token0":
                                    "underlying_token1":
                                },
                                distribution_data: {'token1_balance_in_pool':
                                                    'token_symbol':
                                                    'token_decimals':
                                                    'epoch_duration':
                                                    'reward_calculations':
                                                        {'reward_x_epoch': , 'reward_x_second': , 'reward_yearly': , 'reward_yearly_token0': , 'reward_yearly_token1': 1.1380178571428571e+21, 'reward_yearly_fees': 1.1152574999999998e+23, 'reward_x_epoch_decimal': 12.991071428571429, 'reward_x_second_decimal': 0.0036086309523809526, 'reward_yearly_decimal': 113801.78571428571, 'reward_yearly_token0_decimal': 1138.017857142857, 'reward_yearly_token1_decimal': 1138.017857142857, 'reward_yearly_fees_decimal': 111525.75}
                                                    'reward_x_epoch':
                                                    'reward_x_second':
                                                    'reward_yearly':
                                                    'reward_yearly_token0':
                                                    'reward_yearly_token1':
                                                    'reward_yearly_fees':
                                                    'reward_x_epoch_decimal':
                                                    'reward_x_second_decimal':
                                                    'reward_yearly_decimal':
                                                    'reward_yearly_token0_decimal':
                                                    'reward_yearly_token1_decimal':
                                                    'reward_yearly_fees_decimal':}
                            }

        Returns:
            dict:  {
                "apr": reward_apr,
                "apy": reward_apy,
                "rewardToken_price_usd": ,
                "token0_price_usd": ,
                "token1_price_usd": ,
                "hypervisor_share_price_usd": ,
                "extra": {
                    "baseRewards_apr": ,
                    "baseRewards_apy": ,
                    "boostedRewards_apr": ,
                    "boostedRewards_apy": ,
                },
            }

        Yields:
            Iterator[dict]: _description_
        """
        reward_data = {}

        # apr
        try:
            if len(items_to_calc_apr) == 0:
                raise ValueError(f" There are no items to calc rewards")

            # end control vars
            cum_reward_return = 0
            cum_baseReward_return = 0
            cum_boostedReward_return = 0
            total_period_seconds = 0

            hypervisor_share_price_usd = 0
            baseRewards_apr = 0
            baseRewards_apy = 0
            boostRewards_apr = 0
            boostRewards_apy = 0

            # statics
            day_in_seconds = 60 * 60 * 24
            year_in_seconds = day_in_seconds * 365

            for item in items_to_calc_apr:
                # simplify price access
                hype_token0_price = item["prices"]["token0"]
                hype_token1_price = item["prices"]["token1"]
                rewardToken_price = item["prices"]["rewardToken"]
                xrewardToken_price = item["prices"]["xRewardToken"]

                # discard items with timepassed = 0
                if item["rewards"]["period_time"] == 0:
                    logging.getLogger(__name__).debug(
                        f" ...no time passed found while processing apr for {hypervisor_address} using item {item}"
                    )
                    continue
                if item["hypervisor"]["totalSupply"] == 0:
                    logging.getLogger(__name__).warning(
                        f" ...no supply found while processing apr for {hypervisor_address} using item {item}"
                    )
                    continue

                # calculate price per share for each item using current prices
                tvl = item["hypervisor"]["tvl_usd"]
                staked_tvl = item["hypervisor"]["staked_qtty_usd"]

                # set price per share var ( the last will remain)
                hypervisor_share_price_usd = item["hypervisor"]["hypervisor_share"]

                # item["base_rewards_usd"] = item["base_rewards"] * rewardToken_price
                # item["boosted_rewards_usd"] = (
                #     item["boosted_rewards"] * rewardToken_price
                # )
                # item["total_rewards_usd"] = (
                #     item["base_rewards_usd"] + item["boosted_rewards_usd"]
                # )

                # calculate period yield
                period_yield = (
                    item["rewards"]["period_yield_usd"] / staked_tvl
                    if staked_tvl
                    else 0
                )

                # filter outliers
                if item["period_yield"] > 1:
                    logging.getLogger(__name__).warning(
                        f" found outlier period yield {item['period_yield']} for {hypervisor_address} using item {item}"
                    )
                    item["hypervisor"]["tvl"] = 0
                    item["hypervisor"]["totalStaked_tvl"] = 0
                    item["hypervisor"]["price_per_share"] = 0
                    item["base_rewards_usd"] = 0
                    item["boosted_rewards_usd"] = 0
                    item["total_rewards_usd"] = 0
                    item["period_yield"] = 0
                    continue

                # add to cumulative yield
                if cum_reward_return:
                    cum_reward_return *= 1 + period_yield
                else:
                    cum_reward_return = 1 + period_yield
                # if cum_baseReward_return:
                #     cum_baseReward_return *= 1 + (
                #         item["base_rewards_usd"] / tvl if tvl else 0
                #     )
                # else:
                #     cum_baseReward_return = 1 + (
                #         item["base_rewards_usd"] / tvl if tvl else 0
                #     )
                # if cum_boostedReward_return:
                #     cum_boostedReward_return *= 1 + (
                #         item["boosted_rewards_usd"] / tvl if tvl else 0
                #     )
                # else:
                #     cum_boostedReward_return = 1 + (
                #         item["boosted_rewards_usd"] / tvl if tvl else 0
                #     )

                # extrapolate rewards to a year

                # item["base_rewards_usd_year"] = (
                #     item["base_rewards_usd"] / item["time_passed"]
                # ) * year_in_seconds
                # item["boosted_rewards_usd_year"] = (
                #     item["boosted_rewards_usd"] / item["time_passed"]
                # ) * year_in_seconds
                # item["total_rewards_usd_year"] = (
                #     item["base_rewards_usd_year"] + item["boosted_rewards_usd_year"]
                # )

                # item["total_reward_apr"] = (cum_reward_return - 1) * (
                #     (year_in_seconds) / item["rewards"]["period_time"]
                # )
                # try:
                #     item["total_reward_apy"] = (
                #         1
                #         + (cum_reward_return - 1)
                #         * ((day_in_seconds) / item["rewards"]["period_time"])
                #     ) ** 365 - 1
                # except OverflowError as e:
                #     logging.getLogger(__name__).debug(
                #         f"  cant calc apy Overflow err on  total_reward_apy...{e}"
                #     )
                #     item["total_reward_apy"] = 0

                # item["base_reward_apr"] = (cum_baseReward_return - 1) * (
                #     (year_in_seconds) / item["rewards"]["period_time"]
                # )
                # try:
                #     item["base_reward_apy"] = (
                #         1
                #         + (cum_baseReward_return - 1)
                #         * ((day_in_seconds) / item["rewards"]["period_time"])
                #     ) ** 365 - 1
                # except OverflowError as e:
                #     logging.getLogger(__name__).debug(
                #         f"  cant calc apy Overflow err on  base_reward_apy...{e}"
                #     )
                #     item["base_reward_apy"] = 0

                # item["boosted_reward_apr"] = (cum_boostedReward_return - 1) * (
                #     (year_in_seconds) / item["rewards"]["period_time"]
                # )
                # try:
                #     item["boosted_reward_apy"] = (
                #         1
                #         + (cum_boostedReward_return - 1)
                #         * ((day_in_seconds) / item["rewards"]["period_time"])
                #     ) ** 365 - 1
                # except OverflowError as e:
                #     logging.getLogger(__name__).debug(
                #         f"  cant calc apy Overflow err on  boosted_reward_apy...{e}"
                #     )
                #     item["boosted_reward_apy"] = 0

                total_period_seconds += item["rewards"]["period_time"]

            # calculate total apr
            cum_reward_return -= 1
            cum_baseReward_return -= 1
            cum_boostedReward_return -= 1
            reward_apr = (
                cum_reward_return * ((year_in_seconds) / total_period_seconds)
                if total_period_seconds
                else 0
            )
            try:
                reward_apy = (
                    1 + cum_reward_return * ((day_in_seconds) / total_period_seconds)
                    if total_period_seconds
                    else 0
                ) ** 365 - 1
            except OverflowError as e:
                logging.getLogger(__name__).debug(
                    f"  cant calc apy Overflow err on  reward_apy...{e}"
                )
                reward_apy = 0

            baseRewards_apr = (
                cum_baseReward_return * ((year_in_seconds) / total_period_seconds)
                if total_period_seconds
                else 0
            )
            try:
                baseRewards_apy = (
                    1
                    + cum_baseReward_return * ((day_in_seconds) / total_period_seconds)
                    if total_period_seconds
                    else 0
                ) ** 365 - 1
            except OverflowError as e:
                logging.getLogger(__name__).debug(
                    f"  cant calc apy Overflow err on  baseRewards_apy...{e}"
                )
                baseRewards_apy = 0

            boostRewards_apr = (
                cum_boostedReward_return * ((year_in_seconds) / total_period_seconds)
                if total_period_seconds
                else 0
            )

            try:
                boostRewards_apy = (
                    1
                    + cum_boostedReward_return
                    * ((day_in_seconds) / total_period_seconds)
                    if total_period_seconds
                    else 0
                ) ** 365 - 1
            except OverflowError as e:
                logging.getLogger(__name__).debug(
                    f"  cant calc apy Overflow err on  boostRewards_apy...{e}"
                )
                boostRewards_apy = 0

            # build reward data
            reward_data = {
                "apr": reward_apr if reward_apr > 0 else 0,
                "apy": reward_apy if reward_apy > 0 else 0,
                "rewardToken_price_usd": rewardToken_price,
                "xRewardToken_price_usd": xrewardToken_price,
                "xRewardsToken_percentage": items_to_calc_apr[-1]["rewards"][
                    "xRewardsToken_percentage"
                ],
                "token0_price_usd": hype_token0_price,
                "token1_price_usd": hype_token1_price,
                "hypervisor_share_price_usd": hypervisor_share_price_usd,
                # extra fields
                "extra": {
                    "baseRewards_apr": baseRewards_apr if baseRewards_apr > 0 else 0,
                    "baseRewards_apy": baseRewards_apy if baseRewards_apy > 0 else 0,
                    "boostedRewards_apr": (
                        boostRewards_apr if boostRewards_apr > 0 else 0
                    ),
                    "boostedRewards_apy": (
                        boostRewards_apy if boostRewards_apy > 0 else 0
                    ),
                },
            }

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error while calculating rewards yield for ramses hypervisor {hypervisor_address} reward token {rewardToken_address} at block {block} err: {e}"
            )

        return reward_data
