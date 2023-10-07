import logging
from apps.feeds.utils import get_hypervisor_price_per_share, get_reward_pool_prices
from apps.hypervisor_periods.base import hypervisor_periods_base
from bins.general.enums import Chain, Protocol, rewarderType
from bins.w3.builders import build_db_hypervisor
from bins.w3.protocols.angle.rewarder import angle_merkle_distributor_creator


class hypervisor_periods_angleMerkl(hypervisor_periods_base):
    def __init__(self, chain: Chain, hypervisor_status: dict, rewarder_static: dict):
        """Creates merkle rewards data for the specified hypervisor status

        Args:
            chain (Chain):
            hypervisor_status (dict): hypervisor status ( database structure )
            rewarder_static (dict): rewarder static ( database structure )
        """
        # set main vars
        self.chain = chain
        self.hypervisor_status = hypervisor_status
        self.rewarder_static = rewarder_static

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
        # add distribution data to all items in the loop.
        current_item["distribution_data"] = self._build_distribution_data(
            network=chain.database_name,
            distributor_address=self.rewarder_static["rewarder_registry"],
            block=current_item["block"],
            pool_address=current_item["pool"]["address"],
            hypervisor_address=current_item["address"],
        )

    def _execute_inLoop_startItem(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
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
        # calculate time passed between this and last item
        time_passed = current_item["timestamp"] - last_item["timestamp"]
        rewards_aquired_period_end = 0

        for distribution_data in current_item["distribution_data"]:
            real_rewards_end = self._build_rewards_from_distribution(
                network=chain.database_name,
                hypervisor_status=current_item,
                distribution_data=distribution_data,
                calculations_data=distribution_data["reward_calculations"],
            )

            # add real rewards to distribution_data for later use
            distribution_data["reward_calculations"] = real_rewards_end

            # calculate rewards of the period
            rewards_aquired_period_end = (
                real_rewards_end["reward_x_second_propFees"]
                + real_rewards_end["reward_x_second_propToken0"]
                + real_rewards_end["reward_x_second_propToken1"]
            ) * time_passed

            # add to items to calc apr: transform to float
            self.items_to_calc_apr.append(
                {
                    "base_rewards": rewards_aquired_period_end
                    / (10 ** self.rewarder_static["rewardToken_decimals"]),
                    "boosted_rewards": 0,
                    "time_passed": time_passed,
                    "timestamp_ini": last_item["timestamp"],
                    "timestamp_end": current_item["timestamp"],
                    "pool": {
                        "address": last_item["pool"]["address"],
                        "liquidity": last_item["pool"]["liquidity"],
                    },
                    "hypervisor": {
                        "block": last_item["block"],
                        "base_liquidity": last_item["basePosition"]["liquidity"],
                        "limit_liquidity": last_item["limitPosition"]["liquidity"],
                        "totalSupply": int(last_item["totalSupply"])
                        / (10 ** last_item["decimals"]),
                        "underlying_token0": (
                            float(last_item["totalAmounts"]["total0"])
                            + float(last_item["fees_uncollected"]["qtty_token0"])
                        )
                        / (10 ** last_item["pool"]["token0"]["decimals"]),
                        "underlying_token1": (
                            float(last_item["totalAmounts"]["total1"])
                            + float(last_item["fees_uncollected"]["qtty_token1"])
                        )
                        / (10 ** last_item["pool"]["token1"]["decimals"]),
                    },
                    "distribution_data": distribution_data,
                }
            )

        # add totals

        self.total_baseRewards += (
            rewards_aquired_period_end if rewards_aquired_period_end else 0
        )
        self.total_boostedRewards += 0
        self.total_time_passed += time_passed

    def _execute_postLoop(self, hypervisor_data: dict):
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

        if len(self.items_to_calc_apr) == 0:
            logging.getLogger(__name__).warning(
                f" Merkle rewards: There are no items to calc rewards for {self.chain.database_name}'s {self.hypervisor_status['symbol']} {self.hypervisor_status['address']} at block {self.hypervisor_status['block']}"
            )
            return

        # calculate apr
        if reward_apr_data_to_save := self._create_rewards_status_calculate_apr(
            hypervisor_address=self.hypervisor_status["address"],
            network=self.chain.database_name,
            block=self.hypervisor_status["block"],
            rewardToken_address=self.rewarder_static["rewardToken"],
            items_to_calc_apr=self.items_to_calc_apr,
        ):
            # build reward data
            reward_data = {
                "apr": reward_apr_data_to_save["apr"],
                "apy": reward_apr_data_to_save["apy"],
                "block": self.hypervisor_status["block"],
                "timestamp": self.hypervisor_status["timestamp"],
                "hypervisor_address": self.hypervisor_status["address"],
                "hypervisor_symbol": self.hypervisor_status["symbol"],
                "dex": self.hypervisor_status["dex"],
                "rewarder_address": self.rewarder_static["rewarder_address"].lower(),
                "rewarder_type": rewarderType.ANGLE_MERKLE,
                "rewarder_refIds": [],
                "rewarder_registry": self.rewarder_static["rewarder_registry"].lower(),
                "rewardToken": self.rewarder_static["rewardToken"].lower(),
                "rewardToken_symbol": self.rewarder_static["rewardToken_symbol"],
                "rewardToken_decimals": self.rewarder_static["rewardToken_decimals"],
                "rewardToken_price_usd": reward_apr_data_to_save[
                    "rewardToken_price_usd"
                ],
                "token0_price_usd": reward_apr_data_to_save["token0_price_usd"],
                "token1_price_usd": reward_apr_data_to_save["token1_price_usd"],
                "rewards_perSecond": str(totalRewards_per_second),
                "total_hypervisorToken_qtty": str(
                    self.hypervisor_status["totalSupply"]
                ),
                "hypervisor_share_price_usd": reward_apr_data_to_save[
                    "hypervisor_share_price_usd"
                ],
                # extra fields
                "extra": {
                    "baseRewards": str(self.total_baseRewards),
                    "boostedRewards": str(self.total_boostedRewards),
                    "baseRewards_apr": reward_apr_data_to_save["extra"][
                        "baseRewards_apr"
                    ],
                    "baseRewards_apy": reward_apr_data_to_save["extra"][
                        "baseRewards_apy"
                    ],
                    "boostedRewards_apr": reward_apr_data_to_save["extra"][
                        "boostedRewards_apr"
                    ],
                    "boostedRewards_apy": reward_apr_data_to_save["extra"][
                        "boostedRewards_apy"
                    ],
                    "baseRewards_per_second": str(baseRewards_per_second),
                    "boostedRewards_per_second": str(boostedRewards_per_second),
                    # "raw_data": items_to_calc_apr,
                },
            }
            self.result.append(reward_data)

    def _scrape_last_item(
        self, chain: Chain, hypervisor_address: str, block: int, protocol: Protocol
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
        return super().execute_processes_within_hypervisor_periods(
            chain=self.chain,
            hypervisor_address=self.hypervisor_status["address"],
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end or int(self.hypervisor_status["timestamp"]),
            block_ini=block_ini,
            block_end=block_end or int(self.hypervisor_status["block"]),
            try_solve_errors=try_solve_errors,
            only_use_last_items=6,
        )

    # HELPERS
    # @property
    # def block(self) -> int:
    #     return self.hypervisor_status["block"]

    # @property
    # def timestamp(self) -> int:
    #     return self.hypervisor_status["timestamp"]

    # ANGLE MERKLE DISTRIBUTOR
    # TODO: cache per pool/block
    def _build_distribution_data(
        self,
        network: str,
        distributor_address: str,
        block: int,
        pool_address: str,
        hypervisor_address: str,
    ) -> list:
        """Create the distribution data for the specified pool address / block

        Args:
            network (str):
            distributor_address (str):
            block (int):
            pool_address (str):
            hypervisor_address (str):

        Returns:
            list: _description_
        """

        # try get result from cache
        result = (
            self.cache.get(network, {})
            .get(distributor_address, {})
            .get(block, {})
            .get(pool_address, [])
        )
        if result:
            return result

        # create merkl helper at status block
        distributor_creator = angle_merkle_distributor_creator(
            address=distributor_address,
            network=network,
            block=block,
        )

        # save for later use
        _epoch_duration = distributor_creator.EPOCH_DURATION

        # get active distribution data from merkle
        if distributions := distributor_creator.getActivePoolDistributions(
            address=pool_address
        ):
            for distribution_data in distributions:
                # check if reward token is valid
                if not distributor_creator.isValid_reward_token(
                    reward_address=distribution_data["token"].lower()
                ):
                    # not a valid reward token
                    continue
                if distributor_creator.isBlacklisted(
                    reward_data=distribution_data, hypervisor_address=hypervisor_address
                ):
                    # blacklisted
                    logging.getLogger(__name__).debug(
                        f" {distribution_data['token']} is blacklisted for hype {hypervisor_address} at block {block}"
                    )
                    continue

                distribution_data["epoch_duration"] = _epoch_duration
                distribution_data[
                    "reward_calculations"
                ] = distributor_creator.get_reward_calculations(
                    distribution=distribution_data, _epoch_duration=_epoch_duration
                )

                # add to result
                result.append(distribution_data)

        else:
            # no active distributions
            logging.getLogger(__name__).debug(
                f" no active distributions found for {network}'s pool address {pool_address} at block {block}"
            )

        # try add to cache
        if result:
            try:
                if not network in self.cache:
                    self.cache[network] = {}
                if not distributor_address in self.cache[network]:
                    self.cache[network][distributor_address] = {}
                if not block in self.cache[network][distributor_address]:
                    self.cache[network][distributor_address][block] = {}
                if not pool_address in self.cache[network][distributor_address][block]:
                    self.cache[network][distributor_address][block][pool_address] = {}
                self.cache[network][distributor_address][block][pool_address] = result

            except Exception as e:
                logging.getLogger(__name__).exception(f"  cant add to cache...{e}")

        return result

    def _build_rewards_from_distribution(
        self,
        network: str,
        hypervisor_status: dict,
        distribution_data: dict,
        calculations_data: dict,
    ) -> dict:
        # get token prices
        (
            rewardToken_price,
            token0_price,
            token1_price,
        ) = get_reward_pool_prices(
            network=network,
            block=hypervisor_status["block"],
            reward_token=distribution_data["token"],
            token0=distribution_data["token0_contract"],
            token1=distribution_data["token1_contract"],
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

        hypervisor_total0 = int(hypervisor_status["totalAmounts"]["total0"]) / (
            10 ** hypervisor_status["pool"]["token0"]["decimals"]
        )
        hypervisor_total1 = int(hypervisor_status["totalAmounts"]["total1"]) / (
            10 ** hypervisor_status["pool"]["token1"]["decimals"]
        )

        # pool data
        pool_liquidity = int(hypervisor_status["pool"]["liquidity"])
        pool_total0 = int(distribution_data["token0_balance_in_pool"]) / (
            10 ** int(distribution_data["token0_decimals"])
        )
        pool_total1 = int(distribution_data["token1_balance_in_pool"]) / (
            10 ** int(distribution_data["token1_decimals"])
        )
        pool_tvl_usd = pool_total0 * token0_price + pool_total1 * token1_price

        # check
        if (
            distribution_data["propFees"] / 10000
            + distribution_data["propToken0"] / 10000
            + distribution_data["propToken1"] / 10000
            != 1
        ):
            raise ValueError(
                f" {distribution_data['token']} Angle Merkl distribution data is not valid for hypervisor {hypervisor_status['address']} at block {hypervisor_status['block']}. 'prop___' fields must sum 1. {distribution_data}"
            )

        # reward x second
        reward_x_second_propFees = (
            (
                calculations_data["reward_x_second"]
                * (distribution_data["propFees"] / 10000)
            )
            * (gamma_liquidity_in_range / pool_liquidity)
            if pool_liquidity
            else 0
        )
        reward_x_second_propToken0 = (
            (
                calculations_data["reward_x_second"]
                * (distribution_data["propToken0"] / 10000)
            )
            * (hypervisor_total0 / pool_total0)
            if pool_total0
            else 0
        )
        reward_x_second_propToken1 = (
            (
                calculations_data["reward_x_second"]
                * (distribution_data["propToken1"] / 10000)
            )
            * (hypervisor_total1 / pool_total1)
            if pool_total1
            else 0
        )

        ### CHECKS
        if hype_tvl_usd > 10**18:
            raise ValueError(
                f" hype tvl is too high: {hype_tvl_usd} for {hypervisor_status['symbol']} {hypervisor_status['address']} at block {hypervisor_status['block']}"
            )

        return {
            "reward_x_second_propFees": reward_x_second_propFees,
            "reward_x_second_propToken0": reward_x_second_propToken0,
            "reward_x_second_propToken1": reward_x_second_propToken1,
            "rewardToken_price": rewardToken_price,
            "token0_price": token0_price,
            "token1_price": token1_price,
            "hype_price_per_share": hype_price_per_share,
            "hype_tvl_usd": hype_tvl_usd,
            "hypervisor_liquidity": gamma_liquidity_in_range,
            "hypervisor_total0": hypervisor_total0,
            "hypervisor_total1": hypervisor_total1,
            "pool_liquidity": pool_liquidity,
            "pool_total0": pool_total0,
            "pool_total1": pool_total1,
            "pool_tvl_usd": pool_tvl_usd,
        }

        return {
            "block": hypervisor_status["block"],
            "timestamp": hypervisor_status["timestamp"],
            "rewards": {
                "reward_x_second_propFees": reward_x_second_propFees,
                "reward_x_second_propToken0": reward_x_second_propToken0,
                "reward_x_second_propToken1": reward_x_second_propToken1,
            },
            "pool": {
                "address": hypervisor_status["pool"]["address"],
                "symbol": hypervisor_status["pool"]["symbol"],
                "liquidity_inRange": pool_liquidity,
                "total0": pool_total0,
                "total1": pool_total1,
                "tvl_usd": pool_tvl_usd,
            },
            "hypervisor": {
                "address": hypervisor_status["address"],
                "symbol": hypervisor_status["symbol"],
                "liquidity_total": hypervisor_liquidity,
                "liquidity_inRange": gamma_liquidity_in_range,
                "total0": hypervisor_total0,
                "total1": hypervisor_total1,
                "tvl_usd": hype_tvl_usd,
                "hypervisor_share": hype_price_per_share,
            },
            "prices": {
                "rewardToken": rewardToken_price,
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
                hype_token0_price = item["distribution_data"]["reward_calculations"][
                    "token0_price"
                ]
                hype_token1_price = item["distribution_data"]["reward_calculations"][
                    "token1_price"
                ]
                rewardToken_price = item["distribution_data"]["reward_calculations"][
                    "rewardToken_price"
                ]

                # discard items with timepassed = 0
                if item["time_passed"] == 0:
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
                tvl = item["distribution_data"]["reward_calculations"]["hype_tvl_usd"]
                # tvl = (
                #     item["hypervisor"]["underlying_token0"] * hype_token0_price
                #     + item["hypervisor"]["underlying_token1"] * hype_token1_price
                # )

                # set price per share var ( the last will remain)
                hypervisor_share_price_usd = item["distribution_data"][
                    "reward_calculations"
                ]["hype_price_per_share"]

                item["base_rewards_usd"] = item["base_rewards"] * rewardToken_price
                item["boosted_rewards_usd"] = (
                    item["boosted_rewards"] * rewardToken_price
                )
                item["total_rewards_usd"] = (
                    item["base_rewards_usd"] + item["boosted_rewards_usd"]
                )

                # calculate period yield
                item["period_yield"] = item["total_rewards_usd"] / tvl if tvl else 0

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
                    cum_reward_return *= 1 + item["period_yield"]
                else:
                    cum_reward_return = 1 + item["period_yield"]
                if cum_baseReward_return:
                    cum_baseReward_return *= 1 + (
                        item["base_rewards_usd"] / tvl if tvl else 0
                    )
                else:
                    cum_baseReward_return = 1 + (
                        item["base_rewards_usd"] / tvl if tvl else 0
                    )
                if cum_boostedReward_return:
                    cum_boostedReward_return *= 1 + (
                        item["boosted_rewards_usd"] / tvl if tvl else 0
                    )
                else:
                    cum_boostedReward_return = 1 + (
                        item["boosted_rewards_usd"] / tvl if tvl else 0
                    )

                # extrapolate rewards to a year

                item["base_rewards_usd_year"] = (
                    item["base_rewards_usd"] / item["time_passed"]
                ) * year_in_seconds
                item["boosted_rewards_usd_year"] = (
                    item["boosted_rewards_usd"] / item["time_passed"]
                ) * year_in_seconds
                item["total_rewards_usd_year"] = (
                    item["base_rewards_usd_year"] + item["boosted_rewards_usd_year"]
                )

                item["total_reward_apr"] = (cum_reward_return - 1) * (
                    (year_in_seconds) / item["time_passed"]
                )
                try:
                    item["total_reward_apy"] = (
                        1
                        + (cum_reward_return - 1)
                        * ((day_in_seconds) / item["time_passed"])
                    ) ** 365 - 1
                except OverflowError as e:
                    logging.getLogger(__name__).debug(
                        f"  cant calc apy Overflow err on  total_reward_apy...{e}"
                    )
                    item["total_reward_apy"] = 0

                item["base_reward_apr"] = (cum_baseReward_return - 1) * (
                    (year_in_seconds) / item["time_passed"]
                )
                try:
                    item["base_reward_apy"] = (
                        1
                        + (cum_baseReward_return - 1)
                        * ((day_in_seconds) / item["time_passed"])
                    ) ** 365 - 1
                except OverflowError as e:
                    logging.getLogger(__name__).debug(
                        f"  cant calc apy Overflow err on  base_reward_apy...{e}"
                    )
                    item["base_reward_apy"] = 0

                item["boosted_reward_apr"] = (cum_boostedReward_return - 1) * (
                    (year_in_seconds) / item["time_passed"]
                )
                try:
                    item["boosted_reward_apy"] = (
                        1
                        + (cum_boostedReward_return - 1)
                        * ((day_in_seconds) / item["time_passed"])
                    ) ** 365 - 1
                except OverflowError as e:
                    logging.getLogger(__name__).debug(
                        f"  cant calc apy Overflow err on  boosted_reward_apy...{e}"
                    )
                    item["boosted_reward_apy"] = 0

                total_period_seconds += item["time_passed"]

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
                "token0_price_usd": hype_token0_price,
                "token1_price_usd": hype_token1_price,
                "hypervisor_share_price_usd": hypervisor_share_price_usd,
                # extra fields
                "extra": {
                    "baseRewards_apr": baseRewards_apr if baseRewards_apr > 0 else 0,
                    "baseRewards_apy": baseRewards_apy if baseRewards_apy > 0 else 0,
                    "boostedRewards_apr": boostRewards_apr
                    if boostRewards_apr > 0
                    else 0,
                    "boostedRewards_apy": boostRewards_apy
                    if boostRewards_apy > 0
                    else 0,
                },
            }

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error while calculating rewards yield for ramses hypervisor {hypervisor_address} reward token {rewardToken_address} at block {block} err: {e}"
            )

        return reward_data
