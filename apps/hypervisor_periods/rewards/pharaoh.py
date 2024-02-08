from datetime import datetime
import logging
from apps.feeds.utils import get_hypervisor_price_per_share, get_reward_pool_prices
from apps.hypervisor_periods.base import hypervisor_periods_base
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol, rewarderType
from bins.w3.builders import (
    build_db_hypervisor,
    build_erc20_helper,
    build_db_hypervisor_multicall,
)
from bins.w3.protocols.pharaoh.hypervisor import gamma_hypervisor as pharaoh_hypervisor


class hypervisor_periods_pharaoh(hypervisor_periods_base):
    def __init__(self, chain: Chain, hypervisor_status: dict, rewarder_static: dict):
        """Creates rewards data for the specified hypervisor status

        Args:
            chain (Chain):
            hypervisor_status (dict): hypervisor status ( database structure )
            rewarder_static (dict): rewarder static ( database structure )
        """
        # set main vars
        self.chain = chain
        self.hypervisor_status = hypervisor_status
        self.rewarder_static = rewarder_static

        self.type = rewarderType.PHARAOH

        super().__init__()

    def reset(self):
        # reset all vars
        self.total_baseRewards = 0
        self.total_boostedRewards = 0
        self.total_time_passed = 0
        self.items_to_calc_apr = []

        self.totalStaked = 0
        self.current_period = 0

        super().reset()

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
        # add staked amount to current item

        # create hypervisor
        _hypervisor_w3 = pharaoh_hypervisor(
            address=hypervisor_address,
            network=chain.database_name,
            block=current_item["block"],
        )
        # add totalStaked to item to calc apr
        current_item["totalStaked"] = _hypervisor_w3.receiver.totalStakes

    def _execute_inLoop_endItem(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        # calculate time passed since last item
        time_passed = current_item["timestamp"] - last_item["timestamp"]

        # get rewards for this period using the hype's position
        if rewards_status_qtty := self._calculations(
            hypervisor_address=current_item["address"],
            network=chain.database_name,
            block=current_item["block"],
            rewardToken_address=self.rewarder_static["rewardToken"],
            time_passed=time_passed,
        ):
            # add to items to calc apr: transform to float
            self.items_to_calc_apr.append(
                {
                    "base_rewards": rewards_status_qtty["base_rewards"]
                    / (10 ** self.rewarder_static["rewardToken_decimals"]),
                    "boosted_rewards": rewards_status_qtty["boosted_rewards"]
                    / (10 ** self.rewarder_static["rewardToken_decimals"]),
                    "time_passed": time_passed,
                    "timestamp_ini": last_item["timestamp"],
                    "timestamp_end": current_item["timestamp"],
                    "hypervisor": {
                        "block": last_item["block"],
                        "totalStaked": last_item["totalStaked"]
                        / (10 ** last_item["decimals"]),
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
                }
            )

            # define total staked from last item
            self.totalStaked = last_item["totalStaked"]

            # add totals
            self.total_baseRewards += rewards_status_qtty["base_rewards"]
            self.total_boostedRewards += rewards_status_qtty["boosted_rewards"]
            self.total_time_passed += time_passed

    def _execute_postLoop(self, hypervisor_data: dict):
        if not self.items_to_calc_apr:
            # convert timestamp to datetime for logging
            try:
                _datetimelog = datetime.utcfromtimestamp(
                    self.hypervisor_status["timestamp"]
                )
            except Exception as e:
                _datetimelog = self.hypervisor_status["timestamp"]

            logging.getLogger(__name__).error(
                f" No items to calculate apr found for {self.chain.fantasy_name}'s {self.type} rewards hypervisor {self.hypervisor_status['address']} at block {self.hypervisor_status['block']}  [{_datetimelog}]"
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
        if reward_apr_data_to_save := self._calculate_apr(
            hypervisor_address=self.hypervisor_status["address"],
            network=self.chain.database_name,
            block=self.hypervisor_status["block"],
            rewardToken_address=self.rewarder_static["rewardToken"],
            token0_address=self.hypervisor_status["pool"]["token0"]["address"],
            token1_address=self.hypervisor_status["pool"]["token1"]["address"],
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
                "dex": Protocol.PHARAOH.database_name,
                "rewarder_address": self.rewarder_address,
                "rewarder_type": rewarderType.PHARAOH,
                "rewarder_refIds": [],
                "rewarder_registry": self.rewarder_registry,
                "rewardToken": self.rewarder_static["rewardToken"].lower(),
                "rewardToken_symbol": self.rewarder_static["rewardToken_symbol"],
                "rewardToken_decimals": self.rewarder_static["rewardToken_decimals"],
                "rewardToken_price_usd": reward_apr_data_to_save[
                    "rewardToken_price_usd"
                ],
                "token0_price_usd": reward_apr_data_to_save["token0_price_usd"],
                "token1_price_usd": reward_apr_data_to_save["token1_price_usd"],
                "rewards_perSecond": str(totalRewards_per_second),
                "total_hypervisorToken_qtty": str(self.totalStaked),
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
        self,
        chain: Chain,
        hypervisor_address: str,
        block: int,
        protocol: Protocol,
        hypervisor_status: dict | None = None,
    ) -> dict:
        return None
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
        # initialize period and stake data neede to beguin
        self._init_fill()

        logging.getLogger(__name__).debug(
            f" Getting rewards of {self.chain.database_name} {self.hypervisor_status['address']} timestamp_ini: {timestamp_ini or self.ini_period_timestamp} timestamp_end: {timestamp_end}  block_ini: {block_ini}  block_end: {block_end or int(self.hypervisor_status['block'])}"
        )
        # execute processes
        return super().execute_processes_within_hypervisor_periods(
            chain=self.chain,
            hypervisor_address=self.hypervisor_status["address"],
            timestamp_ini=timestamp_ini or self.get_periods_back_timestamp(1),
            timestamp_end=timestamp_end,
            block_ini=block_ini,
            block_end=block_end or int(self.hypervisor_status["block"]),
            try_solve_errors=try_solve_errors,
            only_use_last_items=6,
        )

    # PROPS
    @property
    def ini_period_timestamp(self) -> int:
        # period number
        period_ini = self.current_period
        # period timestamp
        return period_ini * 60 * 60 * 24 * 7

    @property
    def end_period_timestamp(self) -> int:
        # period timestamp
        return ((self.current_period + 1) * 60 * 60 * 24 * 7) - 1

    def get_periods_back_timestamp(self, periods_back: int) -> int:
        # period number
        period_ini = self.current_period - periods_back
        # period timestamp
        return period_ini * 60 * 60 * 24 * 7

    # HELPERS
    def _init_fill(self):
        # check if hypervisor has supply
        if not self.hypervisor_status["totalSupply"]:
            logging.getLogger(__name__).debug(
                f"Can't calculate rewards status for pharaoh hype {self.hypervisor_status['symbol']} {self.hypervisor_status['address']} because it has no supply at block {self.hypervisor_status['block']}"
            )
            return []

        # create hypervisor
        hypervisor_status_w3 = pharaoh_hypervisor(
            address=self.hypervisor_status["address"],
            network=self.chain.database_name,
            block=self.hypervisor_status["block"],
        )

        # current staked amount
        # self.totalStaked = hypervisor_status_w3.receiver.totalStakes
        # period timeframe
        self.current_period = hypervisor_status_w3.current_period
        self.rewarder_address = hypervisor_status_w3.gauge.address.lower()
        self.rewarder_registry = hypervisor_status_w3.receiver.address.lower()

    def _calculations(
        self,
        hypervisor_address: str,
        network: str,
        block: int,
        rewardToken_address: str,
        time_passed: int,
    ) -> dict:
        # get rewards for this period using the hype's position

        # create hypervisor
        _temp_hype_status = pharaoh_hypervisor(
            address=hypervisor_address,
            network=network,
            block=block,
        )
        try:
            # calculate rewards for this position at period
            _temp_real_rewards = _temp_hype_status.calculate_rewards(
                period=_temp_hype_status.current_period,
                reward_token=rewardToken_address,
            )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Cant calculate rewards for hype {hypervisor_address}  rewardToken {rewardToken_address}. Returning zero -> {e}"
            )
            return {
                "base_rewards": 0,
                "boosted_rewards": 0,
                "total_staked": 0,
            }

        # calculate rewards per second for this position and perood
        baseRewards_per_second = (
            _temp_real_rewards["current_baseRewards"]
            / _temp_real_rewards["current_period_seconds"]
        )
        boostRewards_per_second = (
            _temp_real_rewards["current_boostedRewards"]
            / _temp_real_rewards["current_period_seconds"]
        )

        # current staked
        totalStaked = _temp_hype_status.receiver.totalStakes

        return {
            "base_rewards": (baseRewards_per_second * time_passed),
            "boosted_rewards": (boostRewards_per_second * time_passed),
            "total_staked": totalStaked,
        }

    def _calculate_apr(
        self,
        hypervisor_address: str,
        network: str,
        block: int,
        rewardToken_address: str,
        token0_address: str,
        token1_address: str,
        items_to_calc_apr: list,
    ) -> dict:
        """_summary_

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
                                "hypervisor": {
                                    "totalStaked
                                    "totalSupply":
                                    "underlying_token0":
                                    "underlying_token1":
                                },
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
            # get prices
            (
                rewardToken_price,
                hype_token0_price,
                hype_token1_price,
            ) = get_reward_pool_prices(
                network=network,
                block=block,
                reward_token=rewardToken_address.lower(),
                token0=token0_address.lower(),
                token1=token1_address.lower(),
            )

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
                item["hypervisor"]["tvl"] = (
                    item["hypervisor"]["underlying_token0"] * hype_token0_price
                    + item["hypervisor"]["underlying_token1"] * hype_token1_price
                )
                item["hypervisor"]["price_per_share"] = (
                    item["hypervisor"]["tvl"] / item["hypervisor"]["totalSupply"]
                )
                # calculate how much staked is worth in usd
                item["hypervisor"]["totalStaked_tvl"] = (
                    item["hypervisor"]["totalStaked"]
                    * item["hypervisor"]["price_per_share"]
                )

                # set the TVL value to be used as denominator: totalStaked or totalSupply
                tvl = item["hypervisor"]["totalStaked_tvl"]
                if not tvl:
                    logging.getLogger(__name__).warning(
                        f" using total supply to calc pharaoh reward apr because there is no staked value for {network} {hypervisor_address} using item {item}"
                    )
                    tvl = item["hypervisor"]["tvl"]

                # outlier filter
                if item["hypervisor"]["totalStaked_tvl"] > 10**11:
                    logging.getLogger(__name__).error(
                        f" found outlier staked value {item['hypervisor']['totalStaked_tvl']} for {network} {hypervisor_address} using item {item}"
                    )
                if item["hypervisor"]["tvl"] > 10**12:
                    raise ValueError(
                        f" found outlier tvl value {item['hypervisor']['tvl']} for {network} {hypervisor_address} using item {item}"
                    )

                # set price per share var ( the last will remain)
                hypervisor_share_price_usd = item["hypervisor"]["price_per_share"]

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
                        f" found outlier period yield {item['period_yield']} for {network} {hypervisor_address} using item {item}"
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
                "hypervisor_tvl_usd": tvl,
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
                f" Error while calculating rewards yield for pharaoh hypervisor {hypervisor_address} reward token {rewardToken_address} at block {block} err: {e}"
            )

        return reward_data
