from dataclasses import dataclass
from datetime import datetime, timezone
import logging

from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import get_price_from_db
from bins.general.enums import Period


# calculate APR for ---> periods stablished by operations
""" {
    "address": "0x000000000
    "symbol": "ETH-WETH",                               
    "block": 12345678,
    "timestamp": 12345678,
    "period_seconds": 00000,                     --->  period in seconds between this and last return operation
    "fees_yield: 0.00000000000000,               --->  fees yield using the defined period between current block and last block ( defined in operation and operation-1 blocks )
    "rewards_yield": 0.00000000000000,           --->  rewards yield using the defined period between current block and last block ( defined in operation and operation-1 blocks )

    "total_supply": 0.00000000000000,            --->  total supply at the end of the block
    "underlying_token0_perShare"                 --->  underlying token0 per share ( including uncollected fees) at the end of the block
    "underlying_token1_perShare"                 
    "price_token0": 0.00000000000000,             --->  price of token0 at the end of the block
    "price_token1": 0.00000000000000,            

    "control": {
        "total_deposit_token0_qtty":            --->  total deposited qtty of token0 aggregated from all operations
        "total_deposit_token1_qtty": 
        "total_deposit_value_usd":              --->  total deposited value in usd aggregated from all operations the time it happened
        "total_withdraw_token0_qtty":           --->  total withdrawn qtty of token0 aggregated from all operations
        "total_withdraw_token1_qtty": 
        "total_withdraw_value_usd":             --->  total withdrawn value in usd aggregated from all operations the time it happened
    },
    
    on-the-fly calculations :

                                                            return operations list = all return operations between blocks or timestamps

        "total_pnl" : 0.00000000000000,             --->  total pnl aggregated from all operations
                                                            ----------------------------------------------------------
                                                            total_withdraw_value_usd - 
                                                            total_deposit_value_usd + 
                                                            (total_supply * underlying_token0_perShare * price_token0) + (total_supply * underlying_token1_perShare * price_token1)

        "fees_apr": 0.00000000000000,               --->  fees APR using the defined period 
                                                            ----------------------------------------------------------
                                                            cum_fee_return = prod([1 + fees_yield for all return operations list])
                                                            total_period_seconds = sum all period_seconds fields from return operation list 
                                                            day_in_seconds = Decimal("86400")  |   year_in_seconds = Decimal("365") * day_in_seconds
                                                            fees_apr = (cum_fee_return - 1) * (year_in_seconds / total_period_seconds))
        "fees_apy": 0.00000000000000,                       fees_apy = (1 + (cum_fee_return - 1) * (day_in_seconds / total_period_seconds)) ** 365 - 1
    
        "rewards_apr": 0.00000000000000,            --->  rewards APR using the defined period 
                                                            ----------------------------------------------------------
        

        "lping": 0.00000000000000,                  --->  end-initial  LP value -> Gain/loss from staying in vault, denominated in USD
        "hold_deposited": 0.00000000000000,         --->  deposited qtty value difference between ini&end  ( token0_qtty*token0_price + token1_qtty*token1_price at ini&end )
        "hold_fifty": 0.00000000000000,             --->  50% of deposited value in usd converted to token0 & 50% for token1 as qtty ( token0_qtty*token0_price + token1_qtty*token1_price at ini&end )
        "hold_token0": 0.00000000000000,            --->  100% of deposited value in usd converted to token0 qtty value difference between ini&end
        "hold_token1": 0.00000000000000,            --->  100% of deposited value in usd converted to token1 qtty value difference between ini&end
    
        "fee_result": 0.00000000000000,             --->  fees aquired during the period
        "rewards_result": 0.00000000000000,         --->  rewards aquired during the period
        "market_hedge_result": 0.00000000000000,    --->  (end price_token0 - initial price_token0)* initial(underlying_token0_perShare*total_supply) + (end price_token1 - initial price_token1) * initial(underlying_token1_perShare*total_supply)
        "asset_rebalance_result": 0.00000000000000,     --->  (end underlying_token0_perShare - initial underlying_token0_perShare )*total_supply*initial price_token0 + (end underlying_token1_perShare - initial underlying_token1_perShare )*total_supply*initial price_token1
        
        "gamma_vs_hold": 0.00000000000000,          --->  ( (net_apr+1) / (hold_deposited+1) ) - 1
} """

# return period -> 1day 7days 14days 30days 60days 90days 180days 365days
# return points -> 24h   7p     14p    30p    60p    90p    180p    365p

# 1day apr = +24h apr   -> number of points will be defined by the number of operations in the last 24h
# 7days apr = +7*24h apr


def feed_returns(
    network: str | None = None,
    hypervisors: list[dict] | None = None,
    timestamp_ini: int = None,
    timestamp_end: int = None,
    block_ini: int = None,
    block_end: int = None,
):
    logging.getLogger(__name__).info(f">Feeding {network} returns information")

    # set local database name and create manager
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_gamma"
    local_db = database_local(mongo_url=mongo_url, db_name=db_name)
    batch_size = 50000

    # get static hypervisor information
    for hypervisor in hypervisors or local_db.get_items_from_database(
        collection_name="static", find={}
    ):
        # create control vars
        last_item = None
        # get all hype status changes from creation date
        for idx, data in enumerate(
            local_db.get_items_from_database(
                collection_name="operations",
                aggregate=local_db.query_locs_apr_hypervisor_data_calculation(
                    hypervisor_address=hypervisor["address"],
                    timestamp_ini=timestamp_ini,
                    timestamp_end=timestamp_end,
                    block_ini=block_ini,
                    block_end=block_end,
                ),
                batch_size=batch_size,
            )
        ):
            if not last_item:
                # this is the first item
                last_item = data
                continue

            # zero and par indexes refer to initial values
            if idx == 0 or idx % 2 == 0:
                # this is an initial value
                pass
            else:
                # this is an end value
                yield_data = period_yield_data()

                # get rewards corresponding to hypervisors blocks
                if rewards := local_db.get_items_from_database(
                    collection_name="rewards_status",
                    find={
                        "block": {"$in": [last_item["block"], data["block"]]},
                        "hypervisor_address": hypervisor["address"],
                    },
                    batch_size=batch_size,
                ):
                    # group by block
                    rewards = {last_item["block"]: [], data["block"]: []}
                    for reward in rewards:
                        rewards[reward["block"]].append(reward)

                    yield_data.fill_from_rewards_data(
                        ini_rewards=rewards[last_item["block"]],
                        end_rewards=rewards[data["block"]],
                    )

                data = create_hypervisor_period_percentage_yield(
                    network=network, ini_hype=last_item, end_hype=data
                )

                # TODO: add rewards data
                # create_reward_period_percentage_yield( )

                # create id
                data["id"] = f"{data['address']}_{data['block']}"

                # save to database
                local_db.save_item_to_database(
                    collection_name="returns",
                    data=data,
                )

            # set lastitem
            last_item = data


@dataclass
class period_yield_data:
    address: str
    timestamp: int
    block: int

    period_blocks_qtty: int
    period_seconds: int

    ini_tvl_token0: float
    ini_tvl_token1: float
    # ini_tvl_usd: float

    end_tvl_token0: float
    end_tvl_token1: float
    # end_tvl_usd: float

    ini_hypervisor_supply: float
    end_hypervisor_supply: float

    period_fees_usd: float  # fees aquired during the period ( LPing ) using uncollected fees
    period_rewards_usd: float  # rewards aquired during the period

    period_fees_percentage_yield: float  # collected fees during period / initial tvl
    period_rewards_percentage_yield: float

    token0_price_ini: float
    token0_price_end: float
    token1_price_ini: float
    token1_price_end: float

    @property
    def period_days(self) -> float:
        return self.period_seconds / (24 * 60 * 60)

    @property
    def ini_tvl_usd(self) -> float:
        return (
            self.ini_tvl_token0 * self.token0_price_ini
            + self.ini_tvl_token1 * self.token0_price_end
        )

    @property
    def end_tvl_usd(self) -> float:
        return (
            self.end_tvl_token0 * self.token0_price_end
            + self.end_tvl_token1 * self.token1_price_end
        )

    def fill_from_rewards_data(self, ini_rewards: list[dict], end_rewards: list[dict]):
        """_summary_

        Args:
            ini_rewards (list[dict]): Should have same block and timestamp
            end_rewards (list[dict]): Should have same block and timestamp
        """

        # careful on change
        hypervisor_decimals = 18

        # seconds passed
        period_seconds = end_rewards[0]["timestamp"] - ini_rewards[0]["timestamp"]

        ### TODO:  #################################
        ###########################################
        ### TODO:  #################################
        ### TODO:  #################################
        ### TODO:  #################################
        ### TODO:  #################################
        ### TODO:  #################################
        ### TODO:  #################################
        ### TODO:  #################################
        ### TODO:  #################################
        rewards_perSecond = sum([x["rewards_perSecond"] for x in ini_rewards]) + sum(
            [x["rewards_perSecond"] for x in end_rewards]
        )
        for reward in ini_rewards:
            pass

        # average period rewards perSecond
        average_period_rewards_perSecond = (
            (end_reward["rewards_perSecond"] + ini_reward["rewards_perSecond"]) / 2
        ) / (10 ** end_reward["rewardToken_decimals"])

        # total rewards on this period
        total_rewards_usd = (
            period_seconds
            * average_period_rewards_perSecond
            * end_reward["rewardToken_price_usd"]
        )

        # set variables
        self.address = end_reward["address"]
        self.timestamp = end_reward["timestamp"]
        self.block = end_reward["block"]

        self.period_blocks_qtty = end_reward["block"] - ini_reward["block"]
        self.period_seconds = period_seconds

        self.ini_hypervisor_supply = ini_reward["total_hypervisorToken_qtty"] / (
            10**hypervisor_decimals
        )
        self.end_hypervisor_supply = end_reward["total_hypervisorToken_qtty"] / (
            10**hypervisor_decimals
        )

        self.ini_tvl_usd = (
            ini_reward["hypervisor_share_price_usd"] * self.ini_hypervisor_supply
        )
        self.end_tvl_usd = (
            end_reward["hypervisor_share_price_usd"] * self.end_hypervisor_supply
        )

        self.period_total_rewards_usd = total_rewards_usd

        self.token0_price_ini = ini_reward["token0_price_usd"]
        self.token1_price_ini = ini_reward["token1_price_usd"]
        self.token0_price_end = end_reward["token0_price_usd"]
        self.token1_price_end = end_reward["token1_price_usd"]

    def fill_from_hypervisors_data(
        self, ini_hype: dict, end_hype: dict, network: str | None = None
    ):
        if not network and (
            not self.token0_price_ini
            or not self.token1_price_ini
            or not self.token0_price_end
            or not self.token1_price_end
        ):
            raise Exception(
                " Either network or a previous fill_from_rewards_data call is required to calculate prices"
            )
        elif network and not self.token0_price_ini:
            # get token prices at ini and end blocks from database
            self.token0_price_ini = get_price_from_db(
                network=network,
                block=ini_hype["block"],
                token=ini_hype["pool"]["token0"]["address"],
            )

            self.token0_price_end = (
                get_price_from_db(
                    network=network,
                    block=end_hype["block"],
                    token=end_hype["pool"]["token0"]["address"],
                ),
            )

            self.token1_price_ini = get_price_from_db(
                network=network,
                block=ini_hype["block"],
                token=ini_hype["pool"]["token1"]["address"],
            )

            self.token1_price_end = get_price_from_db(
                network=network,
                block=end_hype["block"],
                token=end_hype["pool"]["token1"]["address"],
            )

            # initial tvl
            self.ini_tvl_token0 = ini_hype["totalAmounts"]["token0"] / (
                10 ** ini_hype["pool"]["token0"]["decimals"]
            )
            self.ini_tvl_token1 = ini_hype["totalAmounts"]["token1"] / (
                10 ** ini_hype["pool"]["token1"]["decimals"]
            )

            # end tvl can differ from ini tvl when asset prices or weights change
            self.end_tvl_token0 = end_hype["totalAmounts"]["token0"] / (
                10 ** end_hype["pool"]["token0"]["decimals"]
            )
            self.end_tvl_token1 = end_hype["totalAmounts"]["token1"] / (
                10 ** end_hype["pool"]["token1"]["decimals"]
            )

        # calculate the fees uncollected on this period
        fees_uncollected_token0 = int(
            end_hype["fees_uncollected"]["qtty_token0"]
        ) - int(ini_hype["fees_uncollected"]["qtty_token0"])
        fees_uncollected_token1 = int(
            end_hype["fees_uncollected"]["qtty_token1"]
        ) - int(ini_hype["fees_uncollected"]["qtty_token1"])
        fees_uncollected_usd = (
            fees_uncollected_token0 * self.token0_price_end
            + fees_uncollected_token1 * self.token1_price_end
        )

        # Yield percentage: percentage of total value staked at ini
        self.period_fees_percentage_yield = (
            (fees_uncollected_usd / self.ini_tvl_usd) if self.ini_tvl_usd else 0
        )

        self.period_fees_usd = fees_uncollected_usd
