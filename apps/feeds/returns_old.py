import copy
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import logging
import time
from apps.feeds.operations import feed_operations

from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_hypervisor_returns
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import (
    get_default_globaldb,
    get_default_localdb,
    get_from_localdb,
    get_price_from_db,
)
from bins.errors.general import ProcessingError
from bins.general.enums import Chain

# from bins.general.enums import Period


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


@dataclass
class time_location:
    timestamp: int = None
    block: int = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "block": self.block,
        }


@dataclass
class period_yield_data:
    address: str = None

    ini_period: time_location = None
    end_period: time_location = None

    ini_underlying_token0: float = None
    ini_underlying_token1: float = None

    end_underlying_token0: float = None
    end_underlying_token1: float = None

    ini_hypervisor_supply: float = None
    end_hypervisor_supply: float = None

    period_fees_token0: float = None  # fees growth
    period_fees_token1: float = None

    period_rewards_usd: float = None  # rewards aquired during the period

    period_fees_percentage_yield: float = (
        None  # collected fees during period / initial tvl
    )
    period_rewards_percentage_yield: float = None

    token0_price_ini: float = None
    token0_price_end: float = None
    token1_price_ini: float = None
    token1_price_end: float = None

    rewards_raw_data: list[dict] = None

    # testing variables
    # fees collected at the start of the period ( fees from a previous period )
    lastperiod_fees_token0_collected: float = None
    lastperiod_fees_token1_collected: float = None

    @property
    def id(self) -> str:
        return create_id_hypervisor_returns(
            hypervisor_address=self.address,
            ini_block=self.ini_period.block,
            end_block=self.end_period.block,
        )

    @property
    def period_blocks_qtty(self) -> int:
        return self.end_period.block - self.ini_period.block

    @property
    def period_seconds(self) -> int:
        return self.end_period.timestamp - self.ini_period.timestamp

    @property
    def period_days(self) -> float:
        return self.period_seconds / (24 * 60 * 60)

    @property
    def ini_underlying_usd(self) -> float:
        return (
            (
                self.ini_underlying_token0 * self.token0_price_ini
                + self.ini_underlying_token1 * self.token1_price_ini
            )
            if self.ini_underlying_token0 and self.ini_underlying_token1
            else 0
        )

    @property
    def end_underlying_usd(self) -> float:
        return (
            (
                self.end_underlying_token0 * self.token0_price_end
                + self.end_underlying_token1 * self.token1_price_end
            )
            if self.end_underlying_token0 and self.end_underlying_token1
            else 0
        )

    @property
    def period_fees_usd(self) -> float:
        """fees aquired during the period ( LPing ) using uncollected fees
            (using end prices)

        Returns:
            float:
        """
        if self.period_fees_token0 and self.period_fees_token1:
            return (
                self.period_fees_token0 * self.token0_price_end
                + self.period_fees_token1 * self.token1_price_end
            )
        return 0

    @property
    def period_impermanent_usd(self) -> float:
        """Impermanent divergence represents the value change in market prices and pool token weights

        Returns:
            float:
        """
        return self.end_underlying_usd - (
            self.ini_underlying_usd + self.period_fees_usd
        )

    @property
    def period_impermanent_token0(self) -> float:
        """Impermanent divergence represents the value change in market prices and pool token weights

        Returns:
            float:
        """
        if (
            self.ini_underlying_token0
            and self.period_fees_token0
            and self.end_underlying_token0
        ):
            return self.end_underlying_token0 - (
                self.ini_underlying_token0 + self.period_fees_token0
            )
        return 0

    @property
    def period_impermanent_token1(self) -> float:
        """Impermanent divergence represents the value change in market prices and pool token weights

        Returns:
            float:
        """
        if (
            self.ini_underlying_token1
            and self.period_fees_token1
            and self.end_underlying_token1
        ):
            return self.end_underlying_token1 - (
                self.ini_underlying_token1 + self.period_fees_token1
            )
        return 0

    @property
    def period_impermanent_percentage_yield(self) -> float:
        """Impermanent divergence represents the value change in market prices and pool token weights

        Returns:
            float: _description_
        """
        return (
            self.period_impermanent_usd / self.ini_underlying_usd
            if self.ini_underlying_usd
            else 0
        )

    def fill_from_rewards_data(self, ini_rewards: list[dict], end_rewards: list[dict]):
        """fill period rewards data from rewards data

        Args:
            ini_rewards (list[dict]): Should have same block and timestamp
            end_rewards (list[dict]): Should have same block and timestamp
        """

        if not ini_rewards:
            logging.getLogger(__name__).debug(f" No initial rewards to process.")
            return
        if not end_rewards:
            logging.getLogger(__name__).debug(f" No end rewards to process.")
            return

        # group ini and end rewards by rewardToken + rewarder_address
        # can happen that no ini rewards are found but end only...
        grouped_rewards = {}  # {tokenAddress_rewarderAddress: { ini: } }
        for end_reward in end_rewards:
            # create key
            _dictkey = f"{end_reward['rewardToken']}_{end_reward['rewarder_address']}"
            # create basic struct
            grouped_rewards[_dictkey] = {"ini": None, "end": end_reward}
            for ini_reward in ini_rewards:
                if (
                    ini_reward["rewarder_address"] == end_reward["rewarder_address"]
                    and ini_reward["rewardToken"] == end_reward["rewardToken"]
                ):
                    grouped_rewards[_dictkey]["ini"] = ini_reward
                    break

        # create control var for later debug/use
        raw_data = {}
        # process grouped rewards
        for item in grouped_rewards.values():
            if not item["ini"]:
                # no ini rewards found for this item
                logging.getLogger(__name__).error(
                    f" No initial rewards found for {item}. Skiping"
                )
                continue

            if not item["end"]:
                # no end rewards found for this item
                raise ValueError(f" No end rewards found for {item}")

            # seconds passed
            period_seconds = item["end"]["timestamp"] - item["ini"]["timestamp"]

            # calculate rewards qtty
            _period_rewards_qtty = 0
            # rewardsPerSecond can differ between ini and end. Check if we have absolute values
            if "extra" in item["ini"] and "extra" in item["end"]:
                # get absolute rewards from ini to end
                _ini_rewards_qtty = (
                    float(item["ini"]["extra"]["baseRewards"])
                    / (10 ** item["ini"]["rewardToken_decimals"])
                ) + (
                    float(item["ini"]["extra"]["boostedRewards"])
                    / (10 ** item["ini"]["rewardToken_decimals"])
                )
                _end_rewards_qtty = (
                    float(item["end"]["extra"]["baseRewards"])
                    / (10 ** item["end"]["rewardToken_decimals"])
                ) + (
                    float(item["end"]["extra"]["boostedRewards"])
                    / (10 ** item["end"]["rewardToken_decimals"])
                )
                _period_rewards_qtty = _end_rewards_qtty - _ini_rewards_qtty
            else:
                logging.getLogger(__name__).warning(
                    f" No absolute rewards found for hype {item['end']['hypervisor_address']} reward status id: {item['end']['id']}. Use non accurate rewardsPerSecond."
                )
                # use last rewards per second to calculate rewards qtty
                _period_rewards_qtty = (
                    float(item["end"]["rewards_perSecond"])
                    / (10 ** item["end"]["rewardToken_decimals"])
                ) * period_seconds

            # add to control var
            if item["end"]["rewardToken"] not in raw_data:
                # add to traceable data
                raw_data[item["end"]["rewardToken"]] = {
                    "period_qtty": _period_rewards_qtty,
                    "period_usd": _period_rewards_qtty
                    * item["end"]["rewardToken_price_usd"],
                }

            # fill self total rewards usd
            if not self.period_rewards_usd:
                self.period_rewards_usd = Decimal("0")
            self.period_rewards_usd += Decimal(
                str(raw_data[item["end"]["rewardToken"]]["period_usd"])
            )

        # calculate total period percentage yield
        self.period_rewards_percentage_yield = (
            self.period_rewards_usd / self.ini_underlying_usd
            if self.ini_underlying_usd
            else 0
        )

        # add raw data to self
        self.rewards_raw_data = raw_data

    def fill_from_hypervisors_data(
        self,
        ini_hype: dict,
        end_hype: dict,
        network: str | None = None,
    ):
        """fill period data from hypervisors data

        Args:
            ini_hype (dict):
            end_hype (dict):
            network (str | None, optional): . Defaults to None.
        """

        # fill basics
        if not self.address:
            self.address = end_hype["address"]
        if not self.ini_period:
            self.ini_period = time_location(
                timestamp=ini_hype["timestamp"], block=ini_hype["block"]
            )
        else:
            po = "po"
        if not self.end_period:
            self.end_period = time_location(
                timestamp=end_hype["timestamp"], block=end_hype["block"]
            )
        else:
            po = "po"

        # supply
        self.ini_hypervisor_supply = Decimal(ini_hype["totalSupply"]) / (
            10 ** ini_hype["decimals"]
        )
        self.end_hypervisor_supply = Decimal(end_hype["totalSupply"]) / (
            10 ** end_hype["decimals"]
        )
        # check if supply at ini and end is the same
        supply_diff = self.end_hypervisor_supply - self.ini_hypervisor_supply
        if abs(supply_diff) > Decimal("0.000000999"):
            # do not process already indentified errors
            if network == Chain.ETHEREUM.database_name and self.address in [
                "0xf0a9f5c64f80fa390a46b298791dab9e2bb29bca"
            ]:
                return

            raise ProcessingError(
                item={
                    "hypervisor_address": self.address,
                    "ini_block": self.ini_period.block,
                    "end_block": self.end_period.block,
                    "supply_difference": supply_diff,
                },
                action="rescrape",
                message=f" Hypervisor supply at START differ {supply_diff:,.10%} from END, meaning there are missing operations in between. Rescrape.",
            )

        if not network and (
            not self.token0_price_ini
            or not self.token1_price_ini
            or not self.token0_price_end
            or not self.token1_price_end
        ):
            raise Exception(
                " Either network or a previous fill_from_rewards_data call is required to calculate prices"
            )

        if network and not self.token0_price_ini:
            # get token prices at ini and end blocks from database
            self.token0_price_ini = Decimal(
                str(
                    get_price_from_db(
                        network=network,
                        block=ini_hype["block"],
                        token_address=ini_hype["pool"]["token0"]["address"],
                    )
                )
            )
        if network and not self.token0_price_end:
            self.token0_price_end = Decimal(
                str(
                    get_price_from_db(
                        network=network,
                        block=end_hype["block"],
                        token_address=end_hype["pool"]["token0"]["address"],
                    )
                )
            )
        if network and not self.token1_price_ini:
            self.token1_price_ini = Decimal(
                str(
                    get_price_from_db(
                        network=network,
                        block=ini_hype["block"],
                        token_address=ini_hype["pool"]["token1"]["address"],
                    )
                )
            )
        if network and not self.token1_price_end:
            self.token1_price_end = Decimal(
                str(
                    get_price_from_db(
                        network=network,
                        block=end_hype["block"],
                        token_address=end_hype["pool"]["token1"]["address"],
                    )
                )
            )

        # calculate this period's fee growth
        self.period_fees_token0 = (
            Decimal(end_hype["fees_uncollected"]["qtty_token0"])
            - Decimal(ini_hype["fees_uncollected"]["qtty_token0"])
        ) / (10 ** ini_hype["pool"]["token0"]["decimals"])
        self.period_fees_token1 = (
            Decimal(end_hype["fees_uncollected"]["qtty_token1"])
            - Decimal(ini_hype["fees_uncollected"]["qtty_token1"])
        ) / (10 ** ini_hype["pool"]["token1"]["decimals"])

        # check for positive fee growth
        if self.period_fees_token0 < 0 or self.period_fees_token1 < 0:
            raise ValueError(
                f" Fees growth can't be negative and they are [0:{self.period_fees_token0} 1:{self.period_fees_token1}] for hypervisor {self.address} end block {self.end_block}"
            )

        # get collected fees within the period, if any
        if ini_hype["operations"]:
            for operation in ini_hype["operations"]:
                if operation["topic"] in ["rebalance", "zeroBurn"]:
                    # initializ vars if needed
                    if not self.lastperiod_fees_token0_collected:
                        self.lastperiod_fees_token0_collected = Decimal("0")
                    if not self.lastperiod_fees_token1_collected:
                        self.lastperiod_fees_token1_collected = Decimal("0")

                    # add collected fees to control vars
                    self.lastperiod_fees_token0_collected += Decimal(
                        str(
                            int(operation["qtty_token0"])
                            / (10 ** operation["decimals_token0"])
                        )
                    )
                    self.lastperiod_fees_token1_collected += Decimal(
                        str(
                            int(operation["qtty_token1"])
                            / (10 ** operation["decimals_token1"])
                        )
                    )

        # initial underlying ( including fees uncollected )
        self.ini_underlying_token0 = (
            Decimal(ini_hype["totalAmounts"]["total0"])
            + Decimal(ini_hype["fees_uncollected"]["qtty_token0"])
        ) / (10 ** ini_hype["pool"]["token0"]["decimals"])
        self.ini_underlying_token1 = (
            Decimal(ini_hype["totalAmounts"]["total1"])
            + Decimal(ini_hype["fees_uncollected"]["qtty_token1"])
        ) / (10 ** ini_hype["pool"]["token1"]["decimals"])

        # end underlying can differ from ini tvl when asset prices or weights change
        self.end_underlying_token0 = (
            Decimal(end_hype["totalAmounts"]["total0"])
            + Decimal(end_hype["fees_uncollected"]["qtty_token0"])
        ) / (10 ** end_hype["pool"]["token0"]["decimals"])
        self.end_underlying_token1 = (
            Decimal(end_hype["totalAmounts"]["total1"])
            + Decimal(end_hype["fees_uncollected"]["qtty_token1"])
        ) / (10 ** end_hype["pool"]["token1"]["decimals"])

        # Yield percentage: percentage of total value staked at ini
        self.period_fees_percentage_yield = (
            (self.period_fees_usd / self.ini_underlying_usd)
            if self.ini_underlying_usd
            else 0
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "address": self.address,
            "period": {
                "ini": self.ini_period.to_dict(),
                "end": self.end_period.to_dict(),
                "blocks_qtty": self.period_blocks_qtty,
                "seconds": self.period_seconds,
            },
            "ini_underlying_token0": self.ini_underlying_token0,
            "ini_underlying_token1": self.ini_underlying_token1,
            "end_underlying_token0": self.end_underlying_token0,
            "end_underlying_token1": self.end_underlying_token1,
            "ini_hypervisor_supply": self.ini_hypervisor_supply,
            "end_hypervisor_supply": self.end_hypervisor_supply,
            "period_fees_token0": self.period_fees_token0,
            "period_fees_token1": self.period_fees_token1,
            "period_rewards_usd": self.period_rewards_usd,
            "period_fees_percentage_yield": self.period_fees_percentage_yield,
            "period_rewards_percentage_yield": self.period_rewards_percentage_yield,
            "token0_price_ini": self.token0_price_ini,
            "token0_price_end": self.token0_price_end,
            "token1_price_ini": self.token1_price_ini,
            "token1_price_end": self.token1_price_end,
        }


def feed_hypervisor_returns(
    chain: Chain, hypervisor_addresses: list[str] | None = None
):
    """Feed hypervisor returns from the specified chain and hypervisor addresses

    Args:
        chain (Chain):
        hypervisor_addresses (list[str]): list of hype addresses

    """

    logging.getLogger(__name__).info(
        f">Feeding {chain.database_name} returns information"
    )

    batch_size = 50000

    query = []
    if (
        _match := {"$match": {"address": {"$in": hypervisor_addresses}}}
        if hypervisor_addresses
        else {}
    ):
        query.append(_match)
    query.append({"$group": {"_id": "$address", "end_block": {"$max": "$end_block"}}})

    # get last block_end hypervisor returns for each hype in the specified list
    # query = [
    #     {"$match": {"address": {"$in": hypervisor_addresses}}},
    #     {"$group": {"_id": "$address", "end_block": {"$max": "$end_block"}}},
    # ]
    if hype_block_data := get_from_localdb(
        network=chain.database_name,
        collection="hypervisor_returns",
        aggregate=query,
        batch_size=batch_size,
    ):
        # get query_locs_apr_hypervisor_data_calculation ( block_ini = block_end +1 )
        for item in hype_block_data:
            hype_address = item["_id"]
            hype_ini_block = item["end_block"] + 1

            # create control vars
            save_hypervisor_returns_to_database(
                chain=chain, hypervisor_address=hype_address, block_ini=hype_ini_block
            )
    else:
        logging.getLogger(__name__).info(
            f" No hypervisor returns found in database. Staring from scratch."
        )
        if hypervisor_addresses:
            find = {"address": {"$in": hypervisor_addresses}}
        else:
            find = {}
        # get a list of hypes to feed
        hypervisors_static = get_from_localdb(
            network=chain.database_name,
            collection="static",
            find=find,
            projection={"address": 1, "timestamp": 1, "_id": 0},
        )

        # create chunks of timeframes to feed data so that we don't overload the database
        #
        # get the lowest timestamp from static data
        min_timestamp = min([hype["timestamp"] for hype in hypervisors_static])

        # define highest timestamp
        max_timestamp = int(time.time())

        # define chunk size
        chunk_size = 86400 * 7 * 4  # 4 week

        # create chunks
        chunks = [
            (i, i + chunk_size) for i in range(min_timestamp, max_timestamp, chunk_size)
        ]

        logging.getLogger(__name__).info(
            f" {len(chunks)} chunks created to feed each hypervisor returns data so that the database is not overloaded"
        )

        # get hypervisor returns for each chunk
        for chunk in chunks:
            for item in hypervisors_static:
                logging.getLogger(__name__).info(
                    f" Feeding chunk {chunk[0]} to {chunk[1]} for {chain.database_name}'s {item['address']} hypervisor"
                )
                save_hypervisor_returns_to_database(
                    chain=chain,
                    hypervisor_address=item["address"],
                    timestamp_ini=chunk[0],
                    timestamp_end=chunk[1],
                )


def save_hypervisor_returns_to_database(
    chain: Chain,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
):
    # create hypervisor returns list
    if data := create_hypervisor_returns(
        chain=chain,
        hypervisor_address=hypervisor_address,
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
        block_ini=block_ini,
        block_end=block_end,
    ):
        # save all at once
        if db_return := get_default_localdb(
            network=chain.database_name
        ).set_hypervisor_return_bulk(data=data):
            logging.getLogger(__name__).debug(
                f"     db return-> del: {db_return.deleted_count}  ins: {db_return.inserted_count}  mod: {db_return.modified_count}  ups: {db_return.upserted_count} matched: {db_return.matched_count}"
            )
        else:
            logging.getLogger(__name__).error(
                f"  database did not return anything while saving {hypervisor_address}s returns at { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end}"
            )
    else:
        logging.getLogger(__name__).debug(
            f" No hypervisor {hypervisor_address} data from { 'blocks' if block_ini and block_end else 'timestamps'} {block_ini if block_ini else timestamp_ini} to {block_end if block_end else timestamp_end} to construct returns "
        )


def create_hypervisor_returns(
    chain: Chain,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    convert_to_dict: bool = True,
    convert_to_d128: bool = True,
) -> list[period_yield_data] | list[dict]:
    #
    result = []

    batch_size = 80000

    # create control vars
    last_item = None

    # build query (ease debuging)
    query = get_default_localdb(
        network=chain.database_name
    ).query_locs_apr_hypervisor_data_calculation(
        hypervisor_address=hypervisor_address,
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
        block_ini=block_ini,
        block_end=block_end,
    )
    # get a list of custom ordered hype status
    if ordered_hype_status_list := get_from_localdb(
        network=chain.database_name,
        collection="operations",
        aggregate=query,
        batch_size=batch_size,
    ):
        # get all prices related to this hypervisor for the specified period
        token_addresses = [
            ordered_hype_status_list[0]["status"][0]["pool"]["token0"]["address"],
            ordered_hype_status_list[0]["status"][0]["pool"]["token1"]["address"],
        ]

        # get the max and min blocks from the ordered hype status list
        min_block = min([x["block"] for x in ordered_hype_status_list[0]["status"]])
        max_block = max([x["block"] for x in ordered_hype_status_list[0]["status"]])

        token_prices = {
            f"{x['address']}_{x['block']}": x["price"]
            for x in get_default_globaldb().get_items_from_database(
                collection_name="usd_prices",
                find={
                    "network": chain.database_name,
                    "address": {"$in": token_addresses},
                    "block": {"$gte": min_block, "$lte": max_block},
                },
            )
        }

        for hypervisor_status in ordered_hype_status_list:
            # hypervisor_status["_id"] = hypervisor_address
            for idx, data in enumerate(hypervisor_status["status"]):
                # zero and par indexes refer to initial values
                if idx == 0 or idx % 2 == 0:
                    # this is an initial value
                    pass
                else:
                    # this is an end value

                    # create yield data and fill from hype status
                    current_period = period_yield_data()
                    try:
                        # check if last_item is not 1 block away from current item
                        if last_item and last_item["block"] + 1 == data["block"]:
                            # there are times blocks are consecutive more than 2 items in a row... that's ok. check if next or previous block is consecutive
                            try:
                                if (
                                    hypervisor_status["status"][idx + 1]["block"]
                                    == data["block"] + 1
                                    or hypervisor_status["status"][idx - 1]["block"]
                                    == data["block"] - 1
                                ):
                                    pass
                                else:
                                    raise ValueError(
                                        f" Blocks are consecutive. Last block: {last_item['block']} Current block: {data['block']}"
                                    )
                            except Exception as e:
                                raise ValueError(
                                    f" Blocks are consecutive. Last block: {last_item['block']} Current block: {data['block']}"
                                )

                        # fill usd price
                        if token0 := data["pool"]["token0"]["address"]:
                            try:
                                current_period.token0_price_end = Decimal(
                                    str(token_prices[f"{token0}_{data['block']}"])
                                )
                                current_period.token0_price_ini = Decimal(
                                    str(token_prices[f"{token0}_{last_item['block']}"])
                                )
                            except Exception as e:
                                pass
                        if token1 := data["pool"]["token1"]["address"]:
                            try:
                                current_period.token1_price_end = Decimal(
                                    str(token_prices[f"{token1}_{data['block']}"])
                                )
                                current_period.token1_price_ini = Decimal(
                                    str(token_prices[f"{token1}_{last_item['block']}"])
                                )
                            except Exception as e:
                                pass

                        # fill from hype status
                        try:
                            current_period.fill_from_hypervisors_data(
                                ini_hype=last_item,
                                end_hype=data,
                                network=chain.database_name,
                            )
                        except ProcessingError as e:
                            logging.getLogger(__name__).error(
                                f" Error while creating hype returns. {e.message}"
                            )
                            if e.action == "rescrape":
                                rescrape_block_ini = e.item["ini_block"]  # + 1
                                rescrape_block_end = e.item["end_block"]  # - 1

                                # if e.item["ini_block"] == e.item["end_block"] - 1:
                                #     # only 1 block apart
                                #     rescrape_block_ini = e.item["ini_block"]
                                #     rescrape_block_end = e.item["end_block"]

                                # rescrape operations for this chain between defined blocks
                                logging.getLogger(__name__).info(
                                    f" Rescraping operations for {chain.database_name} between blocks {rescrape_block_ini} and {rescrape_block_end}"
                                )
                                feed_operations(
                                    protocol="gamma",
                                    network=chain.database_name,
                                    block_ini=rescrape_block_ini,
                                    block_end=rescrape_block_end,
                                )

                        # fill rewards
                        current_period.fill_from_rewards_data(
                            ini_rewards=last_item["rewards_status"],
                            end_rewards=data["rewards_status"],
                        )

                        # convert to dict if needed
                        if convert_to_dict:
                            current_period = current_period.to_dict()

                            # convert to bson 128
                            if convert_to_d128:
                                current_period = get_default_localdb(
                                    network=chain.database_name
                                ).convert_decimal_to_d128(current_period)

                        # append to result
                        result.append(current_period)

                    except Exception as e:
                        logging.getLogger(__name__).exception(
                            f" Error while creating hype returns.  {e}"
                        )

                # log for errors: periods must be consecutive and not overlaped
                if len(result) > 1:
                    for i in range(len(result) - 1, 0, -1):
                        item0 = result[i - 1]
                        item1 = result[i]

                        if convert_to_dict:
                            item0_end_block = item0["end_period"]["block"]
                            item0_ini_block = item0["ini_period"]["block"]
                            item1_ini_block = item1["ini_period"]["block"]
                            item1_end_block = item1["end_period"]["block"]
                        else:
                            item0_end_block = item0.end_period.block
                            item0_ini_block = item0.ini_period.block
                            item1_ini_block = item1.ini_period.block
                            item1_end_block = item1.end_period.block

                        if (
                            item0_end_block > item1_ini_block
                            or item0_ini_block > item1_ini_block
                            or item1_ini_block > item1_end_block
                        ):
                            raise ProcessingError(
                                item={
                                    "item0": item0,
                                    "item1": item1,
                                    "description": " check database query and subsequent processing of data bc period data items overlap.",
                                },
                                action="check_manually",
                                message=f" Overlaped periods found between {item0_end_block} and {item1_ini_block}. Check manually. ",
                            )

                # set lastitem
                last_item = data
        #
    return result
