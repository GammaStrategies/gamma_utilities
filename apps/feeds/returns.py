from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import logging
import time

from bins.configuration import CONFIGURATION
from bins.database.common.database_ids import create_id_hypervisor_returns
from bins.database.common.db_collections_common import database_local
from bins.database.helpers import (
    get_default_localdb,
    get_from_localdb,
    get_price_from_db,
)
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
class period_yield_data:
    address: str = None

    ini_timestamp: int = None
    end_timestamp: int = None

    ini_block: int = None
    end_block: int = None

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

    @property
    def id(self) -> str:
        return create_id_hypervisor_returns(
            hypervisor_address=self.address,
            ini_block=self.ini_block,
            end_block=self.end_block,
        )

    @property
    def period_blocks_qtty(self) -> int:
        return self.end_block - self.ini_block

    @property
    def period_seconds(self) -> int:
        return self.end_timestamp - self.ini_timestamp

    @property
    def period_days(self) -> float:
        return self.period_seconds / (24 * 60 * 60)

    @property
    def ini_underlying_usd(self) -> float:
        return (
            self.ini_underlying_token0 * self.token0_price_ini
            + self.ini_underlying_token1 * self.token0_price_end
        )

    @property
    def end_underlying_usd(self) -> float:
        return (
            self.end_underlying_token0 * self.token0_price_end
            + self.end_underlying_token1 * self.token1_price_end
        )

    @property
    def period_fees_usd(self) -> float:
        """fees aquired during the period ( LPing ) using uncollected fees
            (using end prices)

        Returns:
            float:
        """
        return (
            self.period_fees_token0 * self.token0_price_end
            + self.period_fees_token1 * self.token1_price_end
        )

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

        if len(ini_rewards) != len(end_rewards):
            raise ValueError(" Rewards ini and end lenghts do not match.")

        # seconds passed
        period_seconds = end_rewards[0]["timestamp"] - ini_rewards[0]["timestamp"]

        raw_data = {}
        for i in range(len(ini_rewards)):
            # check all itmes have same block and timestamp
            if (
                ini_rewards[i]["block"] != end_rewards[i]["block"]
                or ini_rewards[i]["timestamp"] != end_rewards[i]["timestamp"]
            ):
                raise ValueError(
                    " Rewards ini and end blocks and timestamps do not match."
                )
            elif (
                ini_rewards[i]["address"] != end_rewards[i]["address"]
                or ini_rewards[i]["rewardToken"] != end_rewards[i]["rewardToken"]
            ):
                raise ValueError(" Rewards ini and end data do not match.")

            if ini_rewards[i]["rewardToken"] not in raw_data:
                # add to traceable data
                raw_data[ini_rewards[i]["rewardToken"]] = {
                    "period_qtty": float(end_rewards[i]["rewards_perSecond"])
                    * period_seconds,
                    "period_usd": float(end_rewards[i]["rewards_perSecond"])
                    * period_seconds
                    * end_rewards[i]["rewardToken_price_usd"],
                }

                # add to total rewards usd
                self.period_rewards_usd += raw_data[ini_rewards[i]["rewardToken"]][
                    "period_usd"
                ]

        ### TODO:  #################################

    def fill_from_hypervisors_data(
        self, ini_hype: dict, end_hype: dict, network: str | None = None
    ):
        # fill basics
        if not self.address:
            self.address = end_hype["address"]
        if not self.ini_timestamp:
            self.ini_timestamp = ini_hype["timestamp"]
        if not self.end_timestamp:
            self.end_timestamp = end_hype["timestamp"]
        if not self.ini_block:
            self.ini_block = ini_hype["block"]
        if not self.end_block:
            self.end_block = end_hype["block"]

        # supply
        self.ini_hypervisor_supply = Decimal(ini_hype["totalSupply"]) / (
            10 ** ini_hype["decimals"]
        )
        self.end_hypervisor_supply = Decimal(end_hype["totalSupply"]) / (
            10 ** end_hype["decimals"]
        )
        # check if supply at ini and end is the same
        if self.ini_hypervisor_supply != self.end_hypervisor_supply:
            raise ValueError(
                f" Hypervisor supply at ini and end is different. Ini: {self.ini_hypervisor_supply} End: {self.end_hypervisor_supply} for hypervisor {self.address} end block {self.block}"
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
        elif network and not self.token0_price_ini:
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

            self.token0_price_end = Decimal(
                str(
                    get_price_from_db(
                        network=network,
                        block=end_hype["block"],
                        token_address=end_hype["pool"]["token0"]["address"],
                    )
                )
            )

            self.token1_price_ini = Decimal(
                str(
                    get_price_from_db(
                        network=network,
                        block=ini_hype["block"],
                        token_address=ini_hype["pool"]["token1"]["address"],
                    )
                )
            )

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
                    f" Fees growth can't be negative and they are [0:{self.period_fees_token0} 1:{self.period_fees_token1}] for hypervisor {self.address} end block {self.block}"
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
            "ini_timestamp": self.ini_timestamp,
            "end_timestamp": self.end_timestamp,
            "ini_block": self.ini_block,
            "end_block": self.end_block,
            "period_blocks_qtty": self.period_blocks_qtty,
            "period_seconds": self.period_seconds,
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
        # get a list of hypes to feed
        hypervisors_static = get_from_localdb(
            network=chain.database_name,
            collection="static",
            find={},
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
) -> list[dict]:
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
    ordered_hype_status_list = get_from_localdb(
        network=chain.database_name,
        collection="operations",
        aggregate=query,
        batch_size=batch_size,
    )

    for hypervisor_status in ordered_hype_status_list:
        # hypervisor_status["_id"] = hypervisor_address
        for idx, data in enumerate(hypervisor_status["status"]):
            # zero and par indexes refer to initial values
            if idx == 0 or idx % 2 == 0:
                # this is an initial value
                pass
            else:
                # this is an end value
                #
                # create yield data and fill from hype status
                current_period = period_yield_data()
                current_period.fill_from_hypervisors_data(
                    ini_hype=last_item, end_hype=data, network=chain.database_name
                )
                # fill rewards
                current_period.fill_from_rewards_data(
                    ini_rewards=last_item["rewards_status"],
                    end_rewards=data["rewards_status"],
                )

                # convert to dict
                current_period = current_period.to_dict()

                # convert to bson compatible and save to database
                result.append(
                    get_default_localdb.convert_decimal_to_d128(current_period)
                )

            # set lastitem
            last_item = data
    #
    return result
