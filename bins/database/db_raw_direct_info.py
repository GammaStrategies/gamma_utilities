import datetime
import logging

from decimal import Decimal, getcontext
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local, database_global


class direct_db_hypervisor_info:
    def __init__(self, hypervisor_address: str, network: str, protocol: str):
        """

        Args:
            hypervisor_address (str):
            network (str):
            protocol (str):
        """

        # set global vars
        self._hypervisor_address = hypervisor_address.lower()
        self._network = network
        self._protocol = protocol

        # load static
        self._static = self._get_static_data()
        # load prices for all status blocks ( speedup process)
        self._prices = self._get_prices()

        # masterchefs
        self._masterchefs = list()

        # control var (itemsprocessed): list of operation ids processed
        self.ids_processed = list()
        # control var time order :  last block always >= current
        self.last_block_processed: int = 0

    # setup
    def _get_static_data(self):
        """_load hypervisor's static data from database"""
        # static
        return self.local_db_manager.get_items_from_database(
            collection_name="static", find={"id": self.address}
        )[0]

    def _get_prices(self) -> dict:
        """_load prices from database"""
        # database link
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        global_db_manager = database_global(mongo_url=mongo_url)

        # define query
        or_query = [
            {"address": self._static["pool"]["token0"]["address"]},
            {"address": self._static["pool"]["token1"]["address"]},
        ]
        find = {"$or": or_query, "network": self.network}
        sort = [("block", 1)]

        result = dict()
        for x in global_db_manager.get_items_from_database(
            collection_name="usd_prices", find=find, sort=sort
        ):
            if not x["block"] in result:
                result[x["block"]] = dict()
            result[x["block"]][x["address"]] = x["price"]

        return result

    @property
    def local_db_manager(self) -> str:
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{self.network}_{self.protocol}"
        return database_local(mongo_url=mongo_url, db_name=db_name)

    # public
    @property
    def address(self) -> str:
        """hypervisor address

        Returns:
            str:
        """
        return self._hypervisor_address

    @property
    def network(self) -> str:
        return self._network

    @property
    def protocol(self) -> str:
        return self._protocol

    @property
    def dex(self) -> str:
        return self._static.get("dex", "")

    @property
    def symbol(self) -> str:
        return self._static.get("symbol", "")

    @property
    def first_status_block(self) -> int:
        """Get status first block

        Returns:
            int: block number
        """
        find = {"address": self.address.lower()}
        sort = [("block", -1)]
        limit = 1
        try:
            return self.local_db_manager.get_items_from_database(
                collection_name="status", find=find, sort=sort, limit=limit
            )[0]["block"]
        except:
            logging.getLogger(__name__).exception(
                " Unexpected error quering first status block. Zero returned"
            )
            return 0

    @property
    def latest_status_block(self) -> int:
        """Get status lates block

        Returns:
            int: block number
        """
        find = {"address": self.address.lower()}
        sort = [("block", 1)]
        limit = 1
        try:
            return self.local_db_manager.get_items_from_database(
                collection_name="status", find=find, sort=sort, limit=limit
            )[0]["block"]
        except:
            logging.getLogger(__name__).exception(
                " Unexpected error quering latest status block. Zero returned"
            )
            return 0

    def latest_operation(
        self,
        block: int = 0,
        logIndex: int = 0,
        block_condition: str = "$lt",
        logIndex_condition: str = "$lte",
        topics: list = ["deposit", "withdraw", "rebalance", "feeBurn"],
    ) -> dict:

        query = [
            {
                "$match": {
                    "address": self.address,
                    "topic": {"$in": topics},
                }
            },
            {"$sort": {"blockNumber": -1, "logIndex": -1}},
            {
                "$group": {
                    "_id": {"address": "$address"},
                    "last_doc": {"$first": "$$ROOT"},
                }
            },
            {"$replaceRoot": {"newRoot": "$last_doc"}},
        ]

        if block != 0:
            query[0]["$match"]["blockNumber"] = {block_condition: block}
        if logIndex != 0:
            query[0]["$match"]["logIndex"] = {logIndex_condition: logIndex}

        return self.local_db_manager.query_items_from_database(
            collection_name="operations", query=query
        )[0]

    def get_operations(self, ini_timestamp: int, end_timestamp: int) -> list[dict]:
        find = {
            "address": self.address,
            "topic": {"$in": ["deposit", "withdraw", "rebalance", "feeBurn"]},
            "$and": [
                {"timestamp": {"$gte": int(ini_timestamp)}},
                {"timestamp": {"$lte": int(end_timestamp)}},
            ],
        }

        sort = [("blockNumber", 1), ("logIndex", 1)]
        return self.local_db_manager.get_items_from_database(
            collection_name="operations", find=find, sort=sort
        )

    def get_status(self, ini_timestamp: int, end_timestamp: int) -> list[dict]:
        find = {
            "address": self.address,
            "$and": [
                {"timestamp": {"$gte": int(ini_timestamp)}},
                {"timestamp": {"$lte": int(end_timestamp)}},
            ],
        }

        sort = [("block", 1)]
        return self.local_db_manager.get_items_from_database(
            collection_name="status", find=find, sort=sort
        )

    def get_data(self, ini_date: datetime = None, end_date: datetime = None) -> dict:
        # convert to timestamps
        ini_timestamp = ini_date.timestamp()
        end_timestamp = end_date.timestamp()

        operations = self.get_operations(
            ini_timestamp=ini_timestamp, end_timestamp=end_timestamp
        )
        status = {
            x["block"]: self.convert_hypervisor_status_fromDb(x)
            for x in self.get_status(
                ini_timestamp=ini_timestamp, end_timestamp=end_timestamp
            )
        }

        result = list()
        for operation in operations:
            latest_operation = self.latest_operation(block=operation["blockNumber"])

            result.append(
                self.calculate(
                    init_status=status[latest_operation["blockNumber"]],
                    end_status=status[operation["blockNumber"] - 1],
                )
            )

        if len(result) == 0:
            init_status = status[min(status.keys())]
            end_status = status[max(status.keys())]
            # no operations exist
            logging.getLogger(__name__).debug(
                " No operations found from {} to {} . Using available status from {} to {}".format(
                    datetime.fromtimestamp(ini_timestamp),
                    datetime.fromtimestamp(end_timestamp),
                    datetime.fromtimestamp(init_status["timestamp"]),
                    datetime.fromtimestamp(end_status["timestamp"]),
                )
            )
            # add to result
            result.append(
                self.calculate(
                    init_status=init_status,
                    end_status=end_status,
                )
            )

        total_secsPassed = 0
        total_yield_period = 0
        total_vs_hodl_usd = 0
        total_vs_hodl_token0 = 0
        total_vs_hodl_token1 = 0
        for x in result:
            secsPassed = x["end_timestamp"] - x["ini_timestamp"]
            yield_period = (
                (x["fees_uncollected_usd"] / secsPassed) * (60 * 60 * 24 * 365)
            ) / totalAmounts_usd

            total_secsPassed += secsPassed
            if total_yield_period != 0:
                total_yield_period = (1 + yield_period) * total_yield_period
            else:
                total_yield_period = 1 + yield_period

            # save only impermanent variation of %
            total_vs_hodl_usd = (
                x["vs_hodl_usd"]
                if total_vs_hodl_usd == 0
                else x["vs_hodl_usd"] - total_vs_hodl_usd
            )
            total_vs_hodl_token0 = (
                x["vs_hodl_token0"]
                if total_vs_hodl_token0 == 0
                else x["vs_hodl_token0"] - total_vs_hodl_token0
            )
            total_vs_hodl_token1 = (
                x["vs_hodl_token1"]
                if total_vs_hodl_token1 == 0
                else x["vs_hodl_token1"] - total_vs_hodl_token1
            )

        feeAPR = ((total_yield_period - 1) * (60 * 60 * 24 * 365)) / total_secsPassed
        feeAPY = (1 + total_yield_period * (60 * 60 * 24) / total_secsPassed) ** 365 - 1

        return {
            "feeAPY": feeAPY,
            "feeAPR": feeAPR,
            "vs_hodl_usd": total_vs_hodl_usd,
            "vs_hodl_token0": total_vs_hodl_token0,
            "vs_hodl_token1": total_vs_hodl_token1,
            "raw_data": result,
        }

    def calculate(self, init_status: dict, end_status: dict) -> dict:

        #### DEBUG TEST #####
        if (
            init_status["totalAmounts"]["total0"]
            != end_status["totalAmounts"]["total0"]
        ):
            logging.getLogger(__name__).error(" total token 0 ini differs from end ")
        if (
            init_status["totalAmounts"]["total1"]
            != end_status["totalAmounts"]["total1"]
        ):
            logging.getLogger(__name__).error(" total token 1 ini differs from end ")

        # usd prices
        ini_price_usd_token0 = self._prices[init_status["block"]][
            init_status["pool"]["token0"]["address"]
        ]
        ini_price_usd_token1 = self._prices[init_status["block"]][
            init_status["pool"]["token1"]["address"]
        ]

        end_price_usd_token0 = self._prices[end_status["block"]][
            end_status["pool"]["token0"]["address"]
        ]
        end_price_usd_token1 = self._prices[end_status["block"]][
            end_status["pool"]["token1"]["address"]
        ]

        # calcs
        seconds_passed = end_status["timestamp"] - init_status["timestamp"]
        fees_uncollected_token0 = (
            end_status["fees_uncollected"]["qtty_token0"]
            - init_status["fees_uncollected"]["qtty_token0"]
        )
        fees_uncollected_token1 = (
            end_status["fees_uncollected"]["qtty_token1"]
            - init_status["fees_uncollected"]["qtty_token1"]
        )
        fees_uncollected_usd = (
            fees_uncollected_token0 * end_price_usd_token0
            + fees_uncollected_token1 * end_price_usd_token1
        )
        totalAmounts_usd = (
            init_status["totalAmounts"]["total0"] * end_price_usd_token0
            + init_status["totalAmounts"]["total1"] * end_price_usd_token1
        )

        # impermanent
        tmp_end_vs_hodl_usd = (
            end_status["totalAmounts"]["total0"] * end_price_usd_token0
            + end_status["totalAmounts"]["total1"] * end_price_usd_token1
        ) / end_status["totalSupply"]
        tmp_ini_vs_hodl_usd = (
            ini_status["totalAmounts"]["total0"] * ini_price_usd_token0
            + ini_status["totalAmounts"]["total1"] * ini_price_usd_token1
        ) / ini_status["totalSupply"]
        vs_hodl_usd = (tmp_end_vs_hodl_usd - tmp_ini_vs_hodl_usd) / tmp_ini_vs_hodl_usd

        tmp_end_vs_hodl_token0 = (
            ini_status["totalAmounts"]["total0"]
            + (
                ini_status["totalAmounts"]["total1"]
                * (end_price_usd_token1 / end_price_usd_token0)
            )
        ) / end_status["totalSupply"]
        tmp_ini_vs_hodl_token0 = (
            ini_status["totalAmounts"]["total0"]
            + (
                ini_status["totalAmounts"]["total1"]
                * (ini_price_usd_token1 / ini_price_usd_token0)
            )
        ) / end_status["totalSupply"]
        vs_hodl_token0 = (
            tmp_end_vs_hodl_token0 - tmp_ini_vs_hodl_token0
        ) / tmp_ini_vs_hodl_token0

        tmp_end_vs_hodl_token1 = (
            ini_status["totalAmounts"]["total1"]
            + (
                ini_status["totalAmounts"]["total0"]
                * (end_price_usd_token0 / end_price_usd_token1)
            )
        ) / end_status["totalSupply"]
        tmp_ini_vs_hodl_token1 = (
            ini_status["totalAmounts"]["total1"]
            + (
                ini_status["totalAmounts"]["total0"]
                * (ini_price_usd_token0 / ini_price_usd_token1)
            )
        ) / end_status["totalSupply"]
        vs_hodl_token1 = (
            tmp_end_vs_hodl_token1 - tmp_ini_vs_hodl_token1
        ) / tmp_ini_vs_hodl_token1

        # return result
        return {
            "ini_timestamp": init_status["timestamp"],
            "end_timestamp": end_status["timestamp"],
            "fees_uncollected_token0": fees_uncollected_token0,
            "fees_uncollected_token1": fees_uncollected_token1,
            "fees_uncollected_usd": fees_uncollected_usd,
            "totalAmounts_token0": init_status["totalAmounts"]["total0"],
            "totalAmounts_token1": init_status["totalAmounts"]["total1"],
            "totalAmounts_usd": totalAmounts_usd,
            "vs_hodl_usd": vs_hodl_usd,
            "vs_hodl_token0": vs_hodl_token0,
            "vs_hodl_token1": vs_hodl_token1,
        }

    def get_price(self, block: int, address: str) -> Decimal:

        ##
        try:
            return Decimal(self._prices[block][address])
        except:
            logging.getLogger(__name__).error(
                f" Can't find {self.network}'s {self.address} usd price for {address} at block {block}. Return Zero"
            )
            return Decimal("0")

    # Transformers

    def convert_hypervisor_status_fromDb(self, hype_status: dict) -> dict:
        """convert database hypervisor status text fields
            to numbers.

        Args:
            hype_status (dict): hypervisor status database obj

        Returns:
            dict: same converted
        """
        # decimals
        decimals_token0 = hype_status["pool"]["token0"]["decimals"]
        decimals_token1 = hype_status["pool"]["token1"]["decimals"]
        decimals_contract = hype_status["decimals"]

        hype_status["baseUpper"] = int(hype_status["baseUpper"])
        hype_status["baseLower"] = int(hype_status["baseLower"])

        hype_status["basePosition"]["liquidity"] = int(
            hype_status["basePosition"]["liquidity"]
        )
        hype_status["basePosition"]["amount0"] = int(
            hype_status["basePosition"]["amount0"]
        )
        hype_status["basePosition"]["amount1"] = int(
            hype_status["basePosition"]["amount1"]
        )
        hype_status["limitPosition"]["liquidity"] = int(
            hype_status["limitPosition"]["liquidity"]
        )
        hype_status["limitPosition"]["amount0"] = int(
            hype_status["limitPosition"]["amount0"]
        )
        hype_status["limitPosition"]["amount1"] = int(
            hype_status["limitPosition"]["amount1"]
        )

        hype_status["currentTick"] = int(hype_status["currentTick"])

        hype_status["deposit0Max"] = Decimal(hype_status["baseLower"]) / Decimal(
            10**decimals_token0
        )
        hype_status["deposit1Max"] = Decimal(hype_status["baseLower"]) / Decimal(
            10**decimals_token1
        )

        hype_status["fees_uncollected"]["qtty_token0"] = Decimal(
            hype_status["fees_uncollected"]["qtty_token0"]
        ) / Decimal(10**decimals_token0)
        hype_status["fees_uncollected"]["qtty_token1"] = Decimal(
            hype_status["fees_uncollected"]["qtty_token1"]
        ) / Decimal(10**decimals_token1)

        hype_status["limitUpper"] = int(hype_status["limitUpper"])
        hype_status["limitLower"] = int(hype_status["limitLower"])

        hype_status["maxTotalSupply"] = int(hype_status["maxTotalSupply"]) / Decimal(
            10**decimals_contract
        )

        hype_status["pool"]["feeGrowthGlobal0X128"] = int(
            hype_status["pool"]["feeGrowthGlobal0X128"]
        )
        hype_status["pool"]["feeGrowthGlobal1X128"] = int(
            hype_status["pool"]["feeGrowthGlobal1X128"]
        )
        hype_status["pool"]["liquidity"] = int(hype_status["pool"]["liquidity"])
        hype_status["pool"]["maxLiquidityPerTick"] = int(
            hype_status["pool"]["maxLiquidityPerTick"]
        )

        # choose by dex
        if hype_status["dex"] == "uniswapv3":
            # uniswap
            hype_status["pool"]["protocolFees"][0] = int(
                hype_status["pool"]["protocolFees"][0]
            )
            hype_status["pool"]["protocolFees"][1] = int(
                hype_status["pool"]["protocolFees"][1]
            )

            hype_status["pool"]["slot0"]["sqrtPriceX96"] = int(
                hype_status["pool"]["slot0"]["sqrtPriceX96"]
            )
            hype_status["pool"]["slot0"]["tick"] = int(
                hype_status["pool"]["slot0"]["tick"]
            )
            hype_status["pool"]["slot0"]["observationIndex"] = int(
                hype_status["pool"]["slot0"]["observationIndex"]
            )
            hype_status["pool"]["slot0"]["observationCardinality"] = int(
                hype_status["pool"]["slot0"]["observationCardinality"]
            )
            hype_status["pool"]["slot0"]["observationCardinalityNext"] = int(
                hype_status["pool"]["slot0"]["observationCardinalityNext"]
            )

            hype_status["pool"]["tickSpacing"] = int(hype_status["pool"]["tickSpacing"])

        elif hype_status["dex"] == "quickswap":
            # quickswap
            hype_status["pool"]["globalState"]["sqrtPriceX96"] = int(
                hype_status["pool"]["globalState"]["sqrtPriceX96"]
            )
            hype_status["pool"]["globalState"]["tick"] = int(
                hype_status["pool"]["globalState"]["tick"]
            )
            hype_status["pool"]["globalState"]["fee"] = int(
                hype_status["pool"]["globalState"]["fee"]
            )
            hype_status["pool"]["globalState"]["timepointIndex"] = int(
                hype_status["pool"]["globalState"]["timepointIndex"]
            )
        else:
            raise NotImplementedError(" dex {} not implemented ")

        hype_status["pool"]["token0"]["totalSupply"] = Decimal(
            hype_status["pool"]["token0"]["totalSupply"]
        ) / Decimal(10**decimals_token0)
        hype_status["pool"]["token1"]["totalSupply"] = Decimal(
            hype_status["pool"]["token1"]["totalSupply"]
        ) / Decimal(10**decimals_token1)

        hype_status["qtty_depoloyed"]["qtty_token0"] = Decimal(
            hype_status["qtty_depoloyed"]["qtty_token0"]
        ) / Decimal(10**decimals_token0)
        hype_status["qtty_depoloyed"]["qtty_token1"] = Decimal(
            hype_status["qtty_depoloyed"]["qtty_token1"]
        ) / Decimal(10**decimals_token1)
        hype_status["qtty_depoloyed"]["fees_owed_token0"] = Decimal(
            hype_status["qtty_depoloyed"]["fees_owed_token0"]
        ) / Decimal(10**decimals_token0)
        hype_status["qtty_depoloyed"]["fees_owed_token1"] = Decimal(
            hype_status["qtty_depoloyed"]["fees_owed_token1"]
        ) / Decimal(10**decimals_token1)

        hype_status["tickSpacing"] = int(hype_status["tickSpacing"])

        hype_status["totalAmounts"]["total0"] = Decimal(
            hype_status["totalAmounts"]["total0"]
        ) / Decimal(10**decimals_token0)
        hype_status["totalAmounts"]["total1"] = Decimal(
            hype_status["totalAmounts"]["total1"]
        ) / Decimal(10**decimals_token1)

        hype_status["totalSupply"] = Decimal(hype_status["totalSupply"]) / Decimal(
            10**decimals_contract
        )

        hype_status["tvl"]["parked_token0"] = Decimal(
            hype_status["tvl"]["parked_token0"]
        ) / Decimal(10**decimals_token0)
        hype_status["tvl"]["parked_token1"] = Decimal(
            hype_status["tvl"]["parked_token1"]
        ) / Decimal(10**decimals_token1)
        hype_status["tvl"]["deployed_token0"] = Decimal(
            hype_status["tvl"]["deployed_token0"]
        ) / Decimal(10**decimals_token0)
        hype_status["tvl"]["deployed_token1"] = Decimal(
            hype_status["tvl"]["deployed_token1"]
        ) / Decimal(10**decimals_token1)
        hype_status["tvl"]["fees_owed_token0"] = Decimal(
            hype_status["tvl"]["fees_owed_token0"]
        ) / Decimal(10**decimals_token0)
        hype_status["tvl"]["fees_owed_token1"] = Decimal(
            hype_status["tvl"]["fees_owed_token1"]
        ) / Decimal(10**decimals_token1)
        hype_status["tvl"]["tvl_token0"] = Decimal(
            hype_status["tvl"]["tvl_token0"]
        ) / Decimal(10**decimals_token0)
        hype_status["tvl"]["tvl_token1"] = Decimal(
            hype_status["tvl"]["tvl_token1"]
        ) / Decimal(10**decimals_token1)

        return hype_status
