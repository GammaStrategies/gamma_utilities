import datetime
import logging

from decimal import Decimal, getcontext
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local, database_global

from datetime import datetime, timedelta


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

    def get_status_byDay(
        self, ini_timestamp: int = 0, end_timestamp: int = 0
    ) -> list[dict]:
        """Get a list of status separated by days
            sorted by date from past to present

        Returns:
            list[int]:
        """

        # get a list of status blocks separated at least by 1 hour
        query = [
            {
                "$match": {
                    "address": self.address,
                }
            },
            {
                "$addFields": {
                    "datetime": {"$toDate": {"$multiply": ["$timestamp", 1000]}}
                }
            },
            {
                "$group": {
                    "_id": {
                        "d": {"$dayOfMonth": "$datetime"},
                        "m": {"$month": "$datetime"},
                        "y": {"$year": "$datetime"},
                    },
                    "status": {"$first": "$$ROOT"},
                }
            },
            {"$sort": {"_id.y": 1, "_id.m": 1, "_id.d": 1}},
        ]
        # filter date if defined
        if ini_timestamp != 0 and end_timestamp != 0:
            query[0]["$match"]["$and"] = [
                {"timestamp": {"$gte": int(ini_timestamp)}},
                {"timestamp": {"$lte": int(end_timestamp)}},
            ]

        elif ini_timestamp != 0:
            query[0]["$match"]["timestamp"] = {"$gte": int(ini_timestamp)}
        elif end_timestamp != 0:
            query[0]["$match"]["timestamp"] = {"$lte": int(end_timestamp)}
        # return status list
        return [
            x["status"]
            for x in self.local_db_manager.query_items_from_database(
                collection_name="status", query=query
            )
        ]

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
            # discard operation if outside timestamp
            if not latest_operation["blockNumber"] in status:
                logging.getLogger(__name__).debug(
                    " Discard block number {} as it falls behind timeframe [{} out of {} <-> {} ]".format(
                        latest_operation["blockNumber"],
                        operation["timestamp"],
                        ini_timestamp,
                        end_timestamp,
                    )
                )
                # loop without adding to result
                continue

            result.append(
                self.calculate(
                    init_status=status[latest_operation["blockNumber"]],
                    end_status=status[operation["blockNumber"] - 1],
                )
            )

        if len(result) == 0:
            ini_status = status[min(status.keys())]
            end_status = status[max(status.keys())]
            # no operations exist
            logging.getLogger(__name__).debug(
                " No operations found from {} to {} . Using available status from {} to {}".format(
                    datetime.fromtimestamp(ini_timestamp),
                    datetime.fromtimestamp(end_timestamp),
                    datetime.fromtimestamp(ini_status["timestamp"]),
                    datetime.fromtimestamp(end_status["timestamp"]),
                )
            )
            # add to result
            result.append(
                self.calculate(
                    ini_status=ini_status,
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
            ) / x["totalAmounts_usd"]

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

    def get_impermanent_data_vOld1(
        self, ini_date: datetime = None, end_date: datetime = None
    ) -> list[dict]:
        """( Relative value % )
            get the variation of X during the timeframe specified:
                (so aggregating all variations of a var will give u the final situation: initial vs end situation)

        Args:
            ini_date (datetime, optional): initial date. Defaults to None.
            end_date (datetime, optional): end date . Defaults to None.

        Returns:
            dict: _description_
        """
        # convert to timestamps
        ini_timestamp = ini_date.timestamp()
        end_timestamp = end_date.timestamp()

        status_list = [
            self.convert_hypervisor_status_fromDb(x)
            for x in self.get_status_byDay(
                ini_timestamp=ini_timestamp, end_timestamp=end_timestamp
            )
        ]

        result = list()
        last_status = None
        last_row = None
        for status in status_list:

            # CHECK: do not process zero supply status
            if status["totalSupply"] == 0:
                # skip till hype has supply status
                logging.getLogger(__name__).warning(
                    " {} has no totalSuply at block {}. Skiping for impermanent calc".format(
                        status["address"], status["block"]
                    )
                )
                continue

            # create row
            row = dict()
            row["block"] = status["block"]
            row["timestamp"] = status["timestamp"]
            row["address"] = status["address"]
            row["symbol"] = status["symbol"]

            row["usd_price_token0"] = Decimal(
                str(
                    self.get_price(
                        block=status["block"],
                        address=status["pool"]["token0"]["address"],
                    )
                )
            )
            row["usd_price_token1"] = Decimal(
                str(
                    self.get_price(
                        block=status["block"],
                        address=status["pool"]["token1"]["address"],
                    )
                )
            )

            # CHECK: do not process price zero status
            if row["usd_price_token0"] == 0 or row["usd_price_token1"] == 0:
                # skip
                logging.getLogger(__name__).error(
                    " {} has no token price at block {}. Skiping for impermanent calc. [prices token0:{}  token1:{}]".format(
                        status["address"],
                        status["block"],
                        row["usd_price_token0"],
                        row["usd_price_token1"],
                    )
                )
                continue

            row["underlying_token0"] = (
                status["totalAmounts"]["total0"]
                + status["fees_uncollected"]["qtty_token0"]
            )
            row["underlying_token1"] = (
                status["totalAmounts"]["total1"]
                + status["fees_uncollected"]["qtty_token1"]
            )
            row["total_underlying_in_usd"] = (
                row["underlying_token0"] * row["usd_price_token0"]
                + row["underlying_token1"] * row["usd_price_token1"]
            )
            row["total_underlying_in_usd_perShare"] = (
                row["total_underlying_in_usd"] / status["totalSupply"]
            )

            row["total_value_in_token0_perShare"] = (
                row["total_underlying_in_usd_perShare"] / row["usd_price_token0"]
            )
            row["total_value_in_token1_perShare"] = (
                row["total_underlying_in_usd_perShare"] / row["usd_price_token1"]
            )

            # current 50% token qtty calculation
            row["fifty_qtty_token0"] = (
                row["total_underlying_in_usd"] * Decimal("0.5")
            ) / row["usd_price_token0"]
            row["fifty_qtty_token1"] = (
                row["total_underlying_in_usd"] * Decimal("0.5")
            ) / row["usd_price_token1"]

            if last_status != None:

                # calculate the current value of the last 50% tokens ( so last 50% token qtty * current prices)
                row["fifty_value_last_usd"] = (
                    last_row["fifty_qtty_token0"] * row["usd_price_token0"]
                    + last_row["fifty_qtty_token1"] * row["usd_price_token1"]
                )
                # price per share ( using last status )
                row["fifty_value_last_usd_perShare"] = (
                    row["fifty_value_last_usd"] / last_status["totalSupply"]
                )

                # set 50% result
                row["hodl_fifty_result_variation"] = (
                    row["fifty_value_last_usd_perShare"]
                    - last_row["total_underlying_in_usd_perShare"]
                ) / last_row["total_underlying_in_usd_perShare"]

                # set HODL result
                row["hodl_token0_result_variation"] = (
                    row["total_value_in_token0_perShare"]
                    - last_row["total_value_in_token0_perShare"]
                ) / last_row["total_value_in_token0_perShare"]
                row["hodl_token1_result_variation"] = (
                    row["total_value_in_token1_perShare"]
                    - last_row["total_value_in_token1_perShare"]
                ) / last_row["total_value_in_token1_perShare"]

                # LPing
                row["lping_result_variation"] = (
                    row["total_underlying_in_usd_perShare"]
                    - last_row["total_underlying_in_usd_perShare"]
                ) / last_row["total_underlying_in_usd_perShare"]

                result.append(row)

            last_status = status
            last_row = row

        return result

    def get_impermanent_data(
        self, ini_date: datetime = None, end_date: datetime = None
    ) -> list[dict]:
        """( Relative value % )
            get the variation of X during the timeframe specified:
                (so aggregating all variations of a var will give u the final situation: initial vs end situation)

        Args:
            ini_date (datetime, optional): initial date. Defaults to None.
            end_date (datetime, optional): end date . Defaults to None.

        Returns:
            dict: _description_
        """
        # convert to timestamps
        ini_timestamp = ini_date.timestamp()
        end_timestamp = end_date.timestamp()

        status_list = [
            self.convert_hypervisor_status_fromDb(x)
            for x in self.get_status_byDay(
                ini_timestamp=ini_timestamp, end_timestamp=end_timestamp
            )
        ]

        result = list()
        last_status = None
        last_row = None

        # total supply at time zero
        timezero_totalSupply = 0
        # 50% token qtty calculation
        timezero_fifty_qtty_token0 = 0
        timezero_fifty_qtty_token1 = 0
        # total token X at time zero
        timezero_total_position_in_token0 = 0
        timezero_total_position_in_token1 = 0
        # total value locked ( including uncollected fees ) at time zero
        timezero_underlying_token0 = 0
        timezero_underlying_token1 = 0
        timezero_underlying_in_usd = 0
        timezero_underlying_in_usd_perShare = 0

        for status in status_list:

            # CHECK: do not process zero supply status
            if status["totalSupply"] == 0:
                # skip till hype has supply status
                logging.getLogger(__name__).warning(
                    " {} has no totalSuply at block {}. Skiping for impermanent calc".format(
                        status["address"], status["block"]
                    )
                )
                continue

            usd_price_token0 = Decimal(
                str(
                    self.get_price(
                        block=status["block"],
                        address=status["pool"]["token0"]["address"],
                    )
                )
            )
            usd_price_token1 = Decimal(
                str(
                    self.get_price(
                        block=status["block"],
                        address=status["pool"]["token1"]["address"],
                    )
                )
            )

            # CHECK: do not process price zero status
            if usd_price_token0 == 0 or usd_price_token1 == 0:
                # skip
                logging.getLogger(__name__).error(
                    " {} has no token price at block {}. Skiping for impermanent calc. [prices token0:{}  token1:{}]".format(
                        status["address"],
                        status["block"],
                        usd_price_token0,
                        usd_price_token1,
                    )
                )
                continue

            if last_status == None:
                # time zero row creation

                timezero_totalSupply = status["totalSupply"]

                timezero_underlying_token0 = (
                    status["totalAmounts"]["total0"]
                    + status["fees_uncollected"]["qtty_token0"]
                )
                timezero_underlying_token1 = (
                    status["totalAmounts"]["total1"]
                    + status["fees_uncollected"]["qtty_token1"]
                )
                timezero_underlying_in_usd = (
                    timezero_underlying_token0 * usd_price_token0
                    + timezero_underlying_token1 * usd_price_token1
                )
                timezero_underlying_in_usd_perShare = (
                    timezero_underlying_in_usd / timezero_totalSupply
                )

                timezero_fifty_qtty_token0 = (
                    timezero_underlying_in_usd * Decimal("0.5")
                ) / usd_price_token0
                timezero_fifty_qtty_token1 = (
                    timezero_underlying_in_usd * Decimal("0.5")
                ) / usd_price_token1

                timezero_total_position_in_token0 = (
                    timezero_underlying_in_usd / usd_price_token0
                )
                timezero_total_position_in_token1 = (
                    timezero_underlying_in_usd / usd_price_token1
                )

            # create row
            row = dict()

            row["usd_price_token0"] = usd_price_token0
            row["usd_price_token1"] = usd_price_token1

            row["underlying_token0"] = (
                status["totalAmounts"]["total0"]
                + status["fees_uncollected"]["qtty_token0"]
            )
            row["underlying_token1"] = (
                status["totalAmounts"]["total1"]
                + status["fees_uncollected"]["qtty_token1"]
            )
            row["total_underlying_in_usd"] = (
                row["underlying_token0"] * row["usd_price_token0"]
                + row["underlying_token1"] * row["usd_price_token1"]
            )
            row["total_underlying_in_usd_perShare"] = (
                row["total_underlying_in_usd"] / status["totalSupply"]
            )

            # HODL token X
            row["total_value_in_token0_perShare"] = (
                timezero_total_position_in_token0 * usd_price_token0
            ) / timezero_totalSupply
            row["total_value_in_token1_perShare"] = (
                timezero_total_position_in_token1 * usd_price_token1
            ) / timezero_totalSupply

            # HODL tokens in time zero proportion
            row["total_value_in_proportion_perShare"] = (
                timezero_underlying_token0 * usd_price_token0
                + timezero_underlying_token1 * usd_price_token1
            ) / timezero_totalSupply

            # calculate the current value of the 50%/50% position now
            row["fifty_value_last_usd"] = (
                timezero_fifty_qtty_token0 * usd_price_token0
                + timezero_fifty_qtty_token1 * usd_price_token1
            )
            # price per share of the 50%/50% position now
            row["fifty_value_last_usd_perShare"] = (
                row["fifty_value_last_usd"] / timezero_totalSupply
            )

            # 50%
            row["hodl_fifty_result_vs_firstRow"] = (
                row["fifty_value_last_usd_perShare"]
                - timezero_underlying_in_usd_perShare
            ) / timezero_underlying_in_usd_perShare
            # tokens
            row["hodl_token0_result_vs_firstRow"] = (
                row["total_value_in_token0_perShare"]
                - timezero_underlying_in_usd_perShare
            ) / timezero_underlying_in_usd_perShare
            row["hodl_token1_result_vs_firstRow"] = (
                row["total_value_in_token1_perShare"]
                - timezero_underlying_in_usd_perShare
            ) / timezero_underlying_in_usd_perShare

            row["hodl_proportion_result_vs_firstRow"] = (
                row["total_value_in_proportion_perShare"]
                - timezero_underlying_in_usd_perShare
            ) / timezero_underlying_in_usd_perShare

            row["lping_result_vs_firstRow"] = (
                row["total_underlying_in_usd_perShare"]
                - timezero_underlying_in_usd_perShare
            ) / timezero_underlying_in_usd_perShare

            if last_status != None:

                # set 50% result
                row["hodl_fifty_result_variation"] = (
                    row["hodl_fifty_result_vs_firstRow"]
                    - last_row["hodl_fifty_result_vs_firstRow"]
                )

                # set HODL result
                row["hodl_token0_result_variation"] = (
                    row["hodl_token0_result_vs_firstRow"]
                    - last_row["hodl_token0_result_vs_firstRow"]
                )
                row["hodl_token1_result_variation"] = (
                    row["hodl_token1_result_vs_firstRow"]
                    - last_row["hodl_token1_result_vs_firstRow"]
                )
                row["hodl_proportion_result_variation"] = (
                    row["hodl_proportion_result_vs_firstRow"]
                    - last_row["hodl_proportion_result_vs_firstRow"]
                )

                # LPing
                row["lping_result_variation"] = (
                    row["lping_result_vs_firstRow"]
                    - last_row["lping_result_vs_firstRow"]
                )

                # return result ( return row for debugging purposes)
                result.append(
                    {
                        "block": status["block"],
                        "timestamp": status["timestamp"],
                        "address": status["address"],
                        "symbol": status["symbol"],
                        "hodl_token0_result_variation": row[
                            "hodl_token0_result_variation"
                        ],
                        "hodl_token1_result_variation": row[
                            "hodl_token1_result_variation"
                        ],
                        "hodl_proportion_result_variation": row[
                            "hodl_proportion_result_variation"
                        ],
                        "lping_result_variation": row["lping_result_variation"],
                    }
                )

            last_status = status
            last_row = row

        return result

    def calculate(self, ini_status: dict, end_status: dict) -> dict:

        ## totalAmounts = tokens depoyed in both positions + tokensOwed0 + unused (balanceOf) in the Hypervisor

        #### DEBUG TEST #####
        if ini_status["totalAmounts"]["total0"] != end_status["totalAmounts"]["total0"]:
            logging.getLogger(__name__).error(" total token 0 ini differs from end ")
        if ini_status["totalAmounts"]["total1"] != end_status["totalAmounts"]["total1"]:
            logging.getLogger(__name__).error(" total token 1 ini differs from end ")

        # usd prices
        ini_price_usd_token0 = Decimal(
            str(
                self._prices[ini_status["block"]][
                    ini_status["pool"]["token0"]["address"]
                ]
            )
        )
        ini_price_usd_token1 = Decimal(
            str(
                self._prices[ini_status["block"]][
                    ini_status["pool"]["token1"]["address"]
                ]
            )
        )
        end_price_usd_token0 = Decimal(
            str(
                self._prices[end_status["block"]][
                    end_status["pool"]["token0"]["address"]
                ]
            )
        )
        end_price_usd_token1 = Decimal(
            str(
                self._prices[end_status["block"]][
                    end_status["pool"]["token1"]["address"]
                ]
            )
        )

        # calcs
        seconds_passed = end_status["timestamp"] - ini_status["timestamp"]
        fees_uncollected_token0 = (
            end_status["fees_uncollected"]["qtty_token0"]
            - ini_status["fees_uncollected"]["qtty_token0"]
        )
        fees_uncollected_token1 = (
            end_status["fees_uncollected"]["qtty_token1"]
            - ini_status["fees_uncollected"]["qtty_token1"]
        )
        fees_uncollected_usd = (
            fees_uncollected_token0 * end_price_usd_token0
            + fees_uncollected_token1 * end_price_usd_token1
        )
        totalAmounts_usd = (
            ini_status["totalAmounts"]["total0"] * end_price_usd_token0
            + ini_status["totalAmounts"]["total1"] * end_price_usd_token1
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
            "ini_timestamp": ini_status["timestamp"],
            "end_timestamp": end_status["timestamp"],
            "fees_uncollected_token0": fees_uncollected_token0,
            "fees_uncollected_token1": fees_uncollected_token1,
            "fees_uncollected_usd": fees_uncollected_usd,
            "totalAmounts_token0": ini_status["totalAmounts"]["total0"],
            "totalAmounts_token1": ini_status["totalAmounts"]["total1"],
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

    def query_status(
        self, address: str, ini_timestamp: int, end_timesatmp: int
    ) -> list[dict]:
        return [
            {
                "$match": {
                    "address": address,
                    "$and": [
                        {"timestamp": {"$gte": ini_timestamp}},
                        {"timestamp": {"$lte": end_timesatmp}},
                    ],
                },
            },
            {"$sort": {"block": -1}},
            {
                "$project": {
                    "tvl0": {"$toDecimal": "$totalAmounts.total0"},
                    "tvl1": {"$toDecimal": "$totalAmounts.total1"},
                    "supply": {"$toDecimal": "$totalSupply"},
                    "fees_uncollected0": {
                        "$toDecimal": "$fees_uncollected.qtty_token0"
                    },
                    "fees_uncollected1": {
                        "$toDecimal": "$fees_uncollected.qtty_token1"
                    },
                    "fees_owed0": {"$toDecimal": "$tvl.fees_owed_token0"},
                    "fees_owed1": {"$toDecimal": "$tvl.fees_owed_token1"},
                    "decimals_token0": "$pool.token0.decimals",
                    "decimals_token1": "$pool.token1.decimals",
                    "decimals_contract": "$decimals",
                    "block": "$block",
                    "timestamp": "$timestamp",
                }
            },
            {
                "$project": {
                    "tvl0": {"$divide": ["$tvl0", {"$pow": [10, "$decimals_token0"]}]},
                    "tvl1": {"$divide": ["$tvl1", {"$pow": [10, "$decimals_token1"]}]},
                    "supply": {
                        "$divide": ["$supply", {"$pow": [10, "$decimals_contract"]}]
                    },
                    "fees_uncollected0": {
                        "$divide": [
                            "$fees_uncollected0",
                            {"$pow": [10, "$decimals_token0"]},
                        ]
                    },
                    "fees_uncollected1": {
                        "$divide": [
                            "$fees_uncollected1",
                            {"$pow": [10, "$decimals_token1"]},
                        ]
                    },
                    "fees_owed0": {
                        "$divide": ["$fees_owed0", {"$pow": [10, "$decimals_token0"]}]
                    },
                    "fees_owed1": {
                        "$divide": ["$fees_owed1", {"$pow": [10, "$decimals_token1"]}]
                    },
                    "block": "$block",
                    "timestamp": "$timestamp",
                }
            },
        ]
