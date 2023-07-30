import datetime
import logging

from decimal import Decimal, getcontext
from ..configuration import CONFIGURATION
from ..database.common.db_collections_common import database_local, database_global
from ..converters.onchain import convert_hypervisor_fromDict

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
        self._masterchefs = []

        # control var (itemsprocessed): list of operation ids processed
        self.ids_processed = []
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

        result = {}
        for x in global_db_manager.get_items_from_database(
            collection_name="usd_prices", find=find, sort=sort
        ):
            if x["block"] not in result:
                result[x["block"]] = {}
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
    def first_status(self) -> dict:
        """Get alltime first status

        Returns:
            dict:
        """
        find = {"address": self.address.lower()}
        sort = [("block", 1)]
        limit = 1
        try:
            return self.local_db_manager.get_items_from_database(
                collection_name="status", find=find, sort=sort, limit=limit
            )[0]
        except Exception:
            logging.getLogger(__name__).exception(
                " Unexpected error quering first status block. Zero returned"
            )
            return {}

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
        except Exception:
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
        topics: list = None,
    ) -> dict:
        if topics is None:
            topics = ["deposit", "withdraw", "rebalance", "feeBurn"]

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
                {"timestamp": {"$gte": ini_timestamp}},
                {"timestamp": {"$lte": end_timestamp}},
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
        self, ini_timestamp: int = None, end_timestamp: int = None
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
        if ini_timestamp and end_timestamp:
            query[0]["$match"]["$and"] = [
                {"timestamp": {"$gte": ini_timestamp}},
                {"timestamp": {"$lte": end_timestamp}},
            ]

        elif ini_timestamp:
            query[0]["$match"]["timestamp"] = {"$gte": ini_timestamp}
        elif end_timestamp:
            query[0]["$match"]["timestamp"] = {"$lte": end_timestamp}
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
            ini_timestamp=int(ini_timestamp), end_timestamp=int(end_timestamp)
        )
        status = {
            x["block"]: convert_hypervisor_fromDict(hypervisor=x, toDecimal=True)
            for x in self.get_status(
                ini_timestamp=int(ini_timestamp), end_timestamp=int(end_timestamp)
            )
        }

        result = []
        for operation in operations:
            latest_operation = self.latest_operation(block=operation["blockNumber"])
            # discard operation if outside timestamp
            if latest_operation["blockNumber"] not in status:
                logging.getLogger(__name__).debug(
                    f' Discard block number {latest_operation["blockNumber"]} as it falls behind timeframe [{operation["timestamp"]} out of {ini_timestamp} <-> {end_timestamp} ]'
                )

                # loop without adding to result
                continue

            result.append(
                self.calculate(
                    init_status=status[latest_operation["blockNumber"]],
                    end_status=status[operation["blockNumber"] - 1],
                )
            )

        if not result:
            ini_status = status[min(status.keys())]
            end_status = status[max(status.keys())]
            # no operations exist
            logging.getLogger(__name__).debug(
                f' No operations found from {datetime.fromtimestamp(ini_timestamp)} to {datetime.fromtimestamp(end_timestamp)} . Using available status from {datetime.fromtimestamp(ini_status["timestamp"])} to {datetime.fromtimestamp(end_status["timestamp"])}'
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
            convert_hypervisor_fromDict(hypervisor=x, toDecimal=True)
            for x in self.get_status_byDay(
                ini_timestamp=int(ini_timestamp), end_timestamp=int(end_timestamp)
            )
        ]

        result = []
        last_status = None
        last_row = None
        for status in status_list:
            # CHECK: do not process zero supply status
            if status["totalSupply"] == 0:
                # skip till hype has supply status
                logging.getLogger(__name__).warning(
                    f' {status["address"]} has no totalSuply at block {status["block"]}. Skiping for impermanent calc'
                )

                continue

            # create row
            row = {
                "block": status["block"],
                "timestamp": status["timestamp"],
                "address": status["address"],
                "symbol": status["symbol"],
                "usd_price_token0": Decimal(
                    str(
                        self.get_price(
                            block=status["block"],
                            address=status["pool"]["token0"]["address"],
                        )
                    )
                ),
            }

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
                    f' {status["address"]} has no token price at block {status["block"]}. Skiping for impermanent calc. [prices token0:{row["usd_price_token0"]}  token1:{row["usd_price_token1"]}]'
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
                self._get_impermanent_data_vOld1_createResult(
                    last_row, row, last_status, result
                )

            last_status = status
            last_row = row

        return result

    # TODO Rename this here and in `get_impermanent_data_vOld1`
    def _get_impermanent_data_vOld1_createResult(
        self, last_row, row, last_status, result
    ):
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

    def get_impermanent_data(
        self, ini_date: datetime = None, end_date: datetime = None
    ) -> list[dict]:
        """( Relative value % )
            get the variation of X during the timeframe specified:
                (so aggregating all variations of a var will give u the final situation: initial vs end situation)

        Args:
            ini_date (datetime): initial date. Defaults to None.
            end_date (datetime): end date . Defaults to None.

        Returns:
            dict: _description_
        """
        # convert to timestamps
        ini_timestamp = int(ini_date.timestamp()) if ini_date else None
        end_timestamp = int(end_date.timestamp()) if end_date else None

        status_list = [
            convert_hypervisor_fromDict(hypervisor=x, toDecimal=True)
            for x in self.get_status_byDay(
                ini_timestamp=ini_timestamp, end_timestamp=end_timestamp
            )
        ]

        result = []
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
                    f' {status["address"]} has no totalSuply at block {status["block"]}. Skiping for impermanent calc'
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
                    f' {status["address"]} has no token price at block {status["block"]}. Skiping for impermanent calc. [prices token0:{usd_price_token0}  token1:{usd_price_token1}]'
                )

                continue

            if last_status is None:
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
            row = {
                "usd_price_token0": usd_price_token0,
                "usd_price_token1": usd_price_token1,
                "underlying_token0": (
                    status["totalAmounts"]["total0"]
                    + status["fees_uncollected"]["qtty_token0"]
                ),
                "underlying_token1": (
                    status["totalAmounts"]["total1"]
                    + status["fees_uncollected"]["qtty_token1"]
                ),
            }

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

            # FeeReturns
            try:
                row["fee_apy"], row["fee_apr"] = self.get_feeReturn(
                    ini_date=datetime.fromtimestamp(last_status["timestamp"]),
                    end_date=datetime.fromtimestamp(status["timestamp"]),
                )
            except Exception:
                row["fee_apy"] = row["fee_apr"] = 0

            if last_status != None:
                self._get_impermanent_data_createResults(row, last_row, result, status)
            last_status = status
            last_row = row

        return result

    def _get_impermanent_data_createResults(self, row, last_row, result, status):
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
            row["lping_result_vs_firstRow"] - last_row["lping_result_vs_firstRow"]
        )

        # FeeReturns
        row["feeApy_result_variation"] = row["fee_apy"] - last_row["fee_apy"]
        row["feeApr_result_variation"] = row["fee_apr"] - last_row["fee_apr"]

        # return result ( return row for debugging purposes)
        result.append(
            {
                "block": status["block"],
                "timestamp": status["timestamp"],
                "address": status["address"],
                "symbol": status["symbol"],
                "hodl_token0_result_variation": row["hodl_token0_result_variation"],
                "hodl_token1_result_variation": row["hodl_token1_result_variation"],
                "hodl_proportion_result_variation": row[
                    "hodl_proportion_result_variation"
                ],
                "lping_result_variation": row["lping_result_variation"],
                "feeApy_result_variation": row["feeApy_result_variation"],
                "feeApr_result_variation": row["feeApr_result_variation"],
            }
        )

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
        except Exception:
            logging.getLogger(__name__).error(
                f" Can't find {self.network}'s {self.address} usd price for {address} at block {block}. Return Zero"
            )
            return Decimal("0")

    def get_feeReturn(self, ini_date: datetime, end_date: datetime) -> tuple:
        timestamp_ini = ini_date.timestamp()
        timestamp_end = end_date.timestamp()
        status_list = [
            self.local_db_manager.convert_d128_to_decimal(x)
            for x in self.local_db_manager.get_status_feeReturn_data(
                hypervisor_address=self.address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
            )
        ]

        day_in_seconds = Decimal("60") * Decimal("60") * Decimal("24")
        year_in_seconds = day_in_seconds * Decimal("365")

        result = []
        initial_status = None
        elapsed_time = 0
        fee0_growth = 0
        fee1_growth = 0
        fee_growth_usd = 0
        period_yield = 0
        yield_per_day = 0
        #
        cum_fee_return = 0
        total_period_seconds = 0

        for status in status_list:
            ini_usd_price_token0 = self.get_price(
                block=status["ini_block"],
                address=self._static["pool"]["token0"]["address"],
            )
            ini_usd_price_token1 = self.get_price(
                block=status["ini_block"],
                address=self._static["pool"]["token1"]["address"],
            )
            end_usd_price_token0 = self.get_price(
                block=status["end_block"],
                address=self._static["pool"]["token0"]["address"],
            )
            end_usd_price_token1 = self.get_price(
                block=status["end_block"],
                address=self._static["pool"]["token1"]["address"],
            )

            elapsed_time = status["end_timestamp"] - status["ini_timestamp"]

            fee0_growth = (
                status["end_fees_uncollected0"] - status["ini_fees_uncollected0"]
            )
            fee1_growth = (
                status["end_fees_uncollected1"] - status["ini_fees_uncollected1"]
            )

            fee_growth_usd = (
                fee0_growth * end_usd_price_token0 + fee1_growth * end_usd_price_token1
            )

            period_yield = fee_growth_usd / (
                status["ini_tvl0"] * end_usd_price_token0
                + status["ini_tvl1"] * end_usd_price_token1
            )

            yield_per_day = period_yield * year_in_seconds / elapsed_time

            if yield_per_day > 300:
                po = ""

            total_period_seconds += elapsed_time

            if cum_fee_return:
                cum_fee_return *= 1 + period_yield
            else:
                cum_fee_return = 1 + period_yield

        cum_fee_return -= 1
        # cum_fee_return = float(cum_fee_return)
        fee_apr = cum_fee_return * (year_in_seconds / total_period_seconds)
        fee_apy = (
            1 + cum_fee_return * (day_in_seconds / total_period_seconds)
        ) ** 365 - 1

        return fee_apy, fee_apr

    def get_feeReturn_and_IL_v1(self, ini_date: datetime, end_date: datetime) -> tuple:
        timestamp_ini = ini_date.timestamp()
        timestamp_end = end_date.timestamp()
        status_list = [
            self.local_db_manager.convert_d128_to_decimal(x)
            for x in self.local_db_manager.get_status_feeReturn_data(
                hypervisor_address=self.address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
            )
        ]

        day_in_seconds = Decimal("60") * Decimal("60") * Decimal("24")
        year_in_seconds = day_in_seconds * Decimal("365")

        ### feeReturn vars
        cum_fee_return = 0
        total_period_seconds = 0

        if status_list:
            ### Impermanent vars
            timezero_usd_price0 = self.get_price(
                block=status_list[0]["ini_block"],
                address=self._static["pool"]["token0"]["address"],
            )
            timezero_usd_price1 = self.get_price(
                block=status_list[0]["ini_block"],
                address=self._static["pool"]["token1"]["address"],
            )

            # total supply at time zero
            timezero_totalSupply = status_list[0]["ini_supply"]
            if not timezero_totalSupply:
                raise ValueError(
                    f""" No initial supply found at block {status_list[0]["ini_block"]} {self.network}'s {self.address}"""
                )

            # total value locked ( including uncollected fees ) at time zero
            timezero_underlying_token0 = (
                status_list[0]["ini_tvl0"] + status_list[0]["ini_fees_uncollected0"]
            )
            timezero_underlying_token1 = (
                status_list[0]["ini_tvl1"] + status_list[0]["ini_fees_uncollected1"]
            )
            timezero_underlying_in_usd = (
                timezero_underlying_token0 * timezero_usd_price0
                + timezero_underlying_token1 * timezero_usd_price1
            )
            timezero_underlying_in_usd_perShare = (
                timezero_underlying_in_usd / timezero_totalSupply
            )

            # 50% token qtty calculation
            timezero_fifty_qtty_token0 = (
                timezero_underlying_in_usd * Decimal("0.5")
            ) / timezero_usd_price0
            timezero_fifty_qtty_token1 = (
                timezero_underlying_in_usd * Decimal("0.5")
            ) / timezero_usd_price1
            timezero_fifty_total_usd = (
                timezero_fifty_qtty_token0 * timezero_usd_price0
                + timezero_fifty_qtty_token1 * timezero_usd_price1
            )
            # total token X at time zero
            timezero_total_position_in_token0 = (
                timezero_underlying_in_usd / timezero_usd_price0
            )
            timezero_total_position_in_token1 = (
                timezero_underlying_in_usd / timezero_usd_price1
            )

            for status in status_list:
                if status["end_block"] == status["ini_block"]:
                    # 0 block period can't be processed
                    logging.getLogger(__name__).debug(
                        f""" Block {status["ini_block"]} discarded while calculating feeReturns and Impermanent data for {self.network}'s {self.address}"""
                    )

                    continue

                ini_usd_price_token0 = self.get_price(
                    block=status["ini_block"],
                    address=self._static["pool"]["token0"]["address"],
                )
                ini_usd_price_token1 = self.get_price(
                    block=status["ini_block"],
                    address=self._static["pool"]["token1"]["address"],
                )
                end_usd_price_token0 = self.get_price(
                    block=status["end_block"],
                    address=self._static["pool"]["token0"]["address"],
                )
                end_usd_price_token1 = self.get_price(
                    block=status["end_block"],
                    address=self._static["pool"]["token1"]["address"],
                )

                elapsed_time = status["end_timestamp"] - status["ini_timestamp"]

                # positions at initial time
                ini_underlying_token0 = (
                    status["ini_tvl0"] + status["ini_fees_uncollected0"]
                )
                ini_underlying_token1 = (
                    status["ini_tvl1"] + status["ini_fees_uncollected1"]
                )
                end_underlying_token0 = (
                    status["end_tvl0"] + status["end_fees_uncollected0"]
                )
                end_underlying_token1 = (
                    status["end_tvl1"] + status["end_fees_uncollected1"]
                )

                fee0_growth = (
                    status["end_fees_uncollected0"] - status["ini_fees_uncollected0"]
                )
                fee1_growth = (
                    status["end_fees_uncollected1"] - status["ini_fees_uncollected1"]
                )
                fee_growth_usd = (
                    fee0_growth * end_usd_price_token0
                    + fee1_growth * end_usd_price_token1
                )

                period_yield = fee_growth_usd / (
                    ini_underlying_token0 * end_usd_price_token0
                    + ini_underlying_token1 * end_usd_price_token1
                )

                yield_per_day = period_yield * year_in_seconds / elapsed_time

                if yield_per_day > 300:
                    raise ValueError(" yield > 300")

                total_period_seconds += elapsed_time

                if cum_fee_return:
                    cum_fee_return *= 1 + period_yield
                else:
                    cum_fee_return = 1 + period_yield

                # impermanent calcs

                # Staying inside pool
                status["result_lping"] = (
                    (
                        (
                            end_underlying_token0 * end_usd_price_token0
                            + end_underlying_token1 * end_usd_price_token1
                        )
                        / status["end_supply"]
                    )
                    - (timezero_underlying_in_usd / timezero_totalSupply)
                ) / (timezero_underlying_in_usd / timezero_totalSupply)

                # Holding 50% tokens outside pool
                status["result_hodl_fifty"] = (
                    (
                        (
                            timezero_fifty_qtty_token0 * end_usd_price_token0
                            + timezero_fifty_qtty_token1 * end_usd_price_token1
                        )
                        / timezero_totalSupply
                    )
                    - (timezero_fifty_total_usd / timezero_totalSupply)
                ) / (timezero_fifty_total_usd / timezero_totalSupply)

                # Holding proportional tokens (as if they were invested in the pool at inital %) outside pool
                status["result_hodl_proportional"] = (
                    (
                        (
                            timezero_underlying_token0 * end_usd_price_token0
                            + timezero_underlying_token1 * end_usd_price_token1
                        )
                        / timezero_totalSupply
                    )
                    - (timezero_underlying_in_usd / timezero_totalSupply)
                ) / (timezero_underlying_in_usd / timezero_totalSupply)
                # Holding tokenX outside the pool
                status["result_hodl_token0"] = (
                    (
                        (timezero_total_position_in_token0 * end_usd_price_token0)
                        / timezero_totalSupply
                    )
                    - (timezero_underlying_in_usd / timezero_totalSupply)
                ) / (timezero_underlying_in_usd / timezero_totalSupply)
                status["result_hodl_token1"] = (
                    (
                        (timezero_total_position_in_token1 * end_usd_price_token1)
                        / timezero_totalSupply
                    )
                    - (timezero_underlying_in_usd / timezero_totalSupply)
                ) / (timezero_underlying_in_usd / timezero_totalSupply)

                # add to status list
                status["result_fee_apr"] = (cum_fee_return - 1) * (
                    year_in_seconds / total_period_seconds
                )
                status["result_fee_apy"] = (
                    1 + (cum_fee_return - 1) * (day_in_seconds / total_period_seconds)
                ) ** 365 - 1

                if status["result_fee_apr"] < 0 or status["result_fee_apy"] < 0:
                    po = ""

            cum_fee_return -= 1
            fee_apr = cum_fee_return * (year_in_seconds / total_period_seconds)
            fee_apy = (
                1 + cum_fee_return * (day_in_seconds / total_period_seconds)
            ) ** 365 - 1

        return status_list

    def get_feeReturn_and_IL(self, ini_date: datetime, end_date: datetime) -> tuple:
        timestamp_ini = ini_date.timestamp()
        timestamp_end = end_date.timestamp()

        status_list = [
            convert_hypervisor_fromDict(hypervisor=x, toDecimal=True)
            for x in self.local_db_manager.get_status_feeReturn_data_alternative(
                hypervisor_address=self.address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
            )
        ]

        # more than 1 result is needed to calc anything
        if len(status_list) < 2:
            raise ValueError(
                f" Insuficient data returned for {self.network}'s {self.address} to calculate returns from timestamp {timestamp_ini} to {timestamp_end}"
            )

        day_in_seconds = Decimal("60") * Decimal("60") * Decimal("24")
        year_in_seconds = day_in_seconds * Decimal("365")

        ### feeReturn vars
        cum_fee_return = 0
        total_period_seconds = 0

        ### Impermanent vars
        timezero_usd_price0 = 0
        timezero_usd_price1 = 0
        # total supply at time zero
        timezero_totalSupply = 0
        # total value locked ( including uncollected fees ) at time zero
        timezero_underlying_token0 = 0
        timezero_underlying_token1 = 0
        timezero_underlying_in_usd = 0
        timezero_underlying_in_usd_perShare = 0
        # 50% token qtty calculation
        timezero_fifty_qtty_token0 = 0
        timezero_fifty_qtty_token1 = 0
        timezero_fifty_total_usd = 0
        # total token X at time zero
        timezero_total_position_in_token0 = 0
        timezero_total_position_in_token1 = 0

        last_status = None
        result = list()
        for idx, status in enumerate(status_list):
            # set time zero
            if (
                timezero_usd_price0 + timezero_usd_price1 == 0
                and status["fees_uncollected"]["qtty_token0"]
                + status["fees_uncollected"]["qtty_token1"]
                == 0
            ):
                # init timezero vars

                ### Impermanent vars
                timezero_usd_price0 = self.get_price(
                    block=status["block"],
                    address=self._static["pool"]["token0"]["address"],
                )
                timezero_usd_price1 = self.get_price(
                    block=status["block"],
                    address=self._static["pool"]["token1"]["address"],
                )

                #
                if (
                    timezero_usd_price0 == 0
                    or timezero_usd_price1 == 0
                    or status["totalSupply"] == 0
                ):
                    logging.getLogger(__name__).debug(
                        " Skiping timezero vars for {}'s {}  {} at block {} because can't find usd prices [{}-{}] or totalSuply [{}] is zero".format(
                            self.network,
                            self.symbol,
                            self.address,
                            status["block"],
                            timezero_usd_price0,
                            timezero_usd_price1,
                            status["totalSupply"],
                        )
                    )
                    # this can't be time zero without price
                    timezero_usd_price0 = timezero_usd_price1 = 0
                    last_status = None
                    continue

                # total supply at time zero
                timezero_totalSupply = status["totalSupply"]
                # # if not timezero_totalSupply:
                # #     raise ValueError(
                # #         " No initial supply found at block {} {}'s {}".format(
                # #             status["block"], self.network, self.address
                # #         )
                # #     )
                # total value locked ( including uncollected fees ) at time zero
                timezero_underlying_token0 = (
                    status["totalAmounts"]["total0"]
                    + status["fees_uncollected"]["qtty_token0"]
                )
                timezero_underlying_token1 = (
                    status["totalAmounts"]["total1"]
                    + status["fees_uncollected"]["qtty_token1"]
                )
                timezero_underlying_in_usd = (
                    timezero_underlying_token0 * timezero_usd_price0
                    + timezero_underlying_token1 * timezero_usd_price1
                )
                timezero_underlying_in_usd_perShare = (
                    timezero_underlying_in_usd / timezero_totalSupply
                )

                # 50% token qtty calculation
                timezero_fifty_qtty_token0 = (
                    timezero_underlying_in_usd * Decimal("0.5")
                ) / timezero_usd_price0
                timezero_fifty_qtty_token1 = (
                    timezero_underlying_in_usd * Decimal("0.5")
                ) / timezero_usd_price1
                timezero_fifty_total_usd = (
                    timezero_fifty_qtty_token0 * timezero_usd_price0
                    + timezero_fifty_qtty_token1 * timezero_usd_price1
                )
                # total token X at time zero
                timezero_total_position_in_token0 = (
                    timezero_underlying_in_usd / timezero_usd_price0
                )
                timezero_total_position_in_token1 = (
                    timezero_underlying_in_usd / timezero_usd_price1
                )

            # timezero vars must be set
            if timezero_usd_price0 + timezero_usd_price1 != 0:
                # last status must be set, (among other specifics)
                if (
                    last_status
                    and last_status["fees_uncollected"]["qtty_token0"]
                    + last_status["fees_uncollected"]["qtty_token1"]
                    == 0
                    # and status["fees_uncollected"]["qtty_token0"]
                    # + status["fees_uncollected"]["qtty_token1"]
                    # > 0
                    and status["block"] != last_status["block"]
                    and status["totalSupply"] == last_status["totalSupply"]
                ):
                    # calc

                    ini_usd_price_token0 = self.get_price(
                        block=last_status["block"],
                        address=self._static["pool"]["token0"]["address"],
                    )
                    ini_usd_price_token1 = self.get_price(
                        block=last_status["block"],
                        address=self._static["pool"]["token1"]["address"],
                    )
                    end_usd_price_token0 = self.get_price(
                        block=status["block"],
                        address=self._static["pool"]["token0"]["address"],
                    )
                    end_usd_price_token1 = self.get_price(
                        block=status["block"],
                        address=self._static["pool"]["token1"]["address"],
                    )

                    elapsed_time = status["timestamp"] - last_status["timestamp"]

                    # positions at initial time
                    ini_underlying_token0 = (
                        last_status["totalAmounts"]["total0"]
                        + last_status["fees_uncollected"]["qtty_token0"]
                    )
                    ini_underlying_token1 = (
                        last_status["totalAmounts"]["total1"]
                        + last_status["fees_uncollected"]["qtty_token1"]
                    )
                    end_underlying_token0 = (
                        status["totalAmounts"]["total0"]
                        + status["fees_uncollected"]["qtty_token0"]
                    )
                    end_underlying_token1 = (
                        status["totalAmounts"]["total1"]
                        + status["fees_uncollected"]["qtty_token1"]
                    )

                    fee0_growth = (
                        status["fees_uncollected"]["qtty_token0"]
                        - last_status["fees_uncollected"]["qtty_token0"]
                    )
                    fee1_growth = (
                        status["fees_uncollected"]["qtty_token1"]
                        - last_status["fees_uncollected"]["qtty_token1"]
                    )
                    fee_growth_usd = (
                        fee0_growth * end_usd_price_token0
                        + fee1_growth * end_usd_price_token1
                    )

                    period_yield = fee_growth_usd / (
                        ini_underlying_token0 * end_usd_price_token0
                        + ini_underlying_token1 * end_usd_price_token1
                    )

                    yield_per_day = period_yield * year_in_seconds / elapsed_time

                    if yield_per_day > 300:
                        logging.getLogger(__name__).warning(
                            " -> yield per day calc. is HIGH [{}] on {}'s {} {} from block {} to {} [ {} seconds period]".format(
                                yield_per_day,
                                self.network,
                                self.symbol,
                                self.address,
                                last_status["block"],
                                status["block"],
                                elapsed_time,
                            )
                        )
                    #    raise ValueError(" yield > 300")

                    total_period_seconds += elapsed_time

                    if cum_fee_return:
                        cum_fee_return *= 1 + period_yield
                    else:
                        cum_fee_return = 1 + period_yield

                    # add vars to status obj
                    status["ini_usd_price_token0"] = ini_usd_price_token0
                    status["ini_usd_price_token1"] = ini_usd_price_token1
                    status["end_usd_price_token0"] = end_usd_price_token0
                    status["end_usd_price_token1"] = end_usd_price_token1

                    status["period_ini_timestamp"] = last_status["timestamp"]
                    status["period_ini_block"] = last_status["block"]
                    status["period_total_seconds"] = elapsed_time
                    status["period_ini_totalSuply"] = last_status["totalSupply"]
                    status["period_ini_underlying_token0"] = ini_underlying_token0
                    status["period_ini_underlying_token1"] = ini_underlying_token1
                    status["period_end_underlying_token0"] = end_underlying_token0
                    status["period_end_underlying_token1"] = end_underlying_token1

                    status["timezero_totalSuply"] = timezero_totalSupply
                    status[
                        "timezero_underlying_in_usd_perShare"
                    ] = timezero_underlying_in_usd_perShare
                    status["timezero_underlying_token0"] = timezero_underlying_token0
                    status["timezero_underlying_token1"] = timezero_underlying_token1
                    # impermanent calcs

                    # Staying inside pool
                    status["result_lping"] = (
                        (
                            (
                                end_underlying_token0 * end_usd_price_token0
                                + end_underlying_token1 * end_usd_price_token1
                            )
                            / last_status["totalSupply"]
                        )
                        - (timezero_underlying_in_usd / timezero_totalSupply)
                    ) / (timezero_underlying_in_usd / timezero_totalSupply)

                    # Holding 50% tokens outside pool
                    status["result_hodl_fifty"] = (
                        (
                            (
                                timezero_fifty_qtty_token0 * end_usd_price_token0
                                + timezero_fifty_qtty_token1 * end_usd_price_token1
                            )
                            / timezero_totalSupply
                        )
                        - (timezero_fifty_total_usd / timezero_totalSupply)
                    ) / (timezero_fifty_total_usd / timezero_totalSupply)

                    # Holding proportional tokens (as if they were invested in the pool at inital %) outside pool
                    status["result_hodl_proportional"] = (
                        (
                            (
                                timezero_underlying_token0 * end_usd_price_token0
                                + timezero_underlying_token1 * end_usd_price_token1
                            )
                            / timezero_totalSupply
                        )
                        - (timezero_underlying_in_usd / timezero_totalSupply)
                    ) / (timezero_underlying_in_usd / timezero_totalSupply)
                    # Holding tokenX outside the pool
                    status["result_hodl_token0"] = (
                        (
                            (timezero_total_position_in_token0 * end_usd_price_token0)
                            / timezero_totalSupply
                        )
                        - (timezero_underlying_in_usd / timezero_totalSupply)
                    ) / (timezero_underlying_in_usd / timezero_totalSupply)
                    status["result_hodl_token1"] = (
                        (
                            (timezero_total_position_in_token1 * end_usd_price_token1)
                            / timezero_totalSupply
                        )
                        - (timezero_underlying_in_usd / timezero_totalSupply)
                    ) / (timezero_underlying_in_usd / timezero_totalSupply)

                    # add to status list
                    status["result_fee_apr"] = (cum_fee_return - 1) * (
                        year_in_seconds / total_period_seconds
                    )
                    status["result_fee_apy"] = (
                        1
                        + (cum_fee_return - 1) * (day_in_seconds / total_period_seconds)
                    ) ** 365 - 1

                    #
                    status["result_LPvsHODL"] = (
                        (status["result_lping"] + 1)
                        / (status["result_hodl_proportional"] + 1)
                    ) - 1

                    status["result_period_Apr"] = (status["result_fee_apr"] / 365) * (
                        total_period_seconds / day_in_seconds
                    )
                    status["result_period_ilg"] = (
                        status["result_lping"] - status["result_period_Apr"]
                    )
                    # TODO: add rewards to netAPR
                    # status["result_period_netApr"] = (
                    #     status["result_period_Apr"] + status["result_period_ilg"]
                    # )

                    # Impermanent result affected by price
                    status["result_period_ilg_price"] = status[
                        "result_hodl_proportional"
                    ]

                    # Impermanent result affected by rebalances and % asset allocation decisions
                    status["result_period_ilg_others"] = (
                        status["result_period_ilg"] - status["result_period_ilg_price"]
                    )

                    # add status to result
                    result.append(status)

                    if status["result_fee_apr"] < 0 or status["result_fee_apy"] < 0:
                        po = ""

                # set last status to be used on next iteration
                last_status = status

        # DEBUG: should match last item
        # cum_fee_return -= 1
        # fee_apr = cum_fee_return * (year_in_seconds / total_period_seconds)
        # fee_apy = (
        #     1 + cum_fee_return * (day_in_seconds / total_period_seconds)
        # ) ** 365 - 1

        return result

    # Transformers

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
