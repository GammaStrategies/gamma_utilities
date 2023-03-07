import sys
import logging
import uuid
import tqdm
import concurrent.futures

from bson.decimal128 import Decimal128
from decimal import Decimal, getcontext
from datetime import datetime

from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local, database_global


# getcontext().prec = 40


class user_status:
    __slots__ = (
        "timestamp",
        "block",
        "logIndex",
        "topic",
        "address",
        "hypervisor_address",
        "secPassed",
        "usd_price_token0",
        "usd_price_token1",
        "investment_qtty_token0",
        "investment_qtty_token1",
        "total_investment_qtty_in_usd",
        "total_investment_qtty_in_token0",
        "total_investment_qtty_in_token1",
        "fees_collected_token0",
        "fees_collected_token1",
        "total_fees_collected_in_usd",
        "fees_uncollected_token0",
        "fees_uncollected_token1",
        "total_fees_uncollected_in_usd",
        "fees_owed_token0",
        "fees_owed_token1",
        "total_fees_owed_in_usd",
        "fees_uncollected_secPassed",
        "divestment_base_qtty_token0",
        "divestment_base_qtty_token1",
        "total_divestment_base_qtty_in_usd",
        "total_divestment_base_qtty_in_token0",
        "total_divestment_base_qtty_in_token1",
        "divestment_fee_qtty_token0",
        "divestment_fee_qtty_token1",
        "total_divestment_fee_qtty_in_usd",
        "impermanent_lp_vs_hodl_usd",
        "impermanent_lp_vs_hodl_token0",
        "impermanent_lp_vs_hodl_token1",
        "current_result_token0",
        "current_result_token1",
        "total_current_result_in_usd",
        "total_current_result_in_token0",
        "total_current_result_in_token1",
        "closed_investment_return_token0",
        "closed_investment_return_token1",
        "total_closed_investment_return_in_usd",
        "total_closed_investment_return_in_token0",
        "total_closed_investment_return_in_token1",
        "shares_qtty",
        "shares_percent",
        "underlying_token0",
        "underlying_token1",
        "total_underlying_in_usd",
        "total_underlying_in_token0",
        "total_underlying_in_token1",
        "last_underlying_token0",
        "last_underlying_token1",
        "last_total_underlying_in_usd",
        "last_total_underlying_in_token0",
        "last_total_underlying_in_token1",
        "tvl_token0",
        "tvl_token1",
        "total_tvl_in_usd",
        "total_tvl_in_token0",
        "total_tvl_in_token1",
        "raw_operation",
    )

    def __init__(
        self,
        timestamp: int = 0,
        block: int = 0,
        logIndex: int = 999999,
        topic: str = "",
        address: str = "",
        hypervisor_address: str = "",
        raw_operation: str = "",
    ):

        # Important:
        # when var name <...in_token1> or <...in_usd>  means position converted to x.
        #
        self.timestamp: int = timestamp
        self.block: int = block
        self.logIndex: int = logIndex
        self.topic: str = topic
        self.hypervisor_address: str = hypervisor_address
        self.address: str = address

        # time passed between operations
        self.secPassed: int = 0

        self.usd_price_token0: Decimal = Decimal("0")
        self.usd_price_token1: Decimal = Decimal("0")

        # investment are deposits.
        # When transfers occur, investment % of the transfer share qtty is also transfered to new account. When investments are closed, the divestment % is substracted
        self.investment_qtty_token0: Decimal = Decimal("0")
        self.investment_qtty_token1: Decimal = Decimal("0")
        self.total_investment_qtty_in_usd: Decimal = Decimal("0")
        self.total_investment_qtty_in_token0: Decimal = Decimal("0")
        self.total_investment_qtty_in_token1: Decimal = Decimal("0")

        # FEES loop ->  [ uncollected > owed > collected (inside underlaying tokens) ]

        # fees always grow in proportion to current block's shares
        self.fees_collected_token0: Decimal = Decimal("0")
        self.fees_collected_token1: Decimal = Decimal("0")
        self.total_fees_collected_in_usd: Decimal = Decimal("0")

        # feeGrowth calculation
        self.fees_uncollected_token0: Decimal = Decimal("0")
        self.fees_uncollected_token1: Decimal = Decimal("0")
        self.total_fees_uncollected_in_usd: Decimal = Decimal("0")

        # owed fees
        self.fees_owed_token0: Decimal = Decimal("0")
        self.fees_owed_token1: Decimal = Decimal("0")
        self.total_fees_owed_in_usd: Decimal = Decimal("0")

        # seconds passed between uncollected fees
        self.fees_uncollected_secPassed: int = 0

        # divestment ( keep track control vars )
        self.divestment_base_qtty_token0: Decimal = Decimal("0")
        self.divestment_base_qtty_token1: Decimal = Decimal("0")
        self.total_divestment_base_qtty_in_usd: Decimal = Decimal("0")
        self.total_divestment_base_qtty_in_token0: Decimal = Decimal("0")
        self.total_divestment_base_qtty_in_token1: Decimal = Decimal("0")

        self.divestment_fee_qtty_token0: Decimal = Decimal("0")
        self.divestment_fee_qtty_token1: Decimal = Decimal("0")
        self.total_divestment_fee_qtty_in_usd: Decimal = Decimal("0")

        # Comparison result
        self.impermanent_lp_vs_hodl_usd: Decimal = Decimal(
            "0"
        )  # current position val - investment value
        self.impermanent_lp_vs_hodl_token0: Decimal = Decimal(
            "0"
        )  # current position val - investment in token qtty at current prices
        self.impermanent_lp_vs_hodl_token1: Decimal = Decimal("0")

        # current result
        self.current_result_token0: Decimal = Decimal("0")
        self.current_result_token1: Decimal = Decimal("0")
        self.total_current_result_in_usd: Decimal = Decimal("0")
        self.total_current_result_in_token0: Decimal = Decimal("0")
        self.total_current_result_in_token1: Decimal = Decimal("0")

        # TODO: closed positions.
        self.closed_investment_return_token0: Decimal = Decimal("0")
        self.closed_investment_return_token1: Decimal = Decimal("0")
        self.total_closed_investment_return_in_usd: Decimal = Decimal("0")
        self.total_closed_investment_return_in_token0: Decimal = Decimal("0")
        self.total_closed_investment_return_in_token1: Decimal = Decimal("0")

        # share qtty
        self.shares_qtty: Decimal = Decimal("0")
        # ( this is % that multiplied by tvl gives u total qtty assets)
        self.shares_percent: Decimal = Decimal("0")

        # underlying assets ( Be Aware: contains uncollected fees .. comparable to totalAmounts + uncollected fees)
        self.underlying_token0: Decimal = Decimal("0")
        self.underlying_token1: Decimal = Decimal("0")
        self.total_underlying_in_usd: Decimal = Decimal("0")
        self.total_underlying_in_token0: Decimal = Decimal("0")
        self.total_underlying_in_token1: Decimal = Decimal("0")

        # last underlying assets ( denominator for apy n stuff)
        self.last_underlying_token0: Decimal = Decimal("0")
        self.last_underlying_token1: Decimal = Decimal("0")
        self.last_total_underlying_in_usd: Decimal = Decimal("0")
        self.last_total_underlying_in_token0: Decimal = Decimal("0")
        self.last_total_underlying_in_token1: Decimal = Decimal("0")

        # total value locked
        self.tvl_token0: Decimal = Decimal("0")
        self.tvl_token1: Decimal = Decimal("0")
        self.total_tvl_in_usd: Decimal = Decimal("0")
        self.total_tvl_in_token0: Decimal = Decimal("0")
        self.total_tvl_in_token1: Decimal = Decimal("0")

        # save the raw operation info here
        self.raw_operation = raw_operation

    def __add__(self, status):
        # create a list of fields that will not be added
        blist = [
            "timestamp",
            "block",
            "logIndex",
            "topic",
            "address",
            "hypervisor_address",
            "raw_operation",
            "usd_price_token0",
            "usd_price_token1",
            "secPassed",
            "fees_uncollected_secPassed",
        ]
        result = self
        for p in [
            a
            for a in dir(status)
            if not a.startswith("__")
            and not callable(getattr(status, a))
            and not a in blist
        ]:
            setattr(result, p, getattr(self, p) + getattr(status, p))
        return result

    def __iadd__(self, status):
        return self + status

    def __sub__(self, status):
        blist = ["hypervisor_address", "logIndex", "topic", "address", "raw_operation"]
        result = self
        for p in [
            a
            for a in dir(status)
            if not a.startswith("__")
            and not callable(getattr(status, a))
            and not a in blist
        ]:
            setattr(result, p, getattr(self, p) - getattr(status, p))

        return result

    def __isub__(self, status):
        return self - status

    # helpers

    def fill_from(self, status):
        """Copy variables from the
            Fields not to copy:
                "timestamp", "block", "topic", "address",
                "raw_operation", "usd_price_token0", "usd_price_token1",
        Args:
            status (super): _description_
        """
        blist = [
            "timestamp",
            "block",
            "logIndex",
            "topic",
            "address",
            "raw_operation",
            "usd_price_token0",
            "usd_price_token1",
        ]
        for p in [
            a
            for a in dir(status)
            if not a.startswith("__")
            and not callable(getattr(status, a))
            and not a in blist
        ]:
            setattr(self, p, getattr(status, p))

    def _get_comparable(self) -> dict:
        """to be able to compare with subgraph

        Returns:
            dict: _description_
        """
        return {
            "fees_collected_token0": self.fees_collected_token0
            + self.divestment_fee_qtty_token0,
            "fees_collected_token1": self.fees_collected_token1
            + self.divestment_fee_qtty_token1,
            "total_invested_token0": self.investment_qtty_token0
            + self.divestment_base_qtty_token0,
            "total_invested_token1": self.investment_qtty_token1
            + self.divestment_base_qtty_token1,
        }


class user_status_hypervisor_builder:
    def __init__(self, hypervisor_address: str, network: str, protocol: str):
        """Fast forward emulate gamma hypervisor contract using database data

        Args:
            hypervisor_address (str):
            network (str):
            protocol (str):
            t_ini (int): initial timestamp
            t_end (int): end timestamp
        """

        # set global vars
        self._hypervisor_address = hypervisor_address.lower()
        self._network = network
        self._protocol = protocol

        self.__blacklist_addresses = ["0x0000000000000000000000000000000000000000"]

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

    @property
    def first_user_block(self) -> int:
        """Get users first block

        Returns:
            int: block number
        """
        find = {"hypervisor_address": self.address.lower()}
        sort = [("block", 1)]
        limit = 1
        try:
            return self.local_db_manager.get_items_from_database(
                collection_name="user_status", find=find, sort=sort, limit=limit
            )[0]["block"]
        except IndexError:
            # no user status for address found in db
            return 0
        except:
            logging.getLogger(__name__).exception(
                " Unexpected error quering first user block. Zero returned"
            )
            return 0

    @property
    def latest_user_block(self) -> int:
        """Get users latest block

        Returns:
            int: block number
        """
        find = {"hypervisor_address": self.address.lower()}
        sort = [("block", -1)]
        limit = 1
        try:
            return self.local_db_manager.get_items_from_database(
                collection_name="user_status", find=find, sort=sort, limit=limit
            )[0]["block"]

        except IndexError:
            # no user status for address found in db
            return 0
        except:
            logging.getLogger(__name__).exception(
                " Unexpected error quering latest user block. Zero returned"
            )
            return 0

    def total_hypervisor_supply(self, block: int) -> Decimal:
        """total hypervisor supply as per contract data
            sourced from status collection

        Args:
            block (int):

        Returns:
            Decimal:
        """
        find = {"address": self.address.lower(), "block": block}
        try:
            tmp_status = self.local_db_manager.get_items_from_database(
                collection_name="status",
                find=find,
            )[0]
            return Decimal(tmp_status["totalSupply"]) / Decimal(
                10 ** tmp_status["decimals"]
            )
        except:
            logging.getLogger(__name__).exception(
                " Unexpected error quering totalSupply. Zero returned"
            )
            return Decimal("0")

    def total_shares(
        self,
        block: int = 0,
        logIndex: int = 0,
        block_condition: str = "$lte",
        logIndex_condition: str = "$lte",
        exclude_address: str = "",
    ) -> Decimal:
        """Return total hypervisor shares calculated from sum of total user shares at the moment we call
            ( this is not totalSupply at all times )
        Args:
            block (int, optional): . Defaults to 0.
            logIndex (int, optional): . Defaults to 0.
            block_condition (str, optional): define condition to apply to block field <logIndex> .Defaults to lower than or equal to.
            logIndex_condition (str, optional): define condition to apply to logIndex field .Defaults to to lower than.
            exclude_hypervisor (str, optional): user address to be excluded from the calculation
        Returns:
            Decimal: total shares
        """

        find = {"hypervisor_address": self.address.lower()}
        if block != 0 and logIndex != 0:
            if "e" in block_condition:
                find["$or"] = [
                    {"block": {block_condition.replace("e", ""): block}},
                    {
                        "$and": [
                            {"block": block},
                            {"logIndex": {logIndex_condition: logIndex}},
                        ]
                    },
                ]
            else:
                raise ValueError(
                    " logIndex was set but defined Block does not permit it to be equal.  [all_operation_blocks]  "
                )
        elif block != 0:
            find["block"] = {block_condition: block}
        elif logIndex != 0:
            raise ValueError(
                " logIndex was set but Block is not.. this is nonesense .. [all_operation_blocks]  "
            )
        # address exculded
        if exclude_address != "":
            find["address"] = {"$not": {"$regex": exclude_address}}

        # build query
        query = [
            {"$match": find},
            {"$sort": {"block": 1, "logIndex": 1}},
            {
                "$group": {
                    "_id": {"address": "$address"},
                    "last_doc": {"$last": "$$ROOT"},
                }
            },
            {"$replaceRoot": {"newRoot": "$last_doc"}},
            {
                "$group": {
                    "_id": "$hypervisor_address",
                    "shares_qtty": {"$sum": "$shares_qtty"},
                    "shares_percent": {"$sum": "$shares_percent"},
                }
            },
        ]
        try:
            db_result = self.local_db_manager.get_items_from_database(
                collection_name="user_status", aggregate=query
            )[0]
            # convert to decimals and return
            return self.local_db_manager.convert_d128_to_decimal(db_result)[
                "shares_qtty"
            ]
        except IndexError:
            if exclude_address == "":
                logging.getLogger(__name__).exception(
                    " Unexpected error calc total shares (exclude_address is set) --> query:  {} ".format(
                        query
                    )
                )
            # may just be that there are no shares besides current excluded address
        except:
            logging.getLogger(__name__).exception(
                " unexpected error calc total shares--> query:  {} ".format(query)
            )

        return Decimal("0")

        # result = Decimal("0")
        # status_lst = self.last_user_status_list(
        #     block=block,
        #     logIndex=logIndex,
        #     with_shares=True,
        #     block_condition=block_condition,
        #     logIndex_condition=logIndex_condition,
        # )
        # for status in status_lst:
        #     if (
        #         exclude_address == ""
        #         or status.address.lower() != exclude_address.lower()
        #     ):
        #         result += status.shares_qtty

        # # shares
        # return result

    def total_fees(
        self,
        block: int = 0,
        logIndex: int = 0,
        block_condition: str = "$lte",
        logIndex_condition: str = "$lte",
    ) -> dict:
        """total fees

        Args:
            block (int, optional): . Defaults to 0.
            logIndex (int, optional): . Defaults to 0.
            block_condition (str, optional): define condition to apply to block field <logIndex> .Defaults to lower than or equal to.
            logIndex_condition (str, optional): define condition to apply to logIndex field .Defaults to to lower than.

        Returns:
            dict: { "token0": Decimal(0),  "token1": Decimal(0) }
        """
        total = {
            "token0": Decimal("0"),
            "token1": Decimal("0"),
        }
        for status in self.last_user_status_list(
            block=block,
            logIndex=logIndex,
            with_shares=True,
            block_condition=block_condition,
            logIndex_condition=logIndex_condition,
        ):
            total["token0"] += status.fees_collected_token0
            total["token1"] += status.fees_collected_token1

        return total

    def total_hypervisor_value_locked(
        self,
        block: int = 0,
        block_condition: str = "$lte",
    ) -> dict:

        find = {"address": self.address.lower()}
        if block != 0:
            find["block"] = block
        sort = [("block", -1)]
        limit = 1

        try:
            data = self.local_db_manager.get_items_from_database(
                collection_name="status", find=find, sort=sort, limit=limit
            )[0]
            # return not calculated field 'totalAmounts' as tvl
            return {
                "token0": Decimal(data["totalAmounts"]["total0"]),
                "token1": Decimal(data["totalAmounts"]["total1"]),
            }
        except:
            logging.getLogger(__name__).exception(
                " Unexpected error quering tvl. Zero returned"
            )
            return {
                "token0": Decimal("0"),
                "token1": Decimal("0"),
            }

    def total_value_locked(
        self,
        block: int = 0,
        logIndex: int = 0,
        block_condition: str = "$lte",
        logIndex_condition: str = "$lte",
    ) -> dict:
        """total_value_locked sourced from user status
        Args:
            block (int, optional): . Defaults to 0.
            logIndex (int, optional): . Defaults to 0.
            block_condition (str, optional): . Defaults to "$lte".
            logIndex_condition (str, optional): . Defaults to "$lte".

        Returns:
            dict: { "token0": Decimal(0), "token1": Decimal(0),}
        """

        total = {
            "token0": Decimal("0"),
            "token1": Decimal("0"),
        }
        for status in self.last_user_status_list(
            block=block,
            logIndex=logIndex,
            with_shares=True,
            block_condition=block_condition,
            logIndex_condition=logIndex_condition,
        ):
            total["token0"] += status.tvl_token0
            total["token1"] += status.tvl_token1

        return total

    def result(self, block: int, block_condition: str = "$lte") -> user_status:
        """Hypervisor last result at block

        Args:
            block (int):
            block_condition

        Returns:
            user_status:
        """
        total_block_status = user_status(
            timestamp=0,
            block=block,
            address=self.address,
            hypervisor_address=self.address,
        )

        for status in self.last_user_status_list(
            block=block,
            block_condition=block_condition,
        ):

            # use last operation for current address
            if status.timestamp > total_block_status.timestamp:
                total_block_status.usd_price_token0 = status.usd_price_token0
                total_block_status.usd_price_token1 = status.usd_price_token1
                total_block_status.timestamp = status.timestamp
                total_block_status.fees_uncollected_secPassed = (
                    status.fees_uncollected_secPassed
                )

            # sum user last operation
            total_block_status += status

        return total_block_status

    def result_list_original(self, b_ini: int = 0, b_end: int = 0) -> list[user_status]:
        """Hypervisor results between the specified blocks

        Args:
            b_ini (int, optional): initial block. Defaults to 0.
            b_end (int, optional): end block. Defaults to 0.

        Returns:
            list[user_status]: of hypervisor status at blocks
        """
        result = list()
        if b_end == 0:
            b_end = sys.maxsize
        # define alowed topics to return ( be aware that blocks not in predefined topics list may not have all addresses in the block, underreporting values)
        topics = ["report", "zeroBurn", "rebalance"]
        # get a unique block list of all hype users status
        condition = {
            "hypervisor_address": self.address,
            "$and": [
                {"block": {"$gte": b_ini}},
                {"block": {"$lte": b_end}},
                {"topic": {"$in": topics}},
            ],
        }
        blocks = sorted(
            self.local_db_manager.get_distinct_items_from_database(
                collection_name="user_status", field="block", condition=condition
            )
        )
        for block in blocks:
            result.append(self.result(block=block))

        return result

    def account_result(
        self, address: str, block: int = 0, logIndex: int = 0
    ) -> user_status:
        """account last result at specified block

        Args:
            address (str): user address
            block (int, optional): block. Defaults to 0.
            logIndex (int, optional)

        Returns:
            user_status: user status or None when not found
        """
        try:
            return self.last_user_status(
                account_address=address, block=block, logIndex=logIndex
            )
        except:
            return None

    def account_result_list(
        self, address: str, b_ini: int = 0, b_end: int = 0
    ) -> list[user_status]:
        """List of account results at specified block

        Args:
            address (str): user address
            b_ini (int, optional): initial block. Defaults to 0.
            b_end (int, optional): end block. Defaults to sys.maxsize.

        Returns:
            list[user_status]: status list
        """
        if b_end == 0:
            b_end = sys.maxsize

        find = {
            "address": address,
            "hypervisor_address": self.address,
            "$and": [
                {"block": {"$gte": b_ini}},
                {"block": {"$lte": b_end}},
            ],
        }
        sort = [("block", 1), ("logIndex", 1)]
        try:
            return [
                self.convert_user_status_fromDb(status=x)
                for x in self.local_db_manager.get_items_from_database(
                    collection_name="user_status", find=find, sort=sort, limit=limit
                )
            ]
        except:
            logging.getLogger(__name__).exception(
                " Unexpected error quering user accounts result list. Zero returned"
            )
            return []

    # General classifier

    # def _mix_operations_status(self) -> list[dict]:
    #     """Add custom made operations with the topic "report" at blocks
    #         where no operations exist but status does...

    #         ( using it just because operation blocks are not sufficient to get the overall current status of
    #         an address/hypervisor...)

    #     Returns:
    #         list[dict]:
    #     """

    #     # get all operations this hypervisor
    #     result = self.get_hypervisor_operations()

    #     # construct block list already in result
    #     all_operations_blocks = set([x["blockNumber"] for x in result])

    #     # control var
    #     initial_length = len(result)

    #     # get last hypervisor status block
    #     # last_hype_block = self.latest_status_block

    #     # for each status block not in operations, add report op
    #     for hype_status in sorted(
    #         self.get_hypervisor_status(), key=lambda x: (x["block"]), reverse=False
    #     )[-10:]:

    #         if not hype_status["block"] in all_operations_blocks:
    #             # add report operation to operations tobe processed
    #             result.append(
    #                 {
    #                     "blockHash": "reportHash",
    #                     "blockNumber": hype_status["block"],
    #                     "address": self.address,
    #                     "timestamp": hype_status["timestamp"],
    #                     "decimals_token0": hype_status["pool"]["token0"]["decimals"],
    #                     "decimals_token1": hype_status["pool"]["token1"]["decimals"],
    #                     "decimals_contract": hype_status["decimals"],
    #                     "topic": "report",
    #                     "logIndex": 100000,
    #                     "id": str(uuid.uuid4()),
    #                 }
    #             )

    #     logging.getLogger(__name__).debug(
    #         f" Added {len(result)-initial_length} report operations from status blocks"
    #     )
    #     # return sorted by block->logindex
    #     return sorted(result, key=lambda x: (x["blockNumber"], x["logIndex"]))

    def _create_operations_to_process(self) -> list[dict]:

        # get all blocks from user status ( what has already been done [ careful because last  =  block + logIndex ] )

        user_status_blocks_processed = sorted(
            self.local_db_manager.get_distinct_items_from_database(
                collection_name="user_status",
                field="block",
                condition={"hypervisor_address": self.address},
            )
        )
        # substract last couple of blocks to make sure database data has not been left between the same block and different logIndexes
        if len(user_status_blocks_processed) > 1:
            try:
                user_status_blocks_processed = set(user_status_blocks_processed[0:-2])
            except:
                logging.getLogger(__name__).error(
                    f" Unexpected error slicing block array while creating operations to process. array length: {len(user_status_blocks_processed)}"
                )
        else:
            # reset and do it all from zero
            user_status_blocks_processed = set()

        # get all available operations
        result = [
            operation
            for operation in self.get_hypervisor_operations()
            if operation["blockNumber"] not in user_status_blocks_processed
        ]

        # construct the todo block list
        blocks_to_process = set([x["blockNumber"] for x in result])

        # mix blocks to process ( extracted from operations) with the last 10 blocks from hypervisor status
        # control var
        initial_length = len(result)

        # for each status block not in operations, add report op
        for hype_status in sorted(
            self.get_hypervisor_status(), key=lambda x: (x["block"]), reverse=False
        )[-10:]:

            if (
                not hype_status["block"] in blocks_to_process
                and not hype_status["block"] in user_status_blocks_processed
            ):
                # add report operation to operations tobe processed
                result.append(
                    {
                        "blockHash": "reportHash",
                        "blockNumber": hype_status["block"],
                        "address": self.address,
                        "timestamp": hype_status["timestamp"],
                        "decimals_token0": hype_status["pool"]["token0"]["decimals"],
                        "decimals_token1": hype_status["pool"]["token1"]["decimals"],
                        "decimals_contract": hype_status["decimals"],
                        "topic": "report",
                        "logIndex": 100000,
                        "id": str(uuid.uuid4()),
                    }
                )

        logging.getLogger(__name__).debug(
            f" Added {len(result)-initial_length} report operations from status blocks. Processed: {len(user_status_blocks_processed)} -> total to process {len(result)}"
        )
        # return sorted by block->logindex
        return sorted(result, key=lambda x: (x["blockNumber"], x["logIndex"]))

    def _process_operations(self):
        """process all operations"""

        # mix operations with status blocks ( status different than operation's)
        operations_to_process = self._create_operations_to_process()

        _errors = 0
        with tqdm.tqdm(total=len(operations_to_process), leave=False) as progress_bar:

            for operation in operations_to_process:
                # progress show
                progress_bar.set_description(
                    " processing 0x..{}  {}  {}".format(
                        operation["address"][-4:],
                        operation["blockNumber"],
                        operation["topic"],
                    )
                )
                progress_bar.refresh()

                if not operation["id"] in self.ids_processed:
                    # linear processing check
                    if operation["blockNumber"] < self.last_block_processed:
                        logging.getLogger(__name__).error(
                            f""" Not processing operation with a lower block than last processed: {operation["blockNumber"]}  CHECK operation id: {operation["id"]}"""
                        )
                        continue

                    # process operation
                    self._process_operation(operation)

                    # add operation as proceesed
                    self.ids_processed.append(operation["id"])

                    # set last block number processed
                    self.last_block_processed = operation["blockNumber"]
                else:
                    logging.getLogger(__name__).debug(
                        f""" Operation already processed {operation["id"]}. Not processing"""
                    )

                # update progress
                progress_bar.update(1)

    def _process_operation(self, operation: dict):

        # set current block
        self.current_block = operation["blockNumber"]
        # set current logIndex
        self.current_logIndex = operation["logIndex"]

        if operation["topic"] == "deposit":
            self._add_user_status(status=self._process_deposit(operation=operation))

        elif operation["topic"] == "withdraw":
            self._add_user_status(status=self._process_withdraw(operation=operation))

        elif operation["topic"] == "transfer":
            # retrieve new status
            op_source, op_destination = self._process_transfer(operation=operation)
            # add to collection
            if op_source:
                self._add_user_status(status=op_source)
            if op_destination:
                self._add_user_status(status=op_destination)

        elif operation["topic"] == "rebalance":
            self._process_rebalance(operation=operation)

        elif operation["topic"] == "approval":
            # TODO: approval topic
            # self._add_user_status(self._process_approval(operation=operation))
            pass

        elif operation["topic"] == "zeroBurn":
            self._process_zeroBurn(operation=operation)

        elif operation["topic"] == "setFee":
            # TODO: setFee topic
            pass

        elif operation["topic"] == "report":
            # global status for all addresses
            self._process_topic_report(operation=operation)

        else:
            raise NotImplementedError(
                f""" {operation["topic"]} topic not implemented yet"""
            )

    # Topic transformers
    def _process_deposit(self, operation: dict) -> user_status:

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()
        # set user address
        account_address = operation["to"].lower()

        # prices
        price_usd_t0 = self.get_price(
            block=block, address=self._static["pool"]["token0"]["address"]
        )
        price_usd_t1 = self.get_price(
            block=block, address=self._static["pool"]["token1"]["address"]
        )

        # create result
        new_user_status = user_status(
            timestamp=operation["timestamp"],
            block=operation["blockNumber"],
            topic=operation["topic"],
            address=account_address,
            hypervisor_address=self.address,
            raw_operation=operation["id"],
            logIndex=operation["logIndex"],
        )

        # get last operation (lower than current logIndex)
        last_op = self.last_user_status(
            account_address=account_address,
            block=operation["blockNumber"],
            logIndex=operation["logIndex"],
        )

        # fill new status item with last data
        new_user_status.fill_from(status=last_op)

        # calc. investment absolute values
        investment_qtty_token0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        investment_qtty_token1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )
        investment_shares_qtty = Decimal(operation["shares"]) / (
            Decimal(10) ** Decimal(operation["decimals_contract"])
        )

        # use above vars to mod status with new investment
        new_user_status.shares_qtty += investment_shares_qtty
        new_user_status.investment_qtty_token0 += investment_qtty_token0
        new_user_status.investment_qtty_token1 += investment_qtty_token1
        new_user_status.total_investment_qtty_in_usd += (
            investment_qtty_token0 * price_usd_t0
            + investment_qtty_token1 * price_usd_t1
        )
        new_user_status.total_investment_qtty_in_token0 += investment_qtty_token0 + (
            investment_qtty_token1 * (price_usd_t1 / price_usd_t0)
        )
        new_user_status.total_investment_qtty_in_token1 += investment_qtty_token1 + (
            investment_qtty_token0 * (price_usd_t0 / price_usd_t1)
        )

        # add global stats
        new_user_status = self._add_globals_to_user_status(
            current_user_status=new_user_status,
            last_user_status_item=last_op,
            price_usd_t0=price_usd_t0,
            price_usd_t1=price_usd_t1,
        )

        # result
        return new_user_status

    def _process_withdraw(self, operation: dict) -> user_status:

        # define ease access vars
        block = operation["blockNumber"]
        contract_address = operation["address"].lower()
        account_address = operation["sender"].lower()

        price_usd_t0 = self.get_price(
            block=block, address=self._static["pool"]["token0"]["address"]
        )
        price_usd_t1 = self.get_price(
            block=block, address=self._static["pool"]["token1"]["address"]
        )

        # create new status item
        new_user_status = user_status(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            address=account_address,
            hypervisor_address=self.address,
            raw_operation=operation["id"],
            logIndex=operation["logIndex"],
        )
        # get last operation ( lower than logIndex)
        last_op = self.last_user_status(
            account_address=account_address, block=block, logIndex=operation["logIndex"]
        )

        # fill new status item with last data
        new_user_status.fill_from(status=last_op)

        # add prices to status
        new_user_status.usd_price_token0 = price_usd_t0
        new_user_status.usd_price_token1 = price_usd_t1

        # calc. divestment absolute values
        divestment_qtty_token0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        divestment_qtty_token1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )
        divestment_shares_qtty = Decimal(operation["shares"]) / (
            Decimal(10) ** Decimal(operation["decimals_contract"])
        )

        # calc. percentage
        # below, divestment has not been substracted from last operation bc transfer bypassed,
        # so we add divestment_shares_qtty to calc percentage divested
        divestment_percentage = divestment_shares_qtty / last_op.shares_qtty

        # calc. qtty of investment to be substracted = shares divested as percentage
        investment_divested_0 = last_op.investment_qtty_token0 * divestment_percentage
        investment_divested_1 = last_op.investment_qtty_token1 * divestment_percentage
        total_investment_divested_in_usd = (
            last_op.total_investment_qtty_in_usd * divestment_percentage
        )
        total_investment_divested_in_token0 = (
            last_op.total_investment_qtty_in_token0 * divestment_percentage
        )
        total_investment_divested_in_token1 = (
            last_op.total_investment_qtty_in_token1 * divestment_percentage
        )
        # calc. qtty of fees to be substracted ( diminishing fees collected in proportion)
        fees_collected_divested_0 = (
            last_op.fees_collected_token0 * divestment_percentage
        )
        fees_collected_divested_1 = (
            last_op.fees_collected_token1 * divestment_percentage
        )
        fees_collected_divested_usd = (
            last_op.total_fees_collected_in_usd * divestment_percentage
        )

        # use all above calculations to modify the user status
        # substract withrawn values from investment and collected fees

        new_user_status.shares_qtty -= divestment_shares_qtty

        new_user_status.investment_qtty_token0 -= investment_divested_0
        new_user_status.investment_qtty_token1 -= investment_divested_1
        new_user_status.total_investment_qtty_in_usd -= total_investment_divested_in_usd
        new_user_status.total_investment_qtty_in_token0 -= (
            total_investment_divested_in_token0
        )
        new_user_status.total_investment_qtty_in_token1 -= (
            total_investment_divested_in_token1
        )
        new_user_status.fees_collected_token0 -= fees_collected_divested_0
        new_user_status.fees_collected_token1 -= fees_collected_divested_1
        new_user_status.total_fees_collected_in_usd -= fees_collected_divested_usd
        # add to divestment vars (to keep track)
        new_user_status.divestment_base_qtty_token0 += investment_divested_0
        new_user_status.divestment_base_qtty_token1 += investment_divested_1
        new_user_status.total_divestment_base_qtty_in_usd += (
            total_investment_divested_in_usd
        )
        new_user_status.total_divestment_base_qtty_in_token0 += (
            total_investment_divested_in_token0
        )
        new_user_status.total_divestment_base_qtty_in_token1 += (
            total_investment_divested_in_token1
        )
        new_user_status.divestment_fee_qtty_token0 += fees_collected_divested_0
        new_user_status.divestment_fee_qtty_token1 += fees_collected_divested_1
        new_user_status.total_divestment_fee_qtty_in_usd += fees_collected_divested_usd

        # ####
        # CLOSED return ( testing )
        try:
            new_user_status.closed_investment_return_token0 += (
                divestment_qtty_token0 - (investment_divested_0)
            )
            new_user_status.closed_investment_return_token1 += (
                divestment_qtty_token1 - (investment_divested_1)
            )

            total_divestment_qtty_in_usd = (
                divestment_qtty_token0 * price_usd_t0
                + divestment_qtty_token1 * price_usd_t1
            )
            total_divestment_qtty_in_token0 = divestment_qtty_token0 + (
                divestment_qtty_token1 * (price_usd_t1 / price_usd_t0)
            )
            total_divestment_qtty_in_token1 = divestment_qtty_token1 + (
                divestment_qtty_token0 * (price_usd_t0 / price_usd_t1)
            )

            new_user_status.total_closed_investment_return_in_usd += (
                total_divestment_qtty_in_usd - (total_investment_divested_in_usd)
            )
            new_user_status.total_closed_investment_return_in_token0 += (
                total_divestment_qtty_in_token0 - (total_investment_divested_in_token0)
            )
            new_user_status.total_closed_investment_return_in_token1 += (
                total_divestment_qtty_in_token1 - (total_investment_divested_in_token1)
            )
        except:
            pass

        # add global stats
        new_user_status = self._add_globals_to_user_status(
            current_user_status=new_user_status,
            last_user_status_item=last_op,
            price_usd_t0=price_usd_t0,
            price_usd_t1=price_usd_t1,
        )

        # result
        return new_user_status

    def _process_transfer(self, operation: dict) -> tuple[user_status, user_status]:

        if operation["dst"] == "0x0000000000000000000000000000000000000000":
            # expect a withdraw topic on next operation ( same block))
            # do nothing
            pass
        elif operation["src"] == "0x0000000000000000000000000000000000000000":
            # expect a deposit topic on next operation ( same block)
            # do nothing
            pass
        elif operation["dst"] in self._masterchefs:
            # TODO: masterchef implementation
            pass
        else:
            # transfer all values to other user address
            return self._transfer_to_user(operation=operation)

        # result
        return None, None

    def _process_rebalance(self, operation: dict):
        """Rebalance affects all users positions

        Args:
            operation (dict):

        Returns:
            user_status: _description_
        """
        # block
        block = operation["blockNumber"]

        # # convert TVL
        # new_tvl_token0 = Decimal(operation["totalAmount0"]) / (
        #     Decimal(10) ** Decimal(operation["decimals_token0"])
        # )
        # new_tvl_token1 = Decimal(operation["totalAmount1"]) / (
        #     Decimal(10) ** Decimal(operation["decimals_token1"])
        # )

        # share fees with all accounts with shares
        self._share_fees_with_acounts(operation)

    def _process_approval(self, operation: dict):
        # TODO: approval
        pass

    def _process_zeroBurn(self, operation: dict):
        # share fees with all acoounts proportionally
        self._share_fees_with_acounts(operation)

    # def _process_topic_report_original(self, operation: dict):
    #     # all report blocks should be final block ( so no operations should exist at this block besides this report)
    #     # for each address
    #     for account_address in self.get_all_account_addresses(
    #         block=operation["blockNumber"], logIndex=operation["logIndex"]
    #     ):

    #         # create result
    #         new_user_status = user_status(
    #             timestamp=operation["timestamp"],
    #             block=operation["blockNumber"],
    #             topic="report",
    #             address=account_address,
    #             hypervisor_address=self.address,
    #             raw_operation=operation["id"],
    #         )
    #         # get last operation ( logIndex here is fake ... not useful)
    #         last_op = self.last_user_status(
    #             account_address=account_address, block=operation["blockNumber"]
    #         )

    #         # fill new status item with last data
    #         new_user_status.fill_from(status=last_op)

    #         # add globals
    #         new_user_status = self._add_globals_to_user_status(
    #             current_user_status=new_user_status, last_user_status_item=last_op
    #         )

    #         # add to result
    #         self._add_user_status(status=new_user_status)

    # def _share_fees_with_acounts_original(self, operation: dict):

    #     # block
    #     block = operation["blockNumber"]
    #     # contract address
    #     contract_address = operation["address"].lower()

    #     # get current total contract_address shares qtty
    #     # check if this is the last operation of the block
    #     if (
    #         self.get_last_logIndex(block=operation["blockNumber"])
    #         == operation["logIndex"]
    #     ):
    #         # get total shares from current users
    #         total_shares = self.total_hypervisor_supply(block=block)
    #     else:
    #         # sum total shares from current users
    #         total_shares = self.total_shares(
    #             block=block, logIndex=operation["logIndex"]
    #         )

    #     fees_collected_token0 = Decimal(operation["qtty_token0"]) / (
    #         Decimal(10) ** Decimal(operation["decimals_token0"])
    #     )
    #     fees_collected_token1 = Decimal(operation["qtty_token1"]) / (
    #         Decimal(10) ** Decimal(operation["decimals_token1"])
    #     )

    #     # check if any fees have actually been collected to proceed ...
    #     if total_shares == 0:
    #         # there is no deposits yet... hypervisor is in testing or seting up mode
    #         if fees_collected_token0 == fees_collected_token1 == 0:
    #             logging.getLogger(__name__).debug(
    #                 f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet and collected fees are zero"
    #             )
    #         else:
    #             logging.getLogger(__name__).warning(
    #                 f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet but fees collected fees are NON zero --> token0: {fees_collected_token0}  token1: {fees_collected_token1}"
    #             )
    #         # exit
    #         return
    #     if fees_collected_token0 == fees_collected_token1 == 0:
    #         # there is no collection made ... but hypervisor changed tick boundaries
    #         logging.getLogger(__name__).debug(
    #             f" Not processing 0x..{self.address[-4:]} fee collection as it has not collected any fees."
    #         )
    #         # exit
    #         return

    #     # USD prices
    #     price_usd_t0 = self.get_price(
    #         block=block, address=self._static["pool"]["token0"]["address"]
    #     )
    #     price_usd_t1 = self.get_price(
    #         block=block, address=self._static["pool"]["token1"]["address"]
    #     )

    #     # control var to keep track of total percentage applied
    #     ctrl_total_percentage_applied = Decimal("0")
    #     ctrl_total_shares_applied = Decimal("0")
    #     # addresses = self.get_all_account_addresses(
    #     #     block=block, logIndex=operation["logIndex"]
    #     # )
    #     # get all last user status
    #     for last_op in self.last_user_status_list(
    #         block=block, logIndex=operation["logIndex"]
    #     ):
    #         # create result
    #         new_user_status = user_status(
    #             timestamp=operation["timestamp"],
    #             block=block,
    #             topic=operation["topic"],
    #             address=last_op.address,
    #             hypervisor_address=self.address,
    #             raw_operation=operation["id"],
    #             logIndex=operation["logIndex"],
    #         )
    #         #  fill new status item with last data
    #         new_user_status.fill_from(status=last_op)

    #         # calc user share in the pool
    #         user_share = new_user_status.shares_qtty / total_shares
    #         #
    #         ctrl_total_shares_applied += new_user_status.shares_qtty

    #         # check inconsistency
    #         if (ctrl_total_percentage_applied + user_share) > 1:
    #             logging.getLogger(__name__).warning(
    #                 " The total percentage applied while calc. user fees exceeds 100% at {}'s {} hype {} -> {}".format(
    #                     self.network,
    #                     self.protocol,
    #                     self.address,
    #                     (ctrl_total_percentage_applied + user_share),
    #                 )
    #             )
    #             logging.getLogger(__name__).warning(
    #                 "          total shares applied {}  total shares {}  diff: {}".format(
    #                     ctrl_total_shares_applied,
    #                     total_shares,
    #                     total_shares - ctrl_total_shares_applied,
    #                 )
    #             )

    #         # add user share to total processed control var
    #         ctrl_total_percentage_applied += user_share

    #         # add fees collected to user
    #         new_user_status.fees_collected_token0 += fees_collected_token0 * user_share
    #         new_user_status.fees_collected_token1 += fees_collected_token1 * user_share
    #         new_user_status.total_fees_collected_in_usd += (
    #             (fees_collected_token0 * price_usd_t0)
    #             + (fees_collected_token1 * price_usd_t1)
    #         ) * user_share

    #         # add global stats
    #         new_user_status = self._add_globals_to_user_status(
    #             current_user_status=new_user_status,
    #             last_user_status_item=last_op,
    #             price_usd_t0=price_usd_t0,
    #             price_usd_t1=price_usd_t1,
    #         )

    #         # add new status to hypervisor
    #         self._add_user_status(status=new_user_status)

    #     # # loop all addresses
    #     # for account_address in addresses:
    #     #     # create result
    #     #     new_user_status = user_status(
    #     #         timestamp=operation["timestamp"],
    #     #         block=block,
    #     #         topic=operation["topic"],
    #     #         address=account_address,
    #     #         hypervisor_address=self.address,
    #     #         raw_operation=operation["id"],
    #     #         logIndex=operation["logIndex"],
    #     #     )
    #     #     # get last address operation (status)
    #     #     last_op = self.last_user_status(
    #     #         account_address=account_address,
    #     #         block=block,
    #     #         logIndex=operation["logIndex"],
    #     #     )

    #     #     #  fill new status item with last data
    #     #     new_user_status.fill_from(status=last_op)

    #     #     # calc user share in the pool
    #     #     user_share = new_user_status.shares_qtty / total_shares
    #     #     #
    #     #     ctrl_total_shares_applied += new_user_status.shares_qtty

    #     #     # check inconsistency
    #     #     if (ctrl_total_percentage_applied + user_share) > 1:
    #     #         logging.getLogger(__name__).warning(
    #     #             " The total percentage applied while calc. user fees exceeds 100% at {}'s {} hype {} -> {}".format(
    #     #                 self.network,
    #     #                 self.protocol,
    #     #                 self.address,
    #     #                 (ctrl_total_percentage_applied + user_share),
    #     #             )
    #     #         )
    #     #         logging.getLogger(__name__).warning(
    #     #             "          total shares applied {}  total shares {}  diff: {}".format(
    #     #                 ctrl_total_shares_applied,
    #     #                 total_shares,
    #     #                 total_shares - ctrl_total_shares_applied,
    #     #             )
    #     #         )

    #     #     # add user share to total processed control var
    #     #     ctrl_total_percentage_applied += user_share

    #     #     # add fees collected to user
    #     #     new_user_status.fees_collected_token0 += fees_collected_token0 * user_share
    #     #     new_user_status.fees_collected_token1 += fees_collected_token1 * user_share
    #     #     new_user_status.total_fees_collected_in_usd += (
    #     #         (fees_collected_token0 * price_usd_t0)
    #     #         + (fees_collected_token1 * price_usd_t1)
    #     #     ) * user_share

    #     #     # add global stats
    #     #     new_user_status = self._add_globals_to_user_status(
    #     #         current_user_status=new_user_status,
    #     #         last_user_status_item=last_op,
    #     #         price_usd_t0=price_usd_t0,
    #     #         price_usd_t1=price_usd_t1,
    #     #     )

    #     #     # add new status to hypervisor
    #     #     self._add_user_status(status=new_user_status)

    #     # save fee remainders data
    #     if ctrl_total_percentage_applied != Decimal("1"):

    #         fee0_remainder = (
    #             Decimal("1") - ctrl_total_percentage_applied
    #         ) * fees_collected_token0
    #         fee1_remainder = (
    #             Decimal("1") - ctrl_total_percentage_applied
    #         ) * fees_collected_token1
    #         feeUsd_remainder = (fee0_remainder * price_usd_t0) + (
    #             fee1_remainder * price_usd_t1
    #         )

    #         logging.getLogger(__name__).warning(
    #             " The total percentage applied while calc. user fees fall behind 100% at {}'s {} hype {} block {} -> {}".format(
    #                 self.network,
    #                 self.protocol,
    #                 self.address,
    #                 block,
    #                 (ctrl_total_percentage_applied),
    #             )
    #         )

    #         # add remainders to global vars
    #         # self._modify_global_data(
    #         #     block=block,
    #         #     add=True,
    #         #     fee0_remainder=fee0_remainder,
    #         #     fee1_remainder=fee1_remainder,
    #         # )

    #         # log if value is significant
    #         if (Decimal("1") - ctrl_total_percentage_applied) > Decimal("0.0001"):
    #             logging.getLogger(__name__).error(
    #                 " Only {:,.2f} of the fees value has been distributed to current accounts. remainder: {}   . num.addss: {}  tot.shares: {}  remainder usd: {}  block:{}".format(
    #                     ctrl_total_percentage_applied,
    #                     (Decimal("1") - ctrl_total_percentage_applied),
    #                     len(addresses),
    #                     ctrl_total_shares_applied,
    #                     feeUsd_remainder,
    #                     block,
    #                 )
    #             )

    def _transfer_to_user(self, operation: dict) -> tuple[user_status, user_status]:

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()
        # set user address
        address_source = operation["src"].lower()
        address_destination = operation["dst"].lower()

        # USD prices
        price_usd_t0 = self.get_price(
            block=block, address=self._static["pool"]["token0"]["address"]
        )
        price_usd_t1 = self.get_price(
            block=block, address=self._static["pool"]["token1"]["address"]
        )

        # get current total shares
        if (
            self.get_last_logIndex(block=operation["blockNumber"])
            == operation["logIndex"]
        ):
            # get total shares from current users
            total_shares = self.total_hypervisor_supply(block=block)
        else:
            # sum total shares from current users
            total_shares = self.total_shares(block=block)

        # create SOURCE result
        new_user_status_source = user_status(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            address=address_source,
            hypervisor_address=self.address,
            raw_operation=operation["id"],
            logIndex=operation["logIndex"],
        )
        # get last operation from source address
        last_op_source = self.last_user_status(
            account_address=address_source, block=block, logIndex=operation["logIndex"]
        )
        # fill new status item with last data
        new_user_status_source.fill_from(status=last_op_source)

        shares_qtty = Decimal(operation["qtty"]) / Decimal(
            10 ** operation["decimals_contract"]
        )
        # self shares percentage used for investments mod calculations
        shares_qtty_percent = (
            (shares_qtty / new_user_status_source.shares_qtty)
            if new_user_status_source.shares_qtty != 0
            else 0
        )

        # calc investment transfered ( with shares)
        investment_qtty_token0_transfer = (
            new_user_status_source.investment_qtty_token0 * shares_qtty_percent
        )
        investment_qtty_token1_transfer = (
            new_user_status_source.investment_qtty_token1 * shares_qtty_percent
        )
        total_investment_qtty_in_usd_transfer = (
            new_user_status_source.total_investment_qtty_in_usd * shares_qtty_percent
        )
        total_investment_qtty_in_token0_transfer = (
            new_user_status_source.total_investment_qtty_in_token0 * shares_qtty_percent
        )
        total_investment_qtty_in_token1_transfer = (
            new_user_status_source.total_investment_qtty_in_token1 * shares_qtty_percent
        )
        # calc fees collected transfered ( with shares)
        fees_collected_token0_transfer = (
            new_user_status_source.fees_collected_token0 * shares_qtty_percent
        )
        fees_collected_token1_transfer = (
            new_user_status_source.fees_collected_token1 * shares_qtty_percent
        )
        total_fees_collected_in_usd_transfer = (
            new_user_status_source.total_fees_collected_in_usd * shares_qtty_percent
        )

        # modify SOURCE address with divestment values ( substract )
        new_user_status_source.shares_qtty -= shares_qtty
        new_user_status_source.investment_qtty_token0 -= investment_qtty_token0_transfer

        new_user_status_source.investment_qtty_token1 -= investment_qtty_token1_transfer

        new_user_status_source.total_investment_qtty_in_usd -= (
            total_investment_qtty_in_usd_transfer
        )

        new_user_status_source.total_investment_qtty_in_token0 -= (
            total_investment_qtty_in_token0_transfer
        )

        new_user_status_source.total_investment_qtty_in_token1 -= (
            total_investment_qtty_in_token1_transfer
        )

        new_user_status_source.fees_collected_token0 -= fees_collected_token0_transfer

        new_user_status_source.fees_collected_token1 -= fees_collected_token1_transfer

        new_user_status_source.total_fees_collected_in_usd -= (
            total_fees_collected_in_usd_transfer
        )

        # refresh shares percentage now
        new_user_status_source.shares_percent = (
            (new_user_status_source.shares_qtty / total_shares)
            if total_shares != 0
            else 0
        )
        # add global stats to status
        new_user_status_source = self._add_globals_to_user_status(
            current_user_status=new_user_status_source,
            last_user_status_item=last_op_source,
            price_usd_t0=price_usd_t0,
            price_usd_t1=price_usd_t1,
        )

        # create DESTINATION result
        new_user_status_destination = user_status(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            address=address_destination,
            hypervisor_address=self.address,
            raw_operation=operation["id"],
            logIndex=operation["logIndex"],
        )

        # get last operation from destination address
        last_op_destination = self.last_user_status(
            account_address=address_destination,
            block=block,
            logIndex=operation["logIndex"],
        )

        # fill new status item with last data
        new_user_status_destination.fill_from(status=last_op_destination)

        # modify DESTINATION:
        new_user_status_destination.shares_qtty += shares_qtty
        new_user_status_destination.investment_qtty_token0 += (
            investment_qtty_token0_transfer
        )

        new_user_status_destination.investment_qtty_token1 += (
            investment_qtty_token1_transfer
        )

        new_user_status_destination.total_investment_qtty_in_usd += (
            total_investment_qtty_in_usd_transfer
        )

        new_user_status_destination.total_investment_qtty_in_token0 += (
            total_investment_qtty_in_token0_transfer
        )
        new_user_status_destination.total_investment_qtty_in_token1 += (
            total_investment_qtty_in_token1_transfer
        )
        new_user_status_destination.fees_collected_token0 += (
            fees_collected_token0_transfer
        )

        new_user_status_destination.fees_collected_token1 += (
            fees_collected_token1_transfer
        )

        new_user_status_destination.total_fees_collected_in_usd += (
            total_fees_collected_in_usd_transfer
        )

        new_user_status_destination.shares_percent = (
            (new_user_status_destination.shares_qtty / total_shares)
            if total_shares != 0
            else 0
        )

        # seconds passed may be zero (first time address) or smaller than source. pick max for destination
        if new_user_status_destination.secPassed < new_user_status_source.secPassed:
            new_user_status_destination.secPassed = new_user_status_source.secPassed

        # uncollected fees seconds passed may be zero due to new address receiving first time funds
        if new_user_status_destination.fees_uncollected_secPassed == 0:
            new_user_status_destination.fees_uncollected_secPassed = (
                new_user_status_source.fees_uncollected_secPassed
            )

        # add global stats
        new_user_status_destination = self._add_globals_to_user_status(
            current_user_status=new_user_status_destination,
            last_user_status_item=last_op_destination,
            price_usd_t0=price_usd_t0,
            price_usd_t1=price_usd_t1,
        )

        # result
        return new_user_status_source, new_user_status_destination

    # Collection

    def _add_user_status(self, status: user_status):
        """add user status to database

        Args:
            status (user_status):
        """
        if not status.address in self.__blacklist_addresses:

            # add status to database
            self.local_db_manager.set_user_status(
                self.convert_user_status_toDb(status=status)
            )

        else:
            # blacklisted
            if not status.address == "0x0000000000000000000000000000000000000000":
                logging.getLogger(__name__).debug(
                    f"Not adding blacklisted account {status.address} user status"
                )

    # General helpers
    def _add_globals_to_user_status(
        self,
        current_user_status: user_status,
        last_user_status_item: user_status,
        price_usd_t0: Decimal = Decimal("0"),
        price_usd_t1: Decimal = Decimal("0"),
        current_status_data: dict = {},
        total_shares: Decimal = Decimal("0"),
    ) -> user_status:

        if price_usd_t0 == Decimal("0"):
            # USD prices
            price_usd_t0 = self.get_price(
                block=current_user_status.block,
                address=self._static["pool"]["token0"]["address"],
            )
        if price_usd_t1 == Decimal("0"):
            price_usd_t1 = self.get_price(
                block=current_user_status.block,
                address=self._static["pool"]["token1"]["address"],
            )

        # add prices to current operation
        current_user_status.usd_price_token0 = price_usd_t0
        current_user_status.usd_price_token1 = price_usd_t1

        # add last underlying values to current
        current_user_status.last_underlying_token0 = (
            last_user_status_item.underlying_token0
        )
        current_user_status.last_underlying_token1 = (
            last_user_status_item.underlying_token1
        )
        current_user_status.last_total_underlying_in_usd = (
            last_user_status_item.total_underlying_in_usd
        )
        current_user_status.last_total_underlying_in_token0 = (
            last_user_status_item.total_underlying_in_token0
        )
        current_user_status.last_total_underlying_in_token1 = (
            last_user_status_item.total_underlying_in_token1
        )

        # get hypervisor status at block
        if len(current_status_data) == 0:
            current_status_data = self.get_hypervisor_status(
                block=current_user_status.block
            )
            if len(current_status_data) == 0:
                raise ValueError(
                    f" No hypervisor status found for {self.network}'s {self.address} at block {current_user_status.block}"
                )
            current_status_data = current_status_data[0]

        # on transfer: total shares is correct ( pre deposit & withraw transfers are not passing thru here)
        # on deposit : total shares is correct
        # on withdraw: total shares is correct
        if total_shares == Decimal("0"):
            total_shares = (
                self.total_shares(
                    block=current_user_status.block,
                    logIndex=current_user_status.logIndex,
                    exclude_address=current_user_status.address,
                )
                + current_user_status.shares_qtty
            )

        current_user_status.shares_percent = (
            (current_user_status.shares_qtty / total_shares) if total_shares != 0 else 0
        )

        # set current_user_status's proportional uncollected fees
        current_user_status.fees_uncollected_token0 = (
            Decimal(current_status_data["fees_uncollected"]["qtty_token0"])
            * current_user_status.shares_percent
        )
        current_user_status.fees_uncollected_token1 = (
            Decimal(current_status_data["fees_uncollected"]["qtty_token1"])
            * current_user_status.shares_percent
        )
        current_user_status.total_fees_uncollected_in_usd = (
            current_user_status.fees_uncollected_token0 * price_usd_t0
            + current_user_status.fees_uncollected_token1 * price_usd_t1
        )

        current_user_status.fees_owed_token0 = (
            Decimal(current_status_data["tvl"]["fees_owed_token0"])
            * current_user_status.shares_percent
        )
        current_user_status.fees_owed_token1 = (
            Decimal(current_status_data["tvl"]["fees_owed_token1"])
            * current_user_status.shares_percent
        )
        current_user_status.total_fees_owed_in_usd = (
            current_user_status.fees_owed_token0 * price_usd_t0
            + current_user_status.fees_owed_token1 * price_usd_t1
        )

        # avoid calc seconds passed on new created items ( think about transfers)
        if last_user_status_item.block != 0:
            # Decide wether to add or set seconds passed based on topic ( always set but not on report op)
            if current_user_status.topic == "report":
                # all secs passed since last block processed
                current_user_status.fees_uncollected_secPassed += (
                    current_user_status.timestamp - last_user_status_item.timestamp
                )
            else:
                # set uncollected fees secs passed since last collection
                current_user_status.fees_uncollected_secPassed = (
                    current_user_status.timestamp - last_user_status_item.timestamp
                )

        # set total value locked
        current_user_status.tvl_token0 = (
            Decimal(current_status_data["totalAmounts"]["total0"])
            * current_user_status.shares_percent
        )
        current_user_status.tvl_token1 = (
            Decimal(current_status_data["totalAmounts"]["total1"])
            * current_user_status.shares_percent
        )
        current_user_status.total_tvl_in_usd = (
            current_user_status.tvl_token0 * price_usd_t0
            + current_user_status.tvl_token1 * price_usd_t1
        )
        current_user_status.total_tvl_in_token0 = current_user_status.tvl_token0 + (
            current_user_status.tvl_token1 * (price_usd_t1 / price_usd_t0)
        )
        current_user_status.total_tvl_in_token1 = current_user_status.tvl_token1 + (
            current_user_status.tvl_token0 * (price_usd_t0 / price_usd_t1)
        )

        # set current_user_status's proportional tvl ( underlying tokens value) + uncollected fees
        # WARN: underlying tokens can be greater than TVL
        current_user_status.underlying_token0 = (
            current_user_status.tvl_token0 + current_user_status.fees_uncollected_token0
        )
        current_user_status.underlying_token1 = (
            current_user_status.tvl_token1 + current_user_status.fees_uncollected_token1
        )
        current_user_status.total_underlying_in_usd = (
            current_user_status.underlying_token0 * price_usd_t0
            + current_user_status.underlying_token1 * price_usd_t1
        )
        current_user_status.total_underlying_in_token0 = (
            current_user_status.underlying_token0
            + (current_user_status.underlying_token1 * (price_usd_t1 / price_usd_t0))
        )
        current_user_status.total_underlying_in_token1 = (
            current_user_status.underlying_token1
            + (current_user_status.underlying_token0 * (price_usd_t0 / price_usd_t1))
        )

        # add seconds passed since
        # avoid calc seconds passed on new created items ( think about transfers)
        if last_user_status_item.block != 0:
            current_user_status.secPassed += (
                current_user_status.timestamp - last_user_status_item.timestamp
            )

        # current absolute result
        current_user_status.current_result_token0 = (
            current_user_status.underlying_token0
            - current_user_status.investment_qtty_token0
        )
        current_user_status.current_result_token1 = (
            current_user_status.underlying_token1
            - current_user_status.investment_qtty_token1
        )
        current_user_status.total_current_result_in_usd = (
            current_user_status.total_underlying_in_usd
            - current_user_status.total_investment_qtty_in_usd
        )
        current_user_status.total_current_result_in_token0 = (
            current_user_status.total_underlying_in_token0
            - current_user_status.total_investment_qtty_in_token0
        )
        current_user_status.total_current_result_in_token1 = (
            current_user_status.total_underlying_in_token1
            - current_user_status.total_investment_qtty_in_token1
        )

        # Comparison results ( impermanent)
        current_user_status.impermanent_lp_vs_hodl_usd = (
            current_user_status.total_underlying_in_usd
            - current_user_status.total_investment_qtty_in_usd
        )
        current_user_status.impermanent_lp_vs_hodl_token0 = (
            current_user_status.total_underlying_in_usd
            - (current_user_status.total_investment_qtty_in_token0 * price_usd_t0)
        )
        current_user_status.impermanent_lp_vs_hodl_token1 = (
            current_user_status.total_underlying_in_usd
            - (current_user_status.total_investment_qtty_in_token1 * price_usd_t1)
        )

        return current_user_status

    def last_user_status(
        self,
        account_address: str,
        block: int = 0,
        logIndex: int = 0,
        block_condition: str = "$lte",
        logIndex_condition: str = "$lt",
    ) -> user_status:
        """Find the last status of a user account

        Args:
            account_address (str): user account
            block (int, optional): . Defaults to 0.
            logIndex (int, optional): .Defaults to 0.
            block_condition (str, optional): define condition to apply to block field <logIndex> .Defaults to lower than or equal to.
            logIndex_condition (str, optional): define condition to apply to logIndex field .Defaults to to lower than.

        Returns:
            user_status: last operation
        """

        find = {"hypervisor_address": self.address.lower(), "address": account_address}
        if block != 0 and logIndex != 0:
            if "e" in block_condition:
                find["$or"] = [
                    {"block": {block_condition.replace("e", ""): block}},
                    {
                        "$and": [
                            {"block": block},
                            {"logIndex": {logIndex_condition: logIndex}},
                        ]
                    },
                ]
            else:
                raise ValueError(
                    " logIndex was set but defined Block does not permit it to be equal.  [all_operation_blocks]  "
                )

        elif block != 0:
            find["block"] = {block_condition: block}
        elif logIndex != 0:
            raise ValueError(
                " logIndex was set but Block is not.. this is nonesense .. [all_operation_blocks]  "
            )
        sort = [("block", -1), ("logIndex", -1)]
        limit = 1
        # debug
        query_check = "({}).sort({{'block':-1}})".format(find)
        try:
            return self.convert_user_status_fromDb(
                status=self.local_db_manager.get_items_from_database(
                    collection_name="user_status", find=find, sort=sort, limit=limit
                )[0]
            )

        except IndexError:
            # does not exist in database
            return user_status(
                timestamp=0,
                block=0,
                topic="",
                address=account_address,
                hypervisor_address=self.address,
            )
        except:
            # not found operation
            logging.getLogger(__name__).exception(
                f" Unexpected error quering last status of {account_address} at {self.address} block:{block} log:{logIndex}. Zero returned"
            )
            return user_status(
                timestamp=0,
                block=0,
                topic="",
                address=account_address,
                hypervisor_address=self.address,
            )

    def last_user_status_list(
        self,
        block: int = 0,
        logIndex: int = 0,
        block_condition: str = "$lte",
        logIndex_condition: str = "$lt",
        with_shares: bool = False,
    ) -> list[user_status]:
        """get a list of all user addresses status

        Args:
            block (int, optional): . Defaults to 0.
            logIndex (int, optional): . Defaults to 0.
            block_condition (str, optional): define condition to apply to block field <logIndex> .Defaults to lower than or equal to.
            logIndex_condition (str, optional): define condition to apply to logIndex field .Defaults to to lower than.
            with_shares (bool, optional): . Defaults to False.

        Returns:
            list[user_status]:
        """

        find = {"hypervisor_address": self.address.lower()}
        if block != 0 and logIndex != 0:
            if "e" in block_condition:
                find["$or"] = [
                    {"block": {block_condition.replace("e", ""): block}},
                    {
                        "$and": [
                            {"block": block},
                            {"logIndex": {logIndex_condition: logIndex}},
                        ]
                    },
                ]
            else:
                raise ValueError(
                    " logIndex was set but defined Block does not permit it to be equal.  [all_operation_blocks]  "
                )

        elif block != 0:
            find["block"] = {block_condition: block}
        elif logIndex != 0:
            raise ValueError(
                " logIndex was set but Block is not.. this is nonesense .. [all_operation_blocks]  "
            )
        # build query
        query = [
            {"$match": find},
            {"$sort": {"block": -1, "logIndex": -1}},
            {
                "$group": {
                    "_id": {"address": "$address"},
                    "last_doc": {"$first": "$$ROOT"},
                }
            },
            {"$replaceRoot": {"newRoot": "$last_doc"}},
        ]

        return [
            self.convert_user_status_fromDb(item)
            for item in self.local_db_manager.get_items_from_database(
                collection_name="user_status", aggregate=query
            )
        ]

    def get_all_account_addresses(
        self,
        block: int = 0,
        logIndex: int = 0,
        with_shares: bool = False,
        block_condition: str = "$lte",
        logIndex_condition: str = "$lte",
    ) -> list[str]:
        """Get a unique list of addresses
            when block

        Args:
            block (int, optional):  . Defaults to 0.
            logIndex (int, optional):  . Defaults to 0.
            with_shares (bool, optional) Include only addresses with shares > 0 ?
            block_condition (str, optional): define condition to apply to block field <logIndex> .Defaults to lower than or equal to.
            logIndex_condition (str, optional): define condition to apply to logIndex field .Defaults to to lower than.

        Returns:
            list[str]: of account addresses
        """
        condition = {"hypervisor_address": self.address}
        if block != 0 and logIndex != 0:
            if "e" in block_condition:
                condition["$or"] = [
                    {"block": {block_condition.replace("e", ""): block}},
                    {
                        "$and": [
                            {"block": block},
                            {"logIndex": {logIndex_condition: logIndex}},
                        ]
                    },
                ]
            else:
                raise ValueError(
                    " logIndex was set but defined Block does not permit it to be equal. Think twice .. "
                )

        elif block != 0:
            condition["block"] = {block_condition: block}
        elif logIndex != 0:
            raise ValueError(
                " logIndex was set but Block is not.. this is nonesense .. "
            )

        if with_shares:
            condition["shares_qtty"] = {"$gt": 0}

        # debug
        query_check = f"{condition}"

        return self.local_db_manager.get_distinct_items_from_database(
            collection_name="user_status", field="address", condition=condition
        )

    def get_all_operation_blocks(
        self,
        block: int = 0,
        logIndex: int = 0,
        block_condition: str = "$lte",
        logIndex_condition: str = "$lte",
    ) -> list[int]:

        condition = {"address": self.address}
        if block != 0 and logIndex != 0:
            if "e" in block_condition:
                condition["$or"] = [
                    {"blockNumber": {block_condition.replace("e", ""): block}},
                    {
                        "$and": [
                            {"block": block},
                            {"logIndex": {logIndex_condition: logIndex}},
                        ]
                    },
                ]
            else:
                raise ValueError(
                    " logIndex was set but defined Block does not permit it to be equal.  [all_operation_blocks]  "
                )

        elif block != 0:
            condition["blockNumber"] = {block_condition: block}
        elif logIndex != 0:
            raise ValueError(
                " logIndex was set but Block is not.. this is nonesense .. [all_operation_blocks]  "
            )

        # debug
        query_check = f"{condition})"

        return self.local_db_manager.get_distinct_items_from_database(
            collection_name="operations", field="blockNumber", condition=condition
        )

    def get_hypervisor_operations(self, block: int = 0) -> list[dict]:
        """Get all found hypervisor operations ordered by block (desc)

        Args:
            block (int, optional): . Defaults to 0.

        Returns:
            list[dict]:
        """

        find = {"address": self.address.lower()}
        if block != 0:
            find["blockNumber"] = block
        sort = [("blockNumber", 1), ("logIndex", 1)]

        return self.local_db_manager.get_items_from_database(
            collection_name="operations", find=find, sort=sort
        )

    def get_hypervisor_status(self, block: int = 0) -> list[dict]:
        """Get all found hypervisor status ordered by block (desc)

        Args:
            block (int, optional): . Defaults to 0.

        Returns:
            list[dict]:
        """

        find = {"address": self.address.lower()}
        if block != 0:
            find["block"] = block
        sort = [("block", 1)]

        return [
            self.convert_hypervisor_status_fromDb(hype_status=x)
            for x in self.local_db_manager.get_items_from_database(
                collection_name="status", find=find, sort=sort
            )
        ]

    def get_last_logIndex(self, block: int) -> int:
        """get the last_logIndex of the specified block

        Args:
            block (int): (must be an operation block)

        Returns:
            int:  last logIndex or 0 if not found
        """
        find = {"address": self.address.lower()}
        sort = [("block", 1), ("logIndex", 1)]
        limit = 1
        try:
            return self.local_db_manager.get_items_from_database(
                collection_name="operations", find=find, sort=sort, limit=limit
            )[0]["logIndex"]
        except:
            logging.getLogger(__name__).exception(
                " Unexpected error quering last logIndex. Zero returned"
            )
            return 0

    def get_price(self, block: int, address: str) -> Decimal:

        ##
        try:
            return Decimal(self._prices[block][address])
        except:
            logging.getLogger(__name__).error(
                f" Can't find {self.network}'s {self.address} usd price for {address} at block {block}. Return Zero"
            )
            return Decimal("0")

        # global_db_manager = database_global(
        #     mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
        # )
        # try:
        #     return Decimal(
        #         global_db_manager.get_price_usd(
        #             network=self.network, block=block, address=address
        #         )[0]["price"]
        #     )
        # except:
        #     logging.getLogger(__name__).error(
        #         f" Can't find {self.network}'s {self.address} usd price for {address} at block {block}. Return Zero"
        #     )
        #     Decimal("0")

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

    def convert_user_status_toDb(self, status: user_status) -> dict:
        """convert user_status type to a suitable format to be uploaded to database

        Args:
            status (user_status):

        Returns:
            dict:
        """
        # convert to dictionary
        result = self.convert_user_status_to_dict(status=status)
        # convert decimal to decimal128
        result = self.local_db_manager.convert_decimal_to_d128(item=result)
        # return
        return result

    def convert_user_status_fromDb(self, status: dict) -> user_status:
        """convert database dict type to user_status

        Args:
            status (user_status):

        Returns:
            dict:
        """
        # convert to decimal
        result = self.local_db_manager.convert_d128_to_decimal(item=status)
        # convert to dictionary
        result = self.convert_user_status_from_dict(status=result)
        # return
        return result

    def convert_user_status_to_dict(self, status: user_status) -> dict:
        fields_excluded = []

        return {
            p: getattr(status, p)
            for p in [
                a
                for a in dir(status)
                if not a.startswith("__")
                and not callable(getattr(status, a))
                and not a in fields_excluded
            ]
        }

    def convert_user_status_from_dict(self, status: dict) -> user_status:
        fields_excluded = []
        result = user_status()
        for p in [
            a
            for a in dir(result)
            if not a.startswith("__")
            and not callable(getattr(result, a))
            and not a in fields_excluded
        ]:
            setattr(result, p, status[p])

        return result

    # threaded shares share
    def _share_fees_with_acounts(self, operation: dict):

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()

        # get current total contract_address shares qtty
        # check if this is the last operation of the block
        if (
            self.get_last_logIndex(block=operation["blockNumber"])
            == operation["logIndex"]
        ):
            # get total shares from current users
            total_shares = self.total_hypervisor_supply(block=block)
        else:
            # sum total shares from current users
            total_shares = self.total_shares(
                block=block, logIndex=operation["logIndex"]
            )

        fees_collected_token0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        fees_collected_token1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )

        # check if any fees have actually been collected to proceed ...
        if total_shares == 0:
            # there is no deposits yet... hypervisor is in testing or seting up mode
            if fees_collected_token0 == fees_collected_token1 == 0:
                logging.getLogger(__name__).debug(
                    f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet and collected fees are zero"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet but fees collected fees are NON zero --> token0: {fees_collected_token0}  token1: {fees_collected_token1}"
                )
            # exit
            return
        if fees_collected_token0 == fees_collected_token1 == 0:
            # there is no collection made ... but hypervisor changed tick boundaries
            logging.getLogger(__name__).debug(
                f" Not processing 0x..{self.address[-4:]} fee collection as it has not collected any fees."
            )
            # exit
            return

        # USD prices
        price_usd_t0 = self.get_price(
            block=block, address=self._static["pool"]["token0"]["address"]
        )
        price_usd_t1 = self.get_price(
            block=block, address=self._static["pool"]["token1"]["address"]
        )

        current_status_data = self.get_hypervisor_status(block=block)
        if len(current_status_data) == 0:
            raise ValueError(
                f" No hypervisor status found for {self.network}'s {self.address} at block {block}"
            )
        current_status_data = current_status_data[0]

        # control var to keep track of total percentage applied
        ctrl_total_percentage_applied = Decimal("0")
        ctrl_total_shares_applied = Decimal("0")
        # addresses = self.get_all_account_addresses(
        #     block=block, logIndex=operation["logIndex"]
        # )

        # create fee sharing loop for threaded processing
        def loop_share_fees(
            last_op,
        ) -> tuple[user_status, Decimal]:
            # create result
            new_user_status = user_status(
                timestamp=operation["timestamp"],
                block=operation["blockNumber"],
                topic=operation["topic"],
                address=last_op.address,
                hypervisor_address=self.address,
                raw_operation=operation["id"],
                logIndex=operation["logIndex"],
            )
            #  fill new status item with last data
            new_user_status.fill_from(status=last_op)

            # calc user share in the pool
            user_share = new_user_status.shares_qtty / total_shares

            # add fees collected to user
            new_user_status.fees_collected_token0 += fees_collected_token0 * user_share
            new_user_status.fees_collected_token1 += fees_collected_token1 * user_share
            new_user_status.total_fees_collected_in_usd += (
                (fees_collected_token0 * price_usd_t0)
                + (fees_collected_token1 * price_usd_t1)
            ) * user_share

            # add global stats
            new_user_status = self._add_globals_to_user_status(
                current_user_status=new_user_status,
                last_user_status_item=last_op,
                price_usd_t0=price_usd_t0,
                price_usd_t1=price_usd_t1,
                current_status_data=current_status_data,
                total_shares=total_shares,
            )

            # return
            return new_user_status, user_share

        # go thread all
        with concurrent.futures.ThreadPoolExecutor() as ex:
            for result_status, result_user_share in ex.map(
                loop_share_fees,
                self.last_user_status_list(block=block, logIndex=operation["logIndex"]),
            ):
                # apply result
                ctrl_total_shares_applied += result_status.shares_qtty
                # add user share to total processed control var
                ctrl_total_percentage_applied += result_user_share
                # add new status to hypervisor
                self._add_user_status(status=result_status)

        # control remainders
        if ctrl_total_percentage_applied != Decimal("1"):

            fee0_remainder = (
                Decimal("1") - ctrl_total_percentage_applied
            ) * fees_collected_token0
            fee1_remainder = (
                Decimal("1") - ctrl_total_percentage_applied
            ) * fees_collected_token1
            feeUsd_remainder = (fee0_remainder * price_usd_t0) + (
                fee1_remainder * price_usd_t1
            )

            if ctrl_total_percentage_applied < Decimal("1"):
                logging.getLogger(__name__).warning(
                    " The total percentage applied while calc. user fees fall behind 100% at {}'s {} hype {} block {} -> {}".format(
                        self.network,
                        self.protocol,
                        self.address,
                        block,
                        (ctrl_total_percentage_applied),
                    )
                )
            else:
                logging.getLogger(__name__).warning(
                    " The total percentage applied while calc. user fees fall exceeds 100% at {}'s {} hype {} block {} -> {}".format(
                        self.network,
                        self.protocol,
                        self.address,
                        block,
                        (ctrl_total_percentage_applied),
                    )
                )

            # add remainders to global vars
            # self._modify_global_data(
            #     block=block,
            #     add=True,
            #     fee0_remainder=fee0_remainder,
            #     fee1_remainder=fee1_remainder,
            # )

            # log error if value is significant
            if (Decimal("1") - ctrl_total_percentage_applied) > Decimal("0.0001"):
                logging.getLogger(__name__).error(
                    " Only {:,.2f} of the fees value has been distributed to current accounts. remainder: {}   . num.addss: {}  tot.shares: {}  remainder usd: {}  block:{}".format(
                        ctrl_total_percentage_applied,
                        (Decimal("1") - ctrl_total_percentage_applied),
                        len(addresses),
                        ctrl_total_shares_applied,
                        feeUsd_remainder,
                        block,
                    )
                )

    def _process_topic_report(self, operation: dict):

        # block
        block = operation["blockNumber"]

        # get total shares from current users
        total_shares = self.total_hypervisor_supply(block=block)

        current_status_data = self.get_hypervisor_status(block=block)
        if len(current_status_data) == 0:
            raise ValueError(
                f" No hypervisor status found for {self.network}'s {self.address} at block {block}"
            )
        current_status_data = current_status_data[0]

        # USD prices
        price_usd_t0 = self.get_price(
            block=block, address=self._static["pool"]["token0"]["address"]
        )
        price_usd_t1 = self.get_price(
            block=block, address=self._static["pool"]["token1"]["address"]
        )

        # create fee sharing loop for threaded processing
        def loop_process_report(
            last_status,
        ) -> user_status:
            # create result
            new_user_status = user_status(
                timestamp=operation["timestamp"],
                block=operation["blockNumber"],
                topic="report",
                address=last_status.address,
                hypervisor_address=self.address,
                raw_operation=operation["id"],
            )

            # fill new status item with last data
            new_user_status.fill_from(status=last_status)

            # add globals
            new_user_status = self._add_globals_to_user_status(
                current_user_status=new_user_status,
                last_user_status_item=last_status,
                price_usd_t0=price_usd_t0,
                price_usd_t1=price_usd_t1,
                current_status_data=current_status_data,
                total_shares=total_shares,
            )

            # return
            return new_user_status

        # go thread all
        with concurrent.futures.ThreadPoolExecutor() as ex:
            for new_user_status in ex.map(
                loop_process_report,
                self.last_user_status_list(block=block, logIndex=operation["logIndex"]),
            ):

                # add to result
                self._add_user_status(status=new_user_status)

    def result_list(self, b_ini: int = 0, b_end: int = 0) -> list[user_status]:
        """Hypervisor results between the specified blocks

        Args:
            b_ini (int, optional): initial block. Defaults to 0.
            b_end (int, optional): end block. Defaults to 0.

        Returns:
            list[user_status]: of hypervisor status at blocks
        """
        result = list()
        if b_end == 0:
            b_end = sys.maxsize
        # define alowed topics to return ( be aware that blocks not in predefined topics list may not have all addresses in the block, underreporting values)
        topics = ["report", "zeroBurn", "rebalance"]
        # get a unique block list of all hype users status
        condition = {
            "hypervisor_address": self.address,
            "$and": [
                {"block": {"$gte": b_ini}},
                {"block": {"$lte": b_end}},
                {"topic": {"$in": topics}},
            ],
        }

        # go thread all
        with concurrent.futures.ThreadPoolExecutor() as ex:
            for item in ex.map(
                self.result,
                sorted(
                    self.local_db_manager.get_distinct_items_from_database(
                        collection_name="user_status",
                        field="block",
                        condition=condition,
                    )
                ),
            ):
                result.append(item)

        return result