import contextlib
import sys
import logging
import uuid
import tqdm
import concurrent.futures

from bson.decimal128 import Decimal128
from decimal import Decimal, getcontext
from datetime import datetime, timedelta

from ..configuration import CONFIGURATION
from ..general.general_utilities import log_execution_time
from ..database.common.db_collections_common import database_local, database_global

from ..converters.onchain import convert_hypervisor_fromDict
from datetime import timezone

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
            and a not in blist
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
            and a not in blist
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
            and a not in blist
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

        # init rewarders masterchefs
        self._rewarders_list = []
        self._rewarders_lastTime_update = None

        # control var (itemsprocessed): list of operation ids processed
        self.ids_processed = []
        # control var time order :  last block always >= current
        self.last_block_processed: int = 0

    # setup
    def _get_static_data(self):
        """_load hypervisor's static data from database"""
        # static
        try:
            return self.local_db_manager.get_items_from_database(
                collection_name="static", find={"id": self.address}
            )[0]
        except IndexError:
            raise ValueError(f"Static data not found for {self.address}")

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
    def rewarders_list(self) -> list:
        """Masterchef addresses that reward this hypervisor

        Returns:
            list: rewarders addresses
        """
        # get rewarders addresses from database every 20 minutes
        if (
            self._rewarders_lastTime_update is None
            or (datetime.utcnow() - self._rewarders_lastTime_update).total_seconds()
            >= 1200
        ):
            # update rewarders
            self._rewarders_list = (
                self.local_db_manager.get_distinct_items_from_database(
                    collection_name="rewards_static",
                    field="address",
                    condition={"hypervisor_address": self._hypervisor_address},
                )
            )

            # add custom rewarders
            if self.network == "polygon":
                self._rewarders_list += [
                    "0xf099FA1Bd92f8AAF4886e8927D7bd3c15bA0BbFd".lower(),  # Masterchef poly Quickswap
                    "0x158B99aE660D4511e4c52799e1c47613cA47a78a".lower(),  # dQuick poly Quickswap
                    "0xFa3deAFecd0Fad0b083AB050cF30E1d541720680".lower(),  # wmatic poly Quickswap
                    "0xc35f556C8Ac05FB484A703eE96A2f997F8CAC957".lower(),  # dQuick poly Quickswap
                    "0x9bA5cE366f99f2C10A38ED35159c60CC558ca626".lower(),  # ankr poly Quickswap
                    "0x5554EdCaf47189894315d845D5B19eeB14D79048".lower(),  # ghst poly Quickswap
                    "0x780e2496141f97dd48ed8cb296f5c0828f1cb317".lower(),  # ldo poly Quickswap
                    "0xae361ab6c12e9c8aff711d3ddc178be6da2a7472".lower(),  # gddy poly Quickswap
                    "0xB6985ce301E6C6c4766b4479dDdc152d4eD0f2d3".lower(),  # davos poly Quickswap
                    "0xC84Ec966b4E6473249d64763366212D175b5c2bd".lower(),  # wmatic2 poly Quickswap
                    "0x7636e51D352cf89a7A05aE7b66f8841c368080Ff".lower(),  # lcd poly Quickswap
                    "0x88F54579fbB2a33c33342709A4B2a9A07dA94EE2".lower(),  # Axl poly Quickswap
                    "0xF5052aD621D4fb273b752536A2625Ae0Fea3eb0E".lower(),  # Wombat poly Quickswap
                    "0x570d60a60baa356d47fda3017a190a48537fcd7d".lower(),  # Masterchef poly uniswapv3
                    "0xdb909073b3815a297024db0d72fafdfc6db5a7b7".lower(),  # Ankr poly Uniswapv3
                    "0x4d7a374fce77eec67b3a002549a3a49deec9307c".lower(),  # Davos poly Uniswapv3
                    "0x68678Cf174695fc2D27bd312DF67A3984364FFDd".lower(),  # Masterchef Quickswap
                    "0x43e867915E4fBf7e3648800bF9bB5A4Bc7A49F37".lower(),  # Giddy poly Quickswap
                ]
            elif self.network == "optimism":
                self._rewarders_list += [
                    "0xc7846d1bc4d8bcf7c45a7c998b77ce9b3c904365".lower(),  # Masterchef Optimism uniswapv3
                    "0xf099FA1Bd92f8AAF4886e8927D7bd3c15bA0BbFd".lower(),  # uniswapv3
                    "0xAfBB6c1a235e105e568CCD4FD915dfFF76C415E1".lower(),  # op labs uniswapv3
                ]
            elif self.network == "arbitrum":
                self._rewarders_list += [
                    "0x72E4CcEe48fB8FEf18D99aF2965Ce6d06D55C8ba".lower(),  # Masterchef Arbitrum zyberswap
                    "0x9BA666165867E916Ee7Ed3a3aE6C19415C2fBDDD".lower(),  # Masterchef Arbitrum zyberswap
                ]
            elif self.network == "binance":
                self._rewarders_list += [
                    "0x374cc2276b842fecd65af36d7c60a5b78373ede1".lower(),  # Voter Binance thena
                    "0x3a1d0952809f4948d15ebce8d345962a282c4fcb".lower(),  #  thena_voter_v3 Binance thena
                ]

            # update time
            self._rewarders_lastTime_update = datetime.utcnow()
        # return rewarders
        return self._rewarders_list

    @property
    def local_db_manager(self) -> database_local:
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
    @log_execution_time
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
                collection_name="status",
                find=find,
                projection={"block": 1},
                sort=sort,
                limit=limit,
            )[0]["block"]
        except Exception:
            logging.getLogger(__name__).exception(
                " Unexpected error quering first status block. Zero returned"
            )
            return 0

    @property
    @log_execution_time
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
                collection_name="status",
                find=find,
                projection={"block": 1},
                sort=sort,
                limit=limit,
            )[0]["block"]
        except Exception:
            logging.getLogger(__name__).exception(
                " Unexpected error quering latest status block. Zero returned"
            )
            return 0

    @property
    @log_execution_time
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
                collection_name="user_status",
                find=find,
                projection={"block": 1},
                sort=sort,
                limit=limit,
            )[0]["block"]
        except IndexError:
            # no user status for address found in db
            return 0
        except Exception:
            logging.getLogger(__name__).exception(
                " Unexpected error quering first user block. Zero returned"
            )
            return 0

    @property
    @log_execution_time
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
                collection_name="user_status",
                find=find,
                projection={"block": 1},
                sort=sort,
                limit=limit,
            )[0]["block"]

        except IndexError:
            # no user status for address found in db
            return 0
        except Exception:
            logging.getLogger(__name__).exception(
                " Unexpected error quering latest user block. Zero returned"
            )
            return 0

    @log_execution_time
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
                projection={"totalSupply": 1, "decimals": 1},
            )[0]
            return Decimal(tmp_status["totalSupply"]) / Decimal(
                10 ** tmp_status["decimals"]
            )
        except Exception:
            logging.getLogger(__name__).exception(
                " Unexpected error quering totalSupply. Zero returned"
            )
            return Decimal("0")

    @log_execution_time
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
            # find["address"] = {"$not": {"$regex": exclude_address}}
            find["address"] = {"$ne": exclude_address}

        # build query
        query = [
            {
                "$project": {
                    "block": "$block",
                    "logIndex": "$logIndex",
                    "address": "$address",
                    "hypervisor_address": "$hypervisor_address",
                    "shares_qtty": "$shares_qtty",
                    "shares_percent": "$shares_percent",
                }
            },
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
        debug_query = f"{query}"
        try:
            db_result = self.local_db_manager.get_items_from_database(
                collection_name="user_status", aggregate=query
            )[0]
            # convert to decimals and return
            return self.local_db_manager.convert_d128_to_decimal(db_result)[
                "shares_qtty"
            ]
        except IndexError:
            if not exclude_address:
                logging.getLogger(__name__).exception(
                    f" [{self.network}] Unexpected error calc total shares (exclude_address is set) --> query:  {query} "
                )

                # may just be that there are no shares besides current excluded address
        except Exception:
            logging.getLogger(__name__).exception(
                f" [{self.network}] Unexpected error calc total shares--> query:  {query} "
            )

        return Decimal("0")

    @log_execution_time
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
            with_shares=False,
            block_condition=block_condition,
            logIndex_condition=logIndex_condition,
        ):
            total["token0"] += status.fees_collected_token0
            total["token1"] += status.fees_collected_token1

        return total

    @log_execution_time
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
                collection_name="status",
                find=find,
                projection={"totalAmounts": 1},
                sort=sort,
                limit=limit,
            )[0]
            # return not calculated field 'totalAmounts' as tvl
            return {
                "token0": Decimal(data["totalAmounts"]["total0"]),
                "token1": Decimal(data["totalAmounts"]["total1"]),
            }
        except Exception:
            logging.getLogger(__name__).exception(
                " Unexpected error quering tvl. Zero returned"
            )
            return {
                "token0": Decimal("0"),
                "token1": Decimal("0"),
            }

    @log_execution_time
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

    @log_execution_time
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

    @log_execution_time
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
        except Exception:
            return None

    @log_execution_time
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
                    collection_name="user_status", find=find, sort=sort
                )
            ]
        except Exception:
            logging.getLogger(__name__).exception(
                " Unexpected error quering user accounts result list. Zero returned"
            )
            return []

    @log_execution_time
    def _create_operations_to_process_old(self) -> list[dict]:
        # at least all operation blocks shall be processed. One operation block missing will cause a gap in the user status

        # get all blocks from user status ( what has already been done [ careful because last  =  block + logIndex ] )
        user_status_blocks_processed = sorted(
            self.local_db_manager.get_distinct_items_from_database(
                collection_name="user_status",
                field="block",
                condition={"hypervisor_address": self.address},
            )
        )
        # substract last couple of blocks to make sure db data has not been left between the same block and different logIndexes
        if len(user_status_blocks_processed) > 1:
            try:
                user_status_blocks_processed = set(user_status_blocks_processed[:-2])
            except Exception:
                logging.getLogger(__name__).error(
                    f" Unexpected error slicing block array while creating operations to process. array length: {len(user_status_blocks_processed)}"
                )
        else:
            # reset and do it all from zero
            user_status_blocks_processed = set()

        # result = self.get_filtered_hypervisor_operations(
        #     exclude_blocks=list(user_status_blocks_processed)
        # )

        result = self.get_hypervisor_operations(
            initial_block=self._get_initial_block_to_process()
        )

        # control var
        initial_length = len(result)

        if initial_length == 0:
            logging.getLogger(__name__).debug(
                f" There are no hypervisor {self.address} operations to process. Zero returned"
            )
            return []

        # loop over result and filter:
        # blocks_to_process = set()  # construct the todo block list
        # excluded_operations_qtty = 0  # count excluded operations
        # index = 0
        # while index < len(result):
        #     x = result[index]
        #     if (
        #         x["topic"] in ["zeroBurn", "rebalance"]
        #         and x["qtty_token0"] == "0"
        #         and x["qtty_token1"] == "0"
        #     ):
        #         # remove operations with zeroBurn and rebalance topics that do not have qtty to share with users ( they only exist to rebalance the pool )
        #         del result[index]
        #         excluded_operations_qtty += 1
        #     else:
        #         # add block to process
        #         blocks_to_process.add(x["blockNumber"])
        #         index += 1

        # if excluded_operations_qtty:
        #     logging.getLogger(__name__).info(
        #         "   {} fee related operations had zero qtty and were excluded. {:,.0%} of total".format(
        #             excluded_operations_qtty, excluded_operations_qtty / initial_length
        #         )
        #     )

        blocks_to_process = set([x["blockNumber"] for x in result])

        # control var
        # initial_length = len(result)

        # mix blocks to process ( extracted from operations)
        combined_blocks = list(blocks_to_process) + list(user_status_blocks_processed)

        # add report dummy operations once every day
        for hype_status in sorted(
            self.get_hypervisor_status_byDay(),
            key=lambda x: (x["block"]),
            reverse=False,
        ):
            if (
                hype_status["block"] not in blocks_to_process
                and hype_status["block"] not in user_status_blocks_processed
            ):
                # discard close blocks ( block <30> block )
                _closest = min(
                    combined_blocks,
                    key=lambda x: abs(x - hype_status["block"]),
                )
                if abs(_closest - hype_status["block"]) < 30:
                    # discard block close to a block to process
                    logging.getLogger(__name__).debug(
                        f' Block {hype_status["block"]} has not been included in {self.address} [{self.symbol}] user status creation because it is {_closest - hype_status["block"]} blocks close to (already/to be) processed block'
                    )

                    continue
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

        logging.getLogger(__name__).info(
            f" Added {len(result)-initial_length} report operations from status blocks. Processed: {len(user_status_blocks_processed)} -> total to process {len(result)}"
        )
        # return sorted by block->logindex
        return sorted(result, key=lambda x: (x["blockNumber"], x["logIndex"]))

    @log_execution_time
    def _create_operations_to_process(self, rewrite: bool | None = None) -> list[dict]:
        # get hypes operations from the last block processed

        if initial_block := self._get_initial_block_to_process(rewrite=rewrite):
            if result := self.get_hypervisor_operations(initial_block=initial_block):
                logging.getLogger(__name__).info(
                    f" Found {len(result)} operations to process for {self.network}'s {self.address} starting from block {initial_block or '-'}"
                )
                return sorted(result, key=lambda x: (x["blockNumber"], x["logIndex"]))

        logging.getLogger(__name__).debug(
            f" There are no operations to process for {self.network}'s {self.address}"
        )
        return []

    def _get_initial_block_to_process(self, rewrite: bool | None = None) -> int | None:
        # at least all operation blocks shall be processed. One operation block missing will cause a gap in the user status

        try:
            if rewrite:
                user_status_blocks_processed = []
            else:
                # get all blocks from user status ( what has already been done [ careful because last  =  block + logIndex ] )
                user_status_blocks_processed = sorted(
                    self.local_db_manager.get_distinct_items_from_database(
                        collection_name="user_status",
                        field="block",
                        condition={"hypervisor_address": self.address},
                    )
                )
                # substract 1 block to make sure db data has not been left between the same block and different logIndexes
                if len(user_status_blocks_processed) > 1:
                    try:
                        user_status_blocks_processed = user_status_blocks_processed[:-1]
                    except Exception:
                        logging.getLogger(__name__).error(
                            f" Unexpected error slicing block array while creating operations to process. array length: {len(user_status_blocks_processed)}"
                        )
                else:
                    # reset and do it all from zero
                    user_status_blocks_processed = []

            # get all operation blocks not already processed and not empty ( fee and transfer related operations)
            if operation_blocks := sorted(
                self.local_db_manager.get_distinct_items_from_database(
                    collection_name="operations",
                    field="blockNumber",
                    condition={
                        "address": self.address,
                        "blockNumber": {"$nin": user_status_blocks_processed},
                        "qtty_token0": {"$ne": "0"},
                        "qtty_token1": {"$ne": "0"},
                        "src": {"$ne": "0x0000000000000000000000000000000000000000"},
                        "dst": {"$ne": "0x0000000000000000000000000000000000000000"},
                        "topic": {
                            "$in": [
                                "transfer",
                                "deposit",
                                "withdraw",
                                "rebalance",
                                "zeroBurn",
                            ]
                        },
                    },
                )
            ):
                # return first operation block to process
                return operation_blocks[0]

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error getting initial block to process for {self.address} {self.symbol} {e}"
            )

        return None

    def _process_operations(self, rewrite: bool = False):
        """process all operations"""

        # mix operations with status blocks ( status different than operation's)
        operations_to_process = self._create_operations_to_process(rewrite=rewrite)

        _errors = 0
        with tqdm.tqdm(total=len(operations_to_process), leave=False) as progress_bar:
            for operation in operations_to_process:
                # progress show
                progress_bar.set_description(
                    f' processing 0x..{operation["address"][-4:]}  {operation["blockNumber"]}  {operation["topic"]}'
                )
                progress_bar.refresh()

                if operation["id"] not in self.ids_processed:
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

    @log_execution_time
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
    @log_execution_time
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

        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, block)

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

    @log_execution_time
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

        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, block)

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
        with contextlib.suppress(Exception):
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
        # add global stats
        new_user_status = self._add_globals_to_user_status(
            current_user_status=new_user_status,
            last_user_status_item=last_op,
            price_usd_t0=price_usd_t0,
            price_usd_t1=price_usd_t1,
        )

        # result
        return new_user_status

    @log_execution_time
    def _process_transfer(self, operation: dict) -> tuple[user_status, user_status]:
        if operation["dst"] == "0x0000000000000000000000000000000000000000":
            # expect a withdraw topic on next operation ( same block))
            # do nothing
            pass
        elif operation["src"] == "0x0000000000000000000000000000000000000000":
            # expect a deposit topic on next operation ( same block)
            # do nothing
            pass
        else:
            # check if transfer is to/from a rewarder
            if operation["dst"].lower() in self.rewarders_list:
                # TODO: staking into masterchef implementation
                pass
            elif operation["src"].lower() in self.rewarders_list:
                # TODO: unstaking out of masterchef implementation
                pass
            else:
                # transfer all values to other user address
                return self._transfer_to_user(operation=operation)

        # result
        return None, None

    @log_execution_time
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

    @log_execution_time
    def _process_approval(self, operation: dict):
        # TODO: approval
        pass

    @log_execution_time
    def _process_zeroBurn(self, operation: dict):
        # share fees with all acoounts proportionally
        self._share_fees_with_acounts(operation)

    @log_execution_time
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

        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, block)

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
    @log_execution_time
    def _add_user_status(self, status: user_status):
        """add user status to database

        Args:
            status (user_status):
        """
        if status.address not in self.__blacklist_addresses:
            # add status to database
            self.local_db_manager.set_user_status(
                self.convert_user_status_toDb(status=status)
            )

        elif status.address != "0x0000000000000000000000000000000000000000":
            logging.getLogger(__name__).debug(
                f"Not adding blacklisted account {status.address} user status"
            )

    @log_execution_time
    def _add_user_status_bulk(self, statuses: list[user_status]):
        """add user status to database all at once

        Args:
            status (user_status):
        """
        # add status to database
        self.local_db_manager.set_user_status_bulk(statuses)

    # General helpers
    @log_execution_time
    def _add_globals_to_user_status(
        self,
        current_user_status: user_status,
        last_user_status_item: user_status,
        price_usd_t0: Decimal = Decimal("0"),
        price_usd_t1: Decimal = Decimal("0"),
        current_status_data: dict | None = None,
        total_shares: Decimal = Decimal("0"),
    ) -> user_status:
        if current_status_data is None:
            current_status_data = {}
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
        if not current_status_data:
            current_status_data = self.get_hypervisor_status(
                block=current_user_status.block
            )
            if len(current_status_data) == 0:
                raise ValueError(
                    f" No hypervisor status found for {self.network}'s {self.address} at block {current_user_status.block}. Solve to continue."
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

        if last_user_status_item.block != 0:
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

    @log_execution_time
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
        except Exception:
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

    @log_execution_time
    def last_user_status_list(
        self,
        block: int = 0,
        logIndex: int = 0,
        block_condition: str = "$lte",
        logIndex_condition: str = "$lt",
        with_shares: bool = False,
        blacklist_addresses: list = [],
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

        if blacklist_addresses:
            find["address"] = {"$nin": blacklist_addresses}

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
        # with shares filter: last user status should have shares
        if with_shares:
            find["shares_qtty"] = {"$gt": 0}

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

    @log_execution_time
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

    @log_execution_time
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

    @log_execution_time
    def get_filtered_hypervisor_operations(
        self, block: int = 0, exclude_blocks: list[str] | None = None
    ) -> list[dict]:
        """Get a filtered list of hypervisor operations ordered by block (desc)
              filter-> exclude zero qtty operations and destination/source zero addresses
        Args:
            block (int, optional): . Defaults to 0.
            exclude_blocks (list[str] | None, optional): . List of blocks to exclude from query Defaults to None.

        Returns:
            list[dict]:
        """

        # if operation["dst"] == "0x0000000000000000000000000000000000000000":

        # build find and sort
        find = {
            "address": self.address.lower(),
            "qtty_token0": {"$ne": "0"},
            "qtty_token1": {"$ne": "0"},
            "src": {"$ne": "0x0000000000000000000000000000000000000000"},
            "dst": {"$ne": "0x0000000000000000000000000000000000000000"},
        }
        if block != 0:
            find["blockNumber"] = block
        elif exclude_blocks:
            find["blockNumber"] = {"$nin": exclude_blocks}
        sort = [("blockNumber", 1), ("logIndex", 1)]

        return self.local_db_manager.get_items_from_database(
            collection_name="operations", find=find, sort=sort
        )

    @log_execution_time
    def get_hypervisor_operations(self, initial_block: int | None) -> list[dict]:
        """Get all hypervisor operations from block to latest
            Discard any topic not in ["transfer","deposit","withdraw","rebalance","zeroBurn"]
        Args:
            initial_block (int, optional): The block from where to start. Defaults to 0.

        Returns:
            list[dict]:
        """

        # if operation["dst"] == "0x0000000000000000000000000000000000000000":

        # build find and sort
        find = {
            "address": self.address.lower(),
            "qtty_token0": {"$ne": "0"},
            "qtty_token1": {"$ne": "0"},
            "src": {"$ne": "0x0000000000000000000000000000000000000000"},
            "dst": {"$ne": "0x0000000000000000000000000000000000000000"},
            "topic": {
                "$in": ["transfer", "deposit", "withdraw", "rebalance", "zeroBurn"]
            },
        }
        if initial_block:
            find["blockNumber"] = {"$gte": initial_block}
        sort = [("blockNumber", 1), ("logIndex", 1)]

        # debug_query = f"{find}"

        return self.local_db_manager.get_items_from_database(
            collection_name="operations", find=find, sort=sort
        )

    @log_execution_time
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
            convert_hypervisor_fromDict(hypervisor=x, toDecimal=True)
            for x in self.local_db_manager.get_items_from_database(
                collection_name="status", find=find, sort=sort
            )
        ]

    @log_execution_time
    def get_hypervisor_status_byDay(self) -> list[dict]:
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
        return [
            x["status"]
            for x in self.local_db_manager.query_items_from_database(
                collection_name="status", query=query
            )
        ]

    @log_execution_time
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
                collection_name="operations",
                find=find,
                projection={"logIndex": 1},
                sort=sort,
                limit=limit,
            )[0]["logIndex"]
        except Exception:
            logging.getLogger(__name__).exception(
                " Unexpected error quering last logIndex. Zero returned"
            )
            return 0

    @log_execution_time
    def get_price(self, block: int, address: str) -> Decimal:
        # TODO: remove this when global db is ready
        try:
            return Decimal(self._prices[block][address])
        except Exception:
            # price is not found in cached data.
            pass

        # Try to get it from global db
        global_db_manager = database_global(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
        )
        try:
            return Decimal(
                global_db_manager.get_price_usd(
                    network=self.network, block=block, address=address
                )[0]["price"]
            )
        except Exception:
            logging.getLogger(__name__).error(
                f" Can't find {self.network}'s {self.address} usd price for {address} at block {block}. Return Zero"
            )
            Decimal("0")

    # Transformers
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
                and a not in fields_excluded
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
            and a not in fields_excluded
        ]:
            setattr(result, p, status[p])

        return result

    # threaded shares share
    def _share_fees_with_acounts(self, operation: dict):
        # block
        block = operation["blockNumber"]

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
        if total_shares == Decimal("0"):
            # there is no deposits yet... hypervisor is in testing or seting up mode
            if fees_collected_token0 == fees_collected_token1 == Decimal("0"):
                logging.getLogger(__name__).debug(
                    f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet and collected fees are zero"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet but fees collected fees are NON zero --> token0: {fees_collected_token0}  token1: {fees_collected_token1}"
                )
            # exit
            return
        if fees_collected_token0 == fees_collected_token1 == Decimal("0"):
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

        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, block)

        current_status_data = self.get_hypervisor_status(block=block)
        if len(current_status_data) == 0:
            raise ValueError(
                f" No hypervisor status found for {self.network}'s {self.address} at block {block}"
            )
        current_status_data = current_status_data[0]

        # control var to keep track of total percentage applied
        ctrl_total_percentage_applied = Decimal("0")
        ctrl_total_shares_applied = Decimal("0")

        last_status_list = self.last_user_status_list(
            block=block,
            logIndex=operation["logIndex"],
            with_shares=True,
            blacklist_addresses=self.__blacklist_addresses,
        )

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

            # convert user status to dict
            new_user_status_converted = self.convert_user_status_toDb(
                status=new_user_status
            )

            # return
            return new_user_status, user_share, new_user_status_converted

        # create list to store results
        user_status_list = []

        # go thread all: Get all user status with shares to share fees with
        with concurrent.futures.ThreadPoolExecutor() as ex:
            for result_status, result_user_share, result_converted in ex.map(
                loop_share_fees,
                last_status_list,
            ):
                if result_converted:
                    # apply result
                    ctrl_total_shares_applied += result_status.shares_qtty
                    # add user share to total processed control var
                    ctrl_total_percentage_applied += result_user_share
                    # add new status to hypervisor
                    # self._add_user_status(status=result_status)

                    # add to list
                    user_status_list.append(result_converted)

        # add resulting user status to mongo database in bulk
        if user_status_list:
            self._add_user_status_bulk(user_status_list)

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
                    " Only {:,.2f} of the fees value has been distributed to current accounts. remainder: {}   tot.shares: {}  remainder usd: {}  block:{}".format(
                        ctrl_total_percentage_applied,
                        (Decimal("1") - ctrl_total_percentage_applied),
                        ctrl_total_shares_applied,
                        feeUsd_remainder,
                        block,
                    )
                )

    def _share_fees_with_acounts_oneByOne(self, operation: dict):
        # block
        block = operation["blockNumber"]

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
        if total_shares == Decimal("0"):
            # there is no deposits yet... hypervisor is in testing or seting up mode
            if fees_collected_token0 == fees_collected_token1 == Decimal("0"):
                logging.getLogger(__name__).debug(
                    f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet and collected fees are zero"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet but fees collected fees are NON zero --> token0: {fees_collected_token0}  token1: {fees_collected_token1}"
                )
            # exit
            return
        if fees_collected_token0 == fees_collected_token1 == Decimal("0"):
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

        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, block)

        current_status_data = self.get_hypervisor_status(block=block)
        if len(current_status_data) == 0:
            raise ValueError(
                f" No hypervisor status found for {self.network}'s {self.address} at block {block}"
            )
        current_status_data = current_status_data[0]

        # control var to keep track of total percentage applied
        ctrl_total_percentage_applied = Decimal("0")
        ctrl_total_shares_applied = Decimal("0")

        last_status_list = self.last_user_status_list(
            block=block, logIndex=operation["logIndex"], with_shares=True
        )

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

            # add new status to hypervisor
            self._add_user_status(status=new_user_status)

            # return
            return new_user_status, user_share

        # go thread all: Get all user status with shares to share fees with
        with concurrent.futures.ThreadPoolExecutor() as ex:
            for result_status, result_user_share, result_converted in ex.map(
                loop_share_fees,
                last_status_list,
            ):
                # apply result
                ctrl_total_shares_applied += result_status.shares_qtty
                # add user share to total processed control var
                ctrl_total_percentage_applied += result_user_share

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

            # log error if value is significant
            if (Decimal("1") - ctrl_total_percentage_applied) > Decimal("0.0001"):
                logging.getLogger(__name__).error(
                    " Only {:,.2f} of the fees value has been distributed to current accounts. remainder: {}   tot.shares: {}  remainder usd: {}  block:{}".format(
                        ctrl_total_percentage_applied,
                        (Decimal("1") - ctrl_total_percentage_applied),
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

        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, block)

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

        # go thread all: only users with shares
        with concurrent.futures.ThreadPoolExecutor() as ex:
            for new_user_status in ex.map(
                loop_process_report,
                self.last_user_status_list(
                    block=block, logIndex=operation["logIndex"], with_shares=True
                ),
            ):
                # add to result
                self._add_user_status(status=new_user_status)

    # Results

    def result_list(self, b_ini: int = 0, b_end: int = 0) -> list[user_status]:
        """Hypervisor results between the specified blocks

        Args:
            b_ini (int, optional): initial block. Defaults to 0.
            b_end (int, optional): end block. Defaults to 0.

        Returns:
            list[user_status]: of hypervisor status at blocks
        """
        result = []
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
            result.extend(
                iter(
                    ex.map(
                        self.result,
                        sorted(
                            self.local_db_manager.get_distinct_items_from_database(
                                collection_name="user_status",
                                field="block",
                                condition=condition,
                            )
                        ),
                    )
                )
            )

        return result

    def result_list_timestamp(
        self, t_ini: int = 0, t_end: int = 0
    ) -> list[user_status]:
        """Hypervisor results between the specified timestamps

        Args:
            t_ini (int, optional): initial timestamp. Defaults to 0.
            t_end (int, optional): end timestamp. Defaults to 0.

        Returns:
            list[user_status]: of hypervisor status (using user_status class)
        """
        result = []
        if t_end == 0:
            t_end = datetime.now(timezone.utc).timestamp()
        if t_ini == 0:
            t_ini = t_end - timedelta(days=1)

        # define alowed topics to return ( be aware that blocks not in predefined topics list may not have all addresses in the block, underreporting values)
        topics = ["report", "zeroBurn", "rebalance"]
        # get a unique block list of all hype users status
        condition = {
            "hypervisor_address": self.address,
            "$and": [
                {"timestamp": {"$gte": t_ini}},
                {"timestamp": {"$lte": t_end}},
                {"topic": {"$in": topics}},
            ],
        }

        # go thread all
        with concurrent.futures.ThreadPoolExecutor() as ex:
            result.extend(
                iter(
                    ex.map(
                        self.result,
                        sorted(
                            self.local_db_manager.get_distinct_items_from_database(
                                collection_name="user_status",
                                field="block",
                                condition=condition,
                            )
                        ),
                    )
                )
            )

        return result

    def sumary_result(self, t_ini: int = 0, t_end: int = 0) -> dict():
        status_list = self.result_list_timestamp(t_ini=t_ini, t_end=t_end)

        return self._compare_status(
            ini_status=status_list[0], end_status=status_list[-1]
        )

    def _compare_status(
        self, ini_status: user_status, end_status: user_status
    ) -> dict():
        days_passed = (end_status.timestamp - ini_status.timestamp) / (60 * 60 * 24)

        # value of investment at the beguining of the period
        ini_investment = (
            ini_status.underlying_token0 * ini_status.usd_price_token0
        ) + (ini_status.underlying_token1 * ini_status.usd_price_token1)
        # add additional investments between periods using the usd value at the time it happened
        ini_investment += (
            end_status.total_investment_qtty_in_usd
            - ini_status.total_investment_qtty_in_usd
        )

        # value of investment at the end of the period
        end_investment = (
            end_status.underlying_token0 * end_status.usd_price_token0
        ) + (end_status.underlying_token1 * end_status.usd_price_token1)

        # fees earned at initial price ( including fees divested). This avoids negative fees due to conversion but needs aditional info ( price variation effect)
        fees_earned = (
            (
                end_status.fees_collected_token0
                + end_status.fees_owed_token0
                + end_status.fees_uncollected_token0
                + end_status.divestment_fee_qtty_token0
                - ini_status.fees_collected_token0
                - ini_status.fees_owed_token0
                - ini_status.fees_uncollected_token0
                - ini_status.divestment_fee_qtty_token0
            )
            * ini_status.usd_price_token0
        ) + (
            (
                end_status.fees_collected_token1
                + end_status.fees_owed_token1
                + end_status.fees_uncollected_token1
                + end_status.divestment_fee_qtty_token1
                - ini_status.fees_collected_token1
                - ini_status.fees_owed_token1
                - ini_status.fees_uncollected_token1
                - ini_status.divestment_fee_qtty_token1
            )
            * ini_status.usd_price_token1
        )

        # value of token price variation affecting the position
        price_variation = (
            end_status.usd_price_token0 - ini_status.usd_price_token0
        ) * ini_status.underlying_token0 + (
            end_status.usd_price_token1 - ini_status.usd_price_token1
        ) * ini_status.underlying_token1

        # to be defined: change of asset allocation effect on position
        asset_alloc_variation = (
            (
                (ini_status.underlying_token0 * ini_status.usd_price_token0)
                / ini_investment
            )
            - (
                (end_status.underlying_token0 * end_status.usd_price_token0)
                / end_investment
            )
        ) * (end_status.usd_price_token0 - ini_status.usd_price_token0)
        asset_alloc_variation += (
            (
                (ini_status.underlying_token1 * ini_status.usd_price_token1)
                / ini_investment
            )
            - (
                (end_status.underlying_token1 * end_status.usd_price_token1)
                / end_investment
            )
        ) * (end_status.usd_price_token1 - ini_status.usd_price_token1)

        return {
            "date_from": ini_status.timestamp,
            "date_to": end_status.timestamp,
            "period": days_passed,
            "block_ini": ini_status.block,
            "block_end": end_status.block,
            "current_return": (end_investment - ini_investment),
            "current_return_percent": (end_investment - ini_investment)
            / ini_investment,
            "yearly_fees": (fees_earned / Decimal(days_passed)) * 365,
            "feeAPY": ((fees_earned / Decimal(days_passed)) / ini_investment + 1)
            ** (365)
            - 1,
            "feeAPR": ((fees_earned / Decimal(days_passed)) / ini_investment) * 365,
            "info_fees_earned": fees_earned,
            "info_price_variation": price_variation,
            "info_asset_alloc_variation": asset_alloc_variation,
            "info_ini_investment": ini_investment,
            "info_end_investment": end_investment,
        }

    # Checks and Error reporting
    def _check_prices(self, price0, price1, block: int):
        """Raise Value error on price0 or price1 not available

        Args:
            price0 (_type_):
            price1 (_type_):
            block (int):

        Raises:
            ValueError:
        """

        message = None
        # check if prices are available
        if not price0 and not price1:
            message = f" Prices not available for {self.network}'s {self._static['pool']['token0']['address']} and {self._static['pool']['token1']['address']} token addresses at {block} block. Solve to continue."
        elif not price0:
            message = f" Price not available for {self.network}'s {self._static['pool']['token0']['address']} token address at {block} block. Solve to continue."
        elif not price1:
            message = f" Price not available for {self.network}'s {self._static['pool']['token1']['address']} token address at {block} block. Solve to continue."

        if message:
            raise ValueError(message)


class user_status_hypervisor_builderV2:
    def __init__(self, network: str, protocol: str):
        """Fast forward emulate gamma hypervisor contract using database data

        Args:
            hypervisor_address (str):
            network (str):
            protocol (str):
            t_ini (int): initial timestamp
            t_end (int): end timestamp
        """

        # set global vars
        self._network = network
        self._protocol = protocol

        self.__blacklist_addresses = ["0x0000000000000000000000000000000000000000"]

        self.batch_size = 50000

        # init rewarders masterchefs
        self._rewarders_list = []
        self._rewarders_lastTime_update = None

    @property
    def local_db_manager(self) -> database_local:
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{self.network}_{self.protocol}"
        return database_local(mongo_url=mongo_url, db_name=db_name)

    @property
    def network(self) -> str:
        return self._network

    @property
    def protocol(self) -> str:
        return self._protocol

    def rewarders_list(self, hypervisor_address: str) -> list:
        """Masterchef addresses that reward this hypervisor

        Returns:
            list: rewarders addresses
        """
        # get rewarders addresses from database every 20 minutes
        if (
            self._rewarders_lastTime_update is None
            or (datetime.utcnow() - self._rewarders_lastTime_update).total_seconds()
            >= 1200
        ):
            # update rewarders
            self._rewarders_list = (
                self.local_db_manager.get_distinct_items_from_database(
                    collection_name="rewards_static",
                    field="address",
                    condition={"hypervisor_address": hypervisor_address},
                )
            )

            # add custom rewarders
            if self.network == "polygon":
                self._rewarders_list += [
                    "0xf099FA1Bd92f8AAF4886e8927D7bd3c15bA0BbFd".lower(),  # Masterchef poly Quickswap
                    "0x158B99aE660D4511e4c52799e1c47613cA47a78a".lower(),  # dQuick poly Quickswap
                    "0xFa3deAFecd0Fad0b083AB050cF30E1d541720680".lower(),  # wmatic poly Quickswap
                    "0xc35f556C8Ac05FB484A703eE96A2f997F8CAC957".lower(),  # dQuick poly Quickswap
                    "0x9bA5cE366f99f2C10A38ED35159c60CC558ca626".lower(),  # ankr poly Quickswap
                    "0x5554EdCaf47189894315d845D5B19eeB14D79048".lower(),  # ghst poly Quickswap
                    "0x780e2496141f97dd48ed8cb296f5c0828f1cb317".lower(),  # ldo poly Quickswap
                    "0xae361ab6c12e9c8aff711d3ddc178be6da2a7472".lower(),  # gddy poly Quickswap
                    "0xB6985ce301E6C6c4766b4479dDdc152d4eD0f2d3".lower(),  # davos poly Quickswap
                    "0xC84Ec966b4E6473249d64763366212D175b5c2bd".lower(),  # wmatic2 poly Quickswap
                    "0x7636e51D352cf89a7A05aE7b66f8841c368080Ff".lower(),  # lcd poly Quickswap
                    "0x88F54579fbB2a33c33342709A4B2a9A07dA94EE2".lower(),  # Axl poly Quickswap
                    "0xF5052aD621D4fb273b752536A2625Ae0Fea3eb0E".lower(),  # Wombat poly Quickswap
                    "0x570d60a60baa356d47fda3017a190a48537fcd7d".lower(),  # Masterchef poly uniswapv3
                    "0xdb909073b3815a297024db0d72fafdfc6db5a7b7".lower(),  # Ankr poly Uniswapv3
                    "0x4d7a374fce77eec67b3a002549a3a49deec9307c".lower(),  # Davos poly Uniswapv3
                    "0x68678Cf174695fc2D27bd312DF67A3984364FFDd".lower(),  # Masterchef Quickswap
                    "0x43e867915E4fBf7e3648800bF9bB5A4Bc7A49F37".lower(),  # Giddy poly Quickswap
                ]
            elif self.network == "optimism":
                self._rewarders_list += [
                    "0xc7846d1bc4d8bcf7c45a7c998b77ce9b3c904365".lower(),  # Masterchef Optimism uniswapv3
                    "0xf099FA1Bd92f8AAF4886e8927D7bd3c15bA0BbFd".lower(),  # uniswapv3
                    "0xAfBB6c1a235e105e568CCD4FD915dfFF76C415E1".lower(),  # op labs uniswapv3
                ]
            elif self.network == "arbitrum":
                self._rewarders_list += [
                    "0x72E4CcEe48fB8FEf18D99aF2965Ce6d06D55C8ba".lower(),  # Masterchef Arbitrum zyberswap
                    "0x9BA666165867E916Ee7Ed3a3aE6C19415C2fBDDD".lower(),  # Masterchef Arbitrum zyberswap
                ]
            elif self.network == "binance":
                self._rewarders_list += [
                    "0x374cc2276b842fecd65af36d7c60a5b78373ede1".lower(),  # Voter Binance thena
                    "0x3a1d0952809f4948d15ebce8d345962a282c4fcb".lower(),  #  thena_voter_v3 Binance thena
                ]

            # update time
            self._rewarders_lastTime_update = datetime.utcnow()
        # return rewarders
        return self._rewarders_list

    def create_empty_userStatus(
        self, user_address: str, hypervisor_address: str
    ) -> dict:
        return {
            "user_address": user_address,
            "hypervisor_address": hypervisor_address,
            "hypervisor_total_shares": 0,
            "hypervisor_share_price_usd": 0,
            "block": 0,
            "timestamp": 0,
            "deposits_usd": 0,
            "deposits_token0": 0,
            "deposits_token1": 0,
            "withdraws_usd": 0,
            "withdraws_token0": 0,
            "withdraws_token1": 0,
            # "transfers_token0": 0,
            # "transfers_token1": 0,
            "fees_usd": 0,
            "fees_token0": 0,
            "fees_token1": 0,
            "underlying_usd": 0,
            "underlying_token0": 0,
            "underlying_token1": 0,
            "shares": 0,
            "token0_price_usd": 0,
            "token1_price_usd": 0,
        }

    def create_empty_networkUserStatus(self, user_address: str) -> dict:
        return {
            "user_address": user_address,
            "total_shares_usd_value": 0,
            "total_deposits_in_usd": 0,
            "total_withdraws_in_usd": 0,
            "total_fees_collected_in_usd": 0,
            "data": {},
        }

    @log_execution_time
    def calculate_status(
        self,
        user_address: str,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
    ):
        network_user_status = self.create_empty_networkUserStatus(
            user_address=user_address
        )

        # get all user operations with this hypervisor
        user_operations = self._get_user_operations(
            user_address=user_address,
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
        )

        with tqdm.tqdm(total=len(user_operations), leave=False) as progress_bar:
            for hypervisor_data in user_operations:
                hypervisor_address = hypervisor_data["hypervisor_address"]
                token0_address = hypervisor_data["user_operations"][0]["status"][
                    "pool"
                ]["token0"]["address"]
                token1_address = hypervisor_data["user_operations"][0]["status"][
                    "pool"
                ]["token1"]["address"]

                # get all available hype prices and place em into a  dict<block:address>
                prices = self._get_prices(
                    token0_address=token0_address, token1_address=token1_address
                )

                initial_deposit_timestamp = hypervisor_data["user_operations"][0][
                    "timestamp"
                ]

                # create an hype total user status
                hypervisor_user_status = self.create_empty_userStatus(
                    user_address=user_address, hypervisor_address=hypervisor_address
                )

                # get all fee operations and mix them with user operations
                if fee_operations := self._get_fee_operations(
                    hypervisor_address=hypervisor_address,
                    timestamp_ini=initial_deposit_timestamp,
                ):
                    hypervisor_data["mixed_operations"] = sorted(
                        hypervisor_data["user_operations"]
                        + fee_operations[0]["fees_operations"],
                        key=lambda x: x["blockNumber"],
                    )
                else:
                    # no fee operations
                    logging.getLogger(__name__).warning(
                        f" No fee operations for {self.network}'s {hypervisor_address} were found in database"
                    )
                    hypervisor_data["mixed_operations"] = hypervisor_data[
                        "user_operations"
                    ]

                # loop thu operations in order and calculate status
                for operation in hypervisor_data["mixed_operations"]:

                    def add_user_status(u_stat: dict):
                        # add user status to network status
                        if hypervisor_address not in network_user_status["data"]:
                            network_user_status["data"][hypervisor_address] = []

                        # add user to hypervisor status
                        for k, v in u_stat.items():
                            if k in [
                                "token0_price_usd",
                                "token1_price_usd",
                                "user_address",
                                "hypervisor_address",
                                "block",
                                "timestamp",
                                "shares",
                                "hypervisor_total_shares",
                                "hypervisor_share_price_usd",
                            ]:
                                hypervisor_user_status[k] = v

                            else:
                                hypervisor_user_status[k] += v

                        # add user to network status
                        network_user_status["data"][hypervisor_address].append(u_stat)

                    # progress show
                    progress_bar.set_description(
                        f' {operation["address"][-4:]}  {operation["blockNumber"]}  {operation["topic"]}'
                    )
                    progress_bar.refresh()

                    # setup new user status
                    user_status = self.create_empty_userStatus(
                        user_address=user_address, hypervisor_address=hypervisor_address
                    )

                    try:
                        user_status["block"] = operation["blockNumber"]
                        user_status["timestamp"] = operation["timestamp"]

                        user_status["token0_price_usd"] = prices[
                            operation["blockNumber"]
                        ][token0_address]
                        user_status["token1_price_usd"] = prices[
                            operation["blockNumber"]
                        ][token1_address]

                        # process operation
                        if operation["topic"] == "deposit":
                            user_status["deposits_token0"] += int(
                                operation["qtty_token0"]
                            ) / (10 ** operation["decimals_token0"])
                            user_status["deposits_token1"] += int(
                                operation["qtty_token1"]
                            ) / (10 ** operation["decimals_token1"])

                            # set shares as in operation
                            user_status["shares"] = int(operation["shares"]) / (
                                10 ** operation["decimals_contract"]
                            )

                            add_user_status(user_status)

                        elif operation["topic"] == "withdraw":
                            user_status["withdraws_token0"] += int(
                                operation["qtty_token0"]
                            ) / (10 ** operation["decimals_token0"])
                            user_status["withdraws_token1"] += int(
                                operation["qtty_token1"]
                            ) / (10 ** operation["decimals_token1"])

                            # set shares as in operation
                            user_status["shares"] = int(operation["shares"]) / (
                                10 ** operation["decimals_contract"]
                            )

                            add_user_status(user_status)

                        elif operation["topic"] == "transfer":
                            if (
                                operation["src"]
                                != "0x0000000000000000000000000000000000000000"
                                and operation["dst"]
                                != "0x0000000000000000000000000000000000000000"
                            ):
                                if operation["dst"].lower() in self.rewarders_list(
                                    hypervisor_address=hypervisor_address
                                ):
                                    # TODO: add rewarder logic
                                    pass
                                elif operation["src"].lower() in self.rewarders_list(
                                    hypervisor_address=hypervisor_address
                                ):
                                    # TODO: add rewarder logic
                                    pass
                                elif operation["src"].lower() == user_address.lower():
                                    # set shares as in operation
                                    user_status["shares"] -= int(operation["qtty"]) / (
                                        10 ** operation["decimals_contract"]
                                    )
                                elif operation["dst"].lower() == user_address.lower():
                                    # set shares as in operation
                                    user_status["shares"] += int(operation["qtty"]) / (
                                        10 ** operation["decimals_contract"]
                                    )

                                add_user_status(user_status)

                        elif (
                            operation["topic"] == "zeroBurn"
                            and hypervisor_user_status["shares"] > 0
                        ):
                            qtty_token0 = int(operation["qtty_token0"]) / (
                                10 ** operation["decimals_token0"]
                            )
                            qtty_token1 = int(operation["qtty_token1"]) / (
                                10 ** operation["decimals_token1"]
                            )

                            current_user_share = hypervisor_user_status["shares"] / (
                                int(operation["status"]["totalSupply"])
                                / (10 ** operation["decimals_contract"])
                            )

                            user_status["fees_token0"] += (
                                qtty_token0 * current_user_share
                            )
                            user_status["fees_token1"] += (
                                qtty_token1 * current_user_share
                            )

                            add_user_status(user_status)

                        elif (
                            operation["topic"] == "rebalance"
                            and hypervisor_user_status["shares"] > 0
                        ):
                            qtty_token0 = int(operation["qtty_token0"]) / (
                                10 ** operation["decimals_token0"]
                            )
                            qtty_token1 = int(operation["qtty_token1"]) / (
                                10 ** operation["decimals_token1"]
                            )

                            current_user_share = hypervisor_user_status["shares"] / (
                                int(operation["status"]["totalSupply"])
                                / (10 ** operation["decimals_contract"])
                            )

                            user_status["fees_token0"] += (
                                qtty_token0 * current_user_share
                            )
                            user_status["fees_token1"] += (
                                qtty_token1 * current_user_share
                            )

                            add_user_status(user_status)

                        else:
                            if hypervisor_user_status["shares"] > 0:
                                raise Exception(f"unknown topic {operation['topic']}")

                    except Exception as e:
                        logging.getLogger(__name__).error(
                            f"Error processing operation {operation} for {self.network}'s {hypervisor_address}: {e}"
                        )

                progress_bar.update(1)

        return network_user_status

    @log_execution_time
    def get_price(self, block: int, address: str) -> Decimal:
        # Try to get it from global db
        global_db_manager = database_global(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
        )
        try:
            return Decimal(
                global_db_manager.get_price_usd(
                    network=self.network, block=block, address=address
                )[0]["price"]
            )
        except Exception:
            logging.getLogger(__name__).error(
                f" Can't find {self.network}'s {self.address} usd price for {address} at block {block}. Return Zero"
            )
            Decimal("0")

    @log_execution_time
    def _get_prices(self, token0_address: str, token1_address: str) -> dict:
        """_load prices from database"""
        # database link
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        global_db_manager = database_global(mongo_url=mongo_url)

        # define query
        or_query = [
            {"address": token0_address},
            {"address": token1_address},
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

    @log_execution_time
    def _get_user_operations(
        self,
        user_address: str,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
    ) -> list[dict] | None:
        match_query = {
            "$or": [
                {"sender": user_address},
                {"to": user_address},
                {"src": user_address},
                {"dst": user_address},
            ]
        }
        if timestamp_ini and timestamp_end:
            match_query["timestamp"] = {"$gte": timestamp_ini, "$lte": timestamp_end}
        elif timestamp_ini:
            match_query["timestamp"] = {"$gte": timestamp_ini}
        elif timestamp_end:
            match_query["timestamp"] = {"$lte": timestamp_end}

        query = [
            {"$sort": {"blockNumber": 1}},
            {"$match": match_query},
            {
                "$lookup": {
                    "from": "status",
                    "let": {
                        "operation_hype_address": "$address",
                        "operation_block": "$blockNumber",
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {
                                            "$eq": [
                                                "$address",
                                                "$$operation_hype_address",
                                            ]
                                        },
                                        {"$eq": ["$block", "$$operation_block"]},
                                    ],
                                }
                            }
                        },
                        {
                            "$project": {
                                "symbol": "$symbol",
                                "totalSupply": "$totalSupply",
                                "pool": "$pool",
                            }
                        },
                        {"$unset": ["_id"]},
                    ],
                    "as": "status",
                }
            },
            {"$unset": ["_id"]},
            {"$unwind": "$status"},
            {"$sort": {"block": 1}},
            {
                "$group": {
                    "_id": "$address",
                    "hypervisor_address": {"$first": "$address"},
                    "user_operations": {"$push": "$$ROOT"},
                }
            },
            {"$unset": ["_id"]},
        ]

        return self.local_db_manager.get_items_from_database(
            aggregate=query, collection_name="operations", batch_size=self.batch_size
        )

    @log_execution_time
    def _get_fee_operations(
        self,
        hypervisor_address: str,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
    ) -> list[dict] | None:
        match_query = {
            "topic": {"$in": ["zeroBurn", "rebalance"]},
            "$or": [{"qtty_token0": {"$ne": "0"}}, {"qtty_token1": {"$ne": "0"}}],
            "address": hypervisor_address,
        }
        if timestamp_ini and timestamp_end:
            match_query["timestamp"] = {"$gte": timestamp_ini, "$lte": timestamp_end}
        elif timestamp_ini:
            match_query["timestamp"] = {"$gte": timestamp_ini}
        elif timestamp_end:
            match_query["timestamp"] = {"$lte": timestamp_end}

        query = [
            {"$match": match_query},
            {"$sort": {"blockNumber": 1}},
            {
                "$lookup": {
                    "from": "status",
                    "let": {
                        "operation_hype_address": "$address",
                        "operation_block": "$blockNumber",
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {
                                            "$eq": [
                                                "$address",
                                                "$$operation_hype_address",
                                            ]
                                        },
                                        {"$eq": ["$block", "$$operation_block"]},
                                    ],
                                }
                            }
                        },
                        {
                            "$project": {
                                "symbol": "$symbol",
                                "totalSupply": "$totalSupply",
                                "pool": "$pool",
                            }
                        },
                        {"$unset": ["_id"]},
                    ],
                    "as": "status",
                }
            },
            {"$unset": ["_id"]},
            {"$unwind": "$status"},
            {"$sort": {"block": 1}},
            {
                "$group": {
                    "_id": "$address",
                    "hypervisor_address": {"$first": "$address"},
                    "fees_operations": {"$push": "$$ROOT"},
                }
            },
            {"$unset": ["_id"]},
        ]

        return self.local_db_manager.get_items_from_database(
            aggregate=query, collection_name="operations", batch_size=self.batch_size
        )
