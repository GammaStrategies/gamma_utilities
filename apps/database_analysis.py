import sys
import os
import logging
import tqdm
import concurrent.futures
import uuid

from web3 import Web3
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict, InitVar
from decimal import Decimal, getcontext

if __name__ == "__main__":
    # append parent directory pth
    CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
    PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
    sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION

from bins.database.common.db_collections_common import database_local, database_global
from bins.general import general_utilities, file_utilities
from bins.apis.thegraph_utilities import gamma_scraper
from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
)


@dataclass
class root_status:
    # Important:
    # when var name <...in_token1> or <...in_usd>  means position converted to x. Normally beguining with a total
    #
    timestamp: float
    block: int
    topic: str = ""
    account_address: str = ""

    secPassed: int = 0  # time passed between operations

    usd_price_token0: Decimal = Decimal("0")
    usd_price_token1: Decimal = Decimal("0")

    # investment are deposits.
    # When transfers occur, investment % of the transfer share qtty is also transfered to new account. When investments are closed, the divestment % is substracted
    investment_qtty_token0: Decimal = Decimal("0")
    investment_qtty_token1: Decimal = Decimal("0")
    total_investment_qtty_in_usd: Decimal = Decimal("0")
    total_investment_qtty_in_token0: Decimal = Decimal("0")
    total_investment_qtty_in_token1: Decimal = Decimal("0")

    # FEES loop ->  [ uncollected > owed > collected (inside underlaying tokens) ]

    # fees always grow in proportion to current block's shares
    fees_collected_token0: Decimal = Decimal("0")
    fees_collected_token1: Decimal = Decimal("0")
    total_fees_collected_in_usd: Decimal = Decimal("0")

    # feeGrowth calculation
    fees_uncollected_token0: Decimal = Decimal("0")
    fees_uncollected_token1: Decimal = Decimal("0")
    total_fees_uncollected_in_usd: Decimal = Decimal("0")

    # owed fees
    fees_owed_token0: Decimal = Decimal("0")
    fees_owed_token1: Decimal = Decimal("0")
    total_fees_owed_in_usd: Decimal = Decimal("0")

    # seconds passed between uncollected fees
    fees_uncollected_secPassed: int = 0

    # divestment ( keep track control vars )
    divestment_base_qtty_token0: Decimal = Decimal("0")
    divestment_base_qtty_token1: Decimal = Decimal("0")
    total_divestment_base_qtty_in_usd: Decimal = Decimal("0")
    total_divestment_base_qtty_in_token0: Decimal = Decimal("0")
    total_divestment_base_qtty_in_token1: Decimal = Decimal("0")

    divestment_fee_qtty_token0: Decimal = Decimal("0")
    divestment_fee_qtty_token1: Decimal = Decimal("0")
    total_divestment_fee_qtty_in_usd: Decimal = Decimal("0")

    # Comparison result
    impermanent_lp_vs_hodl_usd: Decimal = Decimal(
        "0"
    )  # current position val - investment value
    impermanent_lp_vs_hodl_token0: Decimal = Decimal(
        "0"
    )  # current position val - investment in token qtty at current prices
    impermanent_lp_vs_hodl_token1: Decimal = Decimal("0")

    # current result
    current_result_token0: Decimal = Decimal("0")
    current_result_token1: Decimal = Decimal("0")
    total_current_result_in_usd: Decimal = Decimal("0")
    total_current_result_in_token0: Decimal = Decimal("0")
    total_current_result_in_token1: Decimal = Decimal("0")

    # TODO: closed positions.
    closed_investment_return_token0: Decimal = Decimal("0")
    closed_investment_return_token1: Decimal = Decimal("0")
    total_closed_investment_return_in_usd: Decimal = Decimal("0")
    total_closed_investment_return_in_token0: Decimal = Decimal("0")
    total_closed_investment_return_in_token1: Decimal = Decimal("0")

    # share qtty
    shares_qtty: Decimal = Decimal("0")
    # ( this is % that multiplied by tvl gives u total qtty assets)
    shares_percent: Decimal = Decimal("0")

    # underlying assets
    underlying_token0: Decimal = Decimal("0")
    underlying_token1: Decimal = Decimal("0")
    total_underlying_in_usd: Decimal = Decimal("0")
    total_underlying_in_token0: Decimal = Decimal("0")
    total_underlying_in_token1: Decimal = Decimal("0")

    # save the raw operation info here
    raw_operation: dict = field(default_factory=dict)


class user_status(root_status):
    def fill_from_status(self, status: super):
        doNot_fill = [
            "timestamp",
            "block",
            "topic",
            "account_address",
            "raw_operation",
            "usd_price_token0",
            "usd_price_token1",
        ]
        props = [
            a
            for a in dir(status)
            if not a.startswith("__")
            and not callable(getattr(status, a))
            and not a in doNot_fill
        ]
        for p in props:
            setattr(self, p, getattr(status, p))

    def sum_status(self, status: super):
        """sum up all fields

        Args:
            status (super):
        """
        doNot_fill = [
            "timestamp",
            "block",
            "topic",
            "account_address",
            "raw_operation",
            "usd_price_token0",
            "usd_price_token1",
        ]
        props = [
            a
            for a in dir(status)
            if not a.startswith("__")
            and not callable(getattr(status, a))
            and not a in doNot_fill
        ]
        for p in props:
            setattr(self, p, getattr(status, p) + getattr(self, p))

    def substract_status(self, status: super):
        """substract all fields ( including prices)

        Args:
            status (super):
        """
        doNot_fill = ["timestamp", "block", "topic", "account_address", "raw_operation"]
        props = [
            a
            for a in dir(status)
            if not a.startswith("__")
            and not callable(getattr(status, a))
            and not a in doNot_fill
        ]
        for p in props:
            setattr(self, p, getattr(self, p) - getattr(status, p))

    def toDict(self) -> dict:
        result = dict()
        fields_expluded = ["raw_operation"]
        props = [
            a
            for a in dir(self)
            if not a.startswith("__")
            and not callable(getattr(self, a))
            and not a in fields_expluded
        ]
        for p in props:
            result[p] = getattr(self, p)

        return result
        # return asdict(self)
        # return {k: str(v) for k, v in asdict(self).items()}


class hypervisor_db_reader:
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
        self._hypervisor_address = hypervisor_address
        self._network = network
        self._protocol = protocol

        self.__blacklist_addresses = ["0x0000000000000000000000000000000000000000"]

        # user status by block:  { <block>: {<user address>:status list}}
        self._users_by_block = dict()

        # load static
        self._static = self._get_static_data()
        # load status available
        self._status = self._get_status_data()
        self.check_status()
        # load operations available
        self._operations = self._get_operations()
        # load prices for all status blocks
        self._prices = self._get_prices()
        # dummy blocks added to operation's used as report
        self._report_blocks = set()
        # masterchefs
        self._masterchefs = list()

        # control var (itemsprocessed): list of operation ids processed
        self.ids_processed = list()
        # control var time order :  last block always >= current
        self.last_block_processed: int = 0

    # setup
    def _get_static_data(self):
        """_load hypervisor's static data from database"""
        # database link
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{self.network}_{self.protocol}"
        local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

        # static
        return local_db_manager.get_items_from_database(
            collection_name="static", find={"id": self.address}
        )[0]

    def _get_status_data(self):
        """_load hypervisor's status data from database"""
        # database link
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{self.network}_{self.protocol}"
        local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

        find = {"address": self.address.lower()}
        sort = [("block", 1)]
        return {
            x["block"]: self._convert_status(x)
            for x in local_db_manager.get_items_from_database(
                collection_name="status", find=find, sort=sort
            )
        }

    def _get_operations(self) -> dict:
        """_load hypervisor's operations from database"""
        # database link
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{self.network}_{self.protocol}"
        local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

        # define query
        find = {"address": self.address.lower()}
        sort = [("blockNumber", 1), ("logIndex", 1)]
        return local_db_manager.get_items_from_database(
            collection_name="operations", find=find, sort=sort
        )

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

    # public
    @property
    def address(self) -> str:
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
        return min(list(self._status.keys()))

    @property
    def latest_status_block(self) -> int:
        """Get status lates block

        Returns:
            int: block number
        """
        return max(list(self._status.keys()))

    @property
    def first_user_block(self) -> int:
        """Get users first block

        Returns:
            int: block number
        """
        return min(list(self._users_by_block.keys()))

    @property
    def latest_user_block(self) -> int:
        """Get users latest block

        Returns:
            int: block number
        """
        return max(list(self._users_by_block.keys()))

    def total_supply(self, block: int) -> Decimal:
        """total hypervisor supply as per contract data

        Args:
            block (int): _description_

        Returns:
            Decimal: _description_
        """
        return self._status[block]["totalSupply"]

    def total_shares(self, at_block: int = 0) -> Decimal:
        """Return total hypervisor shares calculated from sum of total user shares at the moment we call
            ( this is not totalSupply at all times )
        Args:
            at_block (int, optional): . Defaults to 0.
        Returns:
            Decimal: total shares
        """

        total_shares_addresses = {
            x: Decimal(0) for x in self.get_all_account_addresses(at_block=at_block)
        }

        block_list = reversed(sorted(list(self._users_by_block.keys())))
        if at_block > 0:
            block_list = [x for x in block_list if x <= at_block]

        # set a control var
        processed_addresses = list()  # vs len(total_shares_addresses.keys())
        for block in block_list:
            for address, operations_list in self._users_by_block[block].items():
                if (not address in processed_addresses) and (
                    total_shares_addresses[address] == Decimal("0")
                ):
                    total_shares_addresses[address] = operations_list[-1].shares_qtty
                    processed_addresses.append(address)

            # check if processed all addreses
            if len(processed_addresses) == len(total_shares_addresses.keys()):
                break

        # set result var
        t_shares = sum(list(total_shares_addresses.values()))

        # sum shares
        return t_shares

    def total_fees(self) -> dict:
        total = {
            "token0": Decimal(0),
            "token1": Decimal(0),
        }

        for account in self.get_all_account_addresses():
            last_op = self.last_operation(account_address=account)
            total["token0"] += last_op.fees_collected_token0
            total["token1"] += last_op.fees_collected_token1

        return total

    def tvl(self, at_block: int = 0) -> dict:
        if at_block == 0:
            at_block = max(list(self._status.keys()))

        if not at_block in self._status:
            raise ValueError(
                f" {at_block} block has not been found in status (..and should be there)"
            )

        data = self._status[at_block]

        # return not calculated field 'totalAmounts' as tvl
        return {
            "token0": Decimal(data["totalAmounts"]["total0"]),
            "token1": Decimal(data["totalAmounts"]["total1"]),
            "block": at_block,
        }

    def result(self, b_ini: int = 0, b_end: int = 0) -> tuple[user_status, user_status]:

        total_ini = user_status(timestamp=0, block=0, account_address=self.address)
        total_end = user_status(timestamp=0, block=0, account_address=self.address)

        # control vars
        all_user_accounts = self.get_all_account_addresses()
        ini_mods = 0
        end_mods = 0

        logging.getLogger(__name__).debug(
            f"  Total user accounts {len(all_user_accounts)}"
        )

        for account in all_user_accounts:

            ini_operation = self.last_operation(
                account_address=account, from_block=b_ini
            )
            end_operation = self.last_operation(
                account_address=account, from_block=b_end
            )
            if b_ini != ini_operation.block:
                ini_mods += 1
                logging.getLogger(__name__).warning(
                    f"[{ini_mods} of {len(all_user_accounts)}] Initial result for {account} is {ini_operation.block-b_ini} blocks away from target: {b_ini} <--> {ini_operation.block}"
                )
            if b_end != end_operation.block:
                end_mods += 1
                logging.getLogger(__name__).warning(
                    f"[{end_mods} of {len(all_user_accounts)}] End result for {account} is {end_operation.block-b_ini} blocks away from target {b_end} <--> {end_operation.block}"
                )

            # set price n block (if block > block)
            if ini_operation.block > total_ini.block:
                total_ini.usd_price_token0 = ini_operation.usd_price_token0
                total_ini.usd_price_token1 = ini_operation.usd_price_token1
                total_ini.block = ini_operation.block
                total_ini.timestamp = ini_operation.timestamp
            if end_operation.block > total_end.block:
                total_end.usd_price_token0 = end_operation.usd_price_token0
                total_end.usd_price_token1 = end_operation.usd_price_token1
                total_end.block = end_operation.block
                total_end.timestamp = end_operation.timestamp

            # sum user at ini point
            total_ini.sum_status(status=ini_operation)
            total_end.sum_status(status=end_operation)

        return total_ini, total_end

    def result_list(self, b_ini: int = 0, b_end: int = 0) -> list[user_status]:

        result = list()

        for block, addresses_data in self._users_by_block.items():

            if block >= b_ini and block <= b_end and block in self._report_blocks:

                total_block_status = user_status(
                    timestamp=0, block=block, account_address=self.address
                )
                # create block status
                for address, operations in self._users_by_block[block].items():
                    # if address == "0x0000000000000000000000000000000000000000":
                    #     continue
                    # use last operation for current address
                    if operations[-1].timestamp > total_block_status.timestamp:
                        total_block_status.usd_price_token0 = operations[
                            -1
                        ].usd_price_token0
                        total_block_status.usd_price_token1 = operations[
                            -1
                        ].usd_price_token1
                        total_block_status.timestamp = operations[-1].timestamp

                    # sum user at ini point
                    total_block_status.sum_status(status=operations[-1])

                # add block status to result
                result.append(total_block_status)

        return result

    # General classifier

    def _mix_operations_status(self) -> list[dict]:
        result = list()
        result.extend(self._operations)
        # control var
        initial_length = len(result)

        all_operations_blocks = set([x["blockNumber"] for x in self._operations])

        for block in self._status.keys():
            if not block in all_operations_blocks:
                # add block as report block
                self._report_blocks.add(block)
                # create dummy operation
                dummy = {
                    "blockHash": "dummy",
                    "blockNumber": block,
                    "address": self.address,
                    "timestamp": self._status[block]["timestamp"],
                    "decimals_token0": self._static["pool"]["token0"]["decimals"],
                    "decimals_token1": self._static["pool"]["token1"]["decimals"],
                    "decimals_contract": self._static["decimals"],
                    "topic": "dummy",
                    "logIndex": 10000,
                    "id": str(uuid.uuid4()),
                }
                # add summy op to operations result
                result.append(dummy)

        logging.getLogger(__name__).debug(
            f" Added {len(result)-initial_length} dummy operations from status blocks"
        )
        # return sorted by block->logindex
        return sorted(result, key=lambda x: (x["blockNumber"], x["logIndex"]))

    def _process_operations(self):
        """process all operations mixed with all status blocks: so, status blocks will be dummy operations"""

        # mix operations with status blocks ( status different than operation's)

        for operation in self._mix_operations_status():
            if not operation["id"] in self.ids_processed:
                # check if operation is valid
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
                    f""" Operation already processed {operation["id"]}"""
                )

    def _process_operation(self, operation: dict):

        # set current block
        self.current_block = operation["blockNumber"]
        # set current logIndex
        self.current_logIndex = operation["logIndex"]

        if operation["topic"] == "deposit":
            self._add_operation(operation=self._process_deposit(operation=operation))

        elif operation["topic"] == "withdraw":
            self._add_operation(self._process_withdraw(operation=operation))

        elif operation["topic"] == "transfer":
            # retrieve new status
            op_source, op_destination = self._process_transfer(operation=operation)
            # add to collection
            if op_source:
                self._add_operation(operation=op_source)
            if op_destination:
                self._add_operation(operation=op_destination)

        elif operation["topic"] == "rebalance":
            self._process_rebalance(operation=operation)

        elif operation["topic"] == "approval":
            # TODO: approval topic
            # self._add_operation(self._process_approval(operation=operation))
            pass

        elif operation["topic"] == "zeroBurn":
            self._process_zeroBurn(operation=operation)

        elif operation["topic"] == "setFee":
            # TODO: setFee topic
            pass

        elif operation["topic"] == "dummy":
            # global status for all addresses
            self._process_dummy(operation=operation)

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
        price_usd_t0 = Decimal(
            self._prices[block][self._static["pool"]["token0"]["address"]]
        )
        price_usd_t1 = Decimal(
            self._prices[block][self._static["pool"]["token1"]["address"]]
        )

        # create result
        new_user_status = user_status(
            timestamp=operation["timestamp"],
            block=operation["blockNumber"],
            topic=operation["topic"],
            account_address=account_address,
            raw_operation=operation,
        )

        # get last operation
        last_op = self.last_operation(
            account_address=account_address, from_block=operation["blockNumber"]
        )

        # fill new status item with last data
        new_user_status.fill_from_status(status=last_op)

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
            current_operation=new_user_status, last_operation=last_op
        )

        # result
        return new_user_status

    def _process_withdraw(self, operation: dict) -> user_status:
        # if operation["blockNumber"] == 16709879:
        #     po = ""

        # define ease access vars
        block = operation["blockNumber"]
        contract_address = operation["address"].lower()
        account_address = operation["sender"].lower()

        price_usd_t0 = Decimal(
            self._prices[block][self._static["pool"]["token0"]["address"]]
        )
        price_usd_t1 = Decimal(
            self._prices[block][self._static["pool"]["token1"]["address"]]
        )

        # create new status item
        new_user_status = user_status(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            account_address=account_address,
            raw_operation=operation,
        )
        # get last operation
        last_op = self.last_operation(account_address=account_address, from_block=block)

        # fill new status item with last data
        new_user_status.fill_from_status(status=last_op)

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
            current_operation=new_user_status, last_operation=last_op
        )

        # result
        return new_user_status

    def _process_transfer(self, operation: dict) -> tuple[user_status, user_status]:

        if operation["dst"] == "0x0000000000000000000000000000000000000000":
            # expect a withdraw topic on next operation ( same block))
            pass
        elif operation["src"] == "0x0000000000000000000000000000000000000000":
            # expect a deposit topic on next operation ( same block)
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

    def _process_dummy(self, operation: dict):

        # for each address
        for account_address in self.get_all_account_addresses(
            at_block=operation["blockNumber"]
        ):

            # create result
            new_user_status = user_status(
                timestamp=operation["timestamp"],
                block=operation["blockNumber"],
                topic="dummy",
                account_address=account_address,
                raw_operation=operation,
            )
            # get last operation
            last_op = self.last_operation(
                account_address=account_address, from_block=operation["blockNumber"]
            )

            # fill new status item with last data
            new_user_status.fill_from_status(status=last_op)

            # add globals
            new_user_status = self._add_globals_to_user_status(
                current_operation=new_user_status, last_operation=last_op
            )

            # add to result
            self._add_operation(operation=new_user_status)

    def _share_fees_with_acounts(self, operation: dict):

        # check if this is the last operation of the block
        if not self._is_last_logIndex(
            logIndex=operation["logIndex"], block=operation["blockNumber"]
        ):
            # share fees now
            raise ValueError(
                " Sharing fees in a non closed block !!! there are more operations after thins one ion the same block that may affect fee sharing --> code something "
            )

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()

        # get current total contract_address shares qtty
        # total_shares = self.total_shares(at_block=block)
        total_shares = self.total_supply(block=block)

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
        price_usd_t0 = Decimal(
            self._prices[block][self._static["pool"]["token0"]["address"]]
        )
        price_usd_t1 = Decimal(
            self._prices[block][self._static["pool"]["token1"]["address"]]
        )

        # control var to keep track of total percentage applied
        ctrl_total_percentage_applied = Decimal("0")
        ctrl_total_shares_applied = Decimal("0")

        # loop all addresses
        for account_address in self.get_all_account_addresses(at_block=block):
            # create result
            new_user_status = user_status(
                timestamp=operation["timestamp"],
                block=block,
                topic=operation["topic"],
                account_address=account_address,
                raw_operation=operation,
            )
            # get last address operation (status)
            # fill new status item with last data
            new_user_status.fill_from_status(
                status=self.last_operation(
                    account_address=account_address, from_block=block
                )
            )

            # calc user share in the pool
            user_share = new_user_status.shares_qtty / total_shares
            #
            ctrl_total_shares_applied += new_user_status.shares_qtty

            # check inconsistency
            if (ctrl_total_percentage_applied + user_share) > 1:
                logging.getLogger(__name__).warning(
                    " The total percentage applied while calc. user fees exceeds 100% at {}'s {} hype {} -> {}".format(
                        self.network,
                        self.protocol,
                        self.address,
                        (ctrl_total_percentage_applied + user_share),
                    )
                )
                logging.getLogger(__name__).warning(
                    "          total shares applied {}  total shares {}  diff: {}".format(
                        ctrl_total_shares_applied,
                        total_shares,
                        total_shares - ctrl_total_shares_applied,
                    )
                )

            # add user share to total processed control var
            ctrl_total_percentage_applied += user_share

            # add fees collected to user
            new_user_status.fees_collected_token0 += fees_collected_token0 * user_share
            new_user_status.fees_collected_token1 += fees_collected_token1 * user_share
            new_user_status.total_fees_collected_in_usd += (
                (fees_collected_token0 * price_usd_t0)
                + (fees_collected_token1 * price_usd_t1)
            ) * user_share

            # get last operation from destination address
            last_op = self.last_operation(
                account_address=account_address, from_block=block
            )
            # add global stats
            new_user_status = self._add_globals_to_user_status(
                current_operation=new_user_status, last_operation=last_op
            )

            # add new status to hypervisor
            self._add_operation(operation=new_user_status)

        # save fee remainders data
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

            logging.getLogger(__name__).warning(
                " The total percentage applied while calc. user fees fall behind 100% at {}'s {} hype {} -> {}".format(
                    self.network,
                    self.protocol,
                    self.address,
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

            # log if value is significant
            if (Decimal("1") - ctrl_total_percentage_applied) > Decimal("0.0001"):
                logging.getLogger(__name__).error(
                    " Only {:,.2f} of the rebalance value has been distributed to current accounts. remainder: {} ".format(
                        ctrl_total_percentage_applied,
                        (Decimal("1") - ctrl_total_percentage_applied),
                    )
                )

    def _transfer_to_user(self, operation: dict) -> tuple[user_status, user_status]:

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()
        # set user address
        address_source = operation["src"].lower()
        address_destination = operation["dst"].lower()

        # USD prices
        price_usd_t0 = Decimal(
            self._prices[block][self._static["pool"]["token0"]["address"]]
        )
        price_usd_t1 = Decimal(
            self._prices[block][self._static["pool"]["token1"]["address"]]
        )
        # get current total shares
        total_shares = self.total_supply(block=block)

        # create SOURCE result
        new_user_status_source = user_status(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            account_address=address_source,
            raw_operation=operation,
        )
        # get last operation from source address
        last_op_source = self.last_operation(
            account_address=address_source, from_block=block
        )
        # fill new status item with last data
        new_user_status_source.fill_from_status(status=last_op_source)

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
            current_operation=new_user_status_source,
            last_operation=last_op_source,
        )

        # create DESTINATION result
        new_user_status_destination = user_status(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            account_address=address_destination,
            raw_operation=operation,
        )

        # get last operation from destination address
        last_op_destination = self.last_operation(
            account_address=address_destination, from_block=block
        )

        # fill new status item with last data
        new_user_status_destination.fill_from_status(status=last_op_destination)

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
            current_operation=new_user_status_destination,
            last_operation=last_op_destination,
        )

        # result
        return new_user_status_source, new_user_status_destination

    # Status transformer
    def _convert_status(self, status: dict) -> dict:
        """convert database hypervisor status text fields
            to numbers.

        Args:
            status (dict): hypervisor status database obj

        Returns:
            dict: same converted
        """
        # decimals
        decimals_token0 = status["pool"]["token0"]["decimals"]
        decimals_token1 = status["pool"]["token1"]["decimals"]
        decimals_contract = status["decimals"]

        status["baseUpper"] = int(status["baseUpper"])
        status["baseLower"] = int(status["baseLower"])

        status["basePosition"]["liquidity"] = int(status["basePosition"]["liquidity"])
        status["basePosition"]["amount0"] = int(status["basePosition"]["amount0"])
        status["basePosition"]["amount1"] = int(status["basePosition"]["amount1"])
        status["limitPosition"]["liquidity"] = int(status["limitPosition"]["liquidity"])
        status["limitPosition"]["amount0"] = int(status["limitPosition"]["amount0"])
        status["limitPosition"]["amount1"] = int(status["limitPosition"]["amount1"])

        status["currentTick"] = int(status["currentTick"])

        status["deposit0Max"] = Decimal(status["baseLower"]) / Decimal(
            10**decimals_token0
        )
        status["deposit1Max"] = Decimal(status["baseLower"]) / Decimal(
            10**decimals_token1
        )

        status["fees_uncollected"]["qtty_token0"] = Decimal(
            status["fees_uncollected"]["qtty_token0"]
        ) / Decimal(10**decimals_token0)
        status["fees_uncollected"]["qtty_token1"] = Decimal(
            status["fees_uncollected"]["qtty_token1"]
        ) / Decimal(10**decimals_token1)

        status["limitUpper"] = int(status["limitUpper"])
        status["limitLower"] = int(status["limitLower"])

        status["maxTotalSupply"] = int(status["maxTotalSupply"]) / Decimal(
            10**decimals_contract
        )

        status["pool"]["feeGrowthGlobal0X128"] = int(
            status["pool"]["feeGrowthGlobal0X128"]
        )
        status["pool"]["feeGrowthGlobal1X128"] = int(
            status["pool"]["feeGrowthGlobal1X128"]
        )
        status["pool"]["liquidity"] = int(status["pool"]["liquidity"])
        status["pool"]["maxLiquidityPerTick"] = int(
            status["pool"]["maxLiquidityPerTick"]
        )

        # choose by dex
        if status["dex"] == "uniswapv3":
            # uniswap
            status["pool"]["protocolFees"][0] = int(status["pool"]["protocolFees"][0])
            status["pool"]["protocolFees"][1] = int(status["pool"]["protocolFees"][1])

            status["pool"]["slot0"]["sqrtPriceX96"] = int(
                status["pool"]["slot0"]["sqrtPriceX96"]
            )
            status["pool"]["slot0"]["tick"] = int(status["pool"]["slot0"]["tick"])
            status["pool"]["slot0"]["observationIndex"] = int(
                status["pool"]["slot0"]["observationIndex"]
            )
            status["pool"]["slot0"]["observationCardinality"] = int(
                status["pool"]["slot0"]["observationCardinality"]
            )
            status["pool"]["slot0"]["observationCardinalityNext"] = int(
                status["pool"]["slot0"]["observationCardinalityNext"]
            )

            status["pool"]["tickSpacing"] = int(status["pool"]["tickSpacing"])

        elif status["dex"] == "quickswap":
            # quickswap
            status["pool"]["globalState"]["sqrtPriceX96"] = int(
                status["pool"]["globalState"]["sqrtPriceX96"]
            )
            status["pool"]["globalState"]["tick"] = int(
                status["pool"]["globalState"]["tick"]
            )
            status["pool"]["globalState"]["fee"] = int(
                status["pool"]["globalState"]["fee"]
            )
            status["pool"]["globalState"]["timepointIndex"] = int(
                status["pool"]["globalState"]["timepointIndex"]
            )
        else:
            pio = "error stop"

        status["pool"]["token0"]["totalSupply"] = Decimal(
            status["pool"]["token0"]["totalSupply"]
        ) / Decimal(10**decimals_token0)
        status["pool"]["token1"]["totalSupply"] = Decimal(
            status["pool"]["token1"]["totalSupply"]
        ) / Decimal(10**decimals_token1)

        status["qtty_depoloyed"]["qtty_token0"] = Decimal(
            status["qtty_depoloyed"]["qtty_token0"]
        ) / Decimal(10**decimals_token0)
        status["qtty_depoloyed"]["qtty_token1"] = Decimal(
            status["qtty_depoloyed"]["qtty_token1"]
        ) / Decimal(10**decimals_token1)
        status["qtty_depoloyed"]["fees_owed_token0"] = Decimal(
            status["qtty_depoloyed"]["fees_owed_token0"]
        ) / Decimal(10**decimals_token0)
        status["qtty_depoloyed"]["fees_owed_token1"] = Decimal(
            status["qtty_depoloyed"]["fees_owed_token1"]
        ) / Decimal(10**decimals_token1)

        status["tickSpacing"] = int(status["tickSpacing"])

        status["totalAmounts"]["total0"] = Decimal(
            status["totalAmounts"]["total0"]
        ) / Decimal(10**decimals_token0)
        status["totalAmounts"]["total1"] = Decimal(
            status["totalAmounts"]["total1"]
        ) / Decimal(10**decimals_token1)

        status["totalSupply"] = Decimal(status["totalSupply"]) / Decimal(
            10**decimals_contract
        )

        status["tvl"]["parked_token0"] = Decimal(
            status["tvl"]["parked_token0"]
        ) / Decimal(10**decimals_token0)
        status["tvl"]["parked_token1"] = Decimal(
            status["tvl"]["parked_token1"]
        ) / Decimal(10**decimals_token1)
        status["tvl"]["deployed_token0"] = Decimal(
            status["tvl"]["deployed_token0"]
        ) / Decimal(10**decimals_token0)
        status["tvl"]["deployed_token1"] = Decimal(
            status["tvl"]["deployed_token1"]
        ) / Decimal(10**decimals_token1)
        status["tvl"]["fees_owed_token0"] = Decimal(
            status["tvl"]["fees_owed_token0"]
        ) / Decimal(10**decimals_token0)
        status["tvl"]["fees_owed_token1"] = Decimal(
            status["tvl"]["fees_owed_token1"]
        ) / Decimal(10**decimals_token1)
        status["tvl"]["tvl_token0"] = Decimal(status["tvl"]["tvl_token0"]) / Decimal(
            10**decimals_token0
        )
        status["tvl"]["tvl_token1"] = Decimal(status["tvl"]["tvl_token1"]) / Decimal(
            10**decimals_token1
        )

        return status

    # Collection
    def _add_operation(self, operation: user_status):
        """add operation to users by block

        Args:
            operation (user_status):
        """
        if not operation.account_address in self.__blacklist_addresses:
            # create in users if do not exist
            if not operation.block in self._users_by_block:
                # create a new block
                self._users_by_block[operation.block] = dict()
            if not operation.account_address in self._users_by_block[operation.block]:
                # create a new address in a block
                self._users_by_block[operation.block][
                    operation.account_address
                ] = list()

            # add operation to list
            self._users_by_block[operation.block][operation.account_address].append(
                operation
            )

        else:
            # blacklisted
            if (
                not operation.account_address
                == "0x0000000000000000000000000000000000000000"
            ):
                logging.getLogger(__name__).debug(
                    f"Not adding blacklisted account {operation.account_address} operation"
                )

    # General helpers
    def _add_globals_to_user_status(
        self,
        current_operation: user_status,
        last_operation: user_status,
    ) -> user_status:

        # USD prices
        price_usd_t0 = Decimal(
            self._prices[current_operation.block][
                self._static["pool"]["token0"]["address"]
            ]
        )
        price_usd_t1 = Decimal(
            self._prices[current_operation.block][
                self._static["pool"]["token1"]["address"]
            ]
        )

        # add prices to current operation
        current_operation.usd_price_token0 = price_usd_t0
        current_operation.usd_price_token1 = price_usd_t1

        # get hypervisor status at block
        current_status_data = self._status[current_operation.block]

        # modify shares percentage
        total_shares = self.total_supply(block=current_operation.block)
        current_operation.shares_percent = (
            (current_operation.shares_qtty / total_shares) if total_shares != 0 else 0
        )

        # get current block -1 status
        # avoid first time block ( block == 0)
        # avoid dummy operations (  syntetic created operations mixing status blocks with operations)
        if (
            last_operation.block != 0
            and current_operation.topic != "dummy"
            and not (current_operation.block - 1) in self._status
        ):
            logging.getLogger(__name__).error(
                f" Could not find block {last_operation.block} in status for hype {self.address}"
            )

        # get last operation block status
        if (
            last_operation.block != 0
            and current_operation.topic != "dummy"
            and not last_operation.block in self._status
        ):
            logging.getLogger(__name__).error(
                f" Could not find block {last_operation.block} in status for hype {self.address}"
            )
        # last_status_data = self._status[last_operation.block]

        # set current_operation's proportional uncollected fees
        current_operation.fees_uncollected_token0 = (
            Decimal(current_status_data["fees_uncollected"]["qtty_token0"])
            * current_operation.shares_percent
        )
        current_operation.fees_uncollected_token1 = (
            Decimal(current_status_data["fees_uncollected"]["qtty_token1"])
            * current_operation.shares_percent
        )
        current_operation.total_fees_uncollected_in_usd = (
            current_operation.fees_uncollected_token0 * price_usd_t0
            + current_operation.fees_uncollected_token1 * price_usd_t1
        )

        current_operation.fees_owed_token0 = (
            Decimal(current_status_data["tvl"]["fees_owed_token0"])
            * current_operation.shares_percent
        )
        current_operation.fees_owed_token1 = (
            Decimal(current_status_data["tvl"]["fees_owed_token1"])
            * current_operation.shares_percent
        )
        current_operation.total_fees_owed_in_usd = (
            current_operation.fees_owed_token0 * price_usd_t0
            + current_operation.fees_owed_token1 * price_usd_t1
        )

        # avoid calc seconds passed on new created items ( think about transfers)
        if last_operation.block != 0:
            # Decide wether to add or set seconds passed based on topic ( always set but not on dummy)
            if current_operation.topic == "dummy":
                # all secs passed since last block processed
                current_operation.fees_uncollected_secPassed += (
                    current_operation.timestamp - last_operation.timestamp
                )
            else:
                # set uncollected fees secs passed since last collection
                current_operation.fees_uncollected_secPassed = (
                    current_operation.timestamp - last_operation.timestamp
                )

        # set current_operation's proportional tvl ( underlying tokens value) + uncollected fees
        # WARN: underlying tokens can be greater than TVL
        current_operation.underlying_token0 = (
            Decimal(current_status_data["totalAmounts"]["total0"])
            * current_operation.shares_percent
        ) + current_operation.fees_uncollected_token0
        current_operation.underlying_token1 = (
            Decimal(current_status_data["totalAmounts"]["total1"])
            * current_operation.shares_percent
        ) + current_operation.fees_uncollected_token1
        current_operation.total_underlying_in_usd = (
            current_operation.underlying_token0 * price_usd_t0
            + current_operation.underlying_token1 * price_usd_t1
        )
        current_operation.total_underlying_in_token0 = (
            current_operation.underlying_token0
            + (current_operation.underlying_token1 * (price_usd_t1 / price_usd_t0))
        )
        current_operation.total_underlying_in_token1 = (
            current_operation.underlying_token1
            + (current_operation.underlying_token0 * (price_usd_t0 / price_usd_t1))
        )

        # add seconds passed since
        # avoid calc seconds passed on new created items ( think about transfers)
        if last_operation.block != 0:
            current_operation.secPassed += (
                current_operation.timestamp - last_operation.timestamp
            )

        # current absolute result
        current_operation.current_result_token0 = (
            current_operation.underlying_token0
            - current_operation.investment_qtty_token0
        )
        current_operation.current_result_token1 = (
            current_operation.underlying_token1
            - current_operation.investment_qtty_token1
        )
        current_operation.total_current_result_in_usd = (
            current_operation.total_underlying_in_usd
            - current_operation.total_investment_qtty_in_usd
        )
        current_operation.total_current_result_in_token0 = (
            current_operation.total_underlying_in_token0
            - current_operation.total_investment_qtty_in_token0
        )
        current_operation.total_current_result_in_token1 = (
            current_operation.total_underlying_in_token1
            - current_operation.total_investment_qtty_in_token1
        )

        # Comparison results ( impermanent)
        current_operation.impermanent_lp_vs_hodl_usd = (
            current_operation.total_underlying_in_usd
            - current_operation.total_investment_qtty_in_usd
        )
        current_operation.impermanent_lp_vs_hodl_token0 = (
            current_operation.total_underlying_in_usd
            - (current_operation.total_investment_qtty_in_token0 * price_usd_t0)
        )
        current_operation.impermanent_lp_vs_hodl_token1 = (
            current_operation.total_underlying_in_usd
            - (current_operation.total_investment_qtty_in_token1 * price_usd_t1)
        )

        return current_operation

    def last_operation(self, account_address: str, from_block: int = 0) -> user_status:
        """find the last operation of an account

        Args:
            account_address (str): user account
            from_block (int, optional): find last account operation from a defined block. Defaults to 0.

        Returns:
            user_status: last operation
        """

        block_list = reversed(sorted(list(self._users_by_block.keys())))
        # block_list = sorted(list(self._users_by_block.keys()), reverse=True)
        if from_block > 0:
            block_list = [x for x in block_list if x <= from_block]

        for block in block_list:
            if account_address in self._users_by_block[block]:
                return self._users_by_block[block][account_address][
                    -1
                ]  # last item in list

        return user_status(
            timestamp=0,
            block=0,
            topic="",
            account_address=account_address,
        )

    def get_all_account_addresses(
        self, at_block: int = 0, with_shares: bool = False
    ) -> list[str]:
        """Get a unique list of addresses

        Args:
            at_block (int, optional): . Defaults to 0.
            with_shares (bool, optional) Include only addresses with shares > 0 ?

        Returns:
            list[str]: of account addresses
        """
        addresses_list = set()

        block_list = reversed(sorted(list(self._users_by_block.keys())))
        if at_block > 0:
            block_list = [x for x in block_list if x <= at_block]

        if with_shares:
            for block in block_list:
                for address, status_list in self._users_by_block[block].items():
                    try:
                        if status_list[-1].shares_qtty > 0:
                            addresses_list.add(address)
                    except:
                        po = ""
                        pass
        else:
            # build a list of addresses
            for block in block_list:
                addresses_list.update(list(self._users_by_block[block].keys()))

        # return unique list
        return list(addresses_list)

    def get_account_info(self, account_address: str) -> list[user_status]:
        result = list()
        for block, accounts in self._users_by_block.items():
            if account_address in accounts.keys():
                result.extend(accounts[account_address])

        return sorted(result, key=lambda x: (x.block, x.raw_operation["logIndex"]))

    def _filter_operations(self, block: int) -> int:
        """Get the all operations of the specified block

        Returns:
            int: log Index number
        """
        return sorted(
            [x for x in self._operations if x["blockNumber"] == block],
            key=lambda x: (x["logIndex"]),
        )

    def _is_last_logIndex(self, logIndex: int, block: int) -> bool:
        try:
            lastlLogIndex = self._filter_operations(block=block)

            if lastlLogIndex[-1]["logIndex"] == logIndex:
                # do share fees when fired
                return True

        except:
            # there are no operations means its a dummy unique in a block operation
            return True

        return False

    def _get_block(self, timestamp: int) -> dict:
        # database link
        global_db_manager = database_global(
            mongo_url=CONFIGURATION["sources"]["database"]["mongo_server_url"]
        )
        result = global_db_manager.get_block(network=self.network, timestamp=timestamp)

        if len(result) == 0:
            result = global_db_manager.get_closest_block(
                network=self.network, timestamp=timestamp
            )
            # check max hours difference --> 24hours
            if result[0]["diff"] / (60 * 60) > 24:
                raise ValueError(
                    f" Cant find block at timestamp {timestamp}. The closest block found is {result[0]['doc']} "
                )
            else:
                logging.getLogger(__name__).debug(
                    f""" Cant find block at timestamp {timestamp}. The closest block found ({result[0]['doc']['block']}) has timestamp {result[0]['doc']['timestamp']}  [ {(result[0]['doc']['timestamp']-timestamp)/(60*60*24)}  days diff]"""
                )

            return result[0]["doc"]

        return result[0]

    def _get_closest_users_by_block(self, block=int) -> int:
        """Find the closest block in users by block dict

        Args:
            block (_type_, optional): . Defaults to int.

        Returns:
            int: block
        """
        return min(self._users_by_block.keys(), key=lambda x: abs(x - block))

    # checks
    def check_totalSupply(self, t_shares, at_block: int = 0):

        # check if calc. result var equals direct chain contract call to totalSupply
        # only in the case we call this function post block time
        try:
            t_supply = Decimal("0")
            if at_block == 0:
                t_supply = Decimal(
                    self._status[self.latest_status_block]["totalSupply"]
                )
            else:
                t_supply = Decimal(self._status[at_block]["totalSupply"])

            # small variance may be present due to Decimal vs double conversions
            if t_supply > 0:

                t_shares_diff = t_shares - t_supply
                t_shares_deviation = t_shares_diff / t_supply

                if t_shares_deviation > Decimal("0.001"):
                    logging.getLogger(__name__).warning(
                        " Calculated total supply [{}] for {} is different from its saved status [{}] deviation:{:,.2%}  block {} [{} {}]".format(
                            t_shares,
                            self.address,
                            t_supply,
                            t_shares_deviation,
                            at_block,
                            self.network,
                            self.dex,
                        )
                    )
        except:
            logging.getLogger(__name__).error(
                f" Unexpected error comparing totalSupply calc vs onchain results. err-> {sys.exc_info()[0]}"
            )

    def check_status(self):
        if len(self._status) == 0:
            logging.getLogger(__name__).error(
                f" Hypervisor {self.address} has no status"
            )


def print_user_status(status: user_status):
    logging.getLogger(__name__).info("")
    logging.getLogger(__name__).info(
        f" Status of {status.account_address} at {datetime.fromtimestamp(status.timestamp)} block:{status.block}"
    )
    logging.getLogger(__name__).info("")
    logging.getLogger(__name__).info("\tAbsolute situation:  ( in USD )")
    logging.getLogger(__name__).info(
        "\tMarket value (tvl):\t {:,.2f}\t ".format(
            status.total_underlying_in_usd if status.total_underlying_in_usd else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   HODL token0:\t {:,.2f}\t [{:+,.2%} vs market value]".format(
            status.total_investment_qtty_in_token0 * status.usd_price_token0,
            (
                (status.total_investment_qtty_in_token0 * status.usd_price_token0)
                - status.total_underlying_in_usd
            )
            / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   HODL token1:\t {:,.2f}\t [{:+,.2%} vs market value]".format(
            status.total_investment_qtty_in_token1 * status.usd_price_token1,
            (
                (status.total_investment_qtty_in_token1 * status.usd_price_token1)
                - status.total_underlying_in_usd
            )
            / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )

    logging.getLogger(__name__).info(
        "\tFees generated:\t {:,.2f}\t [{:,.2%} vs investment]".format(
            status.total_fees_collected_in_usd + status.total_fees_uncollected_in_usd,
            (status.total_fees_collected_in_usd + status.total_fees_uncollected_in_usd)
            / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   Fees collected:\t {:,.2f}\t [{:,.2%} vs investment]".format(
            status.total_fees_collected_in_usd,
            status.total_fees_collected_in_usd / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   Fees owed:\t {:,.2f}\t [{:,.2%} vs investment]".format(
            status.total_fees_owed_in_usd,
            status.total_fees_owed_in_usd / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   Fees uncollected:\t {:,.2f}\t [{:,.2%} vs investment]".format(
            status.total_fees_uncollected_in_usd,
            status.total_fees_uncollected_in_usd / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )

    logging.getLogger(__name__).info(
        "\tInvestment:\t {:,.2f}".format(status.total_investment_qtty_in_usd)
    )
    logging.getLogger(__name__).info(
        "\t   total token0:\t {:,.2f}".format(
            status.total_investment_qtty_in_token0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   total token1:\t {:,.2f} ".format(
            status.total_investment_qtty_in_token1,
        )
    )

    logging.getLogger(__name__).info(
        "\tNet market gains:\t {:,.2f}\t [{:+,.2%} vs investment]".format(
            status.total_current_result_in_usd,
            status.total_current_result_in_usd / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0,
        )
    )

    logging.getLogger(__name__).info(
        "\tShares:\t {:,.2f}\t [{:,.2%} over total]".format(
            status.shares_qtty, status.shares_percent
        )
    )
    logging.getLogger(__name__).info("\tImpermanent loss:")
    logging.getLogger(__name__).info(
        "\t   LP vs HOLDING USD:\t {:,.2f}\t [{:,.2%}]".format(
            status.total_underlying_in_usd - status.total_investment_qtty_in_usd,
            (status.total_underlying_in_usd - status.total_investment_qtty_in_usd)
            / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   LP vs HOLDING token0:\t {:,.2f}\t [{:,.2%}]".format(
            status.total_underlying_in_usd
            - (status.total_investment_qtty_in_token0 * status.usd_price_token0),
            (
                status.total_underlying_in_usd
                - (status.total_investment_qtty_in_token0 * status.usd_price_token0)
            )
            / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )
    logging.getLogger(__name__).info(
        "\t   LP vs HOLDING token1:\t {:,.2f}\t [{:,.2%}]".format(
            status.total_underlying_in_usd
            - (status.total_investment_qtty_in_token1 * status.usd_price_token1),
            (
                status.total_underlying_in_usd
                - (status.total_investment_qtty_in_token1 * status.usd_price_token1)
            )
            / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0,
        )
    )

    logging.getLogger(__name__).info("")
    logging.getLogger(__name__).info("\tRelative situation:  ( in USD )")

    anual_fees = (
        (status.total_fees_collected_in_usd + status.total_fees_uncollected_in_usd)
        / status.secPassed
        if status.secPassed
        else 0
    ) * (60 * 60 * 24 * 365)
    anual_roi = (
        (status.total_current_result_in_usd) / status.secPassed
        if status.secPassed
        else 0
    ) * (60 * 60 * 24 * 365)
    # anual_roi_token0 = ((status.current_result_token0)/status.secPassed)*(60*60*24*365)
    # anual_roi_token1 = ((status.current_result_token1)/status.secPassed)*(60*60*24*365)

    logging.getLogger(__name__).info(
        "\tAnualized fees:\t {:,.2%} vs market value".format(
            anual_fees / status.total_underlying_in_usd
            if status.total_underlying_in_usd
            else 0
        )
    )
    logging.getLogger(__name__).info(
        "\tAnualized return on investment:\t {:,.2%}".format(
            anual_roi / status.total_investment_qtty_in_usd
            if status.total_investment_qtty_in_usd
            else 0
        )
    )
    # logging.getLogger(__name__).info("\t   vs HODL token0:\t {:,.2f}\t [{:,.2%} vs market value]".format(anual_roi_token0/status.underlying_token0, (status.investment_qtty_token0*usd_price_token0)-status.total_underlying_in_usd))
    # logging.getLogger(__name__).info("\t   vs HODL token1:\t {:,.2f}\t [{:,.2%} vs market value]".format(status.investment_qtty_token1*usd_price_token1, (status.investment_qtty_token1*usd_price_token1)-status.total_underlying_in_usd))

    logging.getLogger(__name__).info("")


def user_status_to_csv(status_list: list, folder: str, network: str):

    result = list()
    for r in status_list:
        result.append(r.toDict())

    csv_columns = [
        "account_address",
        "block",
        "timestamp",
        "usd_price_token0",
        "usd_price_token1",
        "shares_qtty",
        "shares_percent",
        "secPassed",
        "investment_qtty_token0",
        "investment_qtty_token1",
        "total_investment_qtty_in_usd",
        "total_investment_qtty_in_token0",
        "total_investment_qtty_in_token1",
        "underlying_token0",
        "underlying_token1",
        "total_underlying_in_usd",
        "fees_collected_token0",
        "fees_collected_token1",
        "total_fees_collected_in_usd",
        "fees_owed_token0",
        "fees_owed_token1",
        "total_fees_owed_in_usd",
        "fees_uncollected_token0",
        "fees_uncollected_token1",
        "total_fees_uncollected_in_usd",
        "fees_uncollected_secPassed",
        "current_result_token0",
        "current_result_token1",
        "total_current_result_in_usd",
        "impermanent_lp_vs_hodl_usd",
        "impermanent_lp_vs_hodl_token0",
        "impermanent_lp_vs_hodl_token1",
    ]
    csv_columns.extend([x for x in list(result[-1].keys()) if x not in csv_columns])
    # topic
    # closed_investment_return_token0	closed_investment_return_token1	current_result_token0		divestment_base_qtty_token0	divestment_base_qtty_token1	divestment_fee_qtty_token0	divestment_fee_qtty_token1							total_closed_investment_return_in_token0	total_closed_investment_return_in_token1	total_closed_investment_return_in_usd	total_current_result_in_token0	total_current_result_in_token1		total_divestment_base_qtty_in_token0	total_divestment_base_qtty_in_token1	total_divestment_base_qtty_in_usd	otal_divestment_fee_qtty_in_usd		impermanent_lp_vs_hodl_usd		total_underlying_in_token0	total_underlying_in_token1

    # set filename
    csv_filename = "{}_{}_from_{}_{}.csv".format(
        network, result[-1]["account_address"], result[0]["block"], result[-1]["block"]
    )
    csv_filename = os.path.join(folder, csv_filename)

    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass

    # save result to csv file
    file_utilities.SaveCSV(filename=csv_filename, columns=csv_columns, rows=result)


def get_hypervisor_addresses(network: str, protocol: str) -> list[str]:

    result = list()
    # get database configuration
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"

    # get blacklisted hypervisors
    blacklisted = (
        CONFIGURATION.get("script", {})
        .get("protocols", {})
        .get(protocol, {})
        .get("filters", {})
        .get("hypervisors_not_included", {})
        .get(network, [])
    )
    # check n clean
    if blacklisted == None:
        blacklisted = []

    # retrieve all addresses from database
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    result = local_db_manager.get_distinct_items_from_database(
        collection_name="operations", field="address"
    )

    # apply black list
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process before applying filters: {len(result)}"
    )
    # filter blcacklisted
    result = [x for x in result if not x in blacklisted]
    logging.getLogger(__name__).debug(
        f" Number of hypervisors to process after applying filters: {len(result)}"
    )

    return result


def test():
    network = "polygon"
    protocol = "gamma"

    b_ini = 0
    b_end = 0
    # hypervisor_addresses = get_hypervisor_addresses(network=network, protocol=protocol)
    # hypervisor_addresses = ["0x35abccd8e577607275647edab08c537fa32cc65e".lower()]
    for address in hypervisor_addresses:
        hype = hypervisor_db_reader(
            hypervisor_address=address, network=network, protocol=protocol
        )
        try:
            hype._process_operations()

            # usr_status_list = hype.get_account_info(
            #     account_address="0x09c46a907ba6167c50423ca130ad123dc6ec9862".lower()
            # )  # block 14484376

            # # save
            # user_status_to_csv(
            #     status_list=usr_status_list,
            #     folder=PARENT_FOLDER + "/tests",
            #     network=network,
            # )
            # # print user result
            # print_user_status(usr_status_list[-1])

            # print Total Hypervisor results
            if b_ini == 0:
                b_ini = hype._get_block(
                    timestamp=int((datetime.utcnow() - timedelta(days=7)).timestamp())
                )
                b_end = hype._get_block(timestamp=int(datetime.utcnow().timestamp()))

            hype_status_list = hype.result_list(
                b_ini=b_ini["block"], b_end=b_end["block"]
            )

            user_status_to_csv(
                status_list=hype_status_list,
                folder=PARENT_FOLDER + "/tests",
                network=network,
            )

            print_user_status(hype_status_list[-1])

        except:
            logging.getLogger(__name__).exception(" yeeep ")


# START ####################################################################################################################
if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        " Start {}   ----------------------> ".format(__module_name)
    )
    # start time log
    _startime = datetime.utcnow()

    test()

    # end time log
    _timelapse = datetime.utcnow() - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} seconds to complete".format(_timelapse.total_seconds())
    )
    logging.getLogger(__name__).info(
        " Exit {}    <----------------------".format(__module_name)
    )
