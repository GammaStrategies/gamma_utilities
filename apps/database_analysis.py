import sys
import os
import logging
import tqdm
import concurrent.futures

from web3 import Web3
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict, InitVar
from decimal import Decimal


if __name__ == "__main__":
    # append parent directory pth
    CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
    PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
    sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION

from bins.database.common.db_collections_common import database_local, database_global
from bins.general import general_utilities
from bins.apis.thegraph_utilities import gamma_scraper
from bins.w3.onchain_utilities import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
)


@dataclass
class status_item:
    timestamp: float
    block: int
    topic: str = ""
    account_address: str = ""

    investment_qtty_token0: Decimal = Decimal("0")
    investment_qtty_token1: Decimal = Decimal("0")

    fees_collected_token0: Decimal = Decimal("0")
    fees_collected_token1: Decimal = Decimal("0")

    fees_owed_token0: Decimal = Decimal("0")
    fees_owed_token1: Decimal = Decimal("0")

    closed_investment_return_token0: Decimal = Decimal("0")
    closed_investment_return_token1: Decimal = Decimal("0")
    closed_investment_return_token0_percent: Decimal = Decimal("0")
    closed_investment_return_token1_percent: Decimal = Decimal("0")

    shares_qtty: Decimal = Decimal("0")
    # ( this is % that multiplied by tvl gives u total qtty assets)
    shares_percent: Decimal = Decimal("0")

    raw_operation: dict = field(default_factory=dict)

    def fill_from_status(self, status: super):
        doNot_fill = ["timestamp", "block", "topic", "account_address", "raw_operation"]
        props = [
            a
            for a in dir(status)
            if not a.startswith("__")
            and not callable(getattr(status, a))
            and not a in doNot_fill
        ]
        for p in props:
            setattr(self, p, getattr(status, p))


# TODO: hypervisor_onchain_data_processor is gamma univ3 only: code quickswap
class hypervisor_onchain_data_processor:
    def __init__(self, hypervisor_address: str, network: str, protocol: str):
        self._hypervisor_address = hypervisor_address
        self.__blacklist_addresses = ["0x0000000000000000000000000000000000000000"]
        # { <block>: {<user address>:status list}}
        self._users_by_block = dict()
        # { <block>: {
        #     "tvl0": Decimal("0"),
        #     "tvl1": Decimal("0"),
        #     "tvl0_div": Decimal("0"),  # divergence
        #     "tvl1_div": Decimal("0"),
        #     "fee0_remainder": Decimal("0"),  # remainders
        #     "fee1_remainder": Decimal("0"),
        # }}
        self._global_data_by_block = dict()

        # control var (itemsprocessed): list of operation ids processed
        self.ids_processed = list()
        # control var time order :  last block always >= current
        self.last_block_processed: int = 0

        self.network = network
        self.protocol = protocol

    def tvl(self, at_block: int = 0) -> dict:

        data = self.global_data(at_block=at_block)
        return {
            "token0": data.get("tvl0", Decimal(0)),
            "token1": data.get("tvl1", Decimal(0)),
        }

    def shares_qtty(self) -> Decimal:
        total = Decimal(0)

        for account in self.get_all_account_addresses():
            last_op = self.last_operation(account_address=account)
            total += last_op.shares_qtty

        return total

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

    def global_data(self, at_block: int = 0) -> dict:

        if len(self._global_data_by_block.keys()) > 0:
            # current_block = last block
            block = max(list(self._global_data_by_block.keys()))

            # set at_block if there is actually data in that block
            if at_block > 0 and at_block < block:
                block = at_block

            return self._global_data_by_block[block]
        else:
            logging.getLogger(__name__).warning(
                f" global data has been called when there is no global_data_by_block set yet on hypervisor {self._hypervisor_address} users by block length: {len(self._users_by_block)}"
            )
            return {}

    # Public
    def fill_with_operations(self, operations: list):
        sorted_operations = sorted(
            operations, key=lambda x: (x["blockNumber"], x["logIndex"])
        )
        for operation in sorted_operations:
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

    # General classifier
    def _process_operation(self, operation: dict):

        if operation["topic"] == "deposit":
            self._add_operation(operation=self._process_deposit(operation=operation))

        elif operation["topic"] == "withdraw":
            self._add_operation(self._process_withdraw(operation=operation))

        elif operation["topic"] == "transfer":
            # retrieve new status
            op_source, op_destination = self._process_transfer(operation=operation)
            # add to collection
            self._add_operation(operation=op_source)
            self._add_operation(operation=op_destination)

        elif operation["topic"] == "rebalance":
            self._process_rebalance(operation=operation)

        elif operation["topic"] == "approval":
            # self._add_operation(self._process_approval(operation=operation))
            pass

        elif operation["topic"] == "zeroBurn":
            self._process_zeroBurn(operation=operation)

        else:
            raise NotImplementedError(
                f""" {operation["topic"]} topic not implemented yet"""
            )

    # Topic transformers
    def _process_deposit(self, operation: dict) -> status_item:

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()
        # set user address
        account_address = operation["to"].lower()

        # create result
        new_status_item = status_item(
            timestamp=operation["timestamp"],
            block=operation["blockNumber"],
            topic=operation["topic"],
            account_address=account_address,
            raw_operation=operation,
        )

        # fill new status item with last data
        new_status_item.fill_from_status(
            status=self.last_operation(
                account_address=account_address, from_block=operation["blockNumber"]
            )
        )

        # operation qtty token investment
        qtty0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        qtty1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )

        new_status_item.investment_qtty_token0 += qtty0
        new_status_item.investment_qtty_token1 += qtty1

        # add this deposit to global tvl
        self._modify_global_data(block=block, add=True, tvl0=qtty0, tvl1=qtty1)

        # result
        return new_status_item

    def _process_withdraw(self, operation: dict) -> status_item:

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()
        # set user address
        account_address = operation["sender"].lower()

        # create result
        new_status_item = status_item(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            account_address=account_address,
            raw_operation=operation,
        )

        last_op = self.last_operation(account_address=account_address, from_block=block)

        # fill new status item with last data
        new_status_item.fill_from_status(status=last_op)

        divestment_qtty_token0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        divestment_qtty_token1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )

        divestment_shares_qtty = Decimal(operation["shares"]) / (
            Decimal(10) ** Decimal(operation["decimals_contract"])
        )

        # below, divestment has already been substracted from last operation,
        # so we add divestment_shares_qtty to calc percentage divested
        divestment_percentage = divestment_shares_qtty / (
            last_op.shares_qtty + divestment_shares_qtty
        )

        # what will be substracted from investment
        new_status_item.investment_qtty_token0 -= (
            last_op.investment_qtty_token0 * divestment_percentage
        )
        new_status_item.investment_qtty_token1 -= (
            last_op.investment_qtty_token1 * divestment_percentage
        )
        #
        # divestment result = qtty_token - (divestment% * last_investment_qtty_token)  <-- fees + ilg
        new_status_item.closed_investment_return_token0 += divestment_qtty_token0 - (
            last_op.investment_qtty_token0 * divestment_percentage
        )

        new_status_item.closed_investment_return_token1 += divestment_qtty_token1 - (
            last_op.investment_qtty_token1 * divestment_percentage
        )

        # TODO: correct
        # closed_investment_return_token0_percent
        # closed_investment_return_token1_percent

        # remove this deposit to global tvl
        self._modify_global_data(
            block=block,
            add=True,
            tvl0=-divestment_qtty_token0,
            tvl1=-divestment_qtty_token1,
        )

        # result
        return new_status_item

    def _process_transfer(self, operation: dict) -> tuple[status_item, status_item]:

        # TODO: check if transfer to masterchef rewards
        # transfer to masterchef = stake

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()
        # set user address
        address_source = operation["src"].lower()
        address_destination = operation["dst"].lower()

        # create SOURCE result
        new_status_item_source = status_item(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            account_address=address_source,
            raw_operation=operation,
        )

        # fill new status item with last data
        new_status_item_source.fill_from_status(
            status=self.last_operation(account_address=address_source, from_block=block)
        )

        # operation share participation
        shares_qtty = Decimal(operation["qtty"]) / (
            Decimal(10) ** Decimal(operation["decimals_contract"])
        )
        # shares percentage (to be used for investments)
        shares_qtty_percent = (
            (shares_qtty / new_status_item_source.shares_qtty)
            if new_status_item_source.shares_qtty != 0
            else 0
        )

        # calc investment transfered ( with shares)
        investment_qtty_token0_transfer = (
            new_status_item_source.investment_qtty_token0 * shares_qtty_percent
        )
        investment_qtty_token1_transfer = (
            new_status_item_source.investment_qtty_token1 * shares_qtty_percent
        )

        # create DESTINATION result
        new_status_item_destination = status_item(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            account_address=address_destination,
            raw_operation=operation,
        )

        # fill new status item with last data
        new_status_item_destination.fill_from_status(
            status=self.last_operation(
                account_address=address_destination, from_block=block
            )
        )

        # get current total contract_address shares qtty
        total_shares = self.total_shares(at_block=block)
        # total shares here donot include current operation when depositing
        if operation["src"] == "0x0000000000000000000000000000000000000000":
            # before deposit creation transfer
            # add current created shares to total shares
            total_shares += shares_qtty

        elif operation["dst"] == "0x0000000000000000000000000000000000000000":
            # before withdraw transfer (burn)
            pass

        # modify SOURCE:
        new_status_item_source.shares_qtty -= shares_qtty
        new_status_item_source.investment_qtty_token0 -= investment_qtty_token0_transfer
        new_status_item_source.investment_qtty_token1 -= investment_qtty_token1_transfer
        new_status_item_source.shares_percent = (
            (new_status_item_source.shares_qtty / total_shares)
            if total_shares != 0
            else 0
        )

        # modify DESTINATION:
        new_status_item_destination.shares_qtty += shares_qtty
        new_status_item_destination.investment_qtty_token0 += (
            investment_qtty_token0_transfer
        )
        new_status_item_destination.investment_qtty_token1 += (
            investment_qtty_token1_transfer
        )
        new_status_item_destination.shares_percent = (
            (new_status_item_destination.shares_qtty / total_shares)
            if total_shares != 0
            else 0
        )

        # result
        return new_status_item_source, new_status_item_destination

    def _process_rebalance(self, operation: dict):
        """Rebalance affects all users positions

        Args:
            operation (dict):

        Returns:
            status_item: _description_
        """
        # block
        block = operation["blockNumber"]

        # convert TVL
        new_tvl_token0 = Decimal(operation["totalAmount0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        new_tvl_token1 = Decimal(operation["totalAmount1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )

        # calculate permanent gain/loss and set new tvl
        current_tvl = self.tvl(at_block=block)
        tvl0_div = current_tvl["token0"] - new_tvl_token0
        tvl1_div = current_tvl["token1"] - new_tvl_token1
        # add tvl divergence
        self._modify_global_data(
            block=block, add=True, tvl0_div=tvl0_div, tvl1_div=tvl1_div
        )
        # set new tvl
        self._modify_global_data(
            block=block, add=False, tvl0=new_tvl_token0, tvl1=new_tvl_token1
        )

        # share fees with all accounts with shares
        self._share_fees_with_acounts(operation)

    def _process_approval(self, operation: dict):
        # TODO: approval
        pass

    def _process_zeroBurn(self, operation: dict):
        # share fees with all acoounts proportionally
        self._share_fees_with_acounts(operation)

        # add fees to tvl
        block = operation["blockNumber"]
        fees_collected_token0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        fees_collected_token1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )

        self._modify_global_data(
            block=block,
            add=True,
            tvl0=fees_collected_token0,
            tvl1=fees_collected_token1,
        )

    def _share_fees_with_acounts(self, operation: dict):

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()

        # get current total contract_address shares qtty
        total_shares = self.total_shares(at_block=block)

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
                    f" Not processing 0x..{self._hypervisor_address[-4:]} fee collection as it has no deposits yet and collected fees are zero"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" Not processing 0x..{self._hypervisor_address[-4:]} fee collection as it has no deposits yet but fees collected fees are NON zero --> token0: {fees_collected_token0}  token1: {fees_collected_token1}"
                )
            # exit
            return
        if fees_collected_token0 == fees_collected_token1 == 0:
            # there is no collection made ... but hypervisor changed tick boundaries
            logging.getLogger(__name__).debug(
                f" Not processing 0x..{self._hypervisor_address[-4:]} fee collection as it has not collected any fees."
            )
            # exit
            return

        # control var to keep track of total percentage applied
        total_percentage_applied = Decimal("0")
        # loop all addresses
        for account_address in self.get_all_account_addresses(at_block=block):
            # create result
            new_status_item = status_item(
                timestamp=operation["timestamp"],
                block=block,
                topic=operation["topic"],
                account_address=account_address,
                raw_operation=operation,
            )
            # get last address operation (status)
            # fill new status item with last data
            new_status_item.fill_from_status(
                status=self.last_operation(
                    account_address=account_address, from_block=block
                )
            )

            # calc user share in the pool
            user_share = new_status_item.shares_qtty / total_shares

            if (total_percentage_applied + user_share) > 1:
                poop = ""

            total_percentage_applied += user_share

            new_status_item.fees_collected_token0 += fees_collected_token0 * user_share
            new_status_item.fees_collected_token1 += fees_collected_token1 * user_share

            # add new status to hypervisor
            self._add_operation(operation=new_status_item)

        # save fee remainders data
        if total_percentage_applied != Decimal("1"):

            fee0_remainder = (
                Decimal("1") - total_percentage_applied
            ) * fees_collected_token0
            fee1_remainder = (
                Decimal("1") - total_percentage_applied
            ) * fees_collected_token1

            # add remainders to global vars
            self._modify_global_data(
                block=block,
                add=True,
                fee0_remainder=fee0_remainder,
                fee1_remainder=fee1_remainder,
            )

            # log if value is significant
            if (Decimal("1") - total_percentage_applied) > Decimal("0.0001"):
                logging.getLogger(__name__).error(
                    " Only {:,.2f} of the rebalance value has been distributed to current accounts. remainder: {} ".format(
                        total_percentage_applied,
                        (Decimal("1") - total_percentage_applied),
                    )
                )

    def _modify_global_data(self, block: int, add: bool = True, **kwargs):
        """modify hypervisor global vars status like tvl, divergences ..

        Args:
            block (int): block number
            add (bool, optional): "add" [+=] variables to globals or "set" [=]. Defaults to True.
        """
        if not block in self._global_data_by_block:
            # get currentlast block before creating other
            last_block = (
                max(list(self._global_data_by_block.keys()))
                if len(self._global_data_by_block) > 0
                else block
            )

            # init block
            self._global_data_by_block[block] = {
                "tvl0": Decimal("0"),
                "tvl1": Decimal("0"),
                "tvl0_div": Decimal("0"),  # divergence
                "tvl1_div": Decimal("0"),
                "fee0_remainder": Decimal("0"),  # remainders
                "fee1_remainder": Decimal("0"),
            }
            # add last block data
            if len(self._global_data_by_block) > 0:
                # copy old data to new block
                for k, v in self._global_data_by_block[last_block].items():
                    self._global_data_by_block[block][k] += v

        # add or set new data to new block
        for k in kwargs.keys():
            try:
                if add:
                    self._global_data_by_block[block][k] += kwargs[k]
                else:
                    self._global_data_by_block[block][k] = kwargs[k]
            except:
                logging.getLogger(__name__).exception(
                    f" Unexpected error while updating global data key {k} at block {block} of {self._hypervisor_address} hypervisor"
                )

    # Collection
    def _add_operation(self, operation: status_item):
        """

        Args:
            operation (status_item):
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
    def last_operation(self, account_address: str, from_block: int = 0) -> status_item:
        """find the last operation of an account

        Args:
            account_address (str): user account
            from_block (int, optional): find last account operation from a defined block. Defaults to 0.

        Returns:
            status_item: last operation
        """

        block_list = reversed(sorted(list(self._users_by_block.keys())))
        if from_block > 0:
            block_list = [x for x in block_list if x <= from_block]

        for block in block_list:
            if account_address in self._users_by_block[block]:
                return self._users_by_block[block][account_address][
                    -1
                ]  # last item in list

        return status_item(
            timestamp=0,
            block=0,
            topic="",
            account_address=account_address,
        )

    def total_shares(self, at_block: int = 0) -> Decimal:
        """Return total hypervisor shares

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

        # sum shares
        return sum(list(total_shares_addresses.values()))

    def get_all_account_addresses(self, at_block: int = 0) -> list[str]:
        """Get a unique list of addresses

        Args:
            at_block (int, optional): . Defaults to 0.

        Returns:
            list[str]: of account addresses
        """
        addresses_list = set()

        block_list = reversed(sorted(list(self._users_by_block.keys())))
        if at_block > 0:
            block_list = [x for x in block_list if x <= at_block]

        # build a list of addresses
        for block in block_list:
            addresses_list.update(list(self._users_by_block[block].keys()))

        # return unique list
        return list(addresses_list)


class hypervisor_onchain_data_processor_w3(hypervisor_onchain_data_processor):
    def __init__(self, hypervisor_address: str, network: str, protocol: str):
        self._hypervisor_address = hypervisor_address
        self.__blacklist_addresses = ["0x0000000000000000000000000000000000000000"]
        # { <block>: {<user address>:status list}}
        self._users_by_block = dict()
        # { <block>: {
        #     "tvl0": Decimal("0"),
        #     "tvl1": Decimal("0"),
        #     "tvl0_div": Decimal("0"),  # divergence
        #     "tvl1_div": Decimal("0"),
        #     "fee0_remainder": Decimal("0"),  # remainders
        #     "fee1_remainder": Decimal("0"),
        # }}
        self._global_data_by_block = dict()

        # control var (itemsprocessed): list of operation ids processed
        self.ids_processed = list()
        # control var time order :  last block always >= current
        self.last_block_processed: int = 0

        self.network = network
        self.protocol = protocol

        self.hypervisor_w3Helper = gamma_hypervisor_cached(
            address=hypervisor_address, network=self.network
        )

    def tvl(self, at_block: int = 0) -> dict:

        data = self.global_data(at_block=at_block)
        return {
            "token0": data.get("tvl", {}).get("tvl_token0", {}),
            "token1": data.get("tvl", {}).get("tvl_token1", {}),
        }

    def shares_qtty(self) -> Decimal:
        total = Decimal(0)

        for account in self.get_all_account_addresses():
            last_op = self.last_operation(account_address=account)
            total += last_op.shares_qtty

        total_data = self.global_data()
        if total != total_data:
            po = ""

        return total

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

    def global_data(self, at_block: int = 0) -> dict:

        if len(self._global_data_by_block.keys()) > 0:
            # current_block = last block
            block = max(list(self._global_data_by_block.keys()))

            # set at_block if there is actually data in that block
            if at_block > 0 and at_block < block:
                block = at_block

            return self._global_data_by_block[block]
        else:
            logging.getLogger(__name__).warning(
                f" global data has been called when there is no global_data_by_block set yet on hypervisor {self._hypervisor_address} users by block length: {len(self._users_by_block)}"
            )
            return {}

    # Public
    def fill_with_operations(self, operations: list):
        sorted_operations = sorted(
            operations, key=lambda x: (x["blockNumber"], x["logIndex"])
        )
        for operation in sorted_operations:
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

    # General classifier
    def _process_operation(self, operation: dict):
        ###############################################################
        # create and fill data of block and block-1
        self._set_global_data(operation=operation)
        ################################################################
        if operation["topic"] == "deposit":
            self._add_operation(operation=self._process_deposit(operation=operation))

        elif operation["topic"] == "withdraw":
            self._add_operation(self._process_withdraw(operation=operation))

        elif operation["topic"] == "transfer":
            # retrieve new status
            op_source, op_destination = self._process_transfer(operation=operation)
            # add to collection
            self._add_operation(operation=op_source)
            self._add_operation(operation=op_destination)

        elif operation["topic"] == "rebalance":
            self._process_rebalance(operation=operation)

        elif operation["topic"] == "approval":
            # self._add_operation(self._process_approval(operation=operation))
            pass

        elif operation["topic"] == "zeroBurn":
            self._process_zeroBurn(operation=operation)

        else:
            raise NotImplementedError(
                f""" {operation["topic"]} topic not implemented yet"""
            )

    # Topic transformers
    def _process_deposit(self, operation: dict) -> status_item:

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()
        # set user address
        account_address = operation["to"].lower()

        # create result
        new_status_item = status_item(
            timestamp=operation["timestamp"],
            block=operation["blockNumber"],
            topic=operation["topic"],
            account_address=account_address,
            raw_operation=operation,
        )

        # fill new status item with last data
        new_status_item.fill_from_status(
            status=self.last_operation(
                account_address=account_address, from_block=operation["blockNumber"]
            )
        )

        # operation qtty token investment
        qtty0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        qtty1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )

        new_status_item.investment_qtty_token0 += qtty0
        new_status_item.investment_qtty_token1 += qtty1

        # result
        return new_status_item

    def _process_withdraw(self, operation: dict) -> status_item:

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()
        # set user address
        account_address = operation["sender"].lower()

        # create result
        new_status_item = status_item(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            account_address=account_address,
            raw_operation=operation,
        )

        last_op = self.last_operation(account_address=account_address, from_block=block)

        # fill new status item with last data
        new_status_item.fill_from_status(status=last_op)

        divestment_qtty_token0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        divestment_qtty_token1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )

        divestment_shares_qtty = Decimal(operation["shares"]) / (
            Decimal(10) ** Decimal(operation["decimals_contract"])
        )

        # below, divestment has already been substracted from last operation,
        # so we add divestment_shares_qtty to calc percentage divested
        divestment_percentage = divestment_shares_qtty / (
            last_op.shares_qtty + divestment_shares_qtty
        )

        # what will be substracted from investment
        new_status_item.investment_qtty_token0 -= (
            last_op.investment_qtty_token0 * divestment_percentage
        )
        new_status_item.investment_qtty_token1 -= (
            last_op.investment_qtty_token1 * divestment_percentage
        )
        #
        # divestment result = qtty_token - (divestment% * last_investment_qtty_token)  <-- fees + ilg
        new_status_item.closed_investment_return_token0 += divestment_qtty_token0 - (
            last_op.investment_qtty_token0 * divestment_percentage
        )

        new_status_item.closed_investment_return_token1 += divestment_qtty_token1 - (
            last_op.investment_qtty_token1 * divestment_percentage
        )

        # TODO: correct
        # closed_investment_return_token0_percent
        # closed_investment_return_token1_percent

        # result
        return new_status_item

    def _process_transfer(self, operation: dict) -> tuple[status_item, status_item]:

        # TODO: check if transfer to masterchef rewards
        # transfer to masterchef = stake

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()
        # set user address
        address_source = operation["src"].lower()
        address_destination = operation["dst"].lower()

        # create SOURCE result
        new_status_item_source = status_item(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            account_address=address_source,
            raw_operation=operation,
        )

        # fill new status item with last data
        new_status_item_source.fill_from_status(
            status=self.last_operation(account_address=address_source, from_block=block)
        )

        # operation share participation
        shares_qtty = Decimal(operation["qtty"]) / (
            Decimal(10) ** Decimal(operation["decimals_contract"])
        )
        # shares percentage (to be used for investments)
        shares_qtty_percent = (
            (shares_qtty / new_status_item_source.shares_qtty)
            if new_status_item_source.shares_qtty != 0
            else 0
        )

        # calc investment transfered ( with shares)
        investment_qtty_token0_transfer = (
            new_status_item_source.investment_qtty_token0 * shares_qtty_percent
        )
        investment_qtty_token1_transfer = (
            new_status_item_source.investment_qtty_token1 * shares_qtty_percent
        )

        # create DESTINATION result
        new_status_item_destination = status_item(
            timestamp=operation["timestamp"],
            block=block,
            topic=operation["topic"],
            account_address=address_destination,
            raw_operation=operation,
        )

        # fill new status item with last data
        new_status_item_destination.fill_from_status(
            status=self.last_operation(
                account_address=address_destination, from_block=block
            )
        )

        # get current total contract_address shares qtty
        total_shares = self.total_shares(at_block=block)
        # total shares here donot include current operation when depositing
        if operation["src"] == "0x0000000000000000000000000000000000000000":
            # before deposit creation transfer
            # add current created shares to total shares
            total_shares += shares_qtty

        elif operation["dst"] == "0x0000000000000000000000000000000000000000":
            # before withdraw transfer (burn)
            pass

        # modify SOURCE:
        new_status_item_source.shares_qtty -= shares_qtty
        new_status_item_source.investment_qtty_token0 -= investment_qtty_token0_transfer
        new_status_item_source.investment_qtty_token1 -= investment_qtty_token1_transfer
        new_status_item_source.shares_percent = (
            (new_status_item_source.shares_qtty / total_shares)
            if total_shares != 0
            else 0
        )

        # modify DESTINATION:
        new_status_item_destination.shares_qtty += shares_qtty
        new_status_item_destination.investment_qtty_token0 += (
            investment_qtty_token0_transfer
        )
        new_status_item_destination.investment_qtty_token1 += (
            investment_qtty_token1_transfer
        )
        new_status_item_destination.shares_percent = (
            (new_status_item_destination.shares_qtty / total_shares)
            if total_shares != 0
            else 0
        )

        # result
        return new_status_item_source, new_status_item_destination

    def _process_rebalance(self, operation: dict):
        """Rebalance affects all users positions

        Args:
            operation (dict):

        Returns:
            status_item: _description_
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

        # calculate permanent gain/loss and set new tvl
        # current_tvl = self.tvl(at_block=block)
        # tvl0_div = current_tvl["token0"] - new_tvl_token0
        # tvl1_div = current_tvl["token1"] - new_tvl_token1
        # # add tvl divergence
        # self._modify_global_data(
        #     block=block, add=True, tvl0_div=tvl0_div, tvl1_div=tvl1_div
        # )

        # share fees with all accounts with shares
        self._share_fees_with_acounts(operation)

    def _process_approval(self, operation: dict):
        # TODO: approval
        pass

    def _process_zeroBurn(self, operation: dict):
        # share fees with all acoounts proportionally
        self._share_fees_with_acounts(operation)

    def _share_fees_with_acounts(self, operation: dict):

        # block
        block = operation["blockNumber"]
        # contract address
        contract_address = operation["address"].lower()

        # get current total contract_address shares qtty
        total_shares = self.total_shares(at_block=block)

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
                    f" Not processing 0x..{self._hypervisor_address[-4:]} fee collection as it has no deposits yet and collected fees are zero"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" Not processing 0x..{self._hypervisor_address[-4:]} fee collection as it has no deposits yet but fees collected fees are NON zero --> token0: {fees_collected_token0}  token1: {fees_collected_token1}"
                )
            # exit
            return
        if fees_collected_token0 == fees_collected_token1 == 0:
            # there is no collection made ... but hypervisor changed tick boundaries
            logging.getLogger(__name__).debug(
                f" Not processing 0x..{self._hypervisor_address[-4:]} fee collection as it has not collected any fees."
            )
            # exit
            return

        # control var to keep track of total percentage applied
        total_percentage_applied = Decimal("0")
        # loop all addresses
        for account_address in self.get_all_account_addresses(at_block=block):
            # create result
            new_status_item = status_item(
                timestamp=operation["timestamp"],
                block=block,
                topic=operation["topic"],
                account_address=account_address,
                raw_operation=operation,
            )
            # get last address operation (status)
            # fill new status item with last data
            new_status_item.fill_from_status(
                status=self.last_operation(
                    account_address=account_address, from_block=block
                )
            )

            # calc user share in the pool
            user_share = new_status_item.shares_qtty / total_shares

            if (total_percentage_applied + user_share) > 1:
                poop = ""

            total_percentage_applied += user_share

            new_status_item.fees_collected_token0 += fees_collected_token0 * user_share
            new_status_item.fees_collected_token1 += fees_collected_token1 * user_share

            # add new status to hypervisor
            self._add_operation(operation=new_status_item)

        # save fee remainders data
        if total_percentage_applied != Decimal("1"):

            fee0_remainder = (
                Decimal("1") - total_percentage_applied
            ) * fees_collected_token0
            fee1_remainder = (
                Decimal("1") - total_percentage_applied
            ) * fees_collected_token1

            # add remainders to global vars
            self._modify_global_data(
                block=block,
                add=True,
                fee0_remainder=fee0_remainder,
                fee1_remainder=fee1_remainder,
            )

            # log if value is significant
            if (Decimal("1") - total_percentage_applied) > Decimal("0.0001"):
                logging.getLogger(__name__).error(
                    " Only {:,.2f} of the rebalance value has been distributed to current accounts. remainder: {} ".format(
                        total_percentage_applied,
                        (Decimal("1") - total_percentage_applied),
                    )
                )

    def _modify_global_data(self, block: int, add: bool = True, **kwargs):
        """modify hypervisor global vars status like tvl, divergences ..

        Args:
            block (int): block number
            add (bool, optional): "add" [+=] variables to globals or "set" [=]. Defaults to True.
        """
        pass

    def _set_global_data(self, operation: dict):
        block = operation["blockNumber"]
        if not block in self._global_data_by_block:
            # set base
            self._global_data_by_block[block] = self._fill_w3_info(block=block)
        if not block - 1 in self._global_data_by_block:
            self._global_data_by_block[block - 1] = self._fill_w3_info(block=block - 1)

    def _fill_w3_info(self, block: int) -> dict:
        """get block hypervisor info"""
        # set block
        self.hypervisor_w3Helper.block = block
        with general_utilities.log_time_passed(
            fName=f"filling {self._hypervisor_address} w3 info at block {block}",
            callback=log,
        ):
            # get all possible info
            hype_status = self.hypervisor_w3Helper.as_dict()
            # result
            return hype_status

    # Collection
    def _add_operation(self, operation: status_item):
        """

        Args:
            operation (status_item):
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
    def last_operation(self, account_address: str, from_block: int = 0) -> status_item:
        """find the last operation of an account

        Args:
            account_address (str): user account
            from_block (int, optional): find last account operation from a defined block. Defaults to 0.

        Returns:
            status_item: last operation
        """

        block_list = reversed(sorted(list(self._users_by_block.keys())))
        if from_block > 0:
            block_list = [x for x in block_list if x <= from_block]

        for block in block_list:
            if account_address in self._users_by_block[block]:
                return self._users_by_block[block][account_address][
                    -1
                ]  # last item in list

        return status_item(
            timestamp=0,
            block=0,
            topic="",
            account_address=account_address,
        )

    def total_shares(self, at_block: int = 0) -> Decimal:
        """Return total hypervisor shares

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

        # sum shares
        return sum(list(total_shares_addresses.values()))

    def get_all_account_addresses(self, at_block: int = 0) -> list[str]:
        """Get a unique list of addresses

        Args:
            at_block (int, optional): . Defaults to 0.

        Returns:
            list[str]: of account addresses
        """
        addresses_list = set()

        block_list = reversed(sorted(list(self._users_by_block.keys())))
        if at_block > 0:
            block_list = [x for x in block_list if x <= at_block]

        # build a list of addresses
        for block in block_list:
            addresses_list.update(list(self._users_by_block[block].keys()))

        # return unique list
        return list(addresses_list)


def build_hypervisors(network: str, protocol: str, threaded: bool = True) -> list:

    # init result
    hypervisors_collection = list()

    # get list of addresses to process
    hypervisor_addresses = get_hypervisor_addresses(network=network, protocol=protocol)

    with tqdm.tqdm(total=len(hypervisor_addresses), leave=False) as progress_bar:
        if threaded:
            # threaded
            args = (
                (
                    hypervisor_address,
                    network,
                    protocol,
                )
                for hypervisor_address in hypervisor_addresses
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
                for result in ex.map(lambda p: digest_hypervisor_data(*p), args):
                    # progress
                    progress_bar.set_description(
                        " {} processed ".format(result._hypervisor_address)
                    )
                    hypervisors_collection.append(result)
                    # update progress
                    progress_bar.update(1)
        else:
            # sync
            for hypervisor_address in hypervisor_addresses:
                result = digest_hypervisor_data(hypervisor_address, network, protocol)
                # progress
                progress_bar.set_description(
                    " {} processed ".format(result._hypervisor_address)
                )
                hypervisors_collection.append(result)
                # update progress
                progress_bar.update(1)

    return hypervisors_collection


def digest_hypervisor_data(
    hypervisor_address: str, network: str, protocol: str
) -> hypervisor_onchain_data_processor:

    # setup database manager
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{network}_{protocol}"
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)

    with general_utilities.log_time_passed(
        fName=f"Building {hypervisor_address} status", callback=log
    ):
        # get operations from database
        operations = local_db_manager.get_items_from_database(
            collection_name="operations",
            find={"address": hypervisor_address},
            sort=[("blockNumber", 1), ("logIndex", 1)],
        )
        # setup and use operations to fill hypervisor helper
        hyp_c = hypervisor_onchain_data_processor_w3(
            hypervisor_address=hypervisor_address, network=network, protocol=protocol
        )
        # hyp_c = hypervisor_onchain_data_processor(
        #     hypervisor_address=hypervisor_address, network=network, protocol=protocol
        # )
        hyp_c.fill_with_operations(operations)

    return hyp_c


def compare_with_subgraph(
    hype: hypervisor_onchain_data_processor, network: str, dex: str
):
    try:
        last_block = max(list(hype._users_by_block.keys()))

        graph_helper = gamma_scraper(
            cache=False, cache_savePath="data/cache", convert=True
        )
        hype_graph = graph_helper.get_all_results(
            network=network,
            query_name=f"uniswapV3Hypervisors_{dex}",
            block=""" number:{} """.format(last_block),
            where=f""" id: "{hype._hypervisor_address.lower()}" """,
        )[0]

        fees = hype.total_fees()
        global_data = hype.global_data()
        shares = hype.shares_qtty()

        logging.getLogger(__name__).info(" ")
        logging.getLogger(__name__).info(
            f""" HYPERVISOR: {hype._hypervisor_address} """
        )
        logging.getLogger(__name__).info(""" THE GRAPH <-->  WEB3 """)
        logging.getLogger(__name__).info(""" Total supply: """)
        logging.getLogger(__name__).info(
            """ {}  <--> {}   ==  {} """.format(
                hype_graph["totalSupply"],
                shares,
                Decimal(hype_graph["totalSupply"]) - shares,
            )
        )
        logging.getLogger(__name__).info(""" Total value locked: """)
        logging.getLogger(__name__).info(
            """ token 0:    {:,.2f}  <--> {:,.2f}  ==  {} """.format(
                hype_graph["tvl0"],
                global_data["tvl0"],
                Decimal(hype_graph["tvl0"]) - global_data["tvl0"],
            )
        )
        logging.getLogger(__name__).info(
            """ token 1:    {:,.2f}  <--> {:,.2f}  ==  {}""".format(
                hype_graph["tvl1"],
                global_data["tvl1"],
                Decimal(hype_graph["tvl1"]) - global_data["tvl1"],
            )
        )
        logging.getLogger(__name__).info(""" Total fees: """)
        logging.getLogger(__name__).info(
            """ token 0:    {:,.2f}  <--> {:,.2f}  ==  {}""".format(
                hype_graph["grossFeesClaimed0"],
                fees["token0"],
                Decimal(hype_graph["grossFeesClaimed0"]) - fees["token0"],
            )
        )
        logging.getLogger(__name__).info(
            """ token 1:    {:,.2f}  <--> {:,.2f}   ==  {} """.format(
                hype_graph["grossFeesClaimed1"],
                fees["token1"],
                Decimal(hype_graph["grossFeesClaimed1"]) - fees["token1"],
            )
        )
        # remainder
        logging.getLogger(__name__).info(" ")
        logging.getLogger(__name__).info(
            """ remainder fees : 0: {}    1: {}  """.format(
                global_data["fee0_remainder"], global_data["fee1_remainder"]
            )
        )
    except:
        logging.getLogger(__name__).error(
            f" Unexpected error while comparing {hype._hypervisor_address} result with subgraphs data.  --> err: {sys.exc_info()[0]}"
        )


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
    # add hardcoded hyp addresses to list
    # arbitrage attacker "0x89640eB6c8D72606D6a0FFf45415BFF0aB0e3AE1" affected hypervisor "0x0d3fbebfdd96940952618598a5f012de7240c552" mainnet  Visor HEX-ETH Uni v3
    # blacklisted.append("0x0d3fbebfdd96940952618598a5f012de7240c552")

    # retrieve all addresses from database
    local_db_manager = database_local(mongo_url=mongo_url, db_name=db_name)
    result = local_db_manager.get_distinct_items_from_database(
        collection_name="operations", field="address"
    )

    # # TODO: remove debug
    # hypervisor_addresses = [
    #     "0x0d3fbebfdd96940952618598a5f012de7240c552",
    #     "0x18d3284d9eff64fc97b64ab2b871738e684aa151",
    #     "0x1b56860eaf38f27b99d2b0d8ffac86b0f1173f1a",
    #     "0x33412fef1af035d6dba8b2f9b33b022e4c31dbb4",
    #     "0x33682bfc1d94480a0e3de0a565180b182b71d485",
    #     "0x336d7e0a0f87e2729c0080f86801e6f4becf146f",
    #     "0x34b95494c3c2732aa82e1e56be57074fee7a2b28",
    #     "0x35abccd8e577607275647edab08c537fa32cc65e",
    #     "0x388a3938fb6c9c6cb0415946dd5d026f7d98e22c",
    #     "0x3cca05926af387f1ab4cd45ce8975d31f0469927",
    #     "0x3f805de0ec508bf7311baaa617015809be9ce953",
    #     "0x467414f0312ecedba1e505c12bd97daa3609da87",
    #     "0x46e4ddb5b87152dda90afe75daedfddab1c16513",
    #     "0x4f7997158d66ca31d9734674fdcd12cc74e503a7",
    #     "0x5230371a6d5311b1d7dd30c0f5474c2ef0a24661",
    #     "0x52ce16b1f37ea7be4352b29fcde3331e225380ff",
    #     "0x55eed13ab07f8b5538eff301551492a1d776da7b",
    #     "0x5e6c481de496554b66657dd1ca1f70c61cf11660",
    #     "0x6941b1b6b29948a2f74cad0ef2866e93436a5e2d",
    #     "0x6c8116abe5c5f2c39553c6f4217840e71462539c",
    #     "0x6e67bb258b6485b688cbb526c868d4428b634cf1",
    #     "0x704ececabe7855996cede5cefa660eccd3c01dbe",
    #     "0x705b3acaf102404cfdd5e4a60535e4e70091273c",
    #     "0x716bd8a7f8a44b010969a1825ae5658e7a18630d",
    #     "0x717a3276bd6f9e2f0ae447e0ffb45d0fa1c2dc57",
    #     "0x85a5326f08c44ec673e4bfc666b737f7f3dc6b37",
    #     "0x85cbed523459b7f6f81c11e710df969703a8a70c",
    #     "0x8cd73cb1e1fa35628e36b8c543c5f825cd4e77f1",
    #     "0x9196617815d95853945cd8f5e4e0bb88fdfe0281",
    #     "0x92ccaa1b3dccccae7d68fff50e6e47a747233e62",
    #     "0x93acb12ae1effb3426220c20c6d408eeaae59d72",
    #     "0xa625ea468a4c70f13f9a756ffac3d0d250a5c276",
    #     "0xae29f871c9a4cda7ad2c8dff7193c2a0fe3d0c05",
    #     "0xb542f4cb10b7913307e3ed432acd9bf2e709f5fa",
    #     "0xb666bfdb553a1aff4042c1e4f39e43852ba9731d",
    #     "0xbff4a47a0f77637f735e3a0ce10ff2bf9be12e89",
    #     "0xc92ff322c8a18e38b46393dbcc8a7c5691586497",
    #     "0xd7b990543ea8e9bd0b9ae2deb9c52c4d0e660431",
    #     "0xd8dbdb77305898365d7ba6dd438f2663f7d4e409",
    #     "0xd930ab15c8078ebae4ac8da1098a81583603f7ce",
    #     "0xe1ae05518a67ebe7e1e08e3b22d905d6c05b6c0f",
    #     "0xe8f20fd90161de1d5b4cf7e2b5d92932ca06d5f4",
    #     "0xf0a9f5c64f80fa390a46b298791dab9e2bb29bca",
    #     "0xf19f91d7889668a533f14d076adc187be781a458",
    #     "0xf56abca39c27d5c74f94c901b8c137fdf53b3e80",
    #     "0xf874d4957861e193aec9937223062679c14f9aca",
    # ]

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


def main():

    dex = "uniswap_v3"
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    for protocol in CONFIGURATION["script"]["protocols"].keys():

        filters = CONFIGURATION["script"]["protocols"][protocol]["filters"]

        for network in CONFIGURATION["script"]["protocols"][protocol][
            "networks"
        ].keys():

            hypervisors = build_hypervisors(
                network=network,
                protocol=protocol,
                threaded=True,
            )

            for hype in hypervisors:
                compare_with_subgraph(hype=hype, network=network, dex=dex)


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

    main()

    # end time log
    _timelapse = datetime.utcnow() - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} seconds to complete".format(_timelapse.total_seconds())
    )
    logging.getLogger(__name__).info(
        " Exit {}    <----------------------".format(__module_name)
    )
