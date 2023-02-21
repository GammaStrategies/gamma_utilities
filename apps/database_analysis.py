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
from bins.w3.onchain_utilities.protocols import (
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

    secPassed: int = 0

    # investment are deposits.
    # When transfers occur, investment % of the transfer share qtty is also transfered to new account. When investments are closed, the divestment % is substracted
    investment_qtty_token0: Decimal = Decimal("0")
    investment_qtty_token1: Decimal = Decimal("0")

    # fees always grow in proportion to current block's shares
    fees_collected_token0: Decimal = Decimal("0")
    fees_collected_token1: Decimal = Decimal("0")

    # owed fees + feeGrowth calculation
    fees_uncollected_token0: Decimal = Decimal("0")
    fees_uncollected_token1: Decimal = Decimal("0")
    fees_uncollected_secPassed: int = 0

    # impermanent result token0 token1
    impermanent_result_token0: Decimal = Decimal("0")
    impermanent_result_token1: Decimal = Decimal("0")

    impermanent_result_token0_yearly: Decimal = Decimal("0")
    impermanent_result_token1_yearly: Decimal = Decimal("0")
    impermanent_result_token0_yearly_percent: Decimal = Decimal("0")
    impermanent_result_token1_yearly_percent: Decimal = Decimal("0")

    # fees Yield
    feesYield_result_token0: Decimal = Decimal("0")
    feesYield_result_token1: Decimal = Decimal("0")
    feesYield_result_token0_yearly: Decimal = Decimal("0")
    feesYield_result_token1_yearly: Decimal = Decimal("0")
    feesYield_result_token0_yearly_percent: Decimal = Decimal("0")
    feesYield_result_token1_yearly_percent: Decimal = Decimal("0")

    # current result
    current_result_token0: Decimal = Decimal("0")
    current_result_token1: Decimal = Decimal("0")
    current_result_token0_yearly: Decimal = Decimal("0")
    current_result_token1_yearly: Decimal = Decimal("0")
    current_result_token0_yearly_percent: Decimal = Decimal("0")
    current_result_token1_yearly_percent: Decimal = Decimal("0")

    # every rebalance, there is a closed position.
    closed_investment_return_token0: Decimal = Decimal("0")
    closed_investment_return_token1: Decimal = Decimal("0")
    closed_investment_return_token0_percent: Decimal = Decimal("0")
    closed_investment_return_token1_percent: Decimal = Decimal("0")

    shares_qtty: Decimal = Decimal("0")
    # ( this is % that multiplied by tvl gives u total qtty assets)
    shares_percent: Decimal = Decimal("0")

    # total underlying assets ( Be aware that includes uncollected fees )
    total_underlying_token0: Decimal(0) = Decimal("0")
    total_underlying_token1: Decimal(0) = Decimal("0")

    # save the raw operation info here
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
                    f" Not processing 0x..{self._hypervisor_address[-4:]} fee collection as it has no deposits yet but collected fees are NON zero --> token0: {fees_collected_token0}  token1: {fees_collected_token1}"
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

        result = sum(list(total_shares_addresses.values()))
        result_noCalc = Decimal(self._statuc[at_block]["totalSupply"])
        # TODO: compare both

        # sum shares
        return result

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


class ro_hypervisor_db:
    def __init__(self, hypervisor_address: str, network: str, protocol: str):

        self._hypervisor_address = hypervisor_address
        self.__blacklist_addresses = ["0x0000000000000000000000000000000000000000"]
        # { <block>: {<user address>:status list}}
        self._users_by_block = dict()

        # databse items
        self._status = dict()  # hypervisor status
        self._operations = dict()  # hypervior operations

        # control var (itemsprocessed): list of operation ids processed
        self.ids_processed = list()
        # control var time order :  last block always >= current
        self.last_block_processed: int = 0

        self.network = network
        self.protocol = protocol

        # apy
        self.unc_fee = list[
            dict
        ]  # {"seconds_passed", "uncollected_t0", "uncollected_t1"}

    # Configuration
    def load_from_database(self, db_url: str):
        """load all possible hypervisor data from database

        Args:
            db_url (str): mongo database full url access
        """
        # create database link
        db_name = f"{self.network}_{self.protocol}"
        local_db_manager = database_local(mongo_url=db_url, db_name=db_name)

        # load status available
        self._status = {
            x["block"]: self._convert_status(x)
            for x in local_db_manager.get_all_status(
                hypervisor_address=self._hypervisor_address.lower()
            )
        }
        # load operations available
        self._operations = local_db_manager.get_all_operations(
            hypervisor_address=self._hypervisor_address.lower()
        )

        # fill this hypervisor object with data
        self._fill_with_operations()

    # Public
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
        }

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

        # set result var
        totalSupply = sum(list(total_shares_addresses.values()))

        # check if calc. result var equals direct chain contract call to totalSupply
        try:
            _tsupply = Decimal("0")
            if at_block == 0:
                _tsupply = Decimal(
                    self._status[max(list(self._status.keys()))]["totalSupply"]
                )
            else:
                _tsupply = Decimal(self._status[at_block]["totalSupply"])

            # small variance may be present due to Decimal vs double conversions
            if _tsupply > 0:
                if (totalSupply - _tsupply) / _tsupply > Decimal("0.001"):
                    logging.getLogger(__name__).warning(
                        " Calculated total supply [{}] for {} is different from its saved status [{}]".format(
                            totalSupply, self._hypervisor_address, _tsupply
                        )
                    )
        except:
            logging.getLogger(__name__).error(
                f" Unexpected error comparing totalSupply calc vs onchain results. err-> {sys.exc_info()[0]}"
            )

        # sum shares
        return totalSupply

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

    def result_vars(self) -> dict:
        total = {
            "current_result_token0": Decimal(0),
            "current_result_token1": Decimal(0),
            "impermanent_token0": Decimal(0),
            "impermanent_token1": Decimal(0),
            "investment_qtty_token0": Decimal(0),
            "investment_qtty_token1": Decimal(0),
            "fees_collected_token0": Decimal(0),
            "fees_collected_token1": Decimal(0),
            "feesYield_result_token0": Decimal(0),
            "feesYield_result_token1": Decimal(0),
        }

        for account in self.get_all_account_addresses():
            last_op = self.last_operation(account_address=account)
            total["current_result_token0"] += last_op.current_result_token0
            total["current_result_token1"] += last_op.current_result_token1

            total["investment_qtty_token0"] += last_op.investment_qtty_token0
            total["investment_qtty_token1"] += last_op.investment_qtty_token1

            total["fees_collected_token0"] += last_op.fees_collected_token0
            total["fees_collected_token1"] += last_op.fees_collected_token1

            total["feesYield_result_token0"] += last_op.feesYield_result_token0
            total["feesYield_result_token1"] += last_op.feesYield_result_token1

            total["impermanent_token0"] += last_op.impermanent_result_token0
            total["impermanent_token1"] += last_op.impermanent_result_token1

        return total

    def _fill_with_operations(self):
        # data should already be sorted from source query
        for operation in self._operations:
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
            # TODO: approval topic
            # self._add_operation(self._process_approval(operation=operation))
            pass

        elif operation["topic"] == "zeroBurn":
            self._process_zeroBurn(operation=operation)

        elif operation["topic"] == "setFee":
            # TODO: setFee topic
            pass

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

        # get last operation
        last_op = self.last_operation(
            account_address=account_address, from_block=operation["blockNumber"]
        )

        # fill new status item with last data
        new_status_item.fill_from_status(status=last_op)

        # calc. operation's token investment qtty
        qtty0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        qtty1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )
        # set status investment
        new_status_item.investment_qtty_token0 += qtty0
        new_status_item.investment_qtty_token1 += qtty1

        # add global stats
        new_status_item = self._add_globals_to_status_item(
            current_operation=new_status_item, last_operation=last_op
        )

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
        # get last operation
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
        # add global stats
        new_status_item = self._add_globals_to_status_item(
            current_operation=new_status_item, last_operation=last_op
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

        # get last operation from source address
        last_op_source = self.last_operation(
            account_address=address_source, from_block=block
        )

        # fill new status item with last data
        new_status_item_source.fill_from_status(status=last_op_source)

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

        # get last operation from destination address
        last_op_destination = self.last_operation(
            account_address=address_destination, from_block=block
        )

        # fill new status item with last data
        new_status_item_destination.fill_from_status(status=last_op_destination)

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
        # add global stats
        new_status_item_source = self._add_globals_to_status_item(
            current_operation=new_status_item_source, last_operation=last_op_source
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
        # add global stats
        new_status_item_destination = self._add_globals_to_status_item(
            current_operation=new_status_item_destination,
            last_operation=last_op_destination,
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

            # check inconsistency
            if (total_percentage_applied + user_share) > 1:
                logging.getLogger(__name__).warning(
                    " The total percentage applied while calc. user fees exceeds 100% at {}'s {} hype {} -> {}".format(
                        self.network,
                        self.protocol,
                        self._hypervisor_address,
                        (total_percentage_applied + user_share),
                    )
                )
                # raise ValueError(
                #     " The total percentage applied while calc. user fees exceeds '100%' at {}'s {} hype {} -> {}".format(
                #         self.network, self.protocol, self._hypervisor_address, (total_percentage_applied + user_share)
                #     )
                # )

            # add user share to total processed control var
            total_percentage_applied += user_share

            # add fees collected to user
            new_status_item.fees_collected_token0 += fees_collected_token0 * user_share
            new_status_item.fees_collected_token1 += fees_collected_token1 * user_share

            # get last operation from destination address
            last_op = self.last_operation(
                account_address=account_address, from_block=block
            )
            # add global stats
            new_status_item = self._add_globals_to_status_item(
                current_operation=new_status_item, last_operation=last_op
            )

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
            # self._modify_global_data(
            #     block=block,
            #     add=True,
            #     fee0_remainder=fee0_remainder,
            #     fee1_remainder=fee1_remainder,
            # )

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
    def _add_globals_to_status_item(
        self, current_operation: status_item, last_operation: status_item
    ) -> status_item:

        if last_operation.block == 0:
            # first time return item as it is
            return current_operation

        # get hypervisor status at block
        current_status_data = self._status[current_operation.block]

        # get current block -1 status
        if not (current_operation.block - 1) in self._status:
            logging.getLogger(__name__).error(
                f" Could not find block {last_operation.block} in status for hype {self._hypervisor_address}"
            )
        pre_current_status_data = self._status[current_operation.block - 1]

        # get last operation block status
        if not last_operation.block in self._status:
            logging.getLogger(__name__).error(
                f" Could not find block {last_operation.block} in status for hype {self._hypervisor_address}"
            )
        last_status_data = self._status[last_operation.block]

        # set current_operation's proportional tvl ( underlying tokens value)
        current_operation.total_underlying_token0 = (
            Decimal(current_status_data["totalAmounts"]["total0"])
            * current_operation.shares_percent
        )
        current_operation.total_underlying_token1 = (
            Decimal(current_status_data["totalAmounts"]["total1"])
            * current_operation.shares_percent
        )

        # set current_operation's proportional uncollected fees
        current_operation.fees_uncollected_token0 = (
            Decimal(current_status_data["fees_uncollected"]["qtty_token0"])
            * current_operation.shares_percent
        )
        current_operation.fees_uncollected_token1 = (
            Decimal(current_status_data["fees_uncollected"]["qtty_token1"])
            * current_operation.shares_percent
        )
        current_operation.fees_uncollected_secPassed = (
            current_operation.timestamp - last_operation.timestamp
        )

        # add seconds passed since
        current_operation.secPassed += (
            current_operation.timestamp - last_operation.timestamp
        )

        if current_operation.secPassed > 0:
            # current absolute result
            current_operation.current_result_token0 = (
                current_operation.total_underlying_token0
                - current_operation.investment_qtty_token0
            )
            current_operation.current_result_token1 = (
                current_operation.total_underlying_token1
                - current_operation.investment_qtty_token1
            )
            current_operation.current_result_token0_yearly = (
                current_operation.current_result_token0 / current_operation.secPassed
            ) * (60 * 60 * 24 * 365)
            current_operation.current_result_token1_yearly = (
                current_operation.current_result_token1 / current_operation.secPassed
            ) * (60 * 60 * 24 * 365)
            current_operation.current_result_token0_yearly_percent = (
                (
                    current_operation.current_result_token0_yearly
                    / current_operation.investment_qtty_token0
                )
                if current_operation.investment_qtty_token0 > 0
                else 0
            )
            current_operation.current_result_token1_yearly_percent = (
                (
                    current_operation.current_result_token1_yearly
                    / current_operation.investment_qtty_token1
                )
                if current_operation.investment_qtty_token1 > 0
                else 0
            )

        else:
            # same block operations = transfer -> deposit or withdraw
            return current_operation

        # set impermanent loss
        # i= last operation block
        # e= current operation block -1
        #   IL = TVLi - TVLe
        #   ILyear = ( IL / (Te-Ti) ) * Tyear
        #   IL% = ( ILyear / TVLi )
        #   FeeYield = UFe-UFi
        #   FeeYieldYear = ( FeeYield / (Te-Ti) ) * Tyear
        #   FeeYieldYear% = FeeYieldYear / TVLi
        #

        # time passed between
        # TODO: use pre_current_status_data timestamp and not current operation
        if current_operation.secPassed > 0:
            # define value locked of the position before current operation

            # IL
            current_operation.impermanent_result_token0 = (
                current_operation.current_result_token0
                - current_operation.fees_uncollected_token0
                - current_operation.fees_collected_token0
            )
            current_operation.impermanent_result_token1 = (
                current_operation.current_result_token1
                - current_operation.fees_uncollected_token1
                - current_operation.fees_collected_token1
            )
            current_operation.impermanent_result_token0_yearly = (
                current_operation.impermanent_result_token0
                / current_operation.secPassed
            ) * (60 * 60 * 24 * 365)
            current_operation.impermanent_result_token1_yearly = (
                current_operation.impermanent_result_token1
                / current_operation.secPassed
            ) * (60 * 60 * 24 * 365)
            current_operation.impermanent_result_token0_yearly_percent = (
                (
                    current_operation.impermanent_result_token0_yearly
                    / current_operation.investment_qtty_token0
                )
                if current_operation.investment_qtty_token0 > 0
                else 0
            )
            current_operation.impermanent_result_token1_yearly_percent = (
                (
                    current_operation.impermanent_result_token1_yearly
                    / current_operation.investment_qtty_token1
                )
                if current_operation.investment_qtty_token1 > 0
                else 0
            )

            # define uncollected fees of the position before current operation

            current_operation.feesYield_result_token0 = (
                current_operation.fees_uncollected_token0
                + current_operation.fees_collected_token0
            )
            current_operation.feesYield_result_token1 = (
                current_operation.fees_uncollected_token1
                + current_operation.fees_collected_token1
            )
            current_operation.feesYield_result_token0_yearly = (
                current_operation.feesYield_result_token0 / current_operation.secPassed
            ) * (60 * 60 * 24 * 365)
            current_operation.feesYield_result_token1_yearly = (
                current_operation.feesYield_result_token1 / current_operation.secPassed
            ) * (60 * 60 * 24 * 365)
            current_operation.feesYield_result_token0_yearly_percent = (
                (
                    current_operation.feesYield_result_token0_yearly
                    / current_operation.investment_qtty_token0
                )
                if current_operation.investment_qtty_token0 > 0
                else 0
            )
            current_operation.feesYield_result_token1_yearly_percent = (
                (
                    current_operation.feesYield_result_token1_yearly
                    / current_operation.investment_qtty_token1
                )
                if current_operation.investment_qtty_token1 > 0
                else 0
            )
        else:
            logging.getlogger(__name__).error("seconds passed between operations is 0")

        return current_operation

    def _add_globals_to_status_item_old(
        self, current_operation: status_item, last_operation: status_item
    ) -> status_item:

        if last_operation.block == 0:
            # first time return item as it is
            return current_operation

        # get hypervisor status at block
        current_status_data = self._status[current_operation.block]

        # get current block -1 status
        if not (current_operation.block - 1) in self._status:
            logging.getLogger(__name__).error(
                f" Could not find block {last_operation.block} in status for hype {self._hypervisor_address}"
            )
        pre_current_status_data = self._status[current_operation.block - 1]

        # get last operation block status
        if not last_operation.block in self._status:
            logging.getLogger(__name__).error(
                f" Could not find block {last_operation.block} in status for hype {self._hypervisor_address}"
            )
        last_status_data = self._status[last_operation.block]

        # set current_operation's proportional tvl ( underlying tokens value)
        current_operation.total_underlying_token0 = (
            Decimal(current_status_data["totalAmounts"]["total0"])
            * current_operation.shares_percent
        )
        current_operation.total_underlying_token0 = (
            Decimal(current_status_data["totalAmounts"]["total1"])
            * current_operation.shares_percent
        )

        # set current_operation's proportional uncollected fees
        current_operation.fees_uncollected_token0 = (
            Decimal(current_status_data["fees_uncollected"]["qtty_token0"])
            * current_operation.shares_percent
        )
        current_operation.fees_uncollected_token1 = (
            Decimal(current_status_data["fees_uncollected"]["qtty_token1"])
            * current_operation.shares_percent
        )

        # time passed between
        seconds_passed = current_operation.timestamp - last_operation.timestamp

        if seconds_passed > 0:
            # current absolute result
            current_operation.current_result_token0 = (
                current_operation.total_underlying_token0
                - current_operation.investment_qtty_token0
            )
            current_operation.current_result_token1 = (
                current_operation.total_underlying_token1
                - current_operation.investment_qtty_token1
            )
            current_operation.current_result_token0_yearly = (
                current_operation.current_result_token0 / seconds_passed
            ) * (60 * 60 * 24 * 365)
            current_operation.current_result_token1_yearly = (
                current_operation.current_result_token1 / seconds_passed
            ) * (60 * 60 * 24 * 365)
            current_operation.current_result_token0_yearly_percent = (
                (
                    current_operation.current_result_token0_yearly
                    / current_operation.investment_qtty_token0
                )
                if current_operation.investment_qtty_token0 > 0
                else 0
            )
            current_operation.current_result_token1_yearly_percent = (
                (
                    current_operation.current_result_token1_yearly
                    / current_operation.investment_qtty_token1
                )
                if current_operation.investment_qtty_token1 > 0
                else 0
            )
        else:
            # same block operations = transfer -> deposit or withdraw
            return current_operation

        # set impermanent loss
        # i= last operation block
        # e= current operation block -1
        #   IL = TVLi - TVLe
        #   ILyear = ( IL / (Te-Ti) ) * Tyear
        #   IL% = ( ILyear / TVLi )
        #   FeeYield = UFe-UFi
        #   FeeYieldYear = ( FeeYield / (Te-Ti) ) * Tyear
        #   FeeYieldYear% = FeeYieldYear / TVLi
        #

        # time passed between
        # TODO: use pre_current_status_data timestamp and not current operation
        seconds_passed = current_operation.timestamp - last_operation.timestamp
        if seconds_passed > 0:
            # define value locked of the position before current operation
            tvl0_ini = (
                Decimal(last_status_data["totalAmounts"]["total0"])
                * last_operation.shares_percent
            )
            tvl1_ini = (
                Decimal(last_status_data["totalAmounts"]["total1"])
                * last_operation.shares_percent
            )
            tvl0_end = (
                Decimal(pre_current_status_data["totalAmounts"]["total0"])
                * last_operation.shares_percent
            )
            tvl1_end = (
                Decimal(pre_current_status_data["totalAmounts"]["total1"])
                * last_operation.shares_percent
            )

            # IL
            current_operation.impermanent_result_token0 = tvl0_end - tvl0_ini
            current_operation.impermanent_result_token1 = tvl1_end - tvl1_ini
            current_operation.impermanent_result_token0_yearly = (
                current_operation.impermanent_result_token0 / seconds_passed
            ) * (60 * 60 * 24 * 365)
            current_operation.impermanent_result_token1_yearly = (
                current_operation.impermanent_result_token1 / seconds_passed
            ) * (60 * 60 * 24 * 365)
            current_operation.impermanent_result_token0_yearly_percent = (
                (current_operation.impermanent_result_token0_yearly / tvl0_ini)
                if tvl0_ini > 0
                else 0
            )
            current_operation.impermanent_result_token1_yearly_percent = (
                (current_operation.impermanent_result_token1_yearly / tvl1_ini)
                if tvl1_ini > 0
                else 0
            )

            # define uncollected fees of the position before current operation
            uFees0_ini = (
                last_status_data["fees_uncollected"]["qtty_token0"]
                * last_operation.shares_percent
            )
            uFees1_ini = (
                last_status_data["fees_uncollected"]["qtty_token1"]
                * last_operation.shares_percent
            )

            uFees0_end = (
                pre_current_status_data["fees_uncollected"]["qtty_token0"]
                * last_operation.shares_percent
            )
            uFees1_end = (
                pre_current_status_data["fees_uncollected"]["qtty_token1"]
                * last_operation.shares_percent
            )

            current_operation.feesYield_result_token0 = uFees0_end - uFees0_ini
            current_operation.feesYield_result_token1 = uFees1_end - uFees1_ini
            current_operation.feesYield_result_token0_yearly = (
                current_operation.feesYield_result_token0 / seconds_passed
            ) * (60 * 60 * 24 * 365)
            current_operation.feesYield_result_token1_yearly = (
                current_operation.feesYield_result_token1 / seconds_passed
            ) * (60 * 60 * 24 * 365)
            current_operation.feesYield_result_token0_yearly_percent = (
                (current_operation.feesYield_result_token0_yearly / tvl0_ini)
                if tvl0_ini > 0
                else 0
            )
            current_operation.feesYield_result_token1_yearly_percent = (
                (current_operation.feesYield_result_token1_yearly / tvl1_ini)
                if tvl1_ini > 0
                else 0
            )
        else:
            logging.getlogger(__name__).error("seconds passed between operations is 0")

        return current_operation

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

    def get_account_info(self, account_address: str) -> list[status_item]:
        result = list()
        for block, accounts in self._users_by_block.items():
            if account_address in accounts.keys():
                result.extend(accounts[account_address])

        return sorted(result, key=lambda x: (x.block, x.raw_operation["logIndex"]))


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


def build_hypervisors_test(network: str, protocol: str, maxHypes: int = 10) -> list:

    threaded = False
    # init result
    hypervisors_collection = list()

    # get list of addresses to process
    hypervisor_addresses = get_hypervisor_addresses(network=network, protocol=protocol)

    # select the last
    hypervisor_addresses = hypervisor_addresses[-maxHypes:]

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
        fName=f"Building {hypervisor_address} status",
        callback=logging.getLogger(__name__),
    ):
        # create hypervisor
        hyp_c = ro_hypervisor_db(
            hypervisor_address=hypervisor_address, network=network, protocol=protocol
        )
        try:
            # construct hype from db data
            hyp_c.load_from_database(db_url=mongo_url)
        except ValueError as err:
            logging.getLogger(__name__).error(
                f" Unexpected error while creating hypervisor from db data err: {err}"
            )
        except:
            logging.getLogger(__name__).exception(
                f" Unexpected error while creating hypervisor from db data err: {sys.exc_info()[0]} "
            )

        ######################
        # # get operations from database
        # operations = local_db_manager.get_items_from_database(
        #     collection_name="operations",
        #     find={"address": hypervisor_address},
        #     sort=[("blockNumber", 1), ("logIndex", 1)],
        # )
        # # setup and use operations to fill hypervisor helper
        # hyp_c = hypervisor_onchain_data_processor_w3(
        #     hypervisor_address=hypervisor_address, network=network, protocol=protocol
        # )
        # # hyp_c = hypervisor_onchain_data_processor(
        # #     hypervisor_address=hypervisor_address, network=network, protocol=protocol
        # # )
        # hyp_c.fill_with_operations(operations)

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
        tvl = hype.tvl()
        shares = hype.total_shares()

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
                tvl["token0"],
                Decimal(hype_graph["tvl0"]) - tvl["token0"],
            )
        )
        logging.getLogger(__name__).info(
            """ token 1:    {:,.2f}  <--> {:,.2f}  ==  {}""".format(
                hype_graph["tvl1"],
                tvl["token1"],
                Decimal(hype_graph["tvl1"]) - tvl["token1"],
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
        # logging.getLogger(__name__).info(" ")
        # logging.getLogger(__name__).info(
        #     """ remainder fees : 0: {}    1: {}  """.format(
        #         global_data["fee0_remainder"], global_data["fee1_remainder"]
        #     )
        # )
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


def main():

    dex = "uniswapv3"
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    for protocol in CONFIGURATION["script"]["protocols"].keys():

        filters = CONFIGURATION["script"]["protocols"][protocol]["filters"]

        for network in CONFIGURATION["script"]["protocols"][protocol][
            "networks"
        ].keys():

            hypervisors = build_hypervisors(
                network=network,
                protocol=protocol,
                threaded=False,
            )
            result_vars = list()
            for hype in hypervisors:
                # compare total hypervisor with thegraph data
                compare_with_subgraph(hype=hype, network=network, dex=dex)


def test():
    protocol = "gamma"
    network = "ethereum"
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]

    # hypervisors = build_hypervisors(
    #     network=network,
    #     protocol=protocol,
    #     maxHypes=10,
    # )

    hypervisors = build_hypervisors_test(
        network=network,
        protocol=protocol,
        maxHypes=10,
    )

    logging.getLogger(__name__).info("Total hypervisors: {}".format(len(hypervisors)))

    for hype in hypervisors:
        # compare total hypervisor with thegraph data
        # compare_with_subgraph(hype=hype, network=network, dex=dex)
        try:

            rvars = hype.result_vars()

            logging.getLogger(__name__).info(
                """
               {}: roi  t0:{:,.2%}  t1:{:,.2%}  
                   fees t0:{:,.2%}  t1:{:,.2%}
            """.format(
                    hype._hypervisor_address,
                    rvars["current_result_token0"] / rvars["investment_qtty_token0"],
                    rvars["current_result_token1"] / rvars["investment_qtty_token1"],
                    rvars["fees_collected_token0"] / rvars["investment_qtty_token0"],
                    rvars["fees_collected_token1"] / rvars["investment_qtty_token1"],
                )
            )

        except:
            logging.getLogger(__name__).info(
                "{}:   --> err {}".format(hype._hypervisor_address, sys.exc_info()[0])
            )

        # if (
        #     hype._hypervisor_address
        #     == "0x35abccd8e577607275647edab08c537fa32cc65e".lower()
        # ):
        tst = hype.get_account_info(
            "0x484787baf344005c45e912f3117b6d7fa7507094".lower()
        )
        if len(tst) > 0:
            # hypervisor
            print(f" {hype._hypervisor_address} :")
            fees_token0 = Decimal("0")
            fees_token1 = Decimal("0")
            time = 0
            for op_status in tst:
                fees_token0 += op_status.fees_uncollected_token0
                fees_token1 += op_status.fees_uncollected_token1
                time += op_status.fees_uncollected_secPassed
                if time > 0:
                    t0_res = (
                        op_status.fees_uncollected_token0
                        / op_status.fees_uncollected_secPassed
                    ) * (60 * 60 * 24 * 365)
                    t1_res = (
                        op_status.fees_uncollected_token1
                        / op_status.fees_uncollected_secPassed
                    ) * (60 * 60 * 24 * 365)

                    print(
                        "  {}:  {:,.2%}  {:,.2%}".format(
                            op_status.block, t0_res, t1_res
                        )
                    )

            if time > 0:
                t0_res = (fees_token0 / time) * (60 * 60 * 24 * 365)
                t1_res = (fees_token1 / time) * (60 * 60 * 24 * 365)

                print(" TOTAL: {:,.2%}  {:,.2%}".format(t0_res, t1_res))
            else:
                problm = ""
            ppopo = ""

    popo = ""


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
