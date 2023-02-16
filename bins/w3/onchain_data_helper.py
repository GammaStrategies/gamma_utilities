import sys
import os
import datetime as dt
import logging
from web3 import Web3
from web3.middleware import async_geth_poa_middleware, geth_poa_middleware
from pathlib import Path
import math
import tqdm
import concurrent.futures

from bins.w3.protocol_comparator import template

from bins.w3.onchain_utilities.collectors import data_collector
from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    arrakis_hypervisor_cached,
)

from bins.general import general_utilities, file_utilities
from bins.mixed import price_utilities
from bins.database.common import db_operations_models, db_managers
from bins.log import log_helper

from bins.configuration import CONFIGURATION


class onchain_data_helper:

    # SETUP
    def __init__(self, network: str, protocol: str):

        # set init vars
        self.protocol = protocol
        self.network = network

    # HELPERS
    def create_web3_provider(self, network: str) -> Web3:
        """Create a web3 comm privider_

        Args:
           url (str): https://.....
           network (str): ethereum, optimism, polygon, arbitrum, celo

        Returns:
           Web3:
        """

        w3 = Web3(
            Web3.HTTPProvider(
                CONFIGURATION["sources"]["web3Providers"][network],
                request_kwargs={"timeout": 60},
            )
        )
        # add middleware as needed
        if network != "ethereum":
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        # return result
        return w3

    def create_web3_helper(self, address: str, network: str, block: int = 0):
        """create a helper to interact with the protocol defined

        Args:
           address (str): "0x..."

        Returns:
           _type_: protocol helper for web3 interactions
        """
        if self.protocol == "gamma":
            return gamma_hypervisor_cached(
                address=address, network=network, block=block
            )

        elif self.protocol == "arrakis":
            return arrakis_hypervisor_cached(
                address=address, network=network, block=block
            )

        else:
            raise ValueError(
                " No web3 helper defined for {} protocol".format(self.protocol)
            )

    def create_data_collector(self, network: str) -> data_collector:
        """Create a data collector class

        Args:
           network (str):

        Returns:
           data_collector:
        """
        result = None
        if self.protocol == "gamma":
            result = data_collector(
                topics={
                    "gamma_transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "gamma_rebalance": "0xbc4c20ad04f161d631d9ce94d27659391196415aa3c42f6a71c62e905ece782d",
                    "gamma_deposit": "0x4e2ca0515ed1aef1395f66b5303bb5d6f1bf9d61a353fa53f73f8ac9973fa9f6",
                    "gamma_withdraw": "0xebff2602b3f468259e1e99f613fed6691f3a6526effe6ef3e768ba7ae7a36c4f",
                    "gamma_approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
                    "gamma_setFee": "0x91f2ade82ab0e77bb6823899e6daddc07e3da0e3ad998577e7c09c2f38943c43",
                    "gamma_zeroBurn": "0x4606b8a47eb284e8e80929101ece6ab5fe8d4f8735acc56bd0c92ca872f2cfe7",
                },
                topics_data_decoders={
                    "gamma_transfer": ["uint256"],
                    "gamma_rebalance": [
                        "int24",  # tick
                        "uint256",  # totalAmount0
                        "uint256",  # totalAmount1
                        "uint256",  # feeAmount0
                        "uint256",  # feeAmount1
                        "uint256",  # totalSupply
                    ],
                    "gamma_deposit": ["uint256", "uint256", "uint256"],
                    "gamma_withdraw": ["uint256", "uint256", "uint256"],
                    "gamma_approval": ["uint256"],
                    "gamma_setFee": ["uint8"],
                    "gamma_zeroBurn": [
                        "uint8",
                        "uint256",
                        "uint256",
                    ],  # fee, fees0, fees1
                },
                network=network,
            )
        elif self.protocol == "arrakis":
            result = data_collector(
                topics={
                    "arrakis_transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "arrakis_rebalance": "0xc749f9ae947d4734cf1569606a8a347391ae94a063478aa853aeff48ac5f99e8",
                    "arrakis_deposit": "0x55801cfe493000b734571da1694b21e7f66b11e8ce9fdaa0524ecb59105e73e7",
                    "arrakis_withdraw": "0x7239dff1718b550db7f36cbf69c665cfeb56d0e96b4fb76a5cba712961b65509",
                    "arrakis_fee": "0xc28ad1de9c0c32e5394ba60323e44d8d9536312236a47231772e448a3e49de42",
                },
                topics_data_decoders={
                    "arrakis_transfer": ["uint256"],
                    "arrakis_rebalance": ["int24", "int24", "uint128", "uint128"],
                    "arrakis_deposit": [
                        "address",
                        "uint256",
                        "uint256",
                        "uint256",
                        "uint128",
                    ],
                    "arrakis_withdraw": [
                        "address",
                        "uint256",
                        "uint256",
                        "uint256",
                        "uint128",
                    ],
                    "arrakis_fee": ["uint256", "uint256"],
                },
                network=network,
            )
        elif self.protocol == "uniswapv3":
            result = data_collector(
                topics={
                    "uniswapv3_collect": "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01",
                    "burn": "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
                },
                topics_data_decoders={
                    "uniswapv3_collect": ["uint256", "address", "uint256", "uint256"],
                    "burn": [""],
                },
                network=network,
            )
        else:
            raise ValueError(
                " No web3 helper defined for {} protocol".format(self.protocol)
            )

        return result

    def operations_generator(
        self,
        addresses: list,
        network: str,
        block_ini: int,
        block_end: int,
        progress_callback=None,
        max_blocks=10000,
    ) -> db_operations_models.root_operation:
        """get_all Deposits, Withdraws Rebalances, Fees, Transactions
           from the contracts specified.
           Will scann all defined blocks for data regarding the <addresses> supplied

        Args:
           addresses (list): list of string addresses (hypervisors)
           network (str)
           block_ini (int): starting point
           block_end (int): ending point
           update_progress (function, optional): function accepting text:str, . Defaults to None.
           max_blocks (int): maximum qtty of blocks for each query ( some servers will accept high-low numbers here...)

        """

        # create new data collector helper
        dta_coll = self.create_data_collector(network=network)

        # PROGRESS
        dta_coll.progress_callback = progress_callback

        # loop thru content
        for operation in dta_coll.operations_generator(
            block_ini=block_ini,
            block_end=block_end,
            contracts=[Web3.toChecksumAddress(x) for x in addresses],
            max_blocks=max_blocks,
        ):
            # get data collector's token helper's info to complete later conversion
            token_helper = dta_coll._token_helpers[operation["address"].lower()]

            yield self.convert_operation(
                operation=operation,
                network=network,
                address_token0=token_helper["address_token0"],
                address_token1=token_helper["address_token1"],
            )

    def convert_operation(
        operation: dict, network: str, address_token0: str, address_token1: str
    ) -> db_operations_models.root_operation:

        # operation_hypervisor = operation["address"]
        operation_type = operation["topic"]
        block = operation["blockNumber"]
        transactionHash = operation["transactionHash"]
        blockHash = operation["blockHash"]
        timestamp = operation["timestamp"]

        # get price
        price_usd_token0 = self.price_helper.get_price(
            network=network,
            token_id=address_token0,
            block=block,
            of="USD",
        )
        price_usd_token1 = self.price_helper.get_price(
            network=network,
            token_id=address_token1,
            block=result_item["block"],
            of="USD",
        )

        # create data object
        if operation_type in ["deposit", "withdraw"]:

            originator = (
                operation["sender"] if operation_type == "deposit" else operation["to"]
            )

            shares = operation["shares"]
            qtty_token0 = operation["qtty_token0"]
            qtty_token1 = operation["qtty_token1"]

            # calculations
            qtty_total_in_usd = (
                operation["qtty_token0"] * price_usd_token0
                + operation["qtty_token1"] * price_usd_token1
            )
            qtty_total_in_token0 = (
                (
                    operation["qtty_token0"]
                    + (price_usd_token1 / price_usd_token0) * operation["qtty_token1"]
                )
                if price_usd_token0 > 0
                else 0
            )
            qtty_total_in_token1 = (
                (
                    operation["qtty_token1"]
                    + (price_usd_token0 / price_usd_token1) * operation["qtty_token0"]
                )
                if price_usd_token1 > 0
                else 0
            )

            # set resulting obj
            if operation_type == "deposit":
                result = db_operations_models.deposit_operation(
                    price_usd_token0=price_usd_token0,
                    price_usd_token1=price_usd_token1,
                    operation_type=operation_type,
                    network=network,
                    timestamp=timestamp,
                    transactionHash=transactionHash,
                    block=block,
                    blockHash=blockHash,
                    originator=originator,
                    shares=shares,
                    qtty_token0=qtty_token0,
                    qtty_token1=qtty_token1,
                    qtty_total_in_usd=qtty_total_in_usd,
                    qtty_total_in_token0=qtty_total_in_token0,
                    qtty_total_in_token1=qtty_total_in_token1,
                )
            elif operation_type == "withdraw":
                result = db_operations_models.withdraw_operation(
                    price_usd_token0=price_usd_token0,
                    price_usd_token1=price_usd_token1,
                    operation_type=operation_type,
                    network=network,
                    timestamp=timestamp,
                    transactionHash=transactionHash,
                    block=block,
                    blockHash=blockHash,
                    originator=originator,
                    shares=shares,
                    qtty_token0=qtty_token0,
                    qtty_token1=qtty_token1,
                    qtty_total_in_usd=qtty_total_in_usd,
                    qtty_total_in_token0=qtty_total_in_token0,
                    qtty_total_in_token1=qtty_total_in_token1,
                )
            else:
                raise ValueError("Unexpected operation type {}".format(operation_type))
        elif operation_type == "transfer":

            result = db_operations_models.transfer_operation(
                price_usd_token0=price_usd_token0,
                price_usd_token1=price_usd_token1,
                operation_type=operation_type,
                network=network,
                timestamp=timestamp,
                transactionHash=transactionHash,
                block=block,
                blockHash=blockHash,
                address=operation["address"],
                source=operation["src"],
                destination=operation["dst"],
                qtty=operation["qtty"],
            )
        elif operation_type == "fee":
            result = db_operations_models.collection_operation(
                price_usd_token0=price_usd_token0,
                price_usd_token1=price_usd_token1,
                operation_type=operation_type,
                network=network,
                timestamp=timestamp,
                transactionHash=transactionHash,
                block=block,
                blockHash=blockHash,
                gross_token0=operation["qtty_token0"],
                gross_token1=operation["qtty_token1"],
                gross_total_in_usd=operation["qtty_token0"] * price_usd_token0
                + operation["qtty_token1"] * price_usd_token1,
            )
        elif operation_type == "rebalance":
            result = db_operations_models.rebalance_operation(
                operation_type=operation_type,
                network=network,
                timestamp=timestamp,
                transactionHash=transactionHash,
                block=block,
                blockHash=blockHash,
                upperTick=operation["upperTick"],
                lowerTick=operation["lowerTick"],
            )
        else:
            raise NotImplemented("Topic not implemented: {}".format(operation_type))

        return result

    def get_standard_blockBounds(self, network: str, filters: dict) -> tuple:
        """Return filtered block ini block end or equivalent non filtered

        Args:
           network (str):

        Returns:
           tuple: block_ini, block end
        """

        # create a dummy helper ( use only web3wrap functions)
        dummy_helper = self.create_web3_helper(
            address="0x0000000000000000000000000000000000000000", network=network
        )

        # ease the var access name
        # filters = CONFIGURATION["script"]["protocols"][self.protocol]["filters"]

        # apply filter if defined
        block_ini = block_end = 0
        if "force_timeframe" in filters.keys():
            try:
                start_timestamp = dt.datetime.timestamp(
                    general_utilities.convert_string_datetime(
                        filters["force_timeframe"]["start_time"]
                    )
                )
                end_timestamp = dt.datetime.timestamp(
                    general_utilities.convert_string_datetime(
                        filters["force_timeframe"]["end_time"]
                    )
                )

                # search block number timestamp (bruteforce)
                block_end = dummy_helper.blockNumberFromTimestamp(
                    timestamp=end_timestamp,
                    inexact_mode="before",
                    eq_timestamp_position="last",
                )
                block_ini = dummy_helper.blockNumberFromTimestamp(
                    timestamp=start_timestamp,
                    inexact_mode="after",
                    eq_timestamp_position="first",
                )

                # return result
                return block_ini, block_end

            except:
                logging.getLogger(__name__).exception(
                    " Unexpected error calc. {}'s {} force_timeframe block scan option     .error: {}".format(
                        self.protocol, network, sys.exc_info()[0]
                    )
                )

        # no Force_timeframe field or its processing failed
        # define end as current
        block_end = dummy_helper.w3.eth.get_block("latest").number
        secs = dummy_helper.average_blockTime(
            blocksaway=dummy_helper._w3.eth.get_block("latest").number * 0.85
        )
        blocks_day = math.floor((60 * 60 * 24) / secs)
        block_ini = block_end - (blocks_day * 14)  # 2 weeks

        # return result
        return block_ini, block_end

    def get_networkScan_blockNumbers(
        self, network: str, filters: dict, output: dict
    ) -> tuple:
        """Calculate the initial and end block number to scan a network
           using data already scraped and applying any configuration filter parameter ( like force_timeframe )

        Args:
           network (str): "ethereum" or any other

        Returns:
           int,int: block_ini,block_end   ( WARN: ATM can return zeros )
        """

        # ease the var access name
        # filters = CONFIGURATION["script"]["protocols"][self.protocol]["filters"]
        # output = CONFIGURATION["script"]["protocols"][self.protocol]["output"]

        # get blocks
        block_ini, block_end = self.get_standard_blockBounds(network=network)

        # apply filter if defined
        if "force_timeframe" in filters.keys():
            # return result
            return block_ini, block_end

        # set current working folder
        current_folder = os.path.join(
            output["files"]["save_path"], self.protocol, network
        )

        # load all hypervisors data, if any exists:  load sorted by last time modded so it may beguin from a different point if any interruption rises
        hypervisor_files = (
            sorted(Path(current_folder).iterdir(), key=os.path.getmtime, reverse=False)
            if os.path.isdir(current_folder)
            else []
        )

        # calculate the latest block scraped using the file infos
        block_ini = 0  # TODO: initial block per protocol+network at config.yaml
        if hypervisor_files != None:
            for hyp_file in hypervisor_files:
                # define this hypervisor's last block scraped
                t_last_block_scraped = max(
                    [
                        max([x["blockNumber"] for x in hyp_file["deposits"]])
                        if "deposits" in hyp_file
                        else block_ini,
                        max([x["blockNumber"] for x in hyp_file["withdraws"]])
                        if "withdraws" in hyp_file
                        else block_ini,
                        max([x["blockNumber"] for x in hyp_file["rebalances"]])
                        if "rebalances" in hyp_file
                        else block_ini,
                        max([x["blockNumber"] for x in hyp_file["fees"]])
                        if "fees" in hyp_file
                        else block_ini,
                        max([x["blockNumber"] for x in hyp_file["transactions"]])
                        if "transactions" in hyp_file
                        else block_ini,
                    ]
                )

                # set global last block scraped ( min of all hypervisors)
                block_ini = (
                    min([block_ini, t_last_block_scraped])
                    if block_ini != 0
                    else t_last_block_scraped
                )

        # return result
        return block_ini, block_end

    def get_blocklist_fromDates(
        self, date_ini: dt.datetime, date_end: dt.datetime, network: str
    ) -> list:

        # create a dummy helper ( use only web3wrap functions)
        dummy_helper = self.create_web3_helper(
            address="0x0000000000000000000000000000000000000000", network=network
        )
        secs = dummy_helper.average_blockTime(
            blocksaway=dummy_helper._w3.eth.get_block("latest").number * 0.85
        )

        # define step as 1 day block quantity
        blocks_step = math.floor((60 * 60 * 24) / secs)

        # force seek block numbers from datetime
        block_ini = dummy_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(date_ini),
            inexact_mode="after",
            eq_timestamp_position="first",
        )
        block_end = dummy_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(date_end),
            inexact_mode="before",
            eq_timestamp_position="last",
        )

        # define how many steps fit between blocks
        block_step_reange = math.floor((block_end - block_ini) / blocks_step)

        result = list()
        for i in range(block_step_reange + 2):  # +2 = ini and end blocks
            tmp_block = block_ini + (i * blocks_step)

            if tmp_block < block_end:
                result.append(tmp_block)
            elif tmp_block == block_end:
                result.append(tmp_block)
                break
            else:
                if result[-1] < block_end:
                    result.append(block_end)
                break

        return result

    def get_custom_blockBounds(
        self, date_ini: dt.datetime, date_end: dt.datetime, network: str, step="week"
    ) -> (int, int):

        if step == "week":
            # convert date_ini in that same week first day first hour
            year, week_num, day_of_week = date_ini.isocalendar()
            result_date_ini = dt.datetime.fromisocalendar(year, week_num, 1)

            # convert date_end in that same week last day last hour
            year, week_num, day_of_week = date_end.isocalendar()
            result_date_end = dt.datetime.fromisocalendar(year, week_num, 7)

            step_secs = 60 * 60 * 24 * 7
        elif step == "day":
            # convert date_ini in that same day first hour
            result_date_ini = dt.datetime(
                year=date_ini.year,
                month=date_ini.month,
                day=date_ini.day,
                hour=0,
                minute=0,
                second=0,
            )

            # convert date_end in that same week last day last hour
            result_date_end = dt.datetime(
                year=date_end.year,
                month=date_end.month,
                day=date_end.day,
                hour=23,
                minute=59,
                second=59,
            )

            step_secs = 60 * 60 * 24
        else:
            raise NotImplementedError(
                " blockBounds step not implemented: {}".format(step)
            )

        # create a dummy helper ( use only web3wrap functions)
        dummy_helper = self.create_web3_helper(
            address="0x0000000000000000000000000000000000000000", network=network
        )
        secs = dummy_helper.average_blockTime(
            blocksaway=dummy_helper._w3.eth.get_block("latest").number * 0.85
        )

        # define step as 1 x block quantity
        blocks_step = math.floor(step_secs / secs)

        # force seek block numbers from datetime
        block_ini = dummy_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(result_date_ini),
            inexact_mode="after",
            eq_timestamp_position="first",
        )
        block_end = dummy_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(result_date_end),
            inexact_mode="before",
            eq_timestamp_position="last",
        )

        return block_ini, block_end


class onchain_data_helper2:

    # SETUP
    def __init__(self, protocol: str):

        # set init vars
        self.protocol = protocol

        # create price helper
        self.price_helper = price_utilities.price_scraper(
            cache=CONFIGURATION["cache"]["enabled"],
            cache_filename="uniswapv3_price_cache",
        )

    # HELPERS
    def create_web3_provider(self, network: str) -> Web3:
        """Create a web3 comm privider_

        Args:
           url (str): https://.....
           network (str): ethereum, optimism, polygon, arbitrum, celo

        Returns:
           Web3:
        """

        w3 = Web3(
            Web3.HTTPProvider(
                CONFIGURATION["sources"]["web3Providers"][network],
                request_kwargs={"timeout": 60},
            )
        )
        # add middleware as needed
        if network != "ethereum":
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        # return result
        return w3

    def create_web3_helper(self, address: str, network: str, block: int = 0):
        """create a helper to interact with the protocol defined

        Args:
           address (str): "0x..."

        Returns:
           _type_: protocol helper for web3 interactions
        """
        if self.protocol == "gamma":
            return gamma_hypervisor_cached(
                address=address, network=network, block=block
            )

        elif self.protocol == "arrakis":
            return arrakis_hypervisor_cached(
                address=address, network=network, block=block
            )

        else:
            raise ValueError(
                " No web3 helper defined for {} protocol".format(self.protocol)
            )

    def create_data_collector(self, network: str) -> data_collector:
        """Create a data collector class

        Args:
           network (str):

        Returns:
           data_collector:
        """
        result = None
        if self.protocol == "gamma":
            result = data_collector(
                topics={
                    "gamma_transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # event_signature_hash = web3.keccak(text="transfer(uint32...)").hex()
                    "gamma_rebalance": "0xbc4c20ad04f161d631d9ce94d27659391196415aa3c42f6a71c62e905ece782d",
                    "gamma_deposit": "0x4e2ca0515ed1aef1395f66b5303bb5d6f1bf9d61a353fa53f73f8ac9973fa9f6",
                    "gamma_withdraw": "0xebff2602b3f468259e1e99f613fed6691f3a6526effe6ef3e768ba7ae7a36c4f",
                    "gamma_approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
                    "gamma_setFee": "0x91f2ade82ab0e77bb6823899e6daddc07e3da0e3ad998577e7c09c2f38943c43",
                    "gamma_zeroBurn": "0x4606b8a47eb284e8e80929101ece6ab5fe8d4f8735acc56bd0c92ca872f2cfe7",
                },
                topics_data_decoders={
                    "gamma_transfer": ["uint256"],
                    "gamma_rebalance": [
                        "int24",
                        "uint256",
                        "uint256",
                        "uint256",
                        "uint256",
                        "uint256",
                    ],
                    "gamma_deposit": ["uint256", "uint256", "uint256"],
                    "gamma_withdraw": ["uint256", "uint256", "uint256"],
                    "gamma_approval": ["uint256"],
                    "gamma_setFee": ["uint8"],
                    "gamma_zeroBurn": [
                        "uint8",  # fee
                        "uint256",  # fees0
                        "uint256",  # fees1
                    ],
                },
                network=network,
            )
        elif self.protocol == "arrakis":
            result = data_collector(
                topics={
                    "arrakis_transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "arrakis_rebalance": "0xc749f9ae947d4734cf1569606a8a347391ae94a063478aa853aeff48ac5f99e8",
                    "arrakis_deposit": "0x55801cfe493000b734571da1694b21e7f66b11e8ce9fdaa0524ecb59105e73e7",
                    "arrakis_withdraw": "0x7239dff1718b550db7f36cbf69c665cfeb56d0e96b4fb76a5cba712961b65509",
                    "arrakis_fee": "0xc28ad1de9c0c32e5394ba60323e44d8d9536312236a47231772e448a3e49de42",
                },
                topics_data_decoders={
                    "arrakis_transfer": ["uint256"],
                    "arrakis_rebalance": ["int24", "int24", "uint128", "uint128"],
                    "arrakis_deposit": [
                        "address",
                        "uint256",
                        "uint256",
                        "uint256",
                        "uint128",
                    ],
                    "arrakis_withdraw": [
                        "address",
                        "uint256",
                        "uint256",
                        "uint256",
                        "uint128",
                    ],
                    "arrakis_fee": ["uint256", "uint256"],
                },
                network=network,
            )
        elif self.protocol == "uniswapv3":
            result = data_collector(
                topics={
                    "uniswapv3_collect": "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01",
                },
                topics_data_decoders={
                    "uniswapv3_collect": ["uint256", "address", "uint256", "uint256"],
                },
                network=network,
            )
        else:
            raise ValueError(
                " No web3 helper defined for {} protocol".format(self.protocol)
            )

        return result

    def operations_generator(
        self,
        addresses: list,
        network: str,
        block_ini: int,
        block_end: int,
        progress_callback=None,
        max_blocks=2000,
    ) -> dict:
        """get_all Deposits, Withdraws Rebalances, Fees, Transactions
           from the contracts specified.
           Will scann all defined blocks for data regarding the <addresses> supplied

        Args:
           addresses (list): list of string addresses (hypervisors)
           network (str)
           block_ini (int): starting point
           block_end (int): ending point
           update_progress (function, optional): function accepting text:str, . Defaults to None.
           max_blocks (int): maximum qtty of blocks for each query ( some servers will accept high-low numbers here...)

        """

        # create new data collector helper
        dta_coll = self.create_data_collector(network=network)

        # PROGRESS
        dta_coll.progress_callback = progress_callback

        # loop thru content
        for operation in dta_coll.operations_generator(
            block_ini=block_ini,
            block_end=block_end,
            contracts=[Web3.toChecksumAddress(x) for x in addresses],
            max_blocks=max_blocks,
        ):

            yield operation

    def get_standard_blockBounds(self, network: str) -> tuple:
        """Return filtered block ini block end or equivalent non filtered

        Args:
           network (str):

        Returns:
           tuple: block_ini, block end
        """

        # create a dummy helper ( use only web3wrap functions)
        dummy_helper = self.create_web3_helper(
            address="0x0000000000000000000000000000000000000000", network=network
        )

        # ease the var access name
        filters = CONFIGURATION["script"]["protocols"][self.protocol]["filters"]

        # apply filter if defined
        block_ini = block_end = 0
        if "force_timeframe" in filters.keys():
            try:
                start_timestamp = dt.datetime.timestamp(
                    general_utilities.convert_string_datetime(
                        filters["force_timeframe"]["start_time"]
                    )
                )
                end_timestamp = dt.datetime.timestamp(
                    general_utilities.convert_string_datetime(
                        filters["force_timeframe"]["end_time"]
                    )
                )

                # search block number timestamp (bruteforce)
                block_end = dummy_helper.blockNumberFromTimestamp(
                    timestamp=end_timestamp,
                    inexact_mode="before",
                    eq_timestamp_position="last",
                )
                block_ini = dummy_helper.blockNumberFromTimestamp(
                    timestamp=start_timestamp,
                    inexact_mode="after",
                    eq_timestamp_position="first",
                )

                # return result
                return block_ini, block_end

            except:
                logging.getLogger(__name__).exception(
                    " Unexpected error calc. {}'s {} force_timeframe block scan option     .error: {}".format(
                        self.protocol, network, sys.exc_info()[0]
                    )
                )

        # no Force_timeframe field or its processing failed
        # define end as current
        block_end = dummy_helper.w3.eth.get_block("latest").number
        secs = dummy_helper.average_blockTime(
            blocksaway=dummy_helper._w3.eth.get_block("latest").number * 0.85
        )
        blocks_day = math.floor((60 * 60 * 24) / secs)
        block_ini = block_end - (blocks_day * 14)  # 2 weeks

        # return result
        return block_ini, block_end

    def get_networkScan_blockNumbers(self, network: str) -> tuple:
        """Calculate the initial and end block number to scan a network
           using data already scraped and applying any configuration filter parameter ( like force_timeframe )

        Args:
           network (str): "ethereum" or any other

        Returns:
           int,int: block_ini,block_end   ( WARN: ATM can return zeros )
        """

        # ease the var access name
        filters = CONFIGURATION["script"]["protocols"][self.protocol]["filters"]
        output = CONFIGURATION["script"]["protocols"][self.protocol]["output"]

        # get blocks
        block_ini, block_end = self.get_standard_blockBounds(network=network)

        # apply filter if defined
        if "force_timeframe" in filters.keys():
            # return result
            return block_ini, block_end

        # set current working folder
        current_folder = os.path.join(
            output["files"]["save_path"], self.protocol, network
        )

        # load all hypervisors data, if any exists:  load sorted by last time modded so it may beguin from a different point if any interruption rises
        hypervisor_files = (
            sorted(Path(current_folder).iterdir(), key=os.path.getmtime, reverse=False)
            if os.path.isdir(current_folder)
            else []
        )

        # calculate the latest block scraped using the file infos
        block_ini = 0  # TODO: initial block per protocol+network at config.yaml
        if hypervisor_files != None:
            for hyp_file in hypervisor_files:
                # define this hypervisor's last block scraped
                t_last_block_scraped = max(
                    [
                        max([x["blockNumber"] for x in hyp_file["deposits"]])
                        if "deposits" in hyp_file
                        else block_ini,
                        max([x["blockNumber"] for x in hyp_file["withdraws"]])
                        if "withdraws" in hyp_file
                        else block_ini,
                        max([x["blockNumber"] for x in hyp_file["rebalances"]])
                        if "rebalances" in hyp_file
                        else block_ini,
                        max([x["blockNumber"] for x in hyp_file["fees"]])
                        if "fees" in hyp_file
                        else block_ini,
                        max([x["blockNumber"] for x in hyp_file["transactions"]])
                        if "transactions" in hyp_file
                        else block_ini,
                    ]
                )

                # set global last block scraped ( min of all hypervisors)
                block_ini = (
                    min([block_ini, t_last_block_scraped])
                    if block_ini != 0
                    else t_last_block_scraped
                )

        # return result
        return block_ini, block_end

    def get_blocklist_fromDates(
        self, date_ini: dt.datetime, date_end: dt.datetime, network: str
    ) -> list:

        # create a dummy helper ( use only web3wrap functions)
        dummy_helper = self.create_web3_helper(
            address="0x0000000000000000000000000000000000000000", network=network
        )
        secs = dummy_helper.average_blockTime(
            blocksaway=dummy_helper._w3.eth.get_block("latest").number * 0.85
        )

        # define step as 1 day block quantity
        blocks_step = math.floor((60 * 60 * 24) / secs)

        # force seek block numbers from datetime
        block_ini = dummy_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(date_ini),
            inexact_mode="after",
            eq_timestamp_position="first",
        )
        block_end = dummy_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(date_end),
            inexact_mode="before",
            eq_timestamp_position="last",
        )

        # define how many steps fit between blocks
        block_step_reange = math.floor((block_end - block_ini) / blocks_step)

        result = list()
        for i in range(block_step_reange + 2):  # +2 = ini and end blocks
            tmp_block = block_ini + (i * blocks_step)

            if tmp_block < block_end:
                result.append(tmp_block)
            elif tmp_block == block_end:
                result.append(tmp_block)
                break
            else:
                if result[-1] < block_end:
                    result.append(block_end)
                break

        return result

    def get_custom_blockBounds(
        self, date_ini: dt.datetime, date_end: dt.datetime, network: str, step="week"
    ) -> tuple[int, int]:

        if step == "week":
            # convert date_ini in that same week first day first hour
            year, week_num, day_of_week = date_ini.isocalendar()
            result_date_ini = dt.datetime.fromisocalendar(year, week_num, 1)

            # convert date_end in that same week last day last hour
            year, week_num, day_of_week = date_end.isocalendar()
            result_date_end = dt.datetime.fromisocalendar(year, week_num, 7)

            step_secs = 60 * 60 * 24 * 7
        elif step == "day":
            # convert date_ini in that same day first hour
            result_date_ini = dt.datetime(
                year=date_ini.year,
                month=date_ini.month,
                day=date_ini.day,
                hour=0,
                minute=0,
                second=0,
            )

            # convert date_end in that same week last day last hour
            result_date_end = dt.datetime(
                year=date_end.year,
                month=date_end.month,
                day=date_end.day,
                hour=23,
                minute=59,
                second=59,
            )

            step_secs = 60 * 60 * 24
        else:
            raise NotImplementedError(
                " blockBounds step not implemented: {}".format(step)
            )

        # create a dummy helper ( use only web3wrap functions)
        dummy_helper = self.create_web3_helper(
            address="0x0000000000000000000000000000000000000000", network=network
        )
        secs = dummy_helper.average_blockTime(
            blocksaway=dummy_helper._w3.eth.get_block("latest").number * 0.85
        )

        # define step as 1 week block quantity
        blocks_step = math.floor(step_secs / secs)

        # force seek block numbers from datetime
        block_ini = dummy_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(result_date_ini),
            inexact_mode="after",
            eq_timestamp_position="first",
        )
        try:
            block_end = dummy_helper.blockNumberFromTimestamp(
                timestamp=dt.datetime.timestamp(result_date_end),
                inexact_mode="before",
                eq_timestamp_position="last",
            )
        except:
            # Last chance: get last block
            logging.getLogger(__name__).warning(
                f" Unexpected error converting datetime to block end in {network}. Trying to get last block instead."
            )
            try:
                block_end = dummy_helper.w3.eth.get_block("latest").number
            except:
                logging.getLogger(__name__).exception(
                    f" Unexpected error retrieving {network}'s last block. error->{sys.exc_info()[0]}"
                )

        return block_ini, block_end

    def get_block_fromDatetime(
        self, date: dt.datetime, network: str, step="week"
    ) -> int:

        if step == "week":
            # convert date_ini in that same week first day first hour
            year, week_num, day_of_week = date_ini.isocalendar()
            result_date_ini = dt.datetime.fromisocalendar(year, week_num, 1)

            # convert date_end in that same week last day last hour
            year, week_num, day_of_week = date_end.isocalendar()
            result_date_end = dt.datetime.fromisocalendar(year, week_num, 7)

            step_secs = 60 * 60 * 24 * 7
        elif step == "day":
            # convert date_ini in that same day first hour
            result_date_ini = dt.datetime(
                year=date_ini.year,
                month=date_ini.month,
                day=date_ini.day,
                hour=0,
                minute=0,
                second=0,
            )

            # convert date_end in that same week last day last hour
            result_date_end = dt.datetime(
                year=date_end.year,
                month=date_end.month,
                day=date_end.day,
                hour=23,
                minute=59,
                second=59,
            )

            step_secs = 60 * 60 * 24
        else:
            raise NotImplementedError(
                " blockBounds step not implemented: {}".format(step)
            )

    def convert_datetime_toComparable(
        self, date_ini: dt.datetime = None, date_end: dt.datetime = None, step="week"
    ) -> dict:
        """Converts dates to comparable like, when "day" step is chosen, date_ini is converted in that same day first hour and
            date_end in that same day last hour minute second

        Args:
            date_ini (dt.datetime, optional): initial date to transform. Defaults to None.
            date_end (dt.datetime, optional): end date to transform. Defaults to None.
            step (str, optional): can be day and week (TODO: more). Defaults to "week".

        Raises:
            NotImplementedError: when stem is not defined

        Returns:
            dict: {"date_ini":None, "date_end":None, "step_secs":0}
        """
        result = {"date_ini": None, "date_end": None, "step_secs": 0}

        if step == "week":
            if date_ini:
                # convert date_ini in that same week first day first hour
                year, week_num, day_of_week = date_ini.isocalendar()
                result["date_ini"] = dt.datetime.fromisocalendar(year, week_num, 1)
            if date_end:
                # convert date_end in that same week last day last hour
                year, week_num, day_of_week = date_end.isocalendar()
                result["date_end"] = dt.datetime.fromisocalendar(year, week_num, 7)

            result["step_secs"] = 60 * 60 * 24 * 7
        elif step == "day":
            if date_ini:
                # convert date_ini in that same day first hour
                result["date_ini"] = dt.datetime(
                    year=date_ini.year,
                    month=date_ini.month,
                    day=date_ini.day,
                    hour=0,
                    minute=0,
                    second=0,
                )
            if date_end:
                # convert date_end in that same week last day last hour
                result["date_end"] = dt.datetime(
                    year=date_end.year,
                    month=date_end.month,
                    day=date_end.day,
                    hour=23,
                    minute=59,
                    second=59,
                )

            result["step_secs"] = 60 * 60 * 24
        else:
            raise NotImplementedError(
                " blockBounds step not implemented: {}".format(step)
            )

            return result
