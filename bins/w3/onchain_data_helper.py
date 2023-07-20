import sys
import os
import datetime as dt
import logging
from web3 import Web3, exceptions, types
from web3.middleware import async_geth_poa_middleware, geth_poa_middleware
from pathlib import Path
import math

from bins.w3.protocols.gamma.collectors import data_collector
from bins.w3.protocols.general import erc20, bep20

from bins.general import general_utilities
from bins.mixed import price_utilities

from bins.configuration import CONFIGURATION


class onchain_data_helper:
    # SETUP
    def __init__(self, protocol: str):
        # set init vars
        self.protocol = protocol

        # create price helper
        self.price_helper = price_utilities.price_scraper(
            cache=CONFIGURATION["cache"]["enabled"],
            cache_filename="uniswapv3_price_cache",
            thegraph=False,
        )

    #

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

    def create_erc20_helper(self, network: str) -> erc20 | bep20:
        # define helper
        return (
            bep20(address="0x0000000000000000000000000000000000000000", network=network)
            if network == "binance"
            else erc20(
                address="0x0000000000000000000000000000000000000000", network=network
            )
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

    # Operations generator

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

    # helpers

    def get_standard_blockBounds(self, network: str) -> tuple:
        """Return filtered block ini block end or equivalent non filtered

        Args:
           network (str):

        Returns:
           tuple: block_ini, block end
        """

        erc20_helper = self.create_erc20_helper(network=network)

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
                block_end = erc20_helper.blockNumberFromTimestamp(
                    timestamp=end_timestamp,
                    inexact_mode="before",
                    eq_timestamp_position="last",
                )
                block_ini = erc20_helper.blockNumberFromTimestamp(
                    timestamp=start_timestamp,
                    inexact_mode="after",
                    eq_timestamp_position="first",
                )

                # return result
                return block_ini, block_end

            except Exception:
                logging.getLogger(__name__).exception(
                    " Unexpected error calc. {}'s {} force_timeframe block scan option     .error: {}".format(
                        self.protocol, network, sys.exc_info()[0]
                    )
                )

        # no Force_timeframe field or its processing failed
        # define end as current
        block_end = erc20_helper._getBlockData(block="latest").number
        secs = erc20_helper.average_blockTime(blocksaway=block_end * 0.85)
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
        erc20_helper = self.create_erc20_helper(network=network)
        block_data = erc20_helper._getBlockData(block="latest")
        secs = erc20_helper.average_blockTime(blocksaway=block_data.number * 0.85)

        # define step as 1 day block quantity
        blocks_step = math.floor((60 * 60 * 24) / secs)

        # force seek block numbers from datetime
        block_ini = erc20_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(date_ini),
            inexact_mode="after",
            eq_timestamp_position="first",
        )
        block_end = erc20_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(date_end),
            inexact_mode="before",
            eq_timestamp_position="last",
        )

        # define how many steps fit between blocks
        block_step_range = math.floor((block_end - block_ini) / blocks_step)

        result = list()
        for i in range(block_step_range + 2):  # +2 = ini and end blocks
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
        erc20_helper = self.create_erc20_helper(network=network)
        block_data = erc20_helper._getBlockData(block="latest")
        secs = erc20_helper.average_blockTime(blocksaway=block_data.number * 0.85)

        # define step as 1 week block quantity
        blocks_step = math.floor(step_secs / secs)

        # force seek block numbers from datetime
        block_ini = self.get_blockNumberFromTimestamp(
            network=network,
            timestamp=dt.datetime.timestamp(result_date_ini),
            inexact_mode="after",
            eq_timestamp_position="first",
        )
        try:
            block_end = self.get_blockNumberFromTimestamp(
                network=network,
                timestamp=dt.datetime.timestamp(result_date_end),
                inexact_mode="before",
                eq_timestamp_position="last",
            )
        except Exception:
            # Last chance: get last block
            logging.getLogger(__name__).warning(
                f" Unexpected error converting datetime to block end in {network}. Trying to get last block instead."
            )
            try:
                block_end = erc20_helper._getBlockData(block="latest").number
            except Exception:
                logging.getLogger(__name__).exception(
                    f" Unexpected error retrieving {network}'s last block. error->{sys.exc_info()[0]}"
                )

        return block_ini, block_end

    def get_block_fromDatetime(
        self, date: dt.datetime, network: str, step="week"
    ) -> int:
        if step == "week":
            # convert date_ini in that same week first day first hour
            year, week_num, day_of_week = date.isocalendar()
            result_date_ini = dt.datetime.fromisocalendar(year, week_num, 1)

            # convert date_end in that same week last day last hour
            result_date_end = dt.datetime.fromisocalendar(year, week_num, 7)

            step_secs = 60 * 60 * 24 * 7
        elif step == "day":
            # convert date_ini in that same day first hour
            result_date_ini = dt.datetime(
                year=date.year,
                month=date.month,
                day=date.day,
                hour=0,
                minute=0,
                second=0,
            )

            # convert date_end in that same week last day last hour
            result_date_end = dt.datetime(
                year=date.year,
                month=date.month,
                day=date.day,
                hour=23,
                minute=59,
                second=59,
            )

            step_secs = 60 * 60 * 24
        else:
            raise NotImplementedError(
                " blockBounds step not implemented: {}".format(step)
            )

        raise NotImplementedError("not implemented")

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

    def get_blockData(
        self, network: str, block: int | str | None = None
    ) -> types.BlockData:
        """Returns block data from network and block number

        Args:
            network (str): network to use
            block (str): block number to retrieve

        Returns:
            dict: block data
        """
        return self.create_erc20_helper(network=network)._getBlockData(
            block=block or "latest"
        )

    def get_blockNumberFromTimestamp(
        self,
        network: str,
        timestamp: dt.datetime.timestamp,
        inexact_mode="before",
        eq_timestamp_position="first",
    ) -> int:
        return self.create_erc20_helper(network=network).blockNumberFromTimestamp(
            timestamp=timestamp,
            inexact_mode=inexact_mode,
            eq_timestamp_position=eq_timestamp_position,
        )

    def average_blockTime(self, network: str) -> dt.datetime.timestamp:
        tmp_erc20 = self.create_erc20_helper(network=network)
        block = tmp_erc20._getBlockData("latest").number
        return tmp_erc20.average_blockTime(blocksaway=block * 0.85)
