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


class hypervisor_xploiter:
    def __init__(self, hypervisor_address: str, network: str, protocol: str):

        self._hypervisor_address = hypervisor_address
        self.network = network
        self.protocol = protocol

    def get_results(
        self, timestamp_ini: datetime.timestamp, timestamp_end: datetime.timestamp
    ):
        # create database link
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{self.network}_{self.protocol}"
        local_db_manager = database_local(mongo_url=db_url, db_name=db_name)
        global_db_manager = database_global(mongo_url=mongo_url)

        # load operations available
        self._operations = local_db_manager.get_operations_btwn_timestamps(
            hypervisor_address=self._hypervisor_address.lower(),
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
        )

        # define block range
        block_ini = min([x["blockNumber"] for x in self._operations])
        block_end = max([x["blockNumber"] for x in self._operations])

        # get operations (no transfer, setFee, ...) blocks:
        #       operation block and next operation block -1
        _blocks = dict()
        for operation in self._operations:
            # discard someoperation types
            if not operation["topic"] in ["transfer", "approval", "setFee"]:
                _blocks.append(operation["blockNumber"] - 1)
                _blocks.append(operation["blockNumber"])

        # load status range
        self._status = {
            x["block"]: self._convert_status(x)
            for x in local_db_manager.get_hype_status_btwn_blocks(
                hypervisor_address=self._hypervisor_address.lower(),
                block_ini=block_ini,
                block_end=block_end,
            )
        }

        # process blocks
        self.process_blocks(blocks=_blocks)

    def create_block_list(
        self, timestamp_ini: datetime.timestamp, timestamp_end: datetime.timestamp
    ):
        # operation block = start
        # operation block -1 = end
        result = list[dict]
        # { start_block:<block_num>, end_block:<block_num>, start_timestamp:<timestamp>, end_timestamp:<timestamp>}

        # create database link
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{self.network}_{self.protocol}"
        local_db_manager = database_local(mongo_url=db_url, db_name=db_name)
        global_db_manager = database_global(mongo_url=mongo_url)

        # initial timestamp is either an operation or the closest timestamp in database ( )
        timestamp_ini_standarized = self.standarize_timestamp_to_daily(
            tStamp=timestamp_ini
        )
        # timestamp_end_standarized = self.standarize_timestamp_to_daily(tStamp=timestamp_end)

        # load operations available
        current_op = "start"  # first op is always start
        for operation in local_db_manager.get_operations_btwn_timestamps(
            hypervisor_address=self._hypervisor_address.lower(),
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
        ):
            pass

    def standarize_timestamp_to_daily(
        self, tStamp: datetime.timestamp
    ) -> datetime.timestamp:
        """standarize timestamp to once a day ( so minutes, seconds, etc.. are discarded)

        Args:
            tStamp (datetime.timestamp): timestamp to convert to

        Returns:
            datetime.timestamp:
        """
        tmp_datetime = datetime.fromtimestamp(tStamp)
        return datetime.timestamp(
            datetime(
                year=tmp_datetime.year, month=tmp_datetime.month, day=tmp_datetime.day
            )
        )

    def process_blocks(self, blocks: list):

        result = list[dict]

        last_status = None
        prev_curr_status = None  # current status -1 block
        current_status = None

        for block in sorted(blocks, reverse=True):
            # cant calc without last status
            if last_status:
                # define status
                current_status = self._status[block]
                prev_curr_status = self._status[block - 1]

                # if current block is last+1, this is a start

                # impermanent loss token
                impermanent_tvl0 = (
                    prev_curr_status["totalAmounts"]["total0"]
                    - last_status["totalAmounts"]["total0"]
                )
                impermanent_tvl1 = (
                    prev_curr_status["totalAmounts"]["total1"]
                    - last_status["totalAmounts"]["total1"]
                )

                result.append(
                    {
                        "impermanent_tvl0": impermanent_tvl0,
                        "impermanent_tvl1": impermanent_tvl1,
                    }
                )

            # define last status
            last_status = self._status[block]

    def impermanent_result(
        self,
        token0_ini: Decimal,
        token1_ini: Decimal,
        token0_end: Decimal,
        token1_end: Decimal,
        totalSupply: Decimal,
    ):
        pass
        result = {"token0": 0, "token1": 0}

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
