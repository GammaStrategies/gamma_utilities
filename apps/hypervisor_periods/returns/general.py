from decimal import Decimal
import logging
from apps.feeds.returns.objects import period_yield_data
from apps.feeds.utils import get_hypervisor_price_per_share, get_reward_pool_prices
from apps.hypervisor_periods.base import hypervisor_periods_base
from bins.database.common.objects.hypervisor import (
    hypervisor_status_object,
    transformer_hypervisor_status,
)
from bins.database.helpers import get_default_globaldb
from bins.errors.general import ProcessingError
from bins.general.enums import Chain, Protocol, rewarderType
from bins.w3.builders import build_db_hypervisor
from bins.errors.actions import process_error


class hypervisor_periods_returns(hypervisor_periods_base):
    def __init__(self, chain: Chain, hypervisor_address: str):
        """Creates returns period for the specified hypervisor

        Args:
            chain (Chain):

        """
        # set main vars
        self.chain = chain
        self.hypervisor_address = hypervisor_address

        self.token_prices = {}

        # self.type =

        # rebalance divergence gain/loss
        self.reset_rebalance_divergence()

        super().__init__()

    def reset(self):
        # reset all vars
        super().reset()

    def reset_rebalance_divergence(self):
        self.rebalance_divergence = {"token0": Decimal("0"), "token1": Decimal("0")}

    # MAIN FUNCTIONS
    def _execute_preLoop(self, hypervisor_data: dict):
        # get all prices related to this hypervisor for the specified period
        token_addresses = [
            hypervisor_data["status"][0]["pool"]["token0"]["address"],
            hypervisor_data["status"][0]["pool"]["token1"]["address"],
        ]
        # get the max and min blocks from the ordered hype status list
        min_block = min([x["block"] for x in hypervisor_data["status"]])
        max_block = max([x["block"] for x in hypervisor_data["status"]])
        # get prices
        self.token_prices = {
            f"{x['address']}_{x['block']}": x["price"]
            for x in get_default_globaldb().get_items_from_database(
                collection_name="usd_prices",
                find={
                    "network": self.chain.database_name,
                    "address": {"$in": token_addresses},
                    "block": {"$gte": min_block, "$lte": max_block},
                },
            )
        }

    def _execute_inLoop(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        pass

    def _execute_inLoop_startItem(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        return
        # check if any position changed and save Permanent Divergence Gain/Losses:
        #  Its not really permanent in usd terms but in token terms ( token0 and token1 )
        # if last_item and self._position_changed(
        #     last_item=last_item, current_item=current_item
        # ):
        #     # position changed.

        #     # neutralize operations for last item ( so that can be compared with current item)
        #     current_item_neutralized = self._neutralize_hypervisor_operations(
        #         hypervisor=current_item
        #     )

        #     # convert to objects
        #     _last_item_converted = hypervisor_status_object(
        #         transformer=transformer_hypervisor_status, **last_item
        #     )
        #     _current_item_converted = hypervisor_status_object(
        #         transformer=transformer_hypervisor_status, **current_item_neutralized
        #     )
        #     # compare items
        #     diff = _current_item_converted - _last_item_converted

        #     # WARNING NOT WORKING this call to get_underlying_value will raise errors when calculating negative uncollected fees
        #     divergence = diff.get_underlying_value()

        #     self.rebalance_divergence["token0"] += divergence.token0
        #     self.rebalance_divergence["token1"] += divergence.token1
        #     return

    def _execute_inLoop_endItem(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        # create yield data and fill from hype status
        current_period = period_yield_data()
        try:
            # fill usd price
            try:
                current_period.set_prices(
                    token0_price=Decimal(
                        str(
                            self.token_prices[
                                f"{current_item['pool']['token0']['address']}_{last_item['block']}"
                            ]
                        )
                    ),
                    token1_price=Decimal(
                        str(
                            self.token_prices[
                                f"{current_item['pool']['token1']['address']}_{last_item['block']}"
                            ]
                        )
                    ),
                    position="ini",
                )
                current_period.set_prices(
                    token0_price=Decimal(
                        str(
                            self.token_prices[
                                f"{current_item['pool']['token0']['address']}_{current_item['block']}"
                            ]
                        )
                    ),
                    token1_price=Decimal(
                        str(
                            self.token_prices[
                                f"{current_item['pool']['token1']['address']}_{current_item['block']}"
                            ]
                        )
                    ),
                    position="end",
                )
            except Exception as e:
                # it will be filled later
                pass

            # Manage rebalance divergence gain/loss from current block-1 ( if exists )
            if self.rebalance_divergence["token0"] != Decimal(
                "0"
            ) or self.rebalance_divergence["token1"] != Decimal("0"):
                current_period.set_rebalance_divergence(
                    token0=self.rebalance_divergence["token0"],
                    token1=self.rebalance_divergence["token1"],
                )
                # reset rebalance divergence gain/loss
                self.reset_rebalance_divergence()

            # fill from hype status
            try:
                current_period.fill_from_hypervisors_data(
                    ini_hype=last_item,
                    end_hype=current_item,
                    network=chain.database_name,
                )
            except ProcessingError as e:
                logging.getLogger(__name__).error(
                    f" Error while creating hype returns. {e.message}"
                )
                # process error
                process_error(e)

            # fill rewards
            # TODO: control types
            try:
                current_period.fill_from_rewards_data(
                    ini_rewards=last_item["rewards_status"],
                    end_rewards=current_item["rewards_status"],
                )
            except ProcessingError as e:
                logging.getLogger(__name__).error(
                    f" Error while creating hype returns rewards. {e.message}"
                )
                # process error
                process_error(e)

            # append to result
            self.result.append(current_period)

        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error while creating hype returns.  {e}"
            )

    def _execute_postLoop(self, hypervisor_data: dict):
        pass

    def _scrape_last_item(
        self, chain: Chain, hypervisor_address: str, block: int, protocol: Protocol
    ) -> dict:
        return None
        # to be able to return , multiple issues must be addressed:
        # last item must not have supply changed
        # prices must be scraped on the fly ( bc there will be no prices in database for that block )
        return build_db_hypervisor(
            address=hypervisor_address,
            network=chain.database_name,
            block=block,
            dex=protocol.database_name,
            cached=True,
        )

    def execute_processes_within_hypervisor_periods(
        self,
        timestamp_ini: int = None,
        timestamp_end: int = None,
        block_ini: int = None,
        block_end: int = None,
        try_solve_errors: bool = False,
    ) -> list:
        logging.getLogger(__name__).debug(
            f" Getting returns of {self.chain.database_name} {self.hypervisor_address} timestamp_ini: {timestamp_ini} timestamp_end: {timestamp_end}  block_ini: {block_ini}  block_end: {block_end}"
        )
        return super().execute_processes_within_hypervisor_periods(
            chain=self.chain,
            hypervisor_address=self.hypervisor_address,
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
            block_ini=block_ini,
            block_end=block_end,
            try_solve_errors=try_solve_errors,
        )

    # divergence between hype periods
    def _neutralize_hypervisor_operations(self, hypervisor: dict) -> dict:
        """Neutralize all operations within this hype block
            Deposits will be subtracted
            Withdraws will be added

        Args:
            hypervisor (dict):

        Returns:
            dict:
        """

        def _operate(hyp: dict, t0: int, t1: int, sh: int) -> dict:
            # add qtty to deployed token
            hyp["tvl"]["deployed_token0"] = str(int(hyp["tvl"]["deployed_token0"]) + t0)
            hyp["tvl"]["deployed_token0"] = str(int(hyp["tvl"]["deployed_token0"]) + t1)
            # tvl_token0
            hyp["tvl"]["tvl_token0"] = str(int(hyp["tvl"]["tvl_token0"]) + t0)
            # tvl_token1
            hyp["tvl"]["tvl_token1"] = str(int(hyp["tvl"]["tvl_token1"]) + t1)

            # totalAmounts
            hyp["totalAmounts"]["total0"] = str(int(hyp["totalAmounts"]["total0"]) + t0)
            hyp["totalAmounts"]["total1"] = str(int(hyp["totalAmounts"]["total1"]) + t1)
            # totalSupply
            hyp["totalSupply"] = str(int(hyp["totalSupply"]) + sh)

            # liquidity ?...

            return hyp

        moded_hypervisor = hypervisor.copy()
        # neutralize operations
        if moded_hypervisor["operations"]:
            for operation in moded_hypervisor["operations"]:
                if operation["topic"] in ["rebalance", "zeroBurn"]:
                    # fee qtty should not be neutralized
                    pass
                elif operation["topic"] == "withdraw":
                    # add qtty to token0 and token1
                    moded_hypervisor = _operate(
                        hyp=moded_hypervisor,
                        t0=int(operation["qtty_token0"]),
                        t1=int(operation["qtty_token1"]),
                        sh=int(operation["shares"]),
                    )

                elif operation["topic"] == "deposit":
                    # remove qtty to token0 and token1
                    moded_hypervisor = _operate(
                        hyp=moded_hypervisor,
                        t0=-int(operation["qtty_token0"]),
                        t1=-int(operation["qtty_token1"]),
                        sh=-int(operation["shares"]),
                    )
                else:
                    pass
                    # raise Exception(f"Unknown operation topic {operation['topic']}")

        return moded_hypervisor

    # position changed
    def _position_changed(self, last_item: dict, current_item: dict) -> bool:
        """Check if the position has changed

        Args:
            last_item (dict):
            current_item (dict):

        Returns:
            bool:
        """
        # check if there is a change in position ticks
        if (
            last_item["baseLower"] != current_item["baseLower"]
            or last_item["baseUpper"] != current_item["baseUpper"]
            or last_item["limitLower"] != current_item["limitLower"]
            or last_item["limitUpper"] != current_item["limitUpper"]
        ):
            return True
        else:
            return False
