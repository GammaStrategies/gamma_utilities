from decimal import Decimal
import logging
from apps.feeds.returns.objects import period_yield_data
from apps.feeds.utils import get_hypervisor_price_per_share, get_reward_pool_prices
from apps.hypervisor_periods.base import hypervisor_periods_base
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

        super().__init__()

    def reset(self):
        # reset all vars
        super().reset()

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
        pass

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
