import tqdm
import datetime as dt
import concurrent.futures
import math
import logging

from web3 import Web3
from web3.middleware import geth_poa_middleware

from bins.w3 import onchain_utilities
from bins.mixed import price_utilities
from bins.general import general_utilities


# structure templates
def template(oftype: str) -> dict:
    if oftype == "root":
        return {
            "datetime": dt.datetime.now(),  # scraing datetime
            "account": "",  # main wallet ( wallet from were data gathering was born)
            "linked_accounts": {},  # <network>: list of <wallet address string>  LINKED WALLETS
            "hypervisors": {
                "ethereum": {},  # { _HYPERVISOR ID_: template_hypervisor }
                "polygon": {},
                "arbitrum": {},
                "optimism": {},
                "celo": {},
            },
            "tokens": {},  # {_TOKEN_NAME_: template_token}
            "staking": {
                "qtty_gamma": 0,  # gamma quantity staked
                "price_usd_gamma": 0,  # price of 1 gamma
            },
            "errors": {
                "price_inconsistency": [],
                "value_inconsistency": [],
                "no_investment": 0,
                "no_totals": 0,
                "no_cost": 0,
                "no_account": False,
                "others": [],
            },
        }
    elif oftype == "comparison":
        return {
            "static": {
                "protocol": "",
                "created": None,  # datetime
                "name_token0": "",  # WMATIC
                "name_token1": "",  # USDC
                "fee": 0,  # 500
                "network": "",  # polygon
                "name": "",  # xWMATIC-USDC05
                "pool_id": "",  # pool id
            },
            "chart": {
                # "data": list(),     # list of x labels
                "labels": list(),  # list of dict
                "hypervisors": list(),
            },
            "errors": {
                "others": []  # list of inconsistencies found when scraping info
            },
        }

    elif oftype == "hypervisor":
        return {
            "static": {
                "protocol": "",  # gamma, arrakis, xtoken
                "id": "",  # hypervisor id
                "created": None,  # datetime
                "name_token0": "",  # WMATIC
                "name_token1": "",  # USDC
                "fee": 0,  # 500
                "network": "",  # polygon
                "name": "",  # xWMATIC-USDC05
                "pool_id": "",  # pool id
                "id_token0": "",
                "id_token1": "",
            },
            "status": {
                "active": True,  # active by default
                "last_updated": None,  # datetime field ()
                "last_rebalance": None,  # datetime field ()
                "block": 0,  # status block
                "timestamp": 0,  # status timestamp
                "shares": 0,
                "qtty_token0": 0,  # token qtty   (this is tvl = deployed_qtty + owed fees + parked_qtty )
                "qtty_token1": 0,  #
                "deployed_token0": 0,  # tokens deployed into pool
                "deployed_token1": 0,  #
                "parked_token0": 0,  # tokens sitting in hype contract ( sleeping )
                "parked_token1": 0,  #
                "supply": 0,  # total Suply
                "first_deposit": None,
                "last_withdraw": None,
                "price_usd_token0": 0,
                "price_usd_token1": 0,
                "price_token0": 0,
                "price_token1": 0,
                "cost": {
                    "qtty_token0": 0,
                    "qtty_token1": 0,
                    "qtty_total_in_usd": 0,
                    "qtty_total_in_token0": 0,
                    "qtty_total_in_token1": 0,
                },
                "deposits": {
                    "aggregated_qtty_token0": 0,
                    "aggregated_qtty_token1": 0,
                    "aggregated_total_in_usd": 0,
                    "aggregated_total_in_token0": 0,
                    "aggregated_total_in_token1": 0,
                },
                "withdraws": {
                    "aggregated_qtty_token0": 0,
                    "aggregated_qtty_token1": 0,
                    "aggregated_total_in_usd": 0,
                    "aggregated_total_in_token0": 0,
                    "aggregated_total_in_token1": 0,
                },
                "fees": {
                    "aggregated_qtty_token0": 0,  # aggregated fees found at rebalances or collections ( not including uncollected fees when collected thru withdraws [check on deposits])
                    "aggregated_qtty_token1": 0,
                    "aggregated_total_in_usd": 0,
                    "uncollected_token0": 0,  # fees not collected nor owed yet but certain
                    "uncollected_token1": 0,
                    "uncollected_total_in_usd": 0,
                    "owed_token0": 0,  # fees owed not deployed ( so value is not earning fees )
                    "owed_token1": 0,
                    "owed_total_in_usd": 0,
                },
                "rebalances": {"aggregated_qtty_total": 0},
            },
            "deposits": [],  # list of template_operation
            "withdraws": [],  # list of template_operation
            "rebalances": [],  # list of template_rebalance
            "fees": [],
            "chart": template(oftype="chart"),
            "transfers": [],
            "errors": {
                "others": []  # list of inconsistencies found when scraping info
            },
        }

    elif oftype == "token":
        return {"qtty_total": 0, "value_total_usd": 0}

    elif oftype == "operation":  # deposit or withdraw
        return {
            "id": "",  # "0x4e76b1869353ea95958b2f7d41a1f19a9dffd75215f82699341749265faaba27-271",
            "datetime": None,  # "2022-07-13T18:44:03"
            "block": 0,
            "blockHash": "",
            "action": "",  # "deposit" or withdraw,
            "originator": "",  #  address originating the operation
            "shares": 0,
            "qtty_total_in_usd": 0,  # TVL
            "qtty_total_in_token0": 0,
            "qtty_total_in_token1": 0,
            "qtty_token0": 0,  #
            "qtty_token1": 0,  #
            "price_usd_token0": 0,
            "price_usd_token1": 0,
        }
    elif oftype == "rebalance":
        return {
            "id": "",  # "0x72c28a41fe3f8835075c640150cf5134c82ca51858d0347591ab64a2cbd1b3a7-129",
            "datetime": None,  # "2022-07-14T01:16:55",
            "block": 0,
            "blockHash": "",
            "upperTick": 0,
            "lowerTick": 0,
        }
    elif oftype == "fee":
        return {
            "datetime": None,  # "2022-07-14T01:16:55",
            "block": 0,
            "blockHash": "",
            "gross_total_in_usd": 0,
            "net_total_in_usd": 0,
            "gross_token0": 0,
            "gross_token1": 0,
            "net_token0": 0,
            "net_token1": 0,
            "net_total_in_usd": 0,
            "price_usd_token0": 0,
            "price_usd_token1": 0,
        }
    elif oftype == "transfer":
        return {
            "id": "",
            "datetime": None,  # "2022-07-14T01:16:55",
            "block": 0,
            "blockHash": "",
            "address": "",
            "source": "",
            "destination": "",
            "qtty": 0,
        }

    elif oftype == "chart_datarow":
        return {
            "datetime": None,
            "block": 0,
            "days_passed": 0,  # days passed since first deposit
            "price_usd_token0": 0,
            "price_usd_token1": 0,
            "tvl_total_in_usd": 0,
            "total_supply": 0,
            "price_share_usd": 0,
            "price_share_change": 0,  # price % change ( vs last share price)
            "qtty_token0": 0,  # only token0 qtty
            "qtty_token1": 0,  #
            "aggregatedFees_qtty_token0": 0,  #
            "aggregatedFees_qtty_token1": 0,  #
            "aggregatedFees_total_in_usd": 0,  # fees
            "aggregatedDeposits_token0": 0,
            "aggregatedDeposits_token1": 0,
            "aggregatedDeposits_total_in_usd": 0,
            "aggregatedWithdraws_token0": 0,
            "aggregatedWithdraws_token1": 0,
            "aggregatedWithdraws_total_in_usd": 0,
        }
    elif oftype == "chart":
        return {
            "data": list(),  # list of chart_datarow
            "labels": list(),  # list of labels
            "options": dict(),  # chart options
        }

    else:
        raise ValueError(" No template defined for {} ".format(oftype))


class comparator_v1:

    # SETUP
    def __init__(self, configuration: dict, protocol: str):

        # set init vars
        self.configuration = configuration
        self.protocol = protocol

        # create price helper
        self.price_helper = price_utilities.price_scraper(
            cache=self.configuration["cache"]["enabled"],
            cache_filename="uniswapv3_price_cache",
            cache_folderName=self.configuration["cache"]["save_path"],
        )

    # PUBLIC
    def create_collection(
        self, addresses: list, network: str, progress_callback=None
    ) -> dict:
        """Scrape addresses transactions and events data

           Create a collection of hypervisors: main data ( being depoists, withdraws, fees, rebalances...)

        Args:
           addresses (list): list of hypervisor addresses to scrape
           network (str):
           progress_callback (def, optional):  . Defaults to None.

        Returns:
           dict: {  <hypervisor_address>: }
        """

        # SCAN SCOPE: define timeframe to scan
        # progress
        if progress_callback:
            progress_callback(
                text=" defining {} {}'s initial and end block to scrape".format(
                    self.protocol, network
                )
            )
        block_ini, block_end = self.get_networkScan_blockNumbers(network=network)

        # SCAN: create dict of hypervisor:template.hypervisor.deposits,withdraws,rebalances,fees, etc..
        data = self.get_dwrfts(
            addresses=addresses,
            network=network,
            block_ini=block_ini,
            block_end=block_end,
            progress_callback=progress_callback,
            max_blocks=10000,
        )

        # return result
        return data

    def create_static(self, address: str, block: int, network: str) -> dict:
        """create_static _summary_

        Args:
           address (str): 0x....
           block (int): if zero, latest block will be chosen
           network (str):

        Returns:
           dict: template(oftype="hypervisor")["static"]
        """

        # create base result
        result = template(oftype="hypervisor")["static"]

        # Web3 helper
        web3_helper = self.create_web3_helper(
            address=address, web3Provider=self.create_web3_provider(network=network)
        )
        # define block
        web3_helper.block = (
            block if block != 0 else web3_helper.w3.eth.get_block("latest").number
        )

        # set general STATIC info
        result["protocol"] = self.protocol
        result["id"] = web3_helper.address
        result["created"] = None
        result["name"] = web3_helper.symbol
        result["fee"] = web3_helper.pool.fee
        result["network"] = network
        result["pool_id"] = web3_helper.pool.address  # exchange address

        # hardcodded patch for MKV 32bytes symbol TODO: something different
        if (
            web3_helper.token0.address.lower()
            == "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2".lower()
        ):
            result["name_token0"] = "MKV"
        else:
            result["name_token0"] = web3_helper.token0.symbol
        if (
            web3_helper.token1.address.lower()
            == "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2".lower()
        ):
            result["name_token1"] = "MKV"
        else:
            result["name_token1"] = web3_helper.token1.symbol

        result["id_token0"] = web3_helper.token0.address
        result["id_token1"] = web3_helper.token1.address

        # result
        return result

    def create_status(
        self,
        address: str,
        block: int,
        network: str,
        address_token0=None,
        address_token1=None,
    ) -> dict:
        """Create part of the STATUS hypervisor doc
           Will not populate deposits, cost, fees, etc...

        Args:
           address (str):
           block (int):
           network (str):

        Returns:
           dict: template oftype "hypervisor" ["status"]
        """

        # create base result
        result = template(oftype="hypervisor")["status"]

        # Web3 helper
        web3_helper = self.create_web3_helper(
            address=address,
            web3Provider=self.create_web3_provider(network=network),
            block=block,
        )

        # TVL PRICE and FEES query
        dta_tvl = web3_helper.get_tvl()
        dta_uncollectedFees = web3_helper.get_fees_uncollected()

        #
        result["supply"] = web3_helper.totalSupply
        # set blockchain data
        result["qtty_token0"] = dta_tvl["tvl_token0"]
        result["qtty_token1"] = dta_tvl["tvl_token1"]
        result["deployed_token0"] = dta_tvl["deployed_token0"]
        result["deployed_token1"] = dta_tvl["deployed_token1"]
        result["parked_token0"] = dta_tvl["parked_token0"]
        result["parked_token1"] = dta_tvl["parked_token1"]
        result["price_token0"] = 0  # TODO: price in token
        result["price_token1"] = 0  # TODO: price in token

        result["block"] = web3_helper.block
        result["timestamp"] = web3_helper.w3.eth.get_block(web3_helper.block).timestamp

        result["fees"]["uncollected_token0"] = dta_uncollectedFees["qtty_token0"]
        result["fees"]["uncollected_token1"] = dta_uncollectedFees["qtty_token1"]
        result["fees"]["owed_token0"] = dta_tvl["fees_owed_token0"]
        result["fees"]["owed_token1"] = dta_tvl["fees_owed_token1"]

        # set price
        if not address_token0:
            address_token0 = web3_helper.token0.address
        if not address_token1:
            address_token1 = web3_helper.token1.address

        result["price_usd_token0"] = self.price_helper.get_price(
            network=network, token_id=address_token0, block=block, of="USD"
        )
        result["price_usd_token1"] = self.price_helper.get_price(
            network=network, token_id=address_token1, block=block, of="USD"
        )

        # set usd total values
        result["fees"]["uncollected_total_in_usd"] = (
            result["fees"]["uncollected_token0"] * result["price_usd_token0"]
        ) + (result["fees"]["uncollected_token1"] * result["price_usd_token1"])
        result["fees"]["owed_total_in_usd"] = (
            result["fees"]["owed_token0"] * result["price_usd_token0"]
        ) + (result["fees"]["owed_token1"] * result["price_usd_token1"])

        return result

    def create_status_ofBlocks(
        self, address: str, network: str, block_list: list, progress_callback=None
    ) -> dict:
        """

        Args:
           address (str):
           network (str):
           block_list (list):  list of int
           progress_callback (funct, optional): . Defaults to None.

        Returns:
           dict: {"<block>": template oftype "hypervisor" ["status"] , ... }
        """

        max_workers = 5

        # Onetime get token addresses to decrease onchain queries when scraping status
        web3_helper = self.create_web3_helper(
            address=address, web3Provider=self.create_web3_provider(network=network)
        )
        address_token0 = web3_helper.token0.address
        address_token1 = web3_helper.token1.address

        # result var
        result = dict()

        # progress vars
        rem_progress = tot_progress = len(block_list)

        # each thread arguments
        args = (
            (address, block, network, address_token0, address_token1)
            for block in block_list
        )
        # threaded loop
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            for result_item in ex.map(lambda p: self.create_status(*p), args):
                # build block status
                result[result_item["block"]] = result_item
                # progress
                rem_progress -= 1
                if progress_callback:
                    progress_callback(
                        text=" getting {} {}'s {}-{} status at block {}".format(
                            self.protocol,
                            network,
                            address_token0[-4:],
                            address_token1[-4:],
                            result_item["block"],
                        ),
                        remaining=rem_progress,
                        total=tot_progress,
                    )

        # return result
        return result

    def create_status_autoblocks(
        self, address: str, network: str, progress_callback=None
    ) -> dict:

        # ease the var access name
        filters = self.configuration["script"]["protocols"][self.protocol]["filters"]
        # defaults
        date_end = dt.datetime.utcnow()
        date_ini = date_end - dt.timedelta(days=7 * 4 * 2)  # 8 weeks

        # apply filters, if needed
        if "force_timeframe" in filters:
            if "start_time" in filters["force_timeframe"]:
                date_ini = general_utilities.convert_string_datetime(
                    filters["force_timeframe"]["start_time"]
                )
            if "end_time" in filters["force_timeframe"]:
                date_end = general_utilities.convert_string_datetime(
                    filters["force_timeframe"]["end_time"]
                )

        return self.create_status_ofBlocks(
            address=address,
            network=network,
            block_list=self.get_blocklist_fromDates(
                date_ini=date_ini, date_end=date_end, network=network
            ),
            progress_callback=progress_callback,
        )

    def create_status_fromOPS(self, hypervisor: dict, progress_callback=None) -> dict:
        """Create all status needed to be able to calc fees in LOCstandard:
               fee apr = fees gathered between operations
               ( operation A block --> operation B block -1 )

           affected by force_timeframe filter in configuration

        Args:
           hypervisor (dict): template oftype hypervisor with static, deposits, withdraws, fees and rebalances scraped
           progress_callback (function, optional): _description_. Defaults to None.

        Returns:
           dict: {"<block>": template oftype "hypervisor" ["status"] , ... }
        """
        # tbul
        address = hypervisor["static"]["id"]
        network = hypervisor["static"]["network"]

        # create a list of operation blocks
        block_list = list(
            set(
                [x["block"] for x in hypervisor["deposits"]]
                + [x["block"] for x in hypervisor["withdraws"]]
                + [x["block"] for x in hypervisor["fees"]]
                + [x["block"] for x in hypervisor["rebalances"]]
            )
        )

        # apply filters, if any
        filters = self.configuration["script"]["protocols"][self.protocol]["filters"]
        if (
            "force_timeframe" in filters
            and "start_time" in filters["force_timeframe"]
            and "end_time" in filters["force_timeframe"]
        ):
            # use only the blocks of the specified timeframe
            date_ini = general_utilities.convert_string_datetime(
                filters["force_timeframe"]["start_time"]
            )
            date_end = general_utilities.convert_string_datetime(
                filters["force_timeframe"]["end_time"]
            )
            block_list_dates = sorted(
                self.get_blocklist_fromDates(
                    date_ini=date_ini, date_end=date_end, network=network
                )
            )

            # filter blocks
            block_list = [
                x
                for x in block_list
                if x >= block_list_dates[0] and x <= block_list_dates[-1]
            ]

        # substract one block to every block on the list and append it
        block_list = sorted(block_list + [x - 1 for x in block_list])

        # return result
        return self.create_status_ofBlocks(
            address=address,
            network=network,
            block_list=block_list,
            progress_callback=progress_callback,
        )

    def populate_all_status(self, hypervisor: dict, progress_callback=None):

        # main check
        if not "status" in hypervisor:
            hypervisor["status"] = dict()
        else:
            # integer dict keys are transformed to string when saving json
            hypervisor["status"] = {int(k): v for k, v in hypervisor["status"].items()}
        # ["status"][<BLOCK>][<status data>]
        for block in hypervisor["status"].keys():

            if isinstance(block, str):
                raise ValueError(
                    " Status dictionary has string keys that should be int. Check if they were loaded from a json file without conversion"
                )

            if not block in hypervisor["status"]:
                # init status template at specified block
                hypervisor["status"][block] = template(oftype="hypervisor")["status"]

            # zero all vars to be populated
            hypervisor["status"][block]["shares"] = 0
            hypervisor["status"][block]["deposits"] = template(oftype="hypervisor")[
                "status"
            ]["deposits"]
            hypervisor["status"][block]["cost"] = template(oftype="hypervisor")[
                "status"
            ]["cost"]
            hypervisor["status"][block]["withdraws"] = template(oftype="hypervisor")[
                "status"
            ]["withdraws"]
            hypervisor["status"][block]["rebalances"] = template(oftype="hypervisor")[
                "status"
            ]["rebalances"]

            # there are fees fields to be preserved. ( uncollected and owed fees )
            if not "fees" in hypervisor["status"][block]:
                hypervisor["status"][block]["fees"] = template(oftype="hypervisor")[
                    "status"
                ]["fees"]
            hypervisor["status"][block]["fees"]["aggregated_qtty_token0"] = 0
            hypervisor["status"][block]["fees"]["aggregated_qtty_token1"] = 0
            hypervisor["status"][block]["fees"]["aggregated_total_in_token0"] = 0
            hypervisor["status"][block]["fees"]["aggregated_total_in_token1"] = 0
            hypervisor["status"][block]["fees"]["aggregated_total_in_usd"] = 0

            # TODO: implement mutithread processing and reimplement progress accordingly

            # populate deposits
            if "deposits" in hypervisor:
                rem_progress = len(hypervisor["deposits"])
                tot_progress = len(hypervisor["deposits"])
                for x in hypervisor["deposits"]:
                    # check if inside boundaries
                    if x["block"] <= block:

                        hypervisor["status"][block]["deposits"][
                            "aggregated_qtty_token0"
                        ] += x["qtty_token0"]
                        hypervisor["status"][block]["deposits"][
                            "aggregated_qtty_token1"
                        ] += x["qtty_token1"]

                        # udate first
                        if (
                            not "first_deposit" in hypervisor["status"][block]
                            or hypervisor["status"][block]["first_deposit"] == None
                        ):
                            hypervisor["status"][block]["first_deposit"] = x[
                                "timestamp"
                            ]
                        hypervisor["status"][block]["first_deposit"] = min(
                            hypervisor["status"][block]["first_deposit"], x["timestamp"]
                        )

                        # update shares
                        hypervisor["status"][block]["shares"] += x["shares"]

                        # update cost
                        hypervisor["status"][block]["cost"]["qtty_token0"] += x[
                            "qtty_token0"
                        ]
                        hypervisor["status"][block]["cost"]["qtty_token1"] += x[
                            "qtty_token1"
                        ]

                        # totals conversions
                        if x["price_usd_token0"] != 0 and x["price_usd_token1"] != 0:
                            # deposits
                            hypervisor["status"][block]["deposits"][
                                "aggregated_total_in_token0"
                            ] += x["qtty_token0"] + (
                                x["qtty_token1"]
                                * (x["price_usd_token1"] / x["price_usd_token0"])
                            )
                            hypervisor["status"][block]["deposits"][
                                "aggregated_total_in_token1"
                            ] += x["qtty_token1"] + (
                                x["qtty_token0"]
                                * (x["price_usd_token0"] / x["price_usd_token1"])
                            )
                            hypervisor["status"][block]["deposits"][
                                "aggregated_total_in_usd"
                            ] += (x["qtty_token0"] * x["price_usd_token0"]) + (
                                x["qtty_token1"] * x["price_usd_token1"]
                            )
                            # cost
                            hypervisor["status"][block]["cost"][
                                "qtty_total_in_usd"
                            ] += (x["qtty_token0"] * x["price_usd_token0"]) + (
                                x["qtty_token1"] * x["price_usd_token1"]
                            )
                            hypervisor["status"][block]["cost"][
                                "qtty_total_in_token0"
                            ] += x["qtty_token0"] + (
                                x["qtty_token1"]
                                * (x["price_usd_token1"] / x["price_usd_token0"])
                            )
                            hypervisor["status"][block]["cost"][
                                "qtty_total_in_token1"
                            ] += x["qtty_token1"] + (
                                x["qtty_token0"]
                                * (x["price_usd_token0"] / x["price_usd_token1"])
                            )
                        else:
                            # TODO: log at least
                            pass

                    # progress
                    rem_progress -= 1
                    if progress_callback:
                        try:
                            progress_callback(
                                text=" populating {} {}'s {} status-> deposits".format(
                                    self.protocol,
                                    hypervisor["static"]["network"],
                                    hypervisor["static"]["name"],
                                    block,
                                ),
                                remaining=rem_progress,
                                total=tot_progress,
                            )
                        except:
                            pass

            # populate withdraws
            if "withdraws" in hypervisor:
                rem_progress = len(hypervisor["withdraws"])
                tot_progress = len(hypervisor["withdraws"])
                for x in hypervisor["withdraws"]:
                    # check if inside boundaries
                    if x["block"] <= block:
                        hypervisor["status"][block]["withdraws"][
                            "aggregated_qtty_token0"
                        ] += x["qtty_token0"]
                        hypervisor["status"][block]["withdraws"][
                            "aggregated_qtty_token1"
                        ] += x["qtty_token1"]

                        # update last
                        if (
                            not "last_withdraw" in hypervisor["status"][block]
                            or hypervisor["status"][block]["last_withdraw"] == None
                        ):
                            hypervisor["status"][block]["last_withdraw"] = x[
                                "timestamp"
                            ]
                        hypervisor["status"][block]["last_withdraw"] = max(
                            hypervisor["status"][block]["last_withdraw"], x["timestamp"]
                        )

                        # update shares
                        hypervisor["status"][block]["shares"] -= x["shares"]

                        # update cost
                        hypervisor["status"][block]["cost"]["qtty_token0"] -= x[
                            "qtty_token0"
                        ]
                        hypervisor["status"][block]["cost"]["qtty_token1"] -= x[
                            "qtty_token1"
                        ]

                        # totals conversions
                        if x["price_usd_token0"] != 0 and x["price_usd_token1"] != 0:
                            # withdraws
                            hypervisor["status"][block]["withdraws"][
                                "aggregated_total_in_token0"
                            ] += x["qtty_token0"] + (
                                x["qtty_token1"]
                                * (x["price_usd_token1"] / x["price_usd_token0"])
                            )
                            hypervisor["status"][block]["withdraws"][
                                "aggregated_total_in_token1"
                            ] += x["qtty_token1"] + (
                                x["qtty_token0"]
                                * (x["price_usd_token0"] / x["price_usd_token1"])
                            )
                            hypervisor["status"][block]["withdraws"][
                                "aggregated_total_in_usd"
                            ] += (x["qtty_token0"] * x["price_usd_token0"]) + (
                                x["qtty_token1"] * x["price_usd_token1"]
                            )
                            # cost
                            hypervisor["status"][block]["cost"][
                                "qtty_total_in_usd"
                            ] -= (x["qtty_token0"] * x["price_usd_token0"]) + (
                                x["qtty_token1"] * x["price_usd_token1"]
                            )
                            hypervisor["status"][block]["cost"][
                                "qtty_total_in_token0"
                            ] -= x["qtty_token0"] + (
                                x["qtty_token1"]
                                * (x["price_usd_token1"] / x["price_usd_token0"])
                            )
                            hypervisor["status"][block]["cost"][
                                "qtty_total_in_token1"
                            ] -= x["qtty_token1"] + (
                                x["qtty_token0"]
                                * (x["price_usd_token0"] / x["price_usd_token1"])
                            )
                        else:
                            # TODO: log at least
                            pass

                    # progress
                    rem_progress -= 1
                    if progress_callback:
                        try:
                            progress_callback(
                                text=" populating {} {}'s {} status-> withdraws".format(
                                    self.protocol,
                                    hypervisor["static"]["network"],
                                    hypervisor["static"]["name"],
                                    block,
                                ),
                                remaining=rem_progress,
                                total=tot_progress,
                            )
                        except:
                            pass

            # populate rebalances
            if "rebalances" in hypervisor:
                rem_progress = len(hypervisor["rebalances"])
                tot_progress = len(hypervisor["rebalances"])
                for x in hypervisor["rebalances"]:
                    if x["block"] <= block:
                        # add qtty
                        hypervisor["status"][block]["rebalances"][
                            "aggregated_qtty_total"
                        ] += 1

                        if (
                            not "last_rebalance" in hypervisor["status"][block]
                            or hypervisor["status"][block]["last_rebalance"] == None
                        ):
                            hypervisor["status"][block]["last_rebalance"] = x[
                                "timestamp"
                            ]
                        hypervisor["status"][block]["last_rebalance"] = max(
                            hypervisor["status"][block]["last_rebalance"],
                            x["timestamp"],
                        )

                    # progress
                    rem_progress -= 1
                    if progress_callback:
                        try:
                            progress_callback(
                                text=" populating {} {}'s {} status-> rebalances".format(
                                    self.protocol,
                                    hypervisor["static"]["network"],
                                    hypervisor["static"]["name"],
                                    block,
                                ),
                                remaining=rem_progress,
                                total=tot_progress,
                            )
                        except:
                            pass

            # populate fees
            if "fees" in hypervisor:
                rem_progress = len(hypervisor["fees"])
                tot_progress = len(hypervisor["fees"])
                for x in hypervisor["fees"]:
                    if x["block"] <= block:
                        hypervisor["status"][block]["fees"][
                            "aggregated_qtty_token0"
                        ] += x["gross_token0"]
                        hypervisor["status"][block]["fees"][
                            "aggregated_qtty_token1"
                        ] += x["gross_token1"]

                        # totals conversions
                        if x["price_usd_token0"] != 0 and x["price_usd_token1"] != 0:
                            # fees
                            hypervisor["status"][block]["fees"][
                                "aggregated_total_in_token0"
                            ] += x["gross_token0"] + (
                                x["gross_token1"]
                                * (x["price_usd_token1"] / x["price_usd_token0"])
                            )
                            hypervisor["status"][block]["fees"][
                                "aggregated_total_in_token1"
                            ] += x["gross_token1"] + (
                                x["gross_token0"]
                                * (x["price_usd_token0"] / x["price_usd_token1"])
                            )
                            hypervisor["status"][block]["fees"][
                                "aggregated_total_in_usd"
                            ] += (x["gross_token0"] * x["price_usd_token0"]) + (
                                x["gross_token1"] * x["price_usd_token1"]
                            )

                        else:
                            # TODO: log usd prices not scraped/found/exist
                            pass

                    # progress
                    rem_progress -= 1
                    if progress_callback:
                        try:
                            progress_callback(
                                text=" populating {} {}'s {} status-> fees".format(
                                    self.protocol,
                                    hypervisor["static"]["network"],
                                    hypervisor["static"]["name"],
                                    block,
                                ),
                                remaining=rem_progress,
                                total=tot_progress,
                            )
                        except:
                            pass

            # add uncollected owed fees to totals
            hypervisor["status"][block]["fees"]["aggregated_qtty_token0"] += (
                hypervisor["status"][block]["fees"]["uncollected_token0"]
                + hypervisor["status"][block]["fees"]["owed_token0"]
            )
            hypervisor["status"][block]["fees"]["aggregated_qtty_token1"] += (
                hypervisor["status"][block]["fees"]["uncollected_token1"]
                + hypervisor["status"][block]["fees"]["owed_token1"]
            )
            # totals conversions
            if (
                hypervisor["status"][block]["price_usd_token0"] != 0
                and hypervisor["status"][block]["price_usd_token1"] != 0
            ):
                # fees
                hypervisor["status"][block]["fees"]["aggregated_total_in_token0"] += (
                    hypervisor["status"][block]["fees"]["uncollected_token0"]
                    + hypervisor["status"][block]["fees"]["owed_token0"]
                ) + (
                    (
                        hypervisor["status"][block]["fees"]["uncollected_token1"]
                        + hypervisor["status"][block]["fees"]["owed_token1"]
                    )
                    * (
                        hypervisor["status"][block]["price_usd_token1"]
                        / hypervisor["status"][block]["price_usd_token0"]
                    )
                )
                hypervisor["status"][block]["fees"]["aggregated_total_in_token1"] += (
                    hypervisor["status"][block]["fees"]["uncollected_token1"]
                    + hypervisor["status"][block]["fees"]["owed_token1"]
                ) + (
                    (
                        hypervisor["status"][block]["fees"]["uncollected_token0"]
                        + hypervisor["status"][block]["fees"]["owed_token0"]
                    )
                    * (
                        hypervisor["status"][block]["price_usd_token0"]
                        / hypervisor["status"][block]["price_usd_token1"]
                    )
                )
                hypervisor["status"][block]["fees"]["aggregated_total_in_usd"] += (
                    (
                        hypervisor["status"][block]["fees"]["uncollected_token0"]
                        + hypervisor["status"][block]["fees"]["owed_token0"]
                    )
                    * hypervisor["status"][block]["price_usd_token0"]
                ) + (
                    (
                        hypervisor["status"][block]["fees"]["uncollected_token1"]
                        + hypervisor["status"][block]["fees"]["owed_token1"]
                    )
                    * hypervisor["status"][block]["price_usd_token1"]
                )

    def create_chart(
        self, hypervisor: dict, forced_startime=None, progress_callback=None
    ) -> dict:
        """Create a chart data for the specified hypervisor

        Args:
           hypervisor (dict): _description_
           forced_startime (datetime, optional): Time to forse chart to start from. Has implications on calculated variables. Defaults to None.

        Returns:
           dict: template oftype="chart"
        """

        # TODO: implement start and end time reading values from self.configuration
        # TODO: implement Impermanent L,G and APR,APY relative and absolutes

        # control vars
        control_vars = {
            "last_chart_datarow": {"price_share_usd": 0},
            "last_status": dict(),
            "aggregated_price_per_share": 0,
            "first_datetime": None,
            "last_datetime": dt.datetime.utcnow() - dt.timedelta(days=365 * 50),
            # "initial_fee_status": dict(),   # initial status from wich to calculate fee rewards ( so block -> operation block -1)
        }

        # define startime
        startime = (
            (dt.datetime.utcnow() - dt.timedelta(days=365 * 150))
            if forced_startime == None
            else forced_startime
        )

        # init result
        result = template(oftype="chart")

        # progress data
        hypervisor_id = hypervisor["static"]["id"]
        protocol = hypervisor["static"]["protocol"]
        network = hypervisor["static"]["network"]
        rem_progress = tot_progress = len(hypervisor["status"].keys())
        for block, status in hypervisor["status"].items():

            # progress
            progress_callback(
                text=" creating {} {}'s {} hypervisor chart at block {}".format(
                    protocol, network, hypervisor_id, block
                ),
                remaining=rem_progress,
                total=tot_progress,
            )

            if dt.datetime.fromtimestamp(status["timestamp"]) > startime:
                # init row
                chart_datarow = template(oftype="chart_datarow")

                # fill row
                chart_datarow["datetime"] = dt.datetime.fromtimestamp(
                    status["timestamp"]
                )
                chart_datarow["block"] = status["block"]

                # days passed calc.
                if control_vars["first_datetime"] == None:
                    # set first datetime once
                    control_vars["first_datetime"] = chart_datarow["datetime"]
                chart_datarow["days_passed"] = (
                    chart_datarow["datetime"] - control_vars["first_datetime"]
                ).total_seconds() / (60 * 60 * 24)

                # price
                chart_datarow["price_usd_token0"] = status["price_usd_token0"]
                chart_datarow["price_usd_token1"] = status["price_usd_token1"]

                # tvl
                chart_datarow["qtty_token0"] = status["qtty_token0"]
                chart_datarow["qtty_token1"] = status["qtty_token1"]

                chart_datarow["tvl_total_in_usd"] = (
                    status["qtty_token0"] * status["price_usd_token0"]
                    + status["qtty_token1"] * status["price_usd_token1"]
                )

                # supply
                chart_datarow["total_supply"] = status["supply"]

                # Fees
                chart_datarow["aggregatedFees_qtty_token0"] = status["fees"][
                    "aggregated_qtty_token0"
                ]
                chart_datarow["aggregatedFees_qtty_token1"] = status["fees"][
                    "aggregated_qtty_token1"
                ]
                chart_datarow["aggregatedFees_total_in_usd"] = status["fees"][
                    "aggregated_total_in_usd"
                ]

                # deposits & withdraws
                chart_datarow["aggregatedDeposits_token0"] = status["deposits"][
                    "aggregated_qtty_token0"
                ]
                chart_datarow["aggregatedDeposits_token1"] = status["deposits"][
                    "aggregated_qtty_token1"
                ]
                chart_datarow["aggregatedDeposits_total_in_usd"] = status["deposits"][
                    "aggregated_total_in_usd"
                ]
                chart_datarow["aggregatedWithdraws_token0"] = status["withdraws"][
                    "aggregated_qtty_token0"
                ]
                chart_datarow["aggregatedWithdraws_token1"] = status["withdraws"][
                    "aggregated_qtty_token1"
                ]
                chart_datarow["aggregatedWithdraws_total_in_usd"] = status["withdraws"][
                    "aggregated_total_in_usd"
                ]

                ### CALCULATED FIELDS ###

                # Share price
                chart_datarow["price_share_usd"] = (
                    (chart_datarow["tvl_total_in_usd"] / chart_datarow["total_supply"])
                    if chart_datarow["total_supply"] > 0
                    else 0
                )
                #   price Share percentage change
                control_vars["aggregated_price_per_share"] += (
                    0
                    if control_vars["last_chart_datarow"]["price_share_usd"] == 0
                    else (
                        chart_datarow["price_share_usd"]
                        - control_vars["last_chart_datarow"]["price_share_usd"]
                    )
                    / control_vars["last_chart_datarow"]["price_share_usd"]
                )
                chart_datarow["price_share_change"] = control_vars[
                    "aggregated_price_per_share"
                ]

                # check if not first time loop
                if len(control_vars["last_status"].keys()) > 0:

                    # aggregated fees
                    # sometimes, uncollected fees do not prevail in aggregated fees bc are affected by a withdraw ( to check why, but happens)
                    # add aggregated fees uncollected fees
                    if (
                        status["fees"]["aggregated_qtty_token0"] == 0
                        and control_vars["last_status"]["fees"][
                            "aggregated_qtty_token0"
                        ]
                        > 0
                    ):
                        # uncollected fees have not been added to aggregated
                        pass

                    # fee % fields
                    if (
                        status["fees"]["uncollected_token0"] > 0
                        or status["fees"]["uncollected_token1"] > 0
                    ):
                        # calculate feeAPR from last
                        fees_period_token0 = (
                            status["fees"]["uncollected_token0"]
                            + status["fees"]["owed_token0"]
                            - control_vars["last_status"]["fees"]["uncollected_token0"]
                            - control_vars["last_status"]["fees"]["owed_token0"]
                        )
                        fees_period_token1 = (
                            status["fees"]["uncollected_token1"]
                            + status["fees"]["owed_token1"]
                            - control_vars["last_status"]["fees"]["uncollected_token1"]
                            - control_vars["last_status"]["fees"]["owed_token1"]
                        )
                        fees_period_usd = (
                            status["fees"]["uncollected_total_in_usd"]
                            + status["fees"]["owed_total_in_usd"]
                            - control_vars["last_status"]["fees"][
                                "uncollected_total_in_usd"
                            ]
                            - control_vars["last_status"]["fees"]["owed_total_in_usd"]
                        )

                        chart_datarow["fees_period_timepassed"] = (
                            status["timestamp"]
                            - control_vars["last_status"]["timestamp"]
                        )
                        chart_datarow[
                            "fees_period_generated_token0"
                        ] = fees_period_token0
                        chart_datarow[
                            "fees_period_generated_token1"
                        ] = fees_period_token1
                        chart_datarow["fees_period_generated_usd"] = fees_period_usd

                        chart_datarow["fees_period_start_TVL_token0"] = control_vars[
                            "last_status"
                        ]["qtty_token0"]
                        chart_datarow["fees_period_start_TVL_token1"] = control_vars[
                            "last_status"
                        ]["qtty_token1"]
                        chart_datarow["fees_period_start_TVL_usd"] = control_vars[
                            "last_chart_datarow"
                        ]["tvl_total_in_usd"]

                        chart_datarow["fees_period_end_TVL_token0"] = status[
                            "qtty_token0"
                        ]
                        chart_datarow["fees_period_end_TVL_token1"] = status[
                            "qtty_token1"
                        ]
                        chart_datarow["fees_period_end_TVL_usd"] = chart_datarow[
                            "tvl_total_in_usd"
                        ]

                    # if status["block"] == control_vars["last_status"]["block"]+1:
                    #     # this is the first point in timeframe
                    #     control_var["initial_fee_status"] = status
                    # else:
                    #     # this is last point in timeframe
                    #     fees_period_token0 = status["fees"]["aggregated_qtty_token0"] - control_var["initial_fee_status"]["fees"]["aggregated_qtty_token0"]
                    #     fees_period_token1 = status["fees"]["aggregated_qtty_token1"] - control_var["initial_fee_status"]["fees"]["aggregated_qtty_token1"]

                # set last chart item and status
                control_vars["last_chart_datarow"] = chart_datarow
                control_vars["last_status"] = status

                # add row to chart data
                result["data"].append(chart_datarow)

            # progress
            rem_progress -= 1

        # add labels
        result["labels"] = sorted(list(set([x["datetime"] for x in result["data"]])))

        # return chart
        return result

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
                self.configuration["sources"]["web3Providers"][network],
                request_kwargs={"timeout": 60},
            )
        )
        # add middleware as needed
        if network != "ethereum":
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        # return result
        return w3

    def create_web3_helper(self, address: str, web3Provider: Web3, block: int = 0):
        """create a helper to interact with the protocol defined

        Args:
           address (str): "0x..."
           web3Provider (Web3):

        Returns:
           _type_: protocol helper for web3 interactions
        """
        if self.protocol == "gamma":
            return onchain_utilities.gamma_hypervisor_cached(
                address=address, web3Provider=web3Provider, block=block
            )

        elif self.protocol == "arrakis":
            return onchain_utilities.arrakis_hypervisor_cached(
                address=address, web3Provider=web3Provider, block=block
            )

        else:
            raise ValueError(
                " No web3 helper defined for {} protocol".format(self.protocol)
            )

    def create_data_collector(self, network: str) -> onchain_utilities.data_collector:
        """Create a data collector class

        Args:
           network (str):

        Returns:
           onchain_utilities.data_collector:
        """
        result = None
        if self.protocol == "gamma":
            result = onchain_utilities.data_collector(
                topics={
                    "gamma_transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    "gamma_rebalance": "0xbc4c20ad04f161d631d9ce94d27659391196415aa3c42f6a71c62e905ece782d",
                    "gamma_deposit": "0x4e2ca0515ed1aef1395f66b5303bb5d6f1bf9d61a353fa53f73f8ac9973fa9f6",
                    "gamma_withdraw": "0xebff2602b3f468259e1e99f613fed6691f3a6526effe6ef3e768ba7ae7a36c4f",
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
                },
                web3Provider=self.create_web3_provider(network),
            )
        elif self.protocol == "arrakis":
            result = onchain_utilities.data_collector(
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
                web3Provider=self.create_web3_provider(network),
            )
        else:
            raise ValueError(
                " No web3 helper defined for {} protocol".format(self.protocol)
            )

        return result

    def get_dwrfts(
        self,
        addresses: list,
        network: str,
        block_ini: int,
        block_end: int,
        progress_callback=None,
        max_blocks=10000,
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
           max_blocks (int): maximum number of blocks for each query ( some servers will accept high-low numbers here...)

        Returns:
           dict: { <contract address>:
                       {
                       "deposits":,
                       "withdraws":...
                       ...
                       },
                       formated as defined in template(oftype=" ")
                   ...
                   }
        """

        # create new data collector helper
        dta_coll = self.create_data_collector(network=network)

        # PROGRESS
        dta_coll.progress_callback = progress_callback

        # get all content
        dta_coll.get_all_operations(
            block_ini=block_ini,
            block_end=block_end,
            contracts=[Web3.toChecksumAddress(x) for x in addresses],
            max_blocks=max_blocks,
        )

        # create result var
        result = dict()

        # rem_progress = tot_progress = len(dta_coll._data.keys())
        for k, v in dta_coll._data.items():
            result[k] = self.convert_dtaCollector_item(
                address=k,
                data_item=v,
                network=network,
                progress_callback=progress_callback,
            )

        # return result
        return result

    def convert_dtaCollector_item(
        self, address: str, data_item, network, progress_callback=None
    ):

        max_workers = 5

        result = dict()
        # init result
        result["deposits"] = list()
        result["withdraws"] = list()
        result["fees"] = list()
        result["rebalances"] = list()
        result["transfers"] = list()

        # save token addresses to gather prices from
        w3helper = self.create_web3_helper(
            address=address, web3Provider=self.create_web3_provider(network=network)
        )
        address_token0 = w3helper.token0.address
        address_token1 = w3helper.token1.address

        result["static"] = template(oftype="hypervisor")["static"]
        result["static"]["id_token0"] = address_token0
        result["static"]["id_token1"] = address_token1

        def convert_operation(operation: str, itm: dict, w3helper):
            # create template
            tmp = template(oftype="operation")
            # convert data
            tmp["action"] = operation
            tmp["id"] = itm["transactionHash"]
            tmp["timestamp"] = w3helper.w3.eth.get_block(itm["blockNumber"]).timestamp
            tmp["block"] = itm["blockNumber"]
            tmp["blockHash"] = itm["blockHash"]

            tmp["originator"] = itm["sender"] if operation == "deposit" else itm["to"]

            tmp["shares"] = itm["shares"]
            tmp["qtty_token0"] = itm["qtty_token0"]
            tmp["qtty_token1"] = itm["qtty_token1"]

            # result
            return tmp

        def convert_fee(itm: dict, w3helper):
            # create template
            tmp = template(oftype="fee")
            # convert data
            tmp["id"] = itm["transactionHash"]
            tmp["timestamp"] = w3helper._w3.eth.get_block(itm["blockNumber"]).timestamp
            tmp["block"] = itm["blockNumber"]
            tmp["blockHash"] = itm["blockHash"]

            tmp["gross_token0"] = itm["qtty_token0"]
            tmp["gross_token1"] = itm["qtty_token1"]

            # append to result
            return tmp

        def convert_rebalance(itm: dict, w3helper):
            # create template
            tmp = template(oftype="rebalance")
            # convert data
            tmp["id"] = itm["transactionHash"]
            tmp["timestamp"] = w3helper._w3.eth.get_block(itm["blockNumber"]).timestamp
            tmp["block"] = itm["blockNumber"]
            tmp["blockHash"] = itm["blockHash"]

            tmp["lowerTick"] = itm["lowerTick"]
            tmp["upperTick"] = itm["upperTick"]

            # result
            return tmp

        def convert_transfer(itm: dict, w3helper):
            # transfers struct:
            # "transactionHash": "0x9c963dbca83bef282a683ef3f8af2e632eead6a7f8396e0a31bdcc9b702cfc40",
            # "blockHash": "0xf060cf5b2570a038682a253ff3caf04237892082b04ef3d27443edd2a6c44e44",
            # "blockNumber": 33205337,
            # "address": "0xaED05fdd471a4EecEe48B34d38c59CC76681A6C8",
            # "src": "0x45572a41f33e95ce0980e382654a6a9e42aa5610",
            # "dst": "0xf5fcd3a63abc766ac5ada296b4a4e860dbf9ebb0",
            # "qtty": 1.304670837184294
            # create template
            tmp = template(oftype="transfer")
            # convert data
            tmp["timestamp"] = w3helper._w3.eth.get_block(itm["blockNumber"]).timestamp
            tmp["block"] = itm["blockNumber"]
            tmp["id"] = itm["transactionHash"]
            tmp["blockHash"] = itm["blockHash"]
            tmp["address"] = itm["address"]
            tmp["source"] = itm["src"]
            tmp["destination"] = itm["dst"]
            tmp["qtty"] = itm["qtty"]

            # result
            return tmp

        _defs = {
            "deposits": convert_operation,
            "withdraws": convert_operation,
            "fees": convert_fee,
            "rebalances": convert_rebalance,
            "transfers": convert_transfer,
        }
        for key in data_item.keys():
            # make sure we pick only to be processed keys ( no static, etc..)
            if key in _defs.keys():
                # build arguments
                if key in ["deposits", "withdraws"]:
                    args = (
                        (key[:-1], itm, w3helper) for itm in data_item[key]
                    )  # remove "s" char at key for operation string var
                else:
                    args = ((itm, w3helper) for itm in data_item[key])
                # init progress vars
                _remaining = _totals = len(data_item[key])
                # threaded loop
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as ex:
                    for result_item in ex.map(lambda p: _defs[key](*p), args):
                        # show progress
                        _remaining -= 1
                        if progress_callback:
                            progress_callback(
                                text="processing {}-{} {} ".format(
                                    address_token0, address_token1, key
                                ),
                                remaining=_remaining,
                                total=_totals,
                            )

                        # get price
                        result_item["price_usd_token0"] = self.price_helper.get_price(
                            network=network,
                            token_id=address_token0,
                            block=result_item["block"],
                            of="USD",
                        )
                        result_item["price_usd_token1"] = self.price_helper.get_price(
                            network=network,
                            token_id=address_token1,
                            block=result_item["block"],
                            of="USD",
                        )

                        # append to result
                        result[key].append(result_item)

        # return result
        return result

    def get_standard_blockBounds(self, network: str) -> tuple:
        """Return filtered block ini block end or equivalent non filtered

        Args:
           network (str):

        Returns:
           tuple: block_ini, block end
        """

        # create a dummy helper ( use only web3wrap functions)
        dummy_helper = self.create_web3_helper(
            address="0x0000000000000000000000000000000000000000",
            web3Provider=self.create_web3_provider(network=network),
        )

        # ease the var access name
        filters = self.configuration["script"]["protocols"][self.protocol]["filters"]

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
        filters = self.configuration["script"]["protocols"][self.protocol]["filters"]
        output = self.configuration["script"]["protocols"][self.protocol]["output"]

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
            address="0x0000000000000000000000000000000000000000",
            web3Provider=self.create_web3_provider(network=network),
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
    ) -> tuple:

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
            address="0x0000000000000000000000000000000000000000",
            web3Provider=self.create_web3_provider(network=network),
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
        block_end = dummy_helper.blockNumberFromTimestamp(
            timestamp=dt.datetime.timestamp(result_date_end),
            inexact_mode="before",
            eq_timestamp_position="last",
        )

        return block_ini, block_end

    # CHECKERS
    def check_static_fields(self, hypervisor: dict) -> tuple:
        """Check if hypervisor static fields are present and ok

        Args:
           hypervisor (dict): as in template oftype hypervisor
           progress_callback (def, optional): . Defaults to None.

        Returns:
           tuple:  bool,list :  ok=True, list=[ fields not present or with errors ]
        """

        result = True  # everything ok, not ok
        fields = list()

        if not "static" in hypervisor.keys():
            result = False
        else:
            # check all fields
            for key in list(template(oftype="hypervisor")["static"].keys()):
                if not key in hypervisor["static"].keys():
                    result = False
                    fields.append(key)
                else:
                    # TODO: check types of specific fields
                    pass

        return result, fields

    def check_status_dates(self, hypervisor: dict) -> tuple:
        pass
