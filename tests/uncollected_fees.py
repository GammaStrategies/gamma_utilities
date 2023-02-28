import sys
import os
import datetime as dt
import logging
from web3 import Web3
from web3.middleware import geth_poa_middleware
from pathlib import Path
import tqdm
import concurrent.futures

from decimal import Decimal

# append parent directory pth
CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
sys.path.append(PARENT_FOLDER)

from bins.configuration import CONFIGURATION

from bins.apis import etherscan_utilities, thegraph_utilities
from bins.cache import cache_utilities
from bins.general import net_utilities, file_utilities, general_utilities

from bins.w3.onchain_utilities.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_cached,
    gamma_hypervisor_quickswap_cached,
)

from bins.log import log_helper
from bins.formulas import univ3_formulas


# this comparison contains many differences ( check  tegraph_vs_onchain tests  )
# be aware that gammawire API getsinfo from subgraph so block is behind "latest" onchain..
# so differences are xpected ..
def test_uncollected_fees_comparison_api_onchain(threaded: bool = False):
    """Comparison between uncollected fees data sourced from the gammawire.net endpoint and
       custom calculation method using onchain queries

       Results are saved in a csv file with the same def name

    Args:
       threaded (bool, optional): speedup the scriptwith threading. Defaults to False.
    """
    network = "ethereum"
    web3Provider_url = CONFIGURATION["sources"]["web3Providers"][network]
    official_api_url = "https://gammawire.net/hypervisors/uncollectedFees"

    csv_filename = "{}_test_uncollected_fees_comparison_api_onchain{}.csv".format(
        network, "_threaded" if threaded else ""
    )
    csv_filename = os.path.join(CURRENT_FOLDER, csv_filename)

    def do_loop_work(hyp_id, hypervisor, network: str):

        # get onchain data ( need to force abi path bc current workinfolder is tests)
        w3help = gamma_hypervisor_cached(address=hyp_id, network=network)
        dta_tvl = w3help.get_tvl()
        dta_uncollected = w3help.get_fees_uncollected()

        onchain_totalFees0 = (
            dta_uncollected["qtty_token0"] + dta_tvl["fees_owed_token0"]
        )
        onchain_totalFees1 = (
            dta_uncollected["qtty_token1"] + dta_tvl["fees_owed_token1"]
        )

        # differences situations
        if hypervisor["totalFees0"] == 0 and onchain_totalFees0 > 0:
            difference_token0 = 1
        else:
            difference_token0 = (
                (
                    (hypervisor["totalFees0"] - onchain_totalFees0)
                    / hypervisor["totalFees0"]
                )
                if hypervisor["totalFees0"] != 0
                else 0
            )

        if hypervisor["totalFees1"] == 0 and onchain_totalFees1 > 0:
            difference_token1 = 1
        else:
            difference_token1 = (
                (
                    (hypervisor["totalFees1"] - onchain_totalFees1)
                    / hypervisor["totalFees1"]
                )
                if hypervisor["totalFees1"] != 0
                else 0
            )

        # result
        return {
            "hypervisor_id": hyp_id,
            "symbol": hypervisor["symbol"],
            "api_totalFees0": hypervisor["totalFees0"],
            "api_totalFees1": hypervisor["totalFees1"],
            "onchain_totalFees0": onchain_totalFees0,
            "onchain_totalFees1": onchain_totalFees1,
            "totalFees0_diff": difference_token0,
            "totalFees1_diff": difference_token1,
        }

    # get a list of hypervisors + its data from https://gammawire.net
    official_uncollected_data = net_utilities.get_request(
        official_api_url
    )  # data collected from official endpoint api originaly sourced from subgraph

    web3Provider = Web3(
        Web3.HTTPProvider(web3Provider_url, request_kwargs={"timeout": 60})
    )
    # add middleware as needed
    if network != "ethereum":
        web3Provider.middleware_onion.inject(geth_poa_middleware, layer=0)

    result = list()
    with tqdm.tqdm(total=len(official_uncollected_data.keys())) as progress_bar:

        if not threaded:
            for hyp_id, hypervisor in official_uncollected_data.items():
                progress_bar.set_description(
                    "processing {}'s {}".format(network, hypervisor["symbol"])
                )

                # add data to result
                result_item = do_loop_work(hyp_id, network)

                # result
                result.append(result_item)

                # update progress
                progress_bar.update(1)
        else:
            # threaded
            args = (
                (hyp_id, hypervisor, network)
                for hyp_id, hypervisor in official_uncollected_data.items()
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                for result_item in ex.map(lambda p: do_loop_work(*p), args):
                    # progress
                    progress_bar.set_description(
                        "processed {}'s {}".format(network, result_item["symbol"])
                    )

                    # result
                    result.append(result_item)
                    # update progress
                    progress_bar.update(1)

    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass
    file_utilities.SaveCSV(filename=csv_filename, columns=result[0].keys(), rows=result)


# passed
# this comparison has small differences ( negligible, may be due to code execution [gammawire uses only one query and alternative method uses many] )
def test_uncollected_fees_comparison_api_thegraph(threaded: bool = False):
    """Comparison between uncollected fees data sourced from the gammawire.net endpoint and
       custom calculation method using GAMMA's subgraph

       Results are savedin a csv file with the same def name

    Args:
       threaded (bool, optional): speedup the scriptwith threading. Defaults to False.
    """

    protocol = "gamma"
    network = "ethereum"
    official_api_url = "https://gammawire.net/hypervisors/uncollectedFees"

    csv_filename = "{}_test_uncollected_fees_comparison_api_thegraph{}.csv".format(
        network, "_threaded" if threaded else ""
    )
    csv_filename = os.path.join(CURRENT_FOLDER, csv_filename)

    # get a list of hypervisors + its data from https://gammawire.net
    official_uncollected_data = net_utilities.get_request(
        official_api_url
    )  # data collected from official endpoint api originaly sourced from subgraph

    # thegraph helpers
    uniswapv3_helper = thegraph_utilities.uniswapv3_scraper(
        cache=True, cache_savePath="data/cache", convert=True
    )
    gamma_helper = thegraph_utilities.gamma_scraper(
        cache=True, cache_savePath="data/cache", convert=True
    )

    # utility class tbul
    class tick:
        # value
        # liquidityGross ,
        # liquidityNet ,
        # feeGrowthOutside0X128 uint256,
        # feeGrowthOutside1X128 uint256,
        # tickCumulativeOutside int56,
        # secondsPerLiquidityOutsideX128 uint160,
        # secondsOutside uint32, initialized bool

        def __init__(
            self, value: int, feeGrowthOutside0X128: int, feeGrowthOutside1X128: int
        ):
            self._value = value  # tick
            self._feeGrowthOutside0X128 = feeGrowthOutside0X128
            self._feeGrowthOutside1X128 = feeGrowthOutside1X128

        @property
        def value(self):
            return self._value

        @property
        def feeGrowthOutside0X128(self):
            return self._feeGrowthOutside0X128

        @property
        def feeGrowthOutside1X128(self):
            return self._feeGrowthOutside1X128

    # define a result var
    result = list()

    # create the func to loop thru
    def do_loop_work(hyp_id, hypervisor, gamma_helper, uniswapv3_helper):

        error = False
        subgraph_totalFees0 = (
            subgraph_totalFees1
        ) = difference_token0 = difference_token1 = 0
        try:
            # get hypervisor's data from thegraph
            thegraph_hyperv = gamma_helper.get_all_results(
                network=network,
                query_name="uniswapV3Hypervisors",
                where=""" id: "{}" """.format(hyp_id),
            )[0]

            # get uniswap pool and tick info  [ feeGrowthGlobal0X128 and feeGrowthGlobal1X128]
            univ3_pool = uniswapv3_helper.get_all_results(
                network=network,
                query_name="pools",
                where=""" id: "{}" """.format(thegraph_hyperv["pool"]["id"]),
            )[0]

            # create an array of ticks
            ticks = [
                thegraph_hyperv["baseUpper"],
                thegraph_hyperv["baseLower"],
                thegraph_hyperv["limitUpper"],
                thegraph_hyperv["limitLower"],
            ]
            # get all ticks [feeGrowthOutside1X128 and feeGrowthOutside0X128]
            univ3_ticks = uniswapv3_helper.get_all_results(
                network=network,
                query_name="ticks",
                where=""" tickIdx_in: {}, poolAddress: "{}" """.format(
                    ticks, thegraph_hyperv["pool"]["id"]
                ),
            )

            # classify univ3 ticks in a dict
            feeGrowthOutside = dict()
            for tk in univ3_ticks:
                if not tk["tickIdx"] in feeGrowthOutside:
                    feeGrowthOutside[int(tk["tickIdx"])] = tick(
                        int(tk["tickIdx"]),
                        int(tk["feeGrowthOutside0X128"]),
                        int(tk["feeGrowthOutside1X128"]),
                    )

            decimals_token0 = thegraph_hyperv["pool"]["token0"]["decimals"]
            decimals_token1 = thegraph_hyperv["pool"]["token1"]["decimals"]
            currentTick = int(univ3_pool["tick"])

            # gather BASE position data
            liquidity = int(thegraph_hyperv["baseLiquidity"])
            _lowerTick = int(thegraph_hyperv["baseLower"])
            _upperTick = int(thegraph_hyperv["baseUpper"])

            # define token0 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower = feeGrowthOutsideUpper = 0
            try:
                feeGrowthOutsideLower = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor["symbol"],
                        _lowerTick,
                        ticks,
                        thegraph_hyperv["pool"]["id"],
                    )
                )
                # skip
                feeGrowthOutsideLower = 0
                logging.getLogger(__name__).warning(
                    "{} token0 base position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor["symbol"], thegraph_hyperv["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor["symbol"],
                        _lowerTick,
                        ticks,
                        thegraph_hyperv["pool"]["id"],
                    )
                )
                # skip
                feeGrowthOutsideUpper = 0
                logging.getLogger(__name__).warning(
                    "{} token0 base position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor["symbol"], thegraph_hyperv["id"]
                    )
                )

            base_fees0 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal0X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper,
                feeGrowthInsideLast=int(
                    thegraph_hyperv["baseFeeGrowthInside0LastX128"]
                ),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token0)

            # define token1 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower = feeGrowthOutsideUpper = 0
            try:
                feeGrowthOutsideLower = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor["symbol"],
                        _lowerTick,
                        ticks,
                        thegraph_hyperv["pool"]["id"],
                    )
                )
                # skip
                feeGrowthOutsideLower = 0
                logging.getLogger(__name__).warning(
                    "{} token1 base position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor["symbol"], thegraph_hyperv["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor["symbol"],
                        _lowerTick,
                        ticks,
                        thegraph_hyperv["pool"]["id"],
                    )
                )
                # skip
                feeGrowthOutsideUpper = 0
                logging.getLogger(__name__).warning(
                    "{} token1 base position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor["symbol"], thegraph_hyperv["id"]
                    )
                )

            base_fees1 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal1X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper,
                feeGrowthInsideLast=int(
                    thegraph_hyperv["baseFeeGrowthInside1LastX128"]
                ),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token1)

            # gather LIMIT position data
            liquidity = int(thegraph_hyperv["limitLiquidity"])
            _lowerTick = int(thegraph_hyperv["limitLower"])
            _upperTick = int(thegraph_hyperv["limitUpper"])

            # define token0 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower = feeGrowthOutsideUpper = 0
            try:
                feeGrowthOutsideLower = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor["symbol"],
                        _lowerTick,
                        ticks,
                        thegraph_hyperv["pool"]["id"],
                    )
                )
                # skip
                feeGrowthOutsideLower = 0
                logging.getLogger(__name__).warning(
                    "{} token0 limit position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor["symbol"], thegraph_hyperv["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor["symbol"],
                        _lowerTick,
                        ticks,
                        thegraph_hyperv["pool"]["id"],
                    )
                )
                # skip
                feeGrowthOutsideUpper = 0
                logging.getLogger(__name__).warning(
                    "{} token0 base position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor["symbol"], thegraph_hyperv["id"]
                    )
                )

            limit_fees0 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal0X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper,
                feeGrowthInsideLast=int(
                    thegraph_hyperv["limitFeeGrowthInside0LastX128"]
                ),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token0)

            # define token1 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower = feeGrowthOutsideUpper = 0
            try:
                feeGrowthOutsideLower = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor["symbol"],
                        _lowerTick,
                        ticks,
                        thegraph_hyperv["pool"]["id"],
                    )
                )
                # skip
                feeGrowthOutsideLower = 0
                logging.getLogger(__name__).warning(
                    "{} token0 limit position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor["symbol"], thegraph_hyperv["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor["symbol"],
                        _lowerTick,
                        ticks,
                        thegraph_hyperv["pool"]["id"],
                    )
                )
                # skip
                feeGrowthOutsideUpper = 0
                logging.getLogger(__name__).warning(
                    "{} token0 base position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor["symbol"], thegraph_hyperv["id"]
                    )
                )

            limit_fees1 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal1X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper,
                feeGrowthInsideLast=int(
                    thegraph_hyperv["limitFeeGrowthInside1LastX128"]
                ),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token1)

            # define totalfees
            subgraph_totalFees0 = (
                base_fees0
                + limit_fees0
                + thegraph_hyperv["baseTokensOwed0"]
                + thegraph_hyperv["limitTokensOwed0"]
            )
            subgraph_totalFees1 = (
                base_fees1
                + limit_fees1
                + thegraph_hyperv["baseTokensOwed1"]
                + thegraph_hyperv["limitTokensOwed1"]
            )

            # differences situations
            if hypervisor["totalFees0"] == 0 and subgraph_totalFees0 > 0:
                difference_token0 = 1
            else:
                difference_token0 = (
                    (
                        (hypervisor["totalFees0"] - subgraph_totalFees0)
                        / hypervisor["totalFees0"]
                    )
                    if hypervisor["totalFees0"] != 0
                    else 0
                )

            if hypervisor["totalFees1"] == 0 and subgraph_totalFees1 > 0:
                difference_token1 = 1
            else:
                difference_token1 = (
                    (
                        (hypervisor["totalFees1"] - subgraph_totalFees1)
                        / hypervisor["totalFees1"]
                    )
                    if hypervisor["totalFees1"] != 0
                    else 0
                )

        except:
            logging.getLogger(__name__).exception(
                " Unexpected error     .error: {}".format(sys.exc_info()[0])
            )
            error = True

        # add data to result
        return {
            "hypervisor_id": hyp_id,
            "symbol": hypervisor["symbol"],
            "api_totalFees0": hypervisor["totalFees0"],
            "api_totalFees1": hypervisor["totalFees1"],
            "subgraph_totalFees0": subgraph_totalFees0,
            "subgraph_totalFees1": subgraph_totalFees1,
            "totalFees0_diff": difference_token0,
            "totalFees1_diff": difference_token1,
            "subgraph_errors": error,
        }

    # progress
    with tqdm.tqdm(total=len(official_uncollected_data.keys())) as progress_bar:

        if not threaded:
            # not threaded  (slow )
            for hyp_id, hypervisor in official_uncollected_data.items():

                progress_bar.set_description(
                    "processed {}'s {}".format(network, hypervisor["symbol"])
                )

                # add data to result
                result.append(
                    do_loop_work(hyp_id, hypervisor, gamma_helper, uniswapv3_helper)
                )

                # update progress
                progress_bar.update(1)
        else:
            # threaded
            args = (
                (hyp_id, hypervisor, gamma_helper, uniswapv3_helper)
                for hyp_id, hypervisor in official_uncollected_data.items()
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                for result_item in ex.map(lambda p: do_loop_work(*p), args):
                    # progress
                    progress_bar.set_description(
                        "processed {}'s {}".format(network, result_item["symbol"])
                    )

                    # add data to result
                    result.append(result_item)

                    # update progress
                    progress_bar.update(1)

    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass
    file_utilities.SaveCSV(filename=csv_filename, columns=result[0].keys(), rows=result)


# passed
# equal 100%
def test_uncollected_fees_comparison_formulas_thegraph(threaded: bool = False):
    """Comparison between uncollected fees formula used at gammawire.net endpoint and
       custom calculation method using THEGRAPH data:
           data sourced from thegraph ( same data is used)

       Results are saved in a csv file with the same def name

    Args:
       threaded (bool, optional): speedup the scriptwith threading. Defaults to False.
    """

    protocol = "gamma"
    network = "ethereum"

    csv_filename = "{}_test_uncollected_fees_comparison_formulas_thegraph{}.csv".format(
        network, "_threaded" if threaded else ""
    )
    csv_filename = os.path.join(CURRENT_FOLDER, csv_filename)

    # thegraph helpers
    uniswapv3_helper = thegraph_utilities.uniswapv3_scraper(
        cache=True, cache_savePath="data/cache", convert=True
    )
    gamma_helper = thegraph_utilities.gamma_scraper(
        cache=True, cache_savePath="data/cache", convert=True
    )

    # utility class (helpful later on)
    class tick:
        # value
        # liquidityGross ,
        # liquidityNet ,
        # feeGrowthOutside0X128 uint256,
        # feeGrowthOutside1X128 uint256,
        # tickCumulativeOutside int56,
        # secondsPerLiquidityOutsideX128 uint160,
        # secondsOutside uint32, initialized bool

        def __init__(
            self, value: int, feeGrowthOutside0X128: int, feeGrowthOutside1X128: int
        ):
            self._value = value  # tick
            self._feeGrowthOutside0X128 = feeGrowthOutside0X128
            self._feeGrowthOutside1X128 = feeGrowthOutside1X128

        @property
        def value(self):
            return self._value

        @property
        def feeGrowthOutside0X128(self):
            return self._feeGrowthOutside0X128

        @property
        def feeGrowthOutside1X128(self):
            return self._feeGrowthOutside1X128

    # define a result var
    result = list()

    # create the func to loop thru
    def do_loop_work(hypervisor: dict, gamma_helper, uniswapv3_helper):

        # create status name
        hypervisor_name = "{} {}'s {}-{} ({:,.2%}) ".format(
            protocol,
            network,
            hypervisor["pool"]["token0"]["symbol"],
            hypervisor["pool"]["token1"]["symbol"],
            int(hypervisor["pool"]["fee"]) / 100000,
        )
        # errorsencountered?
        error = False

        subgraph_totalFees0 = (
            subgraph_totalFees1
        ) = difference_token0 = difference_token1 = 0
        try:
            # get uniswap pool and tick info  [ feeGrowthGlobal0X128 and feeGrowthGlobal1X128]
            univ3_pool = uniswapv3_helper.get_all_results(
                network=network,
                query_name="pools",
                where=""" id: "{}" """.format(hypervisor["pool"]["id"]),
            )[0]

            # create an array of ticks
            ticks = [
                hypervisor["baseUpper"],
                hypervisor["baseLower"],
                hypervisor["limitUpper"],
                hypervisor["limitLower"],
            ]
            # get all ticks [feeGrowthOutside1X128 and feeGrowthOutside0X128]
            univ3_ticks = uniswapv3_helper.get_all_results(
                network=network,
                query_name="ticks",
                where=""" tickIdx_in: {}, poolAddress: "{}" """.format(
                    ticks, hypervisor["pool"]["id"]
                ),
            )

            # classify univ3 ticks in a dict
            feeGrowthOutside = dict()
            for tk in univ3_ticks:
                if not tk["tickIdx"] in feeGrowthOutside:
                    feeGrowthOutside[int(tk["tickIdx"])] = tick(
                        int(tk["tickIdx"]),
                        int(tk["feeGrowthOutside0X128"]),
                        int(tk["feeGrowthOutside1X128"]),
                    )

            decimals_token0 = hypervisor["pool"]["token0"]["decimals"]
            decimals_token1 = hypervisor["pool"]["token1"]["decimals"]
            currentTick = int(univ3_pool["tick"])

            # gather BASE position data
            liquidity = int(hypervisor["baseLiquidity"])
            _lowerTick = int(hypervisor["baseLower"])
            _upperTick = int(hypervisor["baseUpper"])

            # define token0 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower_token0 = feeGrowthOutsideUpper_token0 = 0
            try:
                feeGrowthOutsideLower_token0 = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideLower_token0 = 0
                logging.getLogger(__name__).warning(
                    "{} token0 base position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper_token0 = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideUpper_token0 = 0
                logging.getLogger(__name__).warning(
                    "{} token0 base position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )

            # define token1 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower_token1 = feeGrowthOutsideUpper_token1 = 0
            try:
                feeGrowthOutsideLower_token1 = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideLower_token1 = 0
                logging.getLogger(__name__).warning(
                    "{} token1 base position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper_token1 = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideUpper_token1 = 0
                logging.getLogger(__name__).warning(
                    "{} token1 base position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )

            # alternative formula
            base_fees0 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal0X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower_token0,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper_token0,
                feeGrowthInsideLast=int(hypervisor["baseFeeGrowthInside0LastX128"]),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token0)

            base_fees1 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal1X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower_token1,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper_token1,
                feeGrowthInsideLast=int(hypervisor["baseFeeGrowthInside1LastX128"]),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token1)

            # gammawire
            (
                gammawireFormula_base_fees0,
                gammawireFormula_base_fees1,
            ) = univ3_formulas.get_uncollected_fees_vGammawire(
                fee_growth_global_0=int(univ3_pool["feeGrowthGlobal0X128"]),
                fee_growth_global_1=int(univ3_pool["feeGrowthGlobal1X128"]),
                tick_current=currentTick,
                tick_lower=_lowerTick,
                tick_upper=_upperTick,
                fee_growth_outside_0_lower=feeGrowthOutsideLower_token0,
                fee_growth_outside_1_lower=feeGrowthOutsideLower_token1,
                fee_growth_outside_0_upper=feeGrowthOutsideUpper_token0,
                fee_growth_outside_1_upper=feeGrowthOutsideUpper_token1,
                liquidity=liquidity,
                fee_growth_inside_last_0=int(
                    hypervisor["baseFeeGrowthInside0LastX128"]
                ),
                fee_growth_inside_last_1=int(
                    hypervisor["baseFeeGrowthInside1LastX128"]
                ),
            )
            gammawireFormula_base_fees0 /= 10**decimals_token0
            gammawireFormula_base_fees1 /= 10**decimals_token1

            # gather LIMIT position data
            liquidity = int(hypervisor["limitLiquidity"])
            _lowerTick = int(hypervisor["limitLower"])
            _upperTick = int(hypervisor["limitUpper"])

            # define token0 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower_token0 = feeGrowthOutsideUpper_token0 = 0
            try:
                feeGrowthOutsideLower_token0 = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideLower_token0 = 0
                logging.getLogger(__name__).warning(
                    "{} token0 limit position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper_token0 = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideUpper_token0 = 0
                logging.getLogger(__name__).warning(
                    "{} token0 limit position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )

            # define token1 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower_token1 = feeGrowthOutsideUpper_token1 = 0
            try:
                feeGrowthOutsideLower_token1 = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideLower_token1 = 0
                logging.getLogger(__name__).warning(
                    "{} token1 limit position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper_token1 = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideUpper_token1 = 0
                logging.getLogger(__name__).warning(
                    "{} token1 limit position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )

            limit_fees0 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal0X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower_token0,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper_token0,
                feeGrowthInsideLast=int(hypervisor["limitFeeGrowthInside0LastX128"]),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token0)

            limit_fees1 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal1X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower_token1,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper_token1,
                feeGrowthInsideLast=int(hypervisor["limitFeeGrowthInside1LastX128"]),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token1)

            # gammawire
            (
                gammawireFormula_limit_fees0,
                gammawireFormula_limit_fees1,
            ) = univ3_formulas.get_uncollected_fees_vGammawire(
                fee_growth_global_0=int(univ3_pool["feeGrowthGlobal0X128"]),
                fee_growth_global_1=int(univ3_pool["feeGrowthGlobal1X128"]),
                tick_current=currentTick,
                tick_lower=_lowerTick,
                tick_upper=_upperTick,
                fee_growth_outside_0_lower=feeGrowthOutsideLower_token0,
                fee_growth_outside_1_lower=feeGrowthOutsideLower_token1,
                fee_growth_outside_0_upper=feeGrowthOutsideUpper_token0,
                fee_growth_outside_1_upper=feeGrowthOutsideUpper_token1,
                liquidity=liquidity,
                fee_growth_inside_last_0=int(
                    hypervisor["limitFeeGrowthInside0LastX128"]
                ),
                fee_growth_inside_last_1=int(
                    hypervisor["limitFeeGrowthInside1LastX128"]
                ),
            )
            # convert float
            gammawireFormula_limit_fees0 /= 10**decimals_token0
            gammawireFormula_limit_fees1 /= 10**decimals_token1

            # define totalfees
            gammawire_totalFees0 = (
                gammawireFormula_base_fees0
                + gammawireFormula_limit_fees0
                + hypervisor["baseTokensOwed0"]
                + hypervisor["limitTokensOwed0"]
            )
            gammawire_totalFees1 = (
                gammawireFormula_base_fees1
                + gammawireFormula_limit_fees1
                + hypervisor["baseTokensOwed1"]
                + hypervisor["limitTokensOwed1"]
            )

            alternative_totalFees0 = (
                base_fees0
                + limit_fees0
                + hypervisor["baseTokensOwed0"]
                + hypervisor["limitTokensOwed0"]
            )
            alternative_totalFees1 = (
                base_fees1
                + limit_fees1
                + hypervisor["baseTokensOwed1"]
                + hypervisor["limitTokensOwed1"]
            )

            # differences situations
            if gammawire_totalFees0 == 0 and alternative_totalFees0 > 0:
                difference_token0 = 1
            else:
                difference_token0 = (
                    (
                        (gammawire_totalFees0 - alternative_totalFees0)
                        / gammawire_totalFees0
                    )
                    if gammawire_totalFees0 != 0
                    else 0
                )

            if gammawire_totalFees1 == 0 and alternative_totalFees1 > 0:
                difference_token1 = 1
            else:
                difference_token1 = (
                    (
                        (gammawire_totalFees1 - alternative_totalFees1)
                        / gammawire_totalFees1
                    )
                    if gammawire_totalFees1 != 0
                    else 0
                )

        except:
            logging.getLogger(__name__).exception(
                " Unexpected error  while processing {}       .error: {}".format(
                    hypervisor_name, sys.exc_info()[0]
                )
            )
            error = True
            if not gammawire_totalFees0:
                gammawire_totalFees0 = difference_token0 = difference_token1 = 0
            if not gammawire_totalFees1:
                gammawire_totalFees1 = difference_token0 = difference_token1 = 0
            if not alternative_totalFees0:
                alternative_totalFees0 = difference_token0 = difference_token1 = 0
            if not alternative_totalFees1:
                alternative_totalFees1 = difference_token0 = difference_token1 = 0

        # add data to result
        return {
            "hypervisor_id": hypervisor["id"],
            "name": hypervisor_name,
            "gammawire_totalFees0": gammawire_totalFees0,
            "gammawire_totalFees1": gammawire_totalFees1,
            "alternative_totalFees0": alternative_totalFees0,
            "alternative_totalFees1": alternative_totalFees1,
            "totalFees0_diff": difference_token0,
            "totalFees1_diff": difference_token1,
            "subgraph_errors": error,
        }

    # get a list of hypervisors from thegraph
    hypervisors = gamma_helper.get_all_results(
        network=network, query_name="uniswapV3Hypervisors"
    )

    # progress
    with tqdm.tqdm(total=len(hypervisors)) as progress_bar:

        if not threaded:
            # not threaded  (slow )
            for hypervisor in hypervisors:

                # create status name
                hypervisor_name = "{} {}'s {}-{} ({:,.2%}) ".format(
                    protocol,
                    network,
                    hypervisor["pool"]["token0"]["symbol"],
                    hypervisor["pool"]["token1"]["symbol"],
                    int(hypervisor["pool"]["fee"]) / 100000,
                )

                progress_bar.set_description(
                    "processed {}'s {}".format(network, hypervisor_name)
                )

                # add data to result
                result.append(do_loop_work(hypervisor, gamma_helper, uniswapv3_helper))

                # update progress
                progress_bar.update(1)
        else:
            # threaded
            args = (
                (hypervisor, gamma_helper, uniswapv3_helper)
                for hypervisor in hypervisors
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                for result_item in ex.map(lambda p: do_loop_work(*p), args):
                    # progress
                    progress_bar.set_description(
                        "processed {}'s {}".format(network, result_item["name"])
                    )

                    # add data to result
                    result.append(result_item)

                    # update progress
                    progress_bar.update(1)

    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass
    file_utilities.SaveCSV(filename=csv_filename, columns=result[0].keys(), rows=result)


# passed
# equal 100%
def test_uncollected_fees_comparison_formulas_onchain(threaded: bool = False):
    """Comparison between uncollected fees formula used at gammawire.net endpoint and
       custom calculation method using direct chain queries:
           data sourced from chain using web3 provider ( same data is used)

       Results are saved in a csv file with the same def name

    Args:
       threaded (bool, optional): speedup the scriptwith threading. Defaults to False.
    """

    protocol = "gamma"
    network = "ethereum"
    web3Provider_url = CONFIGURATION["sources"]["web3Providers"][network]
    csv_filename = "{}_test_uncollected_fees_comparison_formulas_onchain{}.csv".format(
        network, "_threaded" if threaded else ""
    )
    csv_filename = os.path.join(CURRENT_FOLDER, csv_filename)

    web3Provider = Web3(
        Web3.HTTPProvider(web3Provider_url, request_kwargs={"timeout": 60})
    )
    # add middleware as needed
    if network != "ethereum":
        web3Provider.middleware_onion.inject(geth_poa_middleware, layer=0)

    # thegraph helper to get hypervisor id only
    gamma_helper = thegraph_utilities.gamma_scraper(
        cache=True, cache_savePath="data/cache", convert=True
    )

    # define a result var
    result = list()

    # create the func to loop thru
    def do_loop_work(hyp_id, network):

        error = False

        # setup helper
        gamma_web3Helper = gamma_hypervisor_cached(address=hyp_id, network=network)

        # get name
        hypervisor_name = gamma_web3Helper.symbol

        # decimals
        decimals_token0 = gamma_web3Helper.token0.decimals
        decimals_token1 = gamma_web3Helper.token1.decimals

        tickCurrent = gamma_web3Helper.pool.slot0["tick"]

        #   base
        position_base = gamma_web3Helper.getBasePosition
        tickUpper_base = gamma_web3Helper.baseUpper
        tickLower_base = gamma_web3Helper.baseLower
        pool_position_base = gamma_web3Helper.pool.position(
            ownerAddress=Web3.toChecksumAddress(gamma_web3Helper.address.lower()),
            tickLower=tickLower_base,
            tickUpper=tickUpper_base,
        )
        Ticks_lower_base = gamma_web3Helper.pool.ticks(tickLower_base)
        Ticks_upper_base = gamma_web3Helper.pool.ticks(tickUpper_base)
        #   limit
        position_limit = gamma_web3Helper.getLimitPosition
        tickUpper_limit = gamma_web3Helper.limitUpper
        tickLower_limit = gamma_web3Helper.limitLower
        pool_position_limit = gamma_web3Helper.pool.position(
            ownerAddress=Web3.toChecksumAddress(gamma_web3Helper.address.lower()),
            tickLower=tickLower_limit,
            tickUpper=tickUpper_limit,
        )
        Ticks_lower_limit = gamma_web3Helper.pool.ticks(tickLower_limit)
        Ticks_upper_limit = gamma_web3Helper.pool.ticks(tickUpper_limit)

        feeGrowthGlobal0X128 = gamma_web3Helper.pool.feeGrowthGlobal0X128
        feeGrowthGlobal1X128 = gamma_web3Helper.pool.feeGrowthGlobal1X128

        # BASE
        # gammawire formula
        (
            gammawire_base_fees0,
            gammawire_base_fees1,
        ) = univ3_formulas.get_uncollected_fees_vGammawire(
            fee_growth_global_0=feeGrowthGlobal0X128,
            fee_growth_global_1=feeGrowthGlobal1X128,
            tick_current=tickCurrent,
            tick_lower=tickLower_base,
            tick_upper=tickUpper_base,
            fee_growth_outside_0_lower=Ticks_lower_base["feeGrowthOutside0X128"],
            fee_growth_outside_1_lower=Ticks_lower_base["feeGrowthOutside1X128"],
            fee_growth_outside_0_upper=Ticks_upper_base["feeGrowthOutside0X128"],
            fee_growth_outside_1_upper=Ticks_upper_base["feeGrowthOutside1X128"],
            liquidity=position_base["liquidity"],
            fee_growth_inside_last_0=pool_position_base["feeGrowthInside0LastX128"],
            fee_growth_inside_last_1=pool_position_base["feeGrowthInside1LastX128"],
        )
        gammawire_base_fees0 /= 10**decimals_token0
        gammawire_base_fees1 /= 10**decimals_token1

        # alternative formula
        base_fees0 = univ3_formulas.get_uncollected_fees(
            feeGrowthGlobal=feeGrowthGlobal0X128,
            feeGrowthOutsideLower=Ticks_lower_base["feeGrowthOutside0X128"],
            feeGrowthOutsideUpper=Ticks_upper_base["feeGrowthOutside0X128"],
            feeGrowthInsideLast=pool_position_base["feeGrowthInside0LastX128"],
            tickCurrent=tickCurrent,
            liquidity=position_base["liquidity"],
            tickLower=tickLower_base,
            tickUpper=tickUpper_base,
        ) / (10**decimals_token0)

        base_fees1 = univ3_formulas.get_uncollected_fees(
            feeGrowthGlobal=feeGrowthGlobal1X128,
            feeGrowthOutsideLower=Ticks_lower_base["feeGrowthOutside1X128"],
            feeGrowthOutsideUpper=Ticks_upper_base["feeGrowthOutside1X128"],
            feeGrowthInsideLast=pool_position_base["feeGrowthInside1LastX128"],
            tickCurrent=tickCurrent,
            liquidity=position_base["liquidity"],
            tickLower=tickLower_base,
            tickUpper=tickUpper_base,
        ) / (10**decimals_token1)

        # LIMIT
        # gammawire formula
        (
            gammawire_limit_fees0,
            gammawire_limit_fees1,
        ) = univ3_formulas.get_uncollected_fees_vGammawire(
            fee_growth_global_0=feeGrowthGlobal0X128,
            fee_growth_global_1=feeGrowthGlobal1X128,
            tick_current=tickCurrent,
            tick_lower=tickLower_limit,
            tick_upper=tickUpper_limit,
            fee_growth_outside_0_lower=Ticks_lower_limit["feeGrowthOutside0X128"],
            fee_growth_outside_1_lower=Ticks_lower_limit["feeGrowthOutside1X128"],
            fee_growth_outside_0_upper=Ticks_upper_limit["feeGrowthOutside0X128"],
            fee_growth_outside_1_upper=Ticks_upper_limit["feeGrowthOutside1X128"],
            liquidity=position_limit["liquidity"],
            fee_growth_inside_last_0=pool_position_limit["feeGrowthInside0LastX128"],
            fee_growth_inside_last_1=pool_position_limit["feeGrowthInside1LastX128"],
        )
        gammawire_limit_fees0 /= 10**decimals_token0
        gammawire_limit_fees1 /= 10**decimals_token1

        # alternative formula
        limit_fees0 = univ3_formulas.get_uncollected_fees(
            feeGrowthGlobal=feeGrowthGlobal0X128,
            feeGrowthOutsideLower=Ticks_lower_limit["feeGrowthOutside0X128"],
            feeGrowthOutsideUpper=Ticks_upper_limit["feeGrowthOutside0X128"],
            feeGrowthInsideLast=pool_position_limit["feeGrowthInside0LastX128"],
            tickCurrent=tickCurrent,
            liquidity=position_limit["liquidity"],
            tickLower=tickLower_limit,
            tickUpper=tickUpper_limit,
        ) / (10**decimals_token0)

        limit_fees1 = univ3_formulas.get_uncollected_fees(
            feeGrowthGlobal=feeGrowthGlobal1X128,
            feeGrowthOutsideLower=Ticks_lower_limit["feeGrowthOutside1X128"],
            feeGrowthOutsideUpper=Ticks_upper_limit["feeGrowthOutside1X128"],
            feeGrowthInsideLast=pool_position_limit["feeGrowthInside1LastX128"],
            tickCurrent=tickCurrent,
            liquidity=position_limit["liquidity"],
            tickLower=tickLower_limit,
            tickUpper=tickUpper_limit,
        ) / (10**decimals_token1)

        # define totalfees
        gammawire_totalFees0 = (
            gammawire_base_fees0
            + gammawire_limit_fees0
            + pool_position_base["tokensOwed0"] / (10**decimals_token0)
            + pool_position_limit["tokensOwed0"] / (10**decimals_token0)
        )
        gammawire_totalFees1 = (
            gammawire_base_fees1
            + gammawire_limit_fees1
            + pool_position_base["tokensOwed1"] / (10**decimals_token1)
            + pool_position_limit["tokensOwed1"] / (10**decimals_token1)
        )

        alternative_totalFees0 = (
            base_fees0
            + limit_fees0
            + pool_position_base["tokensOwed0"] / (10**decimals_token0)
            + pool_position_limit["tokensOwed0"] / (10**decimals_token0)
        )
        alternative_totalFees1 = (
            base_fees1
            + limit_fees1
            + pool_position_base["tokensOwed1"] / (10**decimals_token1)
            + pool_position_limit["tokensOwed1"] / (10**decimals_token1)
        )

        # differences situations
        if gammawire_totalFees0 == 0 and alternative_totalFees0 > 0:
            difference_token0 = 1
        else:
            difference_token0 = (
                ((gammawire_totalFees0 - alternative_totalFees0) / gammawire_totalFees0)
                if gammawire_totalFees0 != 0
                else 0
            )

        if gammawire_totalFees1 == 0 and alternative_totalFees1 > 0:
            difference_token1 = 1
        else:
            difference_token1 = (
                ((gammawire_totalFees1 - alternative_totalFees1) / gammawire_totalFees1)
                if gammawire_totalFees1 != 0
                else 0
            )

        # add data to result
        return {
            "hypervisor_id": hyp_id,
            "name": hypervisor_name,
            "gammawire_totalFees0": gammawire_totalFees0,
            "gammawire_totalFees1": gammawire_totalFees1,
            "alternative_totalFees0": alternative_totalFees0,
            "alternative_totalFees1": alternative_totalFees1,
            "totalFees0_diff": difference_token0,
            "totalFees1_diff": difference_token1,
            "subgraph_errors": error,
        }

    # get a list of hypervisors from thegraph
    hypervisors = gamma_helper.get_all_results(
        network=network, query_name="simple_uniswapV3Hypervisors"
    )

    # progress
    with tqdm.tqdm(total=len(hypervisors)) as progress_bar:

        if not threaded:
            # not threaded  (slow )
            for hypervisor in hypervisors:

                progress_bar.set_description(
                    "processed {}'s {}".format(network, hypervisor["id"])
                )

                # add data to result
                result.append(do_loop_work(hypervisor["id"], network))

                # update progress
                progress_bar.update(1)
        else:
            # threaded
            args = ((hypervisor["id"], network) for hypervisor in hypervisors)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                for result_item in ex.map(lambda p: do_loop_work(*p), args):
                    # progress
                    progress_bar.set_description(
                        "processed {}'s {}".format(network, result_item["name"])
                    )

                    # add data to result
                    result.append(result_item)

                    # update progress
                    progress_bar.update(1)

    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass
    file_utilities.SaveCSV(filename=csv_filename, columns=result[0].keys(), rows=result)


from bins.mixed import price_utilities


def test_uncollected_fees_onchain(
    protocol: str,
    network: str,
    dex: str,
    web3Provider_url: str,
    hypervisor_ids: list,
    blocks: list = [],
    threaded: bool = False,
    max_workers: int = 5,
):
    """get multiple onchain fields and save em to csv file

    Args:
       protocol (str): "gamma"
       network (str): "ethereum", "polygon" ...
       dex (str): "uniswapv3" or "quickswap" ...
       web3Provider_url (str): full url to web3 provider
       hypervisor_ids (list):
       blocks (list, optional): if no blocks specified, current block is used. Defaults to [].
       threaded (bool, optional): . Defaults to False.
       max_workers (int, optional): . Defaults to 5.

    Returns:
           saves csv file with rows:
                       "hypervisor_id",
                       "name",
                       "block",
                       "timestamp",
                       "tvl0",
                       "tvl1",
                       "tvlUSD",
                       "deployed_token0",
                       "deployed_token1",
                       "fees_owed_token0",
                       "fees_owed_token1",
                       "parked_token0",
                       "parked_token1",
                       "uncollected_fees0",
                       "uncollected_fees1",
                       "price0",
                       "price1",
                       "elapsedTime",
                       "subgraph_errors"
    """

    web3Provider = Web3(
        Web3.HTTPProvider(web3Provider_url, request_kwargs={"timeout": 60})
    )
    # add middleware as needed
    if network != "ethereum":
        web3Provider.middleware_onion.inject(geth_poa_middleware, layer=0)

    # define blocks to be scraped ( for each hypervisor id)
    if len(blocks) == 0:
        blocks = list()
        bk = web3Provider.eth.getBlock("latest").number
        for i in range(len(hypervisor_ids)):
            blocks.append(bk)

    # create price scrape helper
    price_helper = price_utilities.price_scraper(
        cache=True,
        cache_filename="uniswapv3_price_cache",
    )

    # define a result var
    result = list()

    # create the func to loop thru
    def do_loop_work(hyp_id, web3Provider, block, price_helper, network):

        error = False

        # setup helper
        if dex == "uniswapv3":
            gamma_web3Helper = gamma_hypervisor(
                address=hyp_id, network=network, block=block
            )
        elif dex == "quickswap":
            gamma_web3Helper = gamma_hypervisor_quickswap(
                address=hyp_id, network=network, block=block
            )

        # get name
        hypervisor_name = gamma_web3Helper.symbol

        timestamp = web3Provider.eth.getBlock(block).timestamp

        # TVL direct contract call
        totalAmounts = gamma_web3Helper.getTotalAmounts  # TODO: check no float anymore
        tvl0_direct = Decimal(totalAmounts["total0"]) / Decimal(
            10**gamma_web3Helper.pool.token0.decimals
        )
        tvl1_direct = Decimal(totalAmounts["total1"]) / Decimal(
            10**gamma_web3Helper.pool.token1.decimals
        )
        # TVL indirect univ3 calculation calls
        tvl_indirect = gamma_web3Helper.get_tvl(inDecimal=True)
        tvl0_indirect = tvl_indirect["tvl_token0"]
        tvl1_indirect = tvl_indirect["tvl_token1"]
        # TVL solidity/python deviation
        diff0 = ((tvl0_direct - tvl0_indirect) / tvl0_direct) if tvl0_direct != 0 else 0
        diff1 = ((tvl1_direct - tvl1_indirect) / tvl1_direct) if tvl1_direct != 0 else 0
        if abs(diff0) > 0.0001:
            print(
                "{} {} token0 total difference of {:,.4%} btween direct call to totalAmounts and indirect calls 2 univ3".format(
                    network, hypervisor_name, diff0
                )
            )
        if abs(diff1) > 0.0001:
            print(
                "{} {} token1 total difference of {:,.4%} btween direct call to totalAmounts and indirect calls 2 univ3".format(
                    network, hypervisor_name, diff1
                )
            )

        # PRICE get prices from thegraph or coingecko
        price0 = price_helper.get_price(
            network=network, token_id=gamma_web3Helper.token0.address, block=block
        )
        price1 = price_helper.get_price(
            network=network, token_id=gamma_web3Helper.token1.address, block=block
        )
        # calc USD TVL
        tvlUSD_fromdex = tvl0_indirect * Decimal(price0) + tvl1_indirect * Decimal(
            price1
        )
        tvlUSD = tvl0_direct * Decimal(price0) + tvl1_direct * Decimal(price1)

        # UNCOLLECTED
        fees_uncollected = gamma_web3Helper.get_fees_uncollected(inDecimal=True)

        # add data to result
        return {
            "hypervisor_id": hyp_id,
            "name": hypervisor_name,
            "block": block,
            "timestamp": timestamp,
            "tvl0": tvl0_direct,
            "tvl1": tvl1_direct,
            "tvlUSD": tvlUSD,
            "tvl0_fromdex": tvl0_indirect,
            "tvl1_fromdex": tvl1_indirect,
            "tvlUSD_fromdex": tvlUSD,
            "deployed_token0": tvl_indirect["deployed_token0"],
            "deployed_token1": tvl_indirect["deployed_token1"],
            "fees_owed_token0": tvl_indirect["fees_owed_token0"],
            "fees_owed_token1": tvl_indirect["fees_owed_token1"],
            "parked_token0": tvl_indirect["parked_token0"],
            "parked_token1": tvl_indirect["parked_token1"],
            "uncollected_fees0": fees_uncollected["qtty_token0"],
            "uncollected_fees1": fees_uncollected["qtty_token1"],
            "price0": price0,
            "price1": price1,
            "elapsedTime": 0,
            "subgraph_errors": error,
        }

    # progress
    with tqdm.tqdm(total=len(hypervisor_ids) * len(blocks)) as progress_bar:

        if not threaded:
            # not threaded  (slow )
            for hypervisor_id in hypervisor_ids:

                for block in blocks:

                    progress_bar.set_description(
                        "processed {}'s {}".format(network, hypervisor_id)
                    )

                    # add data to result
                    result.append(
                        do_loop_work(
                            hypervisor_id, web3Provider, block, price_helper, network
                        )
                    )

                    # update progress
                    progress_bar.update(1)
        else:
            # threaded
            args = (
                (hypervisor_id, web3Provider, block, price_helper, network)
                for hypervisor_id in hypervisor_ids
                for block in blocks
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
                for result_item in ex.map(lambda p: do_loop_work(*p), args):
                    # progress
                    progress_bar.set_description(
                        "processed {}'s {}".format(network, result_item["name"])
                    )

                    # add data to result
                    result.append(result_item)

                    # update progress
                    progress_bar.update(1)

    # resuklt
    return result


def test_uncollected_fees_thegraph(
    protocol: str,
    network: str,
    dex: str,
    hypervisor_ids: list,
    blocks: list = [],
    threaded: bool = False,
    max_workers: int = 5,
):
    """get multiple thegraph fields and save em to csv file

    Args:
       protocol (str): "gamma"
       network (str): "ethereum", "polygon" ...
       dex (str): "uniswapv3" or "quickswap" ...
       web3Provider_url (str): full url to web3 provider
       hypervisor_ids (list):
       blocks (list, optional): if no blocks specified, current block is used. Defaults to [].
       threaded (bool, optional): . Defaults to False.
       max_workers (int, optional): . Defaults to 5.

    Returns:
           saves csv file with rows:
                       "hypervisor_id",
                       "name",
                       "block",
                       "timestamp",
                       "tvl0",
                       "tvl1",
                       "tvlUSD",
                       "deployed_token0",
                       "deployed_token1",
                       "fees_owed_token0",
                       "fees_owed_token1",
                       "parked_token0",
                       "parked_token1",
                       "uncollected_fees0",
                       "uncollected_fees1",
                       "price0",
                       "price1",
                       "elapsedTime",
                       "subgraph_errors"
    """

    blocks_scraper = thegraph_utilities.blocks_scraper(
        cache=False, cache_savePath="", convert=True
    )
    # define blocks to be scraped ( for each hypervisor id)
    if len(blocks) == 0:
        blocks = list()
        latest_block = blocks_scraper.get_all_results(
            network=network,
            query_name="blocks",
            first=1,
            orderBy="number",
            orderDirection="desc",
        )[0]
        # for i in range(len(hypervisor_ids)):
        blocks.append(latest_block)

    blocks_info = {}
    for item in blocks_scraper.get_all_results(
        network=network, query_name="blocks", where=" number_in:{}".format(blocks)
    ):
        if not int(item["number"]) in blocks_info:
            blocks_info[int(item["number"])] = item

    # create price scrape helper
    price_helper = price_utilities.price_scraper(
        cache=False,
        cache_filename="uniswapv3_price_cache",
        cache_folderName="data/cache",
    )

    # utility class tbul
    class tick:
        # value
        # liquidityGross ,
        # liquidityNet ,
        # feeGrowthOutside0X128 uint256,
        # feeGrowthOutside1X128 uint256,
        # tickCumulativeOutside int56,
        # secondsPerLiquidityOutsideX128 uint160,
        # secondsOutside uint32, initialized bool

        def __init__(
            self, value: int, feeGrowthOutside0X128: int, feeGrowthOutside1X128: int
        ):
            self._value = value  # tick
            self._feeGrowthOutside0X128 = feeGrowthOutside0X128
            self._feeGrowthOutside1X128 = feeGrowthOutside1X128

        @property
        def value(self):
            return self._value

        @property
        def feeGrowthOutside0X128(self):
            return self._feeGrowthOutside0X128

        @property
        def feeGrowthOutside1X128(self):
            return self._feeGrowthOutside1X128

    # define a result var
    result = list()

    # create the func to loop thru
    def do_loop_work(hyp_id, block, price_helper, network):

        error = False

        # setup helper
        gamma_helper = thegraph_utilities.gamma_scraper(
            cache=False, cache_savePath="data/cache", convert=True
        )
        hypervisor = gamma_helper.get_all_results(
            network=network,
            query_name=f"uniswapV3Hypervisors_{dex}",
            where=""" id: "{}" """.format(hyp_id),
            block="number: {}".format(block),
        )[0]

        if dex == "uniswapv3":
            dexV3_helper = thegraph_utilities.uniswapv3_scraper(
                cache=False, cache_savePath="data/cache", convert=True
            )
        elif dex == "quickswap":
            dexV3_helper = thegraph_utilities.quickswap_scraper(
                cache=False, cache_savePath="data/cache", convert=True
            )

        # get name
        hypervisor_name = hypervisor["symbol"]

        timestamp = blocks_info[block]["timestamp"]

        # TVL direct contract call
        tvl0_direct = hypervisor["tvl0"]
        tvl1_direct = hypervisor["tvl1"]

        # TODO: disabled for now here.
        # TVL indirect univ3 calculation calls
        tvl0_indirect = hypervisor["tvl0"]
        tvl1_indirect = hypervisor["tvl1"]
        # TVL solidity/python deviation
        diff0 = ((tvl0_direct - tvl0_indirect) / tvl0_direct) if tvl0_direct != 0 else 0
        diff1 = ((tvl1_direct - tvl1_indirect) / tvl1_direct) if tvl1_direct != 0 else 0
        if abs(diff0) > 0.0001:
            print(
                "{} {} token0 total difference of {:,.4%} btween direct call to totalAmounts and indirect calls 2 univ3".format(
                    network, hypervisor_name, diff0
                )
            )
        if abs(diff1) > 0.0001:
            print(
                "{} {} token1 total difference of {:,.4%} btween direct call to totalAmounts and indirect calls 2 univ3".format(
                    network, hypervisor_name, diff1
                )
            )

        # PRICE get prices from thegraph or coingecko
        price0 = price_helper.get_price(
            network=network, token_id=hypervisor["pool"]["token0"]["id"], block=block
        )
        price1 = price_helper.get_price(
            network=network, token_id=hypervisor["pool"]["token1"]["id"], block=block
        )
        # calc USD TVL
        tvlUSD = tvl0_indirect * price0 + tvl1_indirect * price1

        # UNCOLLECTED
        fees_uncollected = dict()

        try:
            # get uniswap pool and tick info  [ feeGrowthGlobal0X128 and feeGrowthGlobal1X128]
            univ3_pool = dexV3_helper.get_all_results(
                network=network,
                query_name="pools",
                where=""" id: "{}" """.format(hypervisor["pool"]["id"]),
                block="number: {}".format(block),
            )[0]

            # create an array of ticks
            ticks = [
                hypervisor["baseUpper"],
                hypervisor["baseLower"],
                hypervisor["limitUpper"],
                hypervisor["limitLower"],
            ]
            # get all ticks [feeGrowthOutside1X128 and feeGrowthOutside0X128]
            univ3_ticks = dexV3_helper.get_all_results(
                network=network,
                query_name="ticks",
                where=""" tickIdx_in: {}, poolAddress: "{}" """.format(
                    ticks, hypervisor["pool"]["id"]
                ),
                block="number: {}".format(block),
            )

            # classify univ3 ticks in a dict
            feeGrowthOutside = dict()
            for tk in univ3_ticks:
                if not tk["tickIdx"] in feeGrowthOutside:
                    feeGrowthOutside[int(tk["tickIdx"])] = tick(
                        int(tk["tickIdx"]),
                        int(tk["feeGrowthOutside0X128"]),
                        int(tk["feeGrowthOutside1X128"]),
                    )

            decimals_token0 = hypervisor["pool"]["token0"]["decimals"]
            decimals_token1 = hypervisor["pool"]["token1"]["decimals"]
            currentTick = int(univ3_pool["tick"])

            # gather BASE position data
            liquidity = int(hypervisor["baseLiquidity"])
            _lowerTick = int(hypervisor["baseLower"])
            _upperTick = int(hypervisor["baseUpper"])

            # define token0 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower_token0 = feeGrowthOutsideUpper_token0 = 0
            try:
                feeGrowthOutsideLower_token0 = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideLower_token0 = 0
                logging.getLogger(__name__).warning(
                    "{} token0 base position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper_token0 = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideUpper_token0 = 0
                logging.getLogger(__name__).warning(
                    "{} token0 base position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )

            # define token1 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower_token1 = feeGrowthOutsideUpper_token1 = 0
            try:
                feeGrowthOutsideLower_token1 = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideLower_token1 = 0
                logging.getLogger(__name__).warning(
                    "{} token1 base position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper_token1 = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideUpper_token1 = 0
                logging.getLogger(__name__).warning(
                    "{} token1 base position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )

            # alternative formula
            base_fees0 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal0X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower_token0,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper_token0,
                feeGrowthInsideLast=int(hypervisor["baseFeeGrowthInside0LastX128"]),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token0)

            base_fees1 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal1X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower_token1,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper_token1,
                feeGrowthInsideLast=int(hypervisor["baseFeeGrowthInside1LastX128"]),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token1)

            # gather LIMIT position data
            liquidity = int(hypervisor["limitLiquidity"])
            _lowerTick = int(hypervisor["limitLower"])
            _upperTick = int(hypervisor["limitUpper"])

            # define token0 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower_token0 = feeGrowthOutsideUpper_token0 = 0
            try:
                feeGrowthOutsideLower_token0 = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideLower_token0 = 0
                logging.getLogger(__name__).warning(
                    "{} token0 limit position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper_token0 = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside0X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideUpper_token0 = 0
                logging.getLogger(__name__).warning(
                    "{} token0 limit position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )

            # define token1 feeGrowthOutsideLower & Upper vars
            feeGrowthOutsideLower_token1 = feeGrowthOutsideUpper_token1 = 0
            try:
                feeGrowthOutsideLower_token1 = feeGrowthOutside[
                    _lowerTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query. ".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideLower_token1 = 0
                logging.getLogger(__name__).warning(
                    "{} token1 limit position's feeGrowthOutsideLower of token0 has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )
            try:
                feeGrowthOutsideUpper_token1 = feeGrowthOutside[
                    _upperTick
                ].feeGrowthOutside1X128
            except:
                error = True
                # log error
                logging.getLogger(__name__).error(
                    "{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(
                        hypervisor_name, _lowerTick, ticks, hypervisor["pool"]["id"]
                    )
                )
                # skip
                feeGrowthOutsideUpper_token1 = 0
                logging.getLogger(__name__).warning(
                    "{} token1 limit position's feeGrowthOutsideUpper has been set to zero due to errors encountered   hyp_id:{}".format(
                        hypervisor_name, hypervisor["id"]
                    )
                )

            limit_fees0 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal0X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower_token0,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper_token0,
                feeGrowthInsideLast=int(hypervisor["limitFeeGrowthInside0LastX128"]),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token0)

            limit_fees1 = univ3_formulas.get_uncollected_fees(
                feeGrowthGlobal=int(univ3_pool["feeGrowthGlobal1X128"]),
                feeGrowthOutsideLower=feeGrowthOutsideLower_token1,
                feeGrowthOutsideUpper=feeGrowthOutsideUpper_token1,
                feeGrowthInsideLast=int(hypervisor["limitFeeGrowthInside1LastX128"]),
                tickCurrent=currentTick,
                liquidity=liquidity,
                tickLower=_lowerTick,
                tickUpper=_upperTick,
            ) / (10**decimals_token1)

            fees_uncollected["qtty_token0"] = base_fees0 + limit_fees0
            fees_uncollected["qtty_token1"] = base_fees1 + limit_fees1

        except:
            logging.getLogger(__name__).exception(
                " Unexpected error  while processing {}       .error: {}".format(
                    hypervisor_name, sys.exc_info()[0]
                )
            )
            error = True

        # add data to result
        return {
            "hypervisor_id": hyp_id,
            "name": hypervisor_name,
            "block": block,
            "timestamp": timestamp,
            "tvl0": tvl0_direct,
            "tvl1": tvl1_direct,
            "tvlUSD": tvlUSD,
            "tvl0_fromdex": tvl0_indirect,
            "tvl1_fromdex": tvl1_indirect,
            "tvlUSD_fromdex": tvlUSD,
            "tvl0": tvl0_indirect,
            "tvl1": tvl1_indirect,
            "tvlUSD": tvlUSD,
            "deployed_token0": "",
            "deployed_token1": "",
            "fees_owed_token0": "",
            "fees_owed_token1": "",
            "parked_token0": "",
            "parked_token1": "",
            "uncollected_fees0": fees_uncollected["qtty_token0"],
            "uncollected_fees1": fees_uncollected["qtty_token1"],
            "price0": price0,
            "price1": price1,
            "elapsedTime": 0,
            "subgraph_errors": error,
        }

    # progress
    with tqdm.tqdm(total=len(hypervisor_ids) * len(blocks)) as progress_bar:

        if not threaded:
            # not threaded  (slow )
            for hypervisor_id in hypervisor_ids:

                for block in blocks:

                    progress_bar.set_description(
                        "processed {}'s {}".format(network, hypervisor_id)
                    )

                    # add data to result
                    result.append(
                        do_loop_work(hypervisor_id, block, price_helper, network)
                    )

                    # update progress
                    progress_bar.update(1)
        else:
            # threaded
            args = (
                (hypervisor_id, block, price_helper, network)
                for hypervisor_id in hypervisor_ids
                for block in blocks
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
                for result_item in ex.map(lambda p: do_loop_work(*p), args):
                    # progress
                    progress_bar.set_description(
                        "processed {}'s {}".format(network, result_item["name"])
                    )

                    # add data to result
                    result.append(result_item)

                    # update progress
                    progress_bar.update(1)

    # resuklt
    return result


# START ####################################################################################################################
if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)

    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(
        " Start {}   ----------------------> ".format(__module_name)
    )
    # start time log
    _startime = dt.datetime.utcnow()

    # test_uncollected_fees_comparison_api_onchain(threaded=True )

    # test_uncollected_fees_comparison_api_thegraph(threaded=True )

    # test_uncollected_fees_comparison_formulas_thegraph(threaded=True)

    # test_uncollected_fees_comparison_formulas_onchain( threaded=True)

    # test_uncollected_fees_
    test_name = "onchain"  # << --  onchain or thegraph
    protocol = "gamma"
    dex = "uniswapv3"
    network = "ethereum"
    csv_filename = "{}_test_uncollected_fees_{}.csv".format(network, test_name)
    csv_filename = os.path.join(CURRENT_FOLDER, csv_filename)  # save to test folder
    # list of hypervisors
    hypervisor_ids = ["0x35abccd8e577607275647edab08c537fa32cc65e"]
    # list of blocks to analyze
    blocks = [
        16388680,
        16718556,
        16697059,
        16720218,
    ]
    if test_name == "thegraph":
        result = test_uncollected_fees_thegraph(
            protocol=protocol,
            network=network,
            dex=dex,
            hypervisor_ids=hypervisor_ids,
            blocks=blocks,
            threaded=True,
            max_workers=5,
        )
    elif test_name == "onchain":
        result = test_uncollected_fees_onchain(
            protocol=protocol,
            network=network,
            dex=dex,
            web3Provider_url=CONFIGURATION["sources"]["web3Providers"][network],
            hypervisor_ids=hypervisor_ids,
            blocks=blocks,
            threaded=True,
            max_workers=5,
        )

    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass
    file_utilities.SaveCSV(filename=csv_filename, columns=result[0].keys(), rows=result)

    # end time log
    _timelapse = dt.datetime.utcnow() - _startime
    logging.getLogger(__name__).info(
        " took {:,.2f} seconds to complete".format(_timelapse.total_seconds())
    )
    logging.getLogger(__name__).info(
        " Exit {}    <----------------------".format(__module_name)
    )
