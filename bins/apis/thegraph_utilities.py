import sys

import datetime as dt
import logging
import time

from ..general import net_utilities
from ..cache import cache_utilities


from ratelimit import limits, sleep_and_retry


@sleep_and_retry
@limits(calls=1, period=1)
def place_rate_limited_query(
    url: str,
    query: dict,
    retry: int = 0,
    max_retry: int = 2,
    wait_secs: int = 5,
    timeout_secs: int = 3,
):
    return net_utilities.post_request(
        url=url,
        query=query,
        retry=retry,
        max_retry=max_retry,
        wait_secs=wait_secs,
        timeout_secs=timeout_secs,
    )


## GLOBAL ##
class thegraph_scraper_helper:
    def __init__(
        self,
        cache: bool = True,
        cache_savePath: str = "",
        convert=True,
        timeout_secs: int = 3,
    ):
        """

        Args:
            cache (bool, optional): use cache?. Defaults to True.
            cache_savePath (str, optional): path dir to save cache file to. Defaults to "".
            convert (bool, optional): should data be converted after queried ? . Defaults to True.
        """

        self.init_URLS()

        self._CONVERT = convert

        self._CACHE = None
        if cache:
            # setup file cache ( file cache only saves queries including block)
            self._CACHE = cache_utilities.standard_thegraph_cache(
                filename=self.__class__.__name__, folder_name=cache_savePath
            )

        self.timeout_secs = timeout_secs

    def init_URLS(self):
        self._URLS = {}

    def get_all_results(self, network: str, query_name: str, **kwargs) -> list:
        """
        network:str = "ethereum"
        query_name:str = "uniswapV3Hypervisors" or "accounts"

        kwargs=
            where:str = " id : '0x0000000000' "
            orderby:str= "timestamp"
            orderDirection:str= "asc" or "desc"
            block:str = "number: { 15432282 } "

        """
        result = None
        _skip = kwargs.get("skip", 0)
        _url = self._url_constructor(network, query_name)
        _filter = self._filter_constructor(**kwargs)

        # check cache, if enabled
        if self._CACHE is not None:
            result = self._CACHE.get_data(
                network=network, query_name=query_name, **kwargs
            )

        if result is None:
            # get  data from thegraph
            result = []

            # loop till no more results are retrieved
            while True:
                try:
                    _query, path_to_data = self._query_constructor(
                        skip=_skip, name=query_name, filter=_filter
                    )
                    _data = place_rate_limited_query(
                        url=_url,
                        query=_query,
                        retry=0,
                        max_retry=2,
                        wait_secs=5,
                        timeout_secs=self.timeout_secs,
                    )

                    # add it to result
                    try:
                        _data = _data[path_to_data[0]]

                        for i in range(1, len(path_to_data)):
                            _data = _data[path_to_data[i]]
                    except KeyError as err:
                        if (
                            "errors" in err.args
                            and "message" in err.args["errors"]
                            and (
                                err.args["errors"]["message"]
                                .lower()
                                .contains("database unavailable")
                            )
                        ):
                            # connection error: wait and loop again
                            logging.getLogger(__name__).error(
                                f" Seems like subgraph isnt available temporarily. Retrying in 5sec.  .error: {sys.exc_info()[0]}"
                            )

                            time.sleep(5)
                            continue

                        logging.getLogger(__name__).error(
                            f" Unexpected error retrieving data path  query name:{query_name}   data:{_data}       .error: {sys.exc_info()[0]}"
                        )

                    except Exception:
                        logging.getLogger(__name__).error(
                            f" Unexpected error retrieving data path  query name:{query_name}   data:{_data}       .error: {sys.exc_info()[0]}"
                        )

                    if not _data:
                        # exit loop
                        break

                    # modify pagination var
                    _skip += len(_data)
                    # add to result
                    result.extend(_data)
                    # check if we are done
                    if len(_data) < 1000:
                        # qtty is less than window ("first" var at query)
                        break  # exit loop
                except Exception:
                    logging.getLogger(__name__).exception(
                        f"Unexpected error while retrieving query {query_name}      .error: {sys.exc_info()[0]}"
                    )

                    break  # exit loop

            # return empty list if result contains error
            if "errors" in result:
                logging.getLogger(__name__).warning(
                    f"Errors found in thegraph query result--> network:{network}  query:{query_name}  result:{sys.exc_info()[0]}   . Empty list is returned. "
                )

                return []

            # save it to cache, if enabled
            if (
                self._CACHE is not None
                and not self._CACHE.add_data(
                    data=result, network=network, query_name=query_name, **kwargs
                )
                and "block" in kwargs
            ):
                # not saved to cache
                logging.getLogger(__name__).warning(
                    f"Could not save thegraph data to cache ->  network:{network} query:{query_name} "
                )

        # convert result
        if self._CONVERT:
            for itm in result:
                self._converter(itm, query_name, network)

        # return
        return result

    @property
    def networks(self) -> list[str]:
        """available networks

        Returns:
            list: of networks
        """
        return list(self._URLS.keys())

    # HELPERS
    def _query_constructor(self, skip: int, name: str, filter: str) -> tuple:
        # query name
        return "", []  # return query, list of keys to follow to find root_path to data

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to ... as defined

        Args:
           itm (dict): data item to convert
           query_name (str): what to convert
           network (str):
        """
        pass

    def _filter_constructor(self, **kwargs) -> str:
        """build thegraph filter

        kwargs=
            where :  ' id='0x0000000000' '
            orderby :  ' timestamp '
            orderDirection : ' asc'
            block : number: 12932633
        """

        _filter = "first: 1000"  # max 1 time query
        if "where" in kwargs and kwargs["where"] != "":
            # add comma to where it should be
            _filter += ", where: {{ {} }}".format(kwargs["where"])
        if "orderby" in kwargs and kwargs["orderby"] != "":
            # add comma to where it should be
            _filter += f', orderBy: {kwargs["orderby"]} '
        if "orderDirection" in kwargs and kwargs["orderDirection"] != "":
            _filter += f', orderDirection: {kwargs["orderDirection"]}'
        if "block" in kwargs and kwargs["block"] != "":
            _filter += ", block: {{ {} }}".format(kwargs["block"])
        return _filter

    def _url_constructor(self, network, query_name: str = ""):
        return self._URLS[network]


## SPECIFIC ##
class gamma_scraper(thegraph_scraper_helper):
    def init_URLS(self):
        self._URLS = {
            "ethereum": "https://api.thegraph.com/subgraphs/name/gammastrategies/gamma",
            "polygon": "https://api.thegraph.com/subgraphs/name/gammastrategies/polygon",
            "optimism": "https://api.thegraph.com/subgraphs/name/gammastrategies/optimism",
            "arbitrum": "https://api.thegraph.com/subgraphs/name/gammastrategies/arbitrum",
            "celo": "https://api.thegraph.com/subgraphs/name/gammastrategies/celo",
            "binance": "https://api.thegraph.com/subgraphs/name/gammastrategies/uniswap-bsc",
        }
        self._URLS_messari = {
            "ethereum": "https://api.thegraph.com/subgraphs/name/messari/gamma-ethereum",
            "polygon": "https://api.thegraph.com/subgraphs/name/messari/gamma-polygon",
            "optimism": "",
        }
        self._URLS_quickswap = {
            "polygon": "https://api.thegraph.com/subgraphs/name/gammastrategies/algebra-polygon",
        }
        # TODO: implement zyberswap and Thena
        self._URLS_zyberswap = {
            "arbitrum": "https://api.thegraph.com/subgraphs/name/gammastrategies/zyberswap-arbitrum",
        }
        self._URLS_thena = {
            "binance": "https://api.thegraph.com/subgraphs/name/gammastrategies/thena",
        }

    def _query_constructor(self, skip: int, name: str, filter: str) -> tuple:
        """Create query

        Args:
           name (str): query function name at thegraph
           skip (int): pagination var
           filter (str): filter composed with filter func
        """
        name = name.split("_")[0]

        # try:
        if name == "uniswapV3Hypervisors":
            return """
                {{
                uniswapV3Hypervisors({}, skip: {}) {{
                    accountCount
                    created
                    id
                    symbol
                    lastUpdated
                    totalSupply
                    tvl0
                    tvl1
                    tvlUSD
                    feesReinvested0
                    feesReinvested1
                    feesReinvestedUSD
                    grossFeesClaimed0
                    grossFeesClaimed1
                    grossFeesClaimedUSD
                    protocolFeesCollected0
                    protocolFeesCollected1
                    protocolFeesCollectedUSD
                    pricePerShare
                    baseFeeGrowthInside0LastRebalanceX128
                    baseFeeGrowthInside0LastX128
                    baseFeeGrowthInside1LastRebalanceX128
                    baseFeeGrowthInside1LastX128
                    baseLiquidity
                    baseUpper
                    baseLower
                    limitFeeGrowthInside0LastX128
                    limitFeeGrowthInside0LastRebalanceX128
                    limitFeeGrowthInside1LastRebalanceX128
                    limitFeeGrowthInside1LastX128
                    limitLiquidity
                    limitUpper
                    limitLower
                    baseTokensOwed0
                    baseTokensOwed1
                    limitTokensOwed0
                    limitTokensOwed1
                    pool {{
                        fee
                        id
                        lastHypervisorRefreshTime
                        token0 {{
                            id
                            decimals
                            name
                            symbol
                        }}
                        token1 {{
                            symbol
                            name
                            id
                            decimals
                        }}
                        }}
                    }}
                    }}
                """.format(
                filter, skip
            ), [
                "data",
                "uniswapV3Hypervisors",
            ]

        elif name == "uniswapV3Deposits":
            return """
                    {{
                uniswapV3Deposits({}, skip: {}) {{
                    id
                    timestamp
                    block
                    amount0
                    amount1
                    amountUSD
                    shares
                    sender
                    to
                    hypervisor {{
                        id
                        symbol
                        tick
                        feesReinvested0
                        feesReinvested1
                        feesReinvestedUSD
                        grossFeesClaimed0
                        grossFeesClaimed1
                        grossFeesClaimedUSD
                        protocolFeesCollected0
                        protocolFeesCollected1
                        protocolFeesCollectedUSD
                        lastUpdated
                        created
                        pricePerShare
                        totalSupply
                        tvl0
                        tvl1
                        tvlUSD
                        pool {{
                            fee
                            id
                            token0 {{
                                id
                                name
                                symbol
                                decimals
                            }}
                            token1 {{
                                decimals
                                id
                                name
                                symbol
                            }}
                            lastHypervisorRefreshTime
                            lastSwapTime
                        }}
                        conversion {{
                                priceBaseInUSD
                                priceTokenInBase
                                baseTokenIndex
                                baseToken {{
                                    id
                                    name
                                    symbol
                                    decimals
                                }}
                        }}
                        baseFeeGrowthInside0LastRebalanceX128
                        baseFeeGrowthInside0LastX128
                        baseFeeGrowthInside1LastRebalanceX128
                        baseFeeGrowthInside1LastX128
                        }}
                }}
                    }}
                """.format(
                filter, skip
            ), [
                "data",
                "uniswapV3Deposits",
            ]

        elif name == "uniswapV3Withdraws":
            return """
                    {{
                uniswapV3Withdraws({}, skip: {}) {{
                    id
                    timestamp
                    block
                    amount0
                    amount1
                    amountUSD
                    shares
                    sender
                    to
                    hypervisor {{
                        id
                        symbol
                        tick
                        feesReinvested0
                        feesReinvested1
                        feesReinvestedUSD
                        grossFeesClaimed0
                        grossFeesClaimed1
                        grossFeesClaimedUSD
                        protocolFeesCollected0
                        protocolFeesCollected1
                        protocolFeesCollectedUSD
                        lastUpdated
                        created
                        pricePerShare
                        totalSupply
                        tvl0
                        tvl1
                        tvlUSD                    
                        pool {{
                            fee
                            id
                            token0 {{
                            id
                            name
                            symbol
                            decimals
                            }}
                            token1 {{
                            decimals
                            id
                            name
                            symbol
                            }}
                            lastHypervisorRefreshTime
                            lastSwapTime
                        }}
                        conversion {{
                                priceBaseInUSD
                                priceTokenInBase
                                baseTokenIndex
                                baseToken {{
                                    id
                                    name
                                    symbol
                                    decimals
                                }}
                        }}
                        baseFeeGrowthInside0LastRebalanceX128
                        baseFeeGrowthInside0LastX128
                        baseFeeGrowthInside1LastRebalanceX128
                        baseFeeGrowthInside1LastX128
                        }}
                }}
                    }}
                """.format(
                filter, skip
            ), [
                "data",
                "uniswapV3Withdraws",
            ]

        elif name == "uniswapV3Rebalances":
            return """{{
                    uniswapV3Rebalances({}, skip: {}) {{
                        id
                        timestamp
                        block
                        tick
                        totalSupply
                        totalAmount0
                        totalAmount1
                        totalAmountUSD
                        grossFees0
                        grossFees1
                        grossFeesUSD
                        protocolFees0
                        protocolFees1
                        protocolFeesUSD
                        netFees0
                        netFees1
                        netFeesUSD
                        hypervisor {{
                            id
                            pool {{
                                token0 {{
                                    id
                                    decimals
                                    name
                                    symbol
                                }}
                                token1 {{
                                    id
                                    decimals
                                    name
                                    symbol
                                }}
                                id
                                fee
                            }}
                            conversion {{
                                priceBaseInUSD
                                priceTokenInBase
                                baseTokenIndex
                                baseToken {{
                                    id
                                    name
                                    symbol
                                    decimals
                                }}
                            }}
                        }}
                    }}
                    }}
                """.format(
                filter, skip
            ), [
                "data",
                "uniswapV3Rebalances",
            ]

        elif name == "accounts":
            return """
            {{  accounts({}, skip: {}) {{
                gammaDeposited
                gammaEarnedRealized
                id
                type
                hypervisorShares {{
                    id
                    initialToken0
                    initialToken1
                    initialUSD
                    shares
                    hypervisor {{
                        id
                        symbol
                        tick
                        tvl0
                        tvl1
                        tvlUSD
                        totalSupply
                        created
                        feesReinvested0
                        feesReinvested1
                        feesReinvestedUSD
                        grossFeesClaimed0
                        grossFeesClaimed1
                        grossFeesClaimedUSD
                        lastUpdated
                        baseTokensOwed0
                        baseTokensOwed1
                        limitTokensOwed0
                        limitTokensOwed1
                        baseFeeGrowthInside0LastRebalanceX128
                        baseFeeGrowthInside0LastX128
                        baseFeeGrowthInside1LastRebalanceX128
                        baseFeeGrowthInside1LastX128
                        baseLiquidity
                        limitFeeGrowthInside0LastX128
                        limitFeeGrowthInside0LastRebalanceX128
                        limitFeeGrowthInside1LastRebalanceX128
                        limitFeeGrowthInside1LastX128
                        limitLiquidity
                        pool {{
                            id
                            fee
                            lastSwapTime
                            token0 {{
                                id
                                name
                                symbol
                                decimals
                            }}
                            token1 {{
                                id
                                name
                                symbol
                                decimals
                            }}
                        }}
                        conversion {{
                            priceBaseInUSD
                            priceTokenInBase
                            baseTokenIndex
                            baseToken {{
                                id
                                name
                                symbol
                                decimals
                            }}
                        }}
                    }}
                }}
                masterChefPoolAccounts {{
                    amount
                    id
                    masterChefPool {{
                        allocPoint
                        lastRewardBlock
                        poolId
                        totalStaked
                        id
                        stakeToken {{
                            id
                            decimals
                            name
                            symbol
                        }}
                        masterChef {{
                            rewardPerBlock
                            id
                            totalAllocPoint
                        }}
                    }}
                }}
                rewardHypervisorShares {{
                    shares
                    id
                    rewardHypervisor {{
                        totalGamma
                        totalSupply
                        id
                    }}
                }}
                }}
            }}                    
                """.format(
                filter, skip
            ), [
                "data",
                "accounts",
            ]

        elif name == "tokens":
            return """{{ tokens({}, skip: {}) {{
                    id 
                    decimals 
                    name 
                    symbol
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "tokens",
            ]

        elif name == "uniswapV3Pools":
            return """{{ uniswapV3Pools({}, skip: {}) {{
                    id 
                    hypervisors
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "uniswapV3Pools",
            ]

        elif name == "uniswapV3HypervisorConversions":
            return """ {{
                    uniswapV3HypervisorConversions({}, skip: {}) {{
                        priceBaseInUSD
                        usdPath
                        usdPathIndex
                        priceTokenInBase
                        id
                        baseTokenIndex
                        baseToken {{
                            decimals
                            id
                            name
                            symbol
                            }}
                        hypervisor {{
                            id
                            }}
                }}  }}
                """.format(
                filter, skip
            ), [
                "data",
                "uniswapV3HypervisorConversions",
            ]

        elif name == "simple_uniswapV3Hypervisors":
            return """
                {{
                uniswapV3Hypervisors({}, skip: {}) {{
                    id
                }}
                }}
                """.format(
                filter, skip
            ), [
                "data",
                "uniswapV3Hypervisors",
            ]

        else:
            logging.getLogger(__name__).error(f"No query found with name {name}")
            raise NotImplementedError(f"No gamma query constructor found for: {name} ")
        # except Exception:
        #    logging.getLogger(__name__).exception("Unexpected error while constructing query  .error: {}".format(sys.exc_info()[0]))

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to int or float or date ...

        Args:
           itm (dict): data item to convert
           name (str): what to convert
           network
        """
        # quickswap or univ3
        query_name = query_name.split("_")[0]

        if query_name == "uniswapV3Hypervisors":
            # prepare vars
            c0 = 10 ** itm["pool"]["token0"]["decimals"]
            c1 = 10 ** itm["pool"]["token1"]["decimals"]

            # convert objects
            itm["accountCount"] = int(itm["accountCount"])
            itm["totalSupply"] = int(itm["totalSupply"]) / (10**18)
            itm["created"] = dt.datetime.fromtimestamp(int(itm["created"]))
            itm["lastUpdated"] = dt.datetime.fromtimestamp(int(itm["lastUpdated"]))
            itm["feesReinvested0"] = int(itm["feesReinvested0"]) / c0
            itm["feesReinvested1"] = int(itm["feesReinvested1"]) / c1
            itm["feesReinvestedUSD"] = float(itm["feesReinvestedUSD"])
            itm["grossFeesClaimed0"] = int(itm["grossFeesClaimed0"]) / c0
            itm["grossFeesClaimed1"] = int(itm["grossFeesClaimed1"]) / c1
            itm["grossFeesClaimedUSD"] = float(itm["grossFeesClaimedUSD"])
            itm["protocolFeesCollected0"] = int(itm["protocolFeesCollected0"]) / c0
            itm["protocolFeesCollected1"] = int(itm["protocolFeesCollected1"]) / c1
            itm["protocolFeesCollectedUSD"] = float(itm["protocolFeesCollectedUSD"])
            itm["pricePerShare"] = float(itm["pricePerShare"])

            itm["tvl0"] = int(itm["tvl0"]) / c0
            itm["tvl1"] = int(itm["tvl1"]) / c1
            itm["tvlUSD"] = float(itm["tvlUSD"])
            itm["baseFeeGrowthInside0LastRebalanceX128"] = int(
                itm["baseFeeGrowthInside0LastRebalanceX128"]
            )
            itm["baseFeeGrowthInside0LastX128"] = int(
                itm["baseFeeGrowthInside0LastX128"]
            )
            itm["baseFeeGrowthInside1LastRebalanceX128"] = int(
                itm["baseFeeGrowthInside1LastRebalanceX128"]
            )
            itm["baseFeeGrowthInside1LastX128"] = int(
                itm["baseFeeGrowthInside1LastX128"]
            )
            itm["baseLiquidity"] = int(itm["baseLiquidity"])
            itm["limitFeeGrowthInside0LastX128"] = int(
                itm["limitFeeGrowthInside0LastX128"]
            )
            itm["limitFeeGrowthInside0LastRebalanceX128"] = int(
                itm["limitFeeGrowthInside0LastRebalanceX128"]
            )
            itm["limitFeeGrowthInside1LastRebalanceX128"] = int(
                itm["limitFeeGrowthInside1LastRebalanceX128"]
            )
            itm["limitFeeGrowthInside1LastX128"] = int(
                itm["limitFeeGrowthInside1LastX128"]
            )
            itm["limitLiquidity"] = int(itm["limitLiquidity"])
            itm["baseTokensOwed0"] = int(itm["baseTokensOwed0"]) / c0
            itm["baseTokensOwed1"] = int(itm["baseTokensOwed1"]) / c1
            itm["limitTokensOwed0"] = int(itm["limitTokensOwed0"]) / c0
            itm["limitTokensOwed1"] = int(itm["limitTokensOwed1"]) / c1
        elif query_name == "uniswapV3Deposits" or query_name == "uniswapV3Withdraws":
            # prepare vars
            c0 = 10 ** itm["hypervisor"]["pool"]["token0"]["decimals"]
            c1 = 10 ** itm["hypervisor"]["pool"]["token1"]["decimals"]

            # convert objects
            itm["timestamp"] = dt.datetime.fromtimestamp(int(itm["timestamp"]))
            itm["block"] = int(itm["block"])
            itm["amount0"] = float(int(itm["amount0"]) / c0)
            itm["amount1"] = float(int(itm["amount1"]) / c1)
            itm["amountUSD"] = float(itm["amountUSD"])
            itm["shares"] = int(itm["shares"]) / (10**18)
            itm["hypervisor"]["feesReinvested0"] = (
                int(itm["hypervisor"]["feesReinvested0"]) / c0
            )
            itm["hypervisor"]["feesReinvested1"] = (
                int(itm["hypervisor"]["feesReinvested1"]) / c1
            )
            itm["hypervisor"]["feesReinvestedUSD"] = float(
                itm["hypervisor"]["feesReinvestedUSD"]
            )
            itm["hypervisor"]["grossFeesClaimed0"] = (
                int(itm["hypervisor"]["grossFeesClaimed0"]) / c0
            )
            itm["hypervisor"]["grossFeesClaimed1"] = (
                int(itm["hypervisor"]["grossFeesClaimed1"]) / c1
            )
            itm["hypervisor"]["grossFeesClaimedUSD"] = float(
                itm["hypervisor"]["grossFeesClaimedUSD"]
            )
            itm["hypervisor"]["protocolFeesCollected0"] = (
                int(itm["hypervisor"]["protocolFeesCollected0"]) / c0
            )
            itm["hypervisor"]["protocolFeesCollected1"] = (
                int(itm["hypervisor"]["protocolFeesCollected1"]) / c1
            )
            itm["hypervisor"]["protocolFeesCollectedUSD"] = float(
                itm["hypervisor"]["protocolFeesCollectedUSD"]
            )
            itm["hypervisor"]["lastUpdated"] = dt.datetime.fromtimestamp(
                int(itm["hypervisor"]["lastUpdated"])
            )
            itm["hypervisor"]["created"] = dt.datetime.fromtimestamp(
                int(itm["hypervisor"]["created"])
            )
            itm["hypervisor"]["pricePerShare"] = float(
                itm["hypervisor"]["pricePerShare"]
            )
            itm["hypervisor"]["totalSupply"] = int(itm["hypervisor"]["totalSupply"]) / (
                10**18
            )
            itm["hypervisor"]["tvl0"] = float(itm["hypervisor"]["tvl0"])
            itm["hypervisor"]["tvl1"] = float(itm["hypervisor"]["tvl1"])
            itm["hypervisor"]["tvlUSD"] = float(itm["hypervisor"]["tvlUSD"])
            itm["hypervisor"]["baseFeeGrowthInside0LastRebalanceX128"] = int(
                itm["hypervisor"]["baseFeeGrowthInside0LastRebalanceX128"]
            )
            itm["hypervisor"]["baseFeeGrowthInside0LastX128"] = int(
                itm["hypervisor"]["baseFeeGrowthInside0LastX128"]
            )
            itm["hypervisor"]["baseFeeGrowthInside1LastRebalanceX128"] = int(
                itm["hypervisor"]["baseFeeGrowthInside1LastRebalanceX128"]
            )
            itm["hypervisor"]["baseFeeGrowthInside1LastX128"] = int(
                itm["hypervisor"]["baseFeeGrowthInside1LastX128"]
            )

            itm["hypervisor"]["conversion"]["priceBaseInUSD"] = float(
                itm["hypervisor"]["conversion"]["priceBaseInUSD"]
            )
            itm["hypervisor"]["conversion"]["priceTokenInBase"] = float(
                itm["hypervisor"]["conversion"]["priceTokenInBase"]
            )
        elif query_name == "uniswapV3Rebalances":
            # prepare vars
            c0 = 10 ** itm["hypervisor"]["pool"]["token0"]["decimals"]
            c1 = 10 ** itm["hypervisor"]["pool"]["token1"]["decimals"]

            # convert objects
            itm["timestamp"] = dt.datetime.fromtimestamp(int(itm["timestamp"]))
            itm["block"] = int(itm["block"])
            itm["totalSupply"] = int(itm["totalSupply"]) / (10**18)
            itm["totalAmount0"] = float(int(itm["totalAmount0"]) / c0)
            itm["totalAmount1"] = float(int(itm["totalAmount1"]) / c1)
            itm["totalAmountUSD"] = float(itm["totalAmountUSD"])
            itm["grossFees0"] = float(int(itm["grossFees0"]) / c0)
            itm["grossFees1"] = float(int(itm["grossFees1"]) / c1)
            itm["grossFeesUSD"] = float(itm["grossFeesUSD"])
            itm["protocolFees0"] = float(int(itm["protocolFees0"]) / c0)
            itm["protocolFees1"] = float(int(itm["protocolFees1"]) / c1)
            itm["protocolFeesUSD"] = float(itm["protocolFeesUSD"])
            itm["netFees0"] = float(int(itm["netFees0"]) / c0)
            itm["netFees1"] = float(int(itm["netFees1"]) / c1)
            itm["netFeesUSD"] = float(itm["netFeesUSD"])

            itm["hypervisor"]["conversion"]["priceBaseInUSD"] = float(
                itm["hypervisor"]["conversion"]["priceBaseInUSD"]
            )
            itm["hypervisor"]["conversion"]["priceTokenInBase"] = float(
                itm["hypervisor"]["conversion"]["priceTokenInBase"]
            )
        elif query_name == "accounts":
            # convert vars
            try:
                itm["gammaDeposited"] = float(int(itm["gammaDeposited"]) / (10**18))
                itm["gammaEarnedRealized"] = float(
                    int(itm["gammaEarnedRealized"]) / (10**18)
                )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Unexpected error while converting  accounts gamma info for item  {}        .error: {}".format(
                        itm["id"], sys.exc_info()[0]
                    )
                )

            for hyp_share in itm["hypervisorShares"]:
                try:
                    # prepare vars
                    c0 = 10 ** hyp_share["hypervisor"]["pool"]["token0"]["decimals"]
                    c1 = 10 ** hyp_share["hypervisor"]["pool"]["token1"]["decimals"]
                    # convert vars
                    hyp_share["initialToken0"] = int(hyp_share["initialToken0"]) / c0
                    hyp_share["initialToken1"] = int(hyp_share["initialToken1"]) / c1
                    hyp_share["initialUSD"] = float(hyp_share["initialUSD"])
                    hyp_share["shares"] = int(hyp_share["shares"]) / (10**18)
                    hyp_share["hypervisor"]["totalSupply"] = int(
                        hyp_share["hypervisor"]["totalSupply"]
                    ) / (10**18)
                    hyp_share["hypervisor"]["created"] = dt.datetime.fromtimestamp(
                        int(hyp_share["hypervisor"]["created"])
                    )
                    hyp_share["hypervisor"]["lastUpdated"] = dt.datetime.fromtimestamp(
                        int(hyp_share["hypervisor"]["lastUpdated"])
                    )

                    hyp_share["hypervisor"]["tvl0"] = (
                        int(hyp_share["hypervisor"]["tvl0"]) / c0
                    )
                    hyp_share["hypervisor"]["tvl1"] = (
                        int(hyp_share["hypervisor"]["tvl1"]) / c1
                    )
                    hyp_share["hypervisor"]["tvlUSD"] = float(
                        hyp_share["hypervisor"]["tvlUSD"]
                    )

                    hyp_share["hypervisor"]["feesReinvested0"] = (
                        int(hyp_share["hypervisor"]["feesReinvested0"]) / c0
                    )
                    hyp_share["hypervisor"]["feesReinvested1"] = (
                        int(hyp_share["hypervisor"]["feesReinvested1"]) / c1
                    )
                    hyp_share["hypervisor"]["feesReinvestedUSD"] = float(
                        hyp_share["hypervisor"]["feesReinvestedUSD"]
                    )

                    hyp_share["hypervisor"]["grossFeesClaimed0"] = (
                        int(hyp_share["hypervisor"]["grossFeesClaimed0"]) / c0
                    )
                    hyp_share["hypervisor"]["grossFeesClaimed1"] = (
                        int(hyp_share["hypervisor"]["grossFeesClaimed1"]) / c1
                    )
                    hyp_share["hypervisor"]["grossFeesClaimedUSD"] = float(
                        hyp_share["hypervisor"]["grossFeesClaimedUSD"]
                    )

                    hyp_share["hypervisor"]["conversion"]["priceBaseInUSD"] = float(
                        hyp_share["hypervisor"]["conversion"]["priceBaseInUSD"]
                    )
                    hyp_share["hypervisor"]["conversion"]["priceTokenInBase"] = float(
                        hyp_share["hypervisor"]["conversion"]["priceTokenInBase"]
                    )

                    hyp_share["hypervisor"]["baseTokensOwed0"] = (
                        int(hyp_share["hypervisor"]["baseTokensOwed0"]) / c0
                    )
                    hyp_share["hypervisor"]["baseTokensOwed1"] = (
                        int(hyp_share["hypervisor"]["baseTokensOwed1"]) / c1
                    )
                    hyp_share["hypervisor"]["limitTokensOwed0"] = (
                        int(hyp_share["hypervisor"]["limitTokensOwed0"]) / c0
                    )
                    hyp_share["hypervisor"]["limitTokensOwed1"] = (
                        int(hyp_share["hypervisor"]["limitTokensOwed1"]) / c1
                    )

                    hyp_share["hypervisor"][
                        "baseFeeGrowthInside0LastRebalanceX128"
                    ] = int(
                        hyp_share["hypervisor"]["baseFeeGrowthInside0LastRebalanceX128"]
                    )
                    hyp_share["hypervisor"]["baseFeeGrowthInside0LastX128"] = int(
                        hyp_share["hypervisor"]["baseFeeGrowthInside0LastX128"]
                    )
                    hyp_share["hypervisor"][
                        "baseFeeGrowthInside1LastRebalanceX128"
                    ] = int(
                        hyp_share["hypervisor"]["baseFeeGrowthInside1LastRebalanceX128"]
                    )
                    hyp_share["hypervisor"]["baseFeeGrowthInside1LastX128"] = int(
                        hyp_share["hypervisor"]["baseFeeGrowthInside1LastX128"]
                    )
                    hyp_share["hypervisor"]["baseLiquidity"] = int(
                        hyp_share["hypervisor"]["baseLiquidity"]
                    )
                    hyp_share["hypervisor"]["limitFeeGrowthInside0LastX128"] = int(
                        hyp_share["hypervisor"]["limitFeeGrowthInside0LastX128"]
                    )
                    hyp_share["hypervisor"][
                        "limitFeeGrowthInside0LastRebalanceX128"
                    ] = int(
                        hyp_share["hypervisor"][
                            "limitFeeGrowthInside0LastRebalanceX128"
                        ]
                    )
                    hyp_share["hypervisor"][
                        "limitFeeGrowthInside1LastRebalanceX128"
                    ] = int(
                        hyp_share["hypervisor"][
                            "limitFeeGrowthInside1LastRebalanceX128"
                        ]
                    )
                    hyp_share["hypervisor"]["limitFeeGrowthInside1LastX128"] = int(
                        hyp_share["hypervisor"]["limitFeeGrowthInside1LastX128"]
                    )
                    hyp_share["hypervisor"]["limitLiquidity"] = int(
                        hyp_share["hypervisor"]["limitLiquidity"]
                    )

                except Exception:
                    logging.getLogger(__name__).exception(
                        "Unexpected error while converting  hypervisorShares of item  {}        .error: {}".format(
                            itm["id"], sys.exc_info()[0]
                        )
                    )

            for hyp_mast in itm["masterChefPoolAccounts"]:
                # prepare
                hyp_mast["masterChefPool"]["stakeToken"]["decimals"] = int(
                    hyp_mast["masterChefPool"]["stakeToken"]["decimals"]
                )
                c = 10 ** int(hyp_mast["masterChefPool"]["stakeToken"]["decimals"])

                hyp_mast["masterChefPool"]["allocPoint"] = int(
                    hyp_mast["masterChefPool"]["allocPoint"]
                )
                hyp_mast["masterChefPool"]["masterChef"]["totalAllocPoint"] = int(
                    hyp_mast["masterChefPool"]["masterChef"]["totalAllocPoint"]
                )
                hyp_mast["shares"] = int(hyp_mast["amount"]) / int(
                    hyp_mast["masterChefPool"]["totalStaked"]
                )
                hyp_mast["amount"] = int(hyp_mast["amount"]) / c
                hyp_mast["masterChefPool"]["totalStaked"] = (
                    int(hyp_mast["masterChefPool"]["totalStaked"]) / c
                )

            for hyp_re in itm["rewardHypervisorShares"]:
                hyp_re["rewardHypervisor"]["totalGamma"] = int(
                    hyp_re["rewardHypervisor"]["totalGamma"]
                )
                hyp_re["rewardHypervisor"]["totalSupply"] = int(
                    hyp_re["rewardHypervisor"]["totalSupply"]
                ) / (10**18)
                hyp_re["shares"] = int(hyp_re["shares"]) / (10**18)

        elif query_name == "uniswapV3Pools":
            pass
        elif query_name == "simple_uniswapV3Hypervisors":
            pass  # nothing to convert
        elif query_name == "tokens":
            # do nothing
            pass
        else:
            logging.getLogger(__name__).error(
                "No converter found with name {}".format(query_name)
            )
            raise NotImplementedError(
                "No gamma converter found for: {} ".format(query_name)
            )

        # return result
        return itm

    def _url_constructor(self, network, query_name: str = ""):
        if "uniswapv3" in query_name:
            return self._URLS[network]
        elif "quickswap" in query_name:
            return self._URLS_quickswap[network]
        elif "messari" in query_name:
            return self._URLS_messari[network]

        else:
            return self._URLS[network]


class arrakis_scraper(thegraph_scraper_helper):
    def init_URLS(self):
        self._URLS = {
            "ethereum": "https://api.thegraph.com/subgraphs/name/arrakisfinance/vault-v1-mainnet",
            "optimism": "https://api.thegraph.com/subgraphs/name/arrakisfinance/vault-v1-optimism",
        }
        self._URLS_gelato = {
            "ethereum": "https://api.thegraph.com/subgraphs/name/gelatodigital/g-uni",
        }
        self._URLS_messari = {
            "ethereum": "https://api.thegraph.com/subgraphs/name/messari/arrakis-finance-ethereum",
            "polygon": "https://api.thegraph.com/subgraphs/name/messari/arrakis-finance-polygon",
            "optimism": "https://api.thegraph.com/subgraphs/name/messari/arrakis-finance-optimism",
        }

    def _query_constructor(self, skip: int, name: str, filter: str):
        """Create query

        Args:
           name (str): query function name at thegraph
           skip (int): pagination var
           filter (str): filter composed with filter func
        """
        # official
        if name == "vaults":
            return """ {{
                vaults({}, skip:{}) {{
                    address
                    blockCreated
                    id
                    feeTier
                    feeInfo {{
                        burn0
                        burn1
                        checkpointTimestamp
                        collect0
                        collect1
                        feeCheckpoint0
                        feeCheckpoint1
                        id
                        }}
                    liquidity
                    manager
                    managerFee
                    name
                    numSnapshots
                    positionId
                    timestampCreated
                    totalSupply
                    uniswapPool
                    lowerTick
                    upperTick
                    token1 {{
                        symbol
                        name
                        id
                        decimals
                        address
                        }}
                    token0 {{
                        symbol
                        name
                        id
                        decimals
                        address
                        }}
                    snapshots(first: 1000, skip: 0, orderBy: endTimestamp, orderDirection: asc) {{
                        totalReserves
                        startTimestamp
                        totalFees
                        sqrtPriceX96
                        reserves1
                        reserves0
                        id
                        fees1
                        fees0
                        endTimestamp
                        apr
                        }}
                    reranges(first: 1000, skip: 0, orderBy: timestamp, orderDirection: asc) {{
                        id
                        lowerTick
                        timestamp
                        upperTick
                        }}
                }}
                }}
            """.format(
                filter, skip
            ), [
                "data",
                "vaults",
            ]
        elif name == "vaults_snapshots":
            return """ {{
                vaults({}, skip:0) {{
                    address
                    blockCreated
                    id
                    feeTier
                    feeInfo {{
                        burn0
                        burn1
                        checkpointTimestamp
                        collect0
                        collect1
                        feeCheckpoint0
                        feeCheckpoint1
                        id
                        }}
                    liquidity
                    manager
                    managerFee
                    name
                    numSnapshots
                    positionId
                    timestampCreated
                    totalSupply
                    uniswapPool
                    lowerTick
                    upperTick
                    token1 {{
                        symbol
                        name
                        id
                        decimals
                        address
                        }}
                    token0 {{
                        symbol
                        name
                        id
                        decimals
                        address
                        }}
                    snapshots(first: 1000, skip: {}, orderBy: endTimestamp, orderDirection: asc) {{
                        totalReserves
                        startTimestamp
                        totalFees
                        sqrtPriceX96
                        reserves1
                        reserves0
                        id
                        fees1
                        fees0
                        endTimestamp
                        apr
                        }}
                    reranges(first: 1, skip: 0, orderBy: timestamp, orderDirection: asc) {{
                        id
                        lowerTick
                        timestamp
                        upperTick
                        }}
                }}
                }}
            """.format(
                filter, skip
            ), [
                "data",
                "vaults",
                0,
                "snapshots",
            ]
        elif name == "vaults_reranges":
            return """ {{
                vaults({}, skip:0) {{
                    address
                    blockCreated
                    id
                    feeTier
                    feeInfo {{
                        burn0
                        burn1
                        checkpointTimestamp
                        collect0
                        collect1
                        feeCheckpoint0
                        feeCheckpoint1
                        id
                        }}
                    liquidity
                    manager
                    managerFee
                    name
                    numSnapshots
                    positionId
                    timestampCreated
                    totalSupply
                    uniswapPool
                    lowerTick
                    upperTick
                    token1 {{
                        symbol
                        name
                        id
                        decimals
                        address
                        }}
                    token0 {{
                        symbol
                        name
                        id
                        decimals
                        address
                        }}
                    snapshots(first: 1, skip: 0, orderBy: endTimestamp, orderDirection: asc) {{
                        totalReserves
                        startTimestamp
                        totalFees
                        sqrtPriceX96
                        reserves1
                        reserves0
                        id
                        fees1
                        fees0
                        endTimestamp
                        apr
                        }}
                    reranges(first: 1000, skip: {}, orderBy: timestamp, orderDirection: asc) {{
                        id
                        lowerTick
                        timestamp
                        upperTick
                        }}
                }}
                }}
            """.format(
                filter, skip
            ), [
                "data",
                "vaults",
                0,
                "reranges",
            ]
        elif name == "aprSnapshots":
            # only used to bruteforce Snapshot block num
            return """ {{
                aprSnapshots({}, skip:0) {{
                    id                 
                }}
                }}
            """.format(
                filter, skip
            ), [
                "data",
                "aprSnapshots",
            ]

        # gelato:
        elif name == "pools_gelato":
            return """{{
                        pools({}, skip:{}) {{
                            address
                            blockCreated
                            feeTier
                            id
                            lastTouchWithoutFees
                            liquidity
                            manager
                            managerFee
                            name
                            positionId
                            totalSupply
                            uniswapPool
                            latestInfo {{
                                block
                                id
                                leftover0
                                leftover1
                                reserves0
                                reserves1
                                sqrtPriceX96
                                unclaimedFees0
                                unclaimedFees1
                                }}
                            token0 {{
                                symbol
                                address
                                decimals
                                id
                                name
                                }}
                            token1 {{
                                address
                                decimals
                                id
                                name
                                symbol
                                }}
                            feeSnapshots(first: 1000, skip: 0, orderBy: block, orderDirection: asc) {{
                                id
                                feesEarned1
                                feesEarned0
                                block
                                }}
                            supplySnapshots(first: 1000, orderBy: block, orderDirection: asc, skip: 0) {{
                                block
                                id
                                reserves0
                                reserves1
                                sqrtPriceX96
                                }}
                        }} }}
            """.format(
                filter, skip
            ), [
                "data",
                "pools",
            ]
        elif name == "pools_gelato_supplysnapshots":
            return """{{
                        pools({}, skip:0) {{
                            id
                            token0 {{
                                symbol
                                address
                                decimals
                                id
                                name
                                }}
                            token1 {{
                                address
                                decimals
                                id
                                name
                                symbol
                                }}
                            supplySnapshots(first: 1000, orderBy: block, orderDirection: asc, skip: {}) {{
                                block
                                id
                                reserves0
                                reserves1
                                sqrtPriceX96
                                }}
                        }}
                        }}
            """.format(
                filter, skip
            ), [
                "data",
                "pools",
                0,
                "supplySnapshots",
            ]
        elif name == "pools_gelato_feesnapshots":
            return """{{
                        pools({}, skip:0) {{
                            id
                            token0 {{
                                symbol
                                address
                                decimals
                                id
                                name
                                }}
                            token1 {{
                                address
                                decimals
                                id
                                name
                                symbol
                                }}
                            feeSnapshots(first: 1000, orderBy: block, orderDirection: asc, skip: {}) {{
                                block
                                id
                                feesEarned0
                                feesEarned1
                                }}
                        }}
                        }}
             """.format(
                filter, skip
            ), [
                "data",
                "pools",
                0,
                "feeSnapshots",
            ]
        elif name == "totalsupply_gelato":
            return """{{
                    pools({}, skip:{}) {{
                        address
                        id
                        name
                        totalSupply
                        uniswapPool
                    }}
                    }}
            """.format(
                filter, skip
            )
        elif name == "univ3_gelato":
            return """{{
                        pools({}, skip:{}) {{
                            address
                            id
                            uniswapPool
                        }} }}
            """.format(
                filter, skip
            ), [
                "data",
                "pools",
            ]
        # messari:
        elif name == "vaults_messari":
            return """ {{
                vaults({}, skip:{}) {{
                    id
                    createdBlockNumber
                    createdTimestamp
                    name
                    symbol
                    totalValueLockedUSD
                    inputTokenBalance
                    inputToken {{
                        decimals
                        id
                        symbol
                        name
                        lastPriceBlockNumber
                        lastPriceUSD
                        }}
                    outputTokenSupply
                    outputToken {{
                        decimals
                        id
                        name
                        symbol
                        lastPriceBlockNumber
                        lastPriceUSD
                        }}
                    outputTokenPriceUSD
                    pricePerShare
                    stakedOutputTokenAmount
                    }}
                }}
                """.format(
                filter, skip
            ), [
                "data",
                "vaults",
            ]
        elif name == "underlyingTokens_messari":
            return """ {{
                underlyingTokens({}, skip:{}) {{
                    lastAmountBlockNumber
                    lastAmount1
                    lastAmount0
                    token1 {{
                        decimals
                        id
                        lastPriceBlockNumber
                        lastPriceUSD
                        name
                        symbol
                        }}
                    token0 {{
                        decimals
                        symbol
                        name
                        lastPriceUSD
                        lastPriceBlockNumber
                        id
                        }}
                    id
                }}
                }}
                """.format(
                filter, skip
            ), [
                "data",
                "underlyingTokens",
            ]
        elif name == "feesEarneds_messari":
            return """ {{
                feesEarneds({}, skip:{}) {{
                    blockNumber
                    fees0
                    fees1
                    feesUSD
                    from
                    hash
                    id
                    logIndex
                    timestamp
                    to
                    vault {{
                        id
                        }}
                }}
                }}
                """.format(
                filter, skip
            ), [
                "data",
                "feesEarneds",
            ]
        elif name == "withdraws_messari":
            return """ {{
                withdraws({}, skip:{}) {{
                    amount
                    blockNumber
                    from
                    hash
                    id
                    logIndex
                    to
                    timestamp
                    amountUSD
                    asset {{
                        decimals
                        lastPriceUSD
                        name
                        symbol
                        lastPriceBlockNumber
                        id
                        }}
                    vault {{
                        totalValueLockedUSD
                        id
                        inputTokenBalance
                        outputTokenSupply
                        outputTokenPriceUSD
                        name
                        }}
                }} }}
                """.format(
                filter, skip
            ), [
                "data",
                "withdraws",
            ]
        elif name == "deposits_messari":
            return """ {{
                deposits({}, skip:{}) {{
                    amount
                    blockNumber
                    from
                    hash
                    id
                    logIndex
                    to
                    timestamp
                    amountUSD
                    asset {{
                        decimals
                        lastPriceUSD
                        name
                        symbol
                        lastPriceBlockNumber
                        id
                        }}
                    vault {{
                        totalValueLockedUSD
                        id
                        inputTokenBalance
                        outputTokenSupply
                        outputTokenPriceUSD
                        name
                        }}
                }} }}
                """.format(
                filter, skip
            ), [
                "data",
                "deposits",
            ]
        elif name == "simple_vaults_messari":
            return """ {{
                vaults({}, skip:{}) {{
                    id
                }}
                }}
                """.format(
                filter, skip
            ), [
                "data",
                "vaults",
            ]
        elif name == "vaultHourlySnapshots_messari":
            return """ {{
                vaultHourlySnapshots({}, skip:{}) {{
                    blockNumber
                    cumulativeProtocolSideRevenueUSD
                    cumulativeSupplySideRevenueUSD
                    cumulativeTotalRevenueUSD
                    hourlyProtocolSideRevenueUSD
                    hourlySupplySideRevenueUSD
                    hourlyTotalRevenueUSD
                    inputTokenBalance
                    outputTokenPriceUSD
                    outputTokenSupply
                    pricePerShare
                    rewardTokenEmissionsAmount
                    stakedOutputTokenAmount
                    timestamp
                    totalValueLockedUSD
                    id
                    rewardTokenEmissionsUSD
                    vault {{
                        id
                        }}
                }} }}
                """.format(
                filter, skip
            ), [
                "data",
                "vaultHourlySnapshots",
            ]
        else:
            logging.getLogger(__name__).error(
                "No query found with name {}".format(name)
            )
            raise NotImplementedError(
                "No arrakis query constructor found for: {} ".format(name)
            )

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to int or float or date ...

        Args:
           itm (dict): data to convert
           query_name (str): what to convert
           network
        """

        # official
        if query_name == "vaults":
            itm["token0"]["decimals"] = int(itm["token0"]["decimals"])
            itm["token1"]["decimals"] = int(itm["token1"]["decimals"])

            # prepare vars
            c0 = 10 ** itm["token0"]["decimals"]
            c1 = 10 ** itm["token1"]["decimals"]

            itm["blockCreated"] = int(itm["blockCreated"])
            itm["feeTier"] = int(itm["feeTier"])

            itm["feeInfo"]["burn0"] = int(itm["feeInfo"]["burn0"]) / c0
            itm["feeInfo"]["burn1"] = int(itm["feeInfo"]["burn1"]) / c1
            itm["feeInfo"]["checkpointTimestamp"] = int(
                itm["feeInfo"]["checkpointTimestamp"]
            )
            itm["feeInfo"]["collect0"] = int(itm["feeInfo"]["collect0"]) / c0
            itm["feeInfo"]["collect1"] = int(itm["feeInfo"]["collect1"]) / c1
            itm["feeInfo"]["feeCheckpoint0"] = (
                int(itm["feeInfo"]["feeCheckpoint0"]) / c0
            )
            itm["feeInfo"]["feeCheckpoint1"] = (
                int(itm["feeInfo"]["feeCheckpoint1"]) / c1
            )
            itm["liquidity"] = int(itm["liquidity"])
            itm["managerFee"] = int(itm["managerFee"])
            itm["numSnapshots"] = int(itm["numSnapshots"])
            itm["timestampCreated"] = int(itm["timestampCreated"])
            itm["totalSupply"] = int(itm["totalSupply"]) / (10**18)

            # snapshots
            # convert current suplpy snaps list
            for snap in itm["snapshots"]:
                snap["totalReserves"] = int(snap["totalReserves"])
                snap["startTimestamp"] = int(snap["startTimestamp"])
                snap["totalFees"] = int(snap["totalFees"])
                snap["sqrtPriceX96"] = int(snap["sqrtPriceX96"])
                snap["reserves1"] = int(snap["reserves1"]) / c1
                snap["reserves0"] = int(snap["reserves0"]) / c0
                snap["fees1"] = int(snap["fees1"]) / c1
                snap["fees0"] = int(snap["fees0"]) / c0
                snap["endTimestamp"] = int(snap["endTimestamp"])
                snap["apr"] = float(snap["apr"])

            # check snapshots qtty:
            if len(itm["snapshots"]) == 1000:
                # get extra snapshots
                logging.getLogger(__name__).debug(
                    "getting extra snapshots for {}  vault id: {}  network:{}".format(
                        itm["name"], itm["id"], network
                    )
                )
                whr_snap = """ id: "{}" """.format(itm["id"])
                # get all snapshors and convert subitems here
                tmp_raw_snapshots = self.get_all_results(
                    network=network,
                    query_name=query_name + "_snapshots",
                    where=whr_snap,
                    skip=1000,
                )
                for snap in tmp_raw_snapshots:
                    snap["totalReserves"] = int(snap["totalReserves"])
                    snap["startTimestamp"] = int(snap["startTimestamp"])
                    snap["totalFees"] = int(snap["totalFees"])
                    snap["sqrtPriceX96"] = int(snap["sqrtPriceX96"])
                    snap["reserves1"] = int(snap["reserves1"]) / c1
                    snap["reserves0"] = int(snap["reserves0"]) / c0
                    snap["fees1"] = int(snap["fees1"]) / c1
                    snap["fees0"] = int(snap["fees0"]) / c0
                    snap["endTimestamp"] = int(snap["endTimestamp"])
                    snap["apr"] = float(snap["apr"])
                # itm["snapshots"] += self.get_all_results(network=network, query_name=query_name+"_snapshots", where=whr_snap, skip=1000)[0]["snapshots"]

            # Fee snapshots
            # convert current fee snaps list
            for snap in itm["reranges"]:
                snap["timestamp"] = int(snap["timestamp"])

            # check snapshots qtty:
            if len(itm["reranges"]) == 1000:
                # get extra snapshots
                logging.getLogger(__name__).debug(
                    "getting extra reranges for {}  vault id: {} network:{}".format(
                        itm["name"], itm["id"], network
                    )
                )
                whr_snap = """ id: "{}" """.format(itm["id"])
                itm["reranges"] += self.get_all_results(
                    network=network,
                    query_name=query_name + "_reranges",
                    where=whr_snap,
                    skip=1000,
                )  # [0]["reranges"]
        elif query_name == "vaults_snapshots":
            # can't convert token0 / 1 decimals here

            # itm["token0"]["decimals"] = int(itm["token0"]["decimals"])
            # itm["token1"]["decimals"] = int(itm["token1"]["decimals"])

            # # prepare vars
            # c0 = (10**itm["token0"]["decimals"])
            # c1 = (10**itm["token1"]["decimals"])

            # for snap in itm["snapshots"]:
            itm["totalReserves"] = int(itm["totalReserves"])
            itm["startTimestamp"] = int(itm["startTimestamp"])
            itm["totalFees"] = int(itm["totalFees"])
            itm["sqrtPriceX96"] = int(itm["sqrtPriceX96"])
            # snap["reserves1"] = int(snap["reserves1"])/c1
            # snap["reserves0"] = int(snap["reserves0"])/c0
            # snap["fees1"] = int(snap["fees1"])/c1
            # snap["fees0"] = int(snap["fees0"])/c0
            itm["endTimestamp"] = int(itm["endTimestamp"])
            itm["apr"] = float(itm["apr"])
        elif query_name == "vaults_reranges":
            # for snap in itm["reranges"]:
            itm["timestamp"] = int(itm["timestamp"])
        elif query_name == "aprSnapshots":
            # only used to bruteforce Snapshot block num
            pass

        # gelato
        elif query_name == "pools_gelato":
            itm["token0"]["decimals"] = int(itm["token0"]["decimals"])
            itm["token1"]["decimals"] = int(itm["token1"]["decimals"])

            # prepare vars
            c0 = 10 ** itm["token0"]["decimals"]
            c1 = 10 ** itm["token1"]["decimals"]

            itm["feeTier"] = int(itm["feeTier"])
            itm["liquidity"] = int(itm["liquidity"])
            itm["managerFee"] = int(itm["managerFee"])
            itm["totalSupply"] = int(itm["totalSupply"]) / (10**18)
            itm["latestInfo"]["block"] = int(itm["latestInfo"]["block"])
            itm["latestInfo"]["leftover0"] = int(itm["latestInfo"]["leftover0"]) / c0
            itm["latestInfo"]["leftover1"] = int(itm["latestInfo"]["leftover1"]) / c1
            itm["latestInfo"]["reserves0"] = int(itm["latestInfo"]["reserves0"]) / c0
            itm["latestInfo"]["reserves1"] = int(itm["latestInfo"]["reserves1"]) / c1
            itm["latestInfo"]["unclaimedFees0"] = (
                int(itm["latestInfo"]["unclaimedFees0"]) / c0
            )
            itm["latestInfo"]["unclaimedFees1"] = (
                int(itm["latestInfo"]["unclaimedFees1"]) / c1
            )

            itm["latestInfo"]["sqrtPriceX96"] = int(itm["latestInfo"]["sqrtPriceX96"])

            # Supply snapshots
            # convert current suplpy snaps list
            for snap in itm["supplySnapshots"]:
                snap["block"] = int(snap["block"])
                snap["reserves0"] = int(snap["reserves0"]) / c0
                snap["reserves1"] = int(snap["reserves1"]) / c1
                snap["sqrtPriceX96"] = int(snap["sqrtPriceX96"])
            # check snapshots qtty:
            if len(itm["supplySnapshots"]) == 1000:
                # get extra snapshots
                logging.getLogger(__name__).debug(
                    "getting extra supply snapshots for {}  vault id: {}  network:{}".format(
                        itm["name"], itm["id"], network
                    )
                )
                whr_snap = """ id: "{}" """.format(itm["id"])
                itm["supplySnapshots"] += self.get_all_results(
                    network=network,
                    query_name=query_name + "_supplysnapshots",
                    where=whr_snap,
                    skip=1000,
                )

            # Fee snapshots
            # convert current fee snaps list
            for snap in itm["feeSnapshots"]:
                snap["block"] = int(snap["block"])
                snap["feesEarned0"] = int(snap["feesEarned0"]) / c0
                snap["feesEarned1"] = int(snap["feesEarned1"]) / c1
            # check snapshots qtty:
            if len(itm["feeSnapshots"]) == 1000:
                # get extra snapshots
                logging.getLogger(__name__).debug(
                    "getting extra fee snapshots for {}  vault id: {} network:{}".format(
                        itm["name"], itm["id"], network
                    )
                )
                whr_snap = """ id: "{}" """.format(itm["id"])
                itm["feeSnapshots"] += self.get_all_results(
                    network=network,
                    query_name=query_name + "_feesnapshots",
                    where=whr_snap,
                    skip=1000,
                )
        elif query_name == "pools_gelato_supplysnapshots":
            raise NotImplemented(
                " pools_gelato_supplysnapshots has to be reimplemented "
            )
            itm["block"] = int(itm["block"])
            itm["reserves0"] = int(itm["reserves0"]) / c0
            itm["reserves1"] = int(itm["reserves1"]) / c1
            itm["sqrtPriceX96"] = int(itm["sqrtPriceX96"])
        elif query_name == "pools_gelato_feesnapshots":
            raise NotImplemented(" pools_gelato_feesnapshots has to be reimplemented ")
            itm["block"] = int(itm["block"])
            itm["reserves0"] = int(itm["reserves0"]) / c0
            itm["reserves1"] = int(itm["reserves1"]) / c1
            itm["sqrtPriceX96"] = int(itm["sqrtPriceX96"])
        elif query_name == "totalsupply_gelato":
            itm["totalSupply"] = int(itm["totalSupply"]) / (10**18)
        elif query_name == "univ3_gelato":
            pass
        # messari
        elif query_name == "vaults_messari":
            itm["createdBlockNumber"] = int(itm["createdBlockNumber"])
            itm["createdTimestamp"] = int(itm["createdTimestamp"])
            itm["totalValueLockedUSD"] = float(itm["totalValueLockedUSD"])

            itm["outputToken"]["decimals"] = int(itm["outputToken"]["decimals"])
            itm["inputToken"]["decimals"] = int(itm["outputToken"]["decimals"])

            # TotalSupply of the vault tokens
            itm["outputTokenSupply"] = int(itm["outputTokenSupply"]) / (
                10 ** itm["outputToken"]["decimals"]
            )
            itm["inputTokenBalance"] = int(itm["inputTokenBalance"]) / (
                10 ** itm["inputToken"]["decimals"]
            )

            itm["outputTokenPriceUSD"] = float(itm["outputTokenPriceUSD"])

            # todo: it should be divided by 10^outputtokendecimals
            itm["stakedOutputTokenAmount"] = int(itm["stakedOutputTokenAmount"])
        elif query_name == "underlyingTokens_messari":
            itm["token0"]["decimals"] = int(itm["token0"]["decimals"])
            itm["token1"]["decimals"] = int(itm["token1"]["decimals"])
            # prepare vars
            c0 = 10 ** itm["token0"]["decimals"]
            c1 = 10 ** itm["token1"]["decimals"]

            itm["lastAmountBlockNumber"] = int(itm["lastAmountBlockNumber"])

            itm["lastAmount0"] = int(itm["lastAmount0"]) / c0
            itm["lastAmount1"] = int(itm["lastAmount1"]) / c1
        elif query_name == "feesEarneds_messari":
            # todo:  CAREFUL: Token qtties are not converted to float bc no info avaliable in this point in code..
            itm["blockNumber"] = int(itm["blockNumber"])
            itm["fees0"] = int(itm["fees0"])
            itm["fees1"] = int(itm["fees1"])
            itm["feesUSD"] = float(itm["feesUSD"])
            itm["timestamp"] = dt.datetime.fromtimestamp(int(itm["timestamp"]))
        elif query_name in ["deposits_messari", "withdraws_messari"]:
            itm["blockNumber"] = int(itm["blockNumber"])
            itm["timestamp"] = dt.datetime.fromtimestamp(int(itm["timestamp"]))
            itm["amountUSD"] = float(itm["amountUSD"])

            itm["asset"]["lastPriceUSD"] = float(itm["asset"]["lastPriceUSD"])
            itm["asset"]["lastPriceBlockNumber"] = int(
                itm["asset"]["lastPriceBlockNumber"]
            )

            itm["vault"]["totalValueLockedUSD"] = float(
                itm["vault"]["totalValueLockedUSD"]
            )
        elif query_name == "simple_vaults_messari":
            pass  # nothing to convert
        elif query_name == "vaultHourlySnapshots_messari":
            itm["blockNumber"] = int(itm["blockNumber"])
            itm["timestamp"] = dt.datetime.fromtimestamp(int(itm["timestamp"]))

            itm["cumulativeProtocolSideRevenueUSD"] = float(
                itm["cumulativeProtocolSideRevenueUSD"]
            )
            itm["cumulativeSupplySideRevenueUSD"] = float(
                itm["cumulativeSupplySideRevenueUSD"]
            )
            itm["cumulativeTotalRevenueUSD"] = float(itm["cumulativeTotalRevenueUSD"])
            itm["hourlyProtocolSideRevenueUSD"] = float(
                itm["hourlyProtocolSideRevenueUSD"]
            )
            itm["hourlySupplySideRevenueUSD"] = float(itm["hourlySupplySideRevenueUSD"])
            itm["hourlyTotalRevenueUSD"] = float(itm["hourlyTotalRevenueUSD"])
            itm["inputTokenBalance"] = int(itm["inputTokenBalance"])
            itm["outputTokenPriceUSD"] = float(itm["outputTokenPriceUSD"])
            itm["outputTokenSupply"] = int(itm["outputTokenSupply"])
            # itm["pricePerShare"] = itm["pricePerShare"]
            # itm["rewardTokenEmissionsAmount"] = itm["rewardTokenEmissionsAmount"]
            itm["stakedOutputTokenAmount"] = int(itm["stakedOutputTokenAmount"])
            itm["totalValueLockedUSD"] = float(itm["totalValueLockedUSD"])
            # itm["rewardTokenEmissionsUSD"] = float(itm["rewardTokenEmissionsUSD"])

        else:
            logging.getLogger(__name__).error(
                "No converter found with name {}".format(query_name)
            )

            raise NotImplementedError(
                "No arrakis converter found for: {} ".format(query_name)
            )

        # retrun result
        return itm

    def _url_constructor(self, network, query_name: str = ""):
        if "messari" in query_name:
            return self._URLS_messari[network]
        elif "gelato" in query_name:
            return self._URLS_gelato[network]
        else:
            return self._URLS[network]


class xtoken_scraper(thegraph_scraper_helper):
    def init_URLS(self):
        self._URLS = {
            "ethereum": "https://api.thegraph.com/subgraphs/name/xtokenmarket/terminal-mainnet",
            "polygon": "https://api.thegraph.com/subgraphs/name/xtokenmarket/terminal-polygon",
            "optimism": "https://api.thegraph.com/subgraphs/name/xtokenmarket/terminal-optimism",
            "arbitrum": "https://api.thegraph.com/subgraphs/name/xtokenmarket/terminal-arbitrum",
        }

    def _query_constructor(self, skip: int, name: str, filter: str):
        """Create query

        Args:
           name (str): query function name at thegraph
           skip (int): pagination var
           filter (str): filter composed with filter func
        """

        if name == "pools":
            return """{{pools({}, skip: {}) {{
                        rewards {{
                            amount
                            id
                            token {{
                                name
                                symbol
                                decimals
                            }}
                            amountPerWeek
                            }}
                        price
                        tradeFee
                        id
                        vestingPeriod
                        tokenId
                        stakedTokenBalance
                        rewardsAreEscrowed
                        rewardAmounts
                        rewardDuration
                        rewardAmountsPerWeek
                        manager {{
                            id
                            }}
                        createdAt
                        bufferTokenBalance
                        isReward
                        periodFinish
                        poolFee
                        token0 {{
                            symbol
                            name
                            decimals
                            id
                            }}
                        token1 {{
                            symbol
                            name
                            decimals
                            id
                            }}
                        uniswapPool {{
                            id
                            }}
                    }}}}""".format(
                filter, skip
            ), [
                "data",
                "pools",
            ]
        elif name == "deposits":
            return """ {{
                        deposits({}, skip: {}) {{
                            user {{
                                id
                                }}
                            amount0
                            amount1
                            id
                            timestamp
                            pool {{
                                id
                                uniswapPool {{
                                    id
                                }}
                                token1 {{
                                    decimals
                                    name
                                    symbol
                                    id
                                }}
                                token0 {{
                                    decimals
                                    name
                                    symbol
                                    id
                                }}
                            }}
                        }}
                        }}
                """.format(
                filter, skip
            ), [
                "data",
                "deposits",
            ]
        elif name == "withdrawals":
            return """ {{
                        withdrawals({}, skip: {}) {{
                            user {{
                                id
                                }}
                            amount0
                            amount1
                            id
                            timestamp
                            pool {{
                                id
                                uniswapPool {{
                                    id
                                }}
                                token1 {{
                                    decimals
                                    name
                                    symbol
                                    id
                                }}
                                token0 {{
                                    decimals
                                    name
                                    symbol
                                    id
                                }}
                            }}
                        }}
                        }}
                """.format(
                filter, skip
            ), [
                "data",
                "withdrawals",
            ]
        elif name == "collects":
            return """ {{
                        collects({}, skip: {}) {{
                                token0Fee
                                token1Fee
                                pool {{
                                    id
                                    uniswapPool {{
                                        id
                                    }}
                                    token1 {{
                                        symbol
                                        decimals
                                        name
                                        id
                                    }}
                                    token0 {{
                                        symbol
                                        decimals
                                        name
                                        id
                                    }}
                                }}
                                id
                                timestamp
                            }}
                        }}
                """.format(
                filter, skip
            ), [
                "data",
                "collects",
            ]
        elif name == "uniswaps":
            return """{{
                        uniswaps({}, skip: {}) {{ 
                            id
                            pool {{
                                id
                            }}
                            }}
                        }}
                """.format(
                filter, skip
            ), [
                "data",
                "uniswaps",
            ]
        elif name == "users":
            return """{{
                        users({}, skip: {}) {{ 
                            id
                            claimedRewards {
                                amount
                                id
                                timestamp
                                txHash
                                pool {{
                                    id
                                }}
                            }}
                            deposits {{
                                amount1
                                amount0
                                id
                                timestamp
                                pool {{
                                    id
                                }}
                            }}
                            managingPools {{
                                id
                            }}
                            ownedPools {{
                                id
                            }}
                            rewardInitiations {{
                                amounts
                                duration
                                id
                                timestamp
                            }}
                            vests {{
                                id
                                period
                                timestamp
                                txHash
                                value
                            }}
                            withdrawals {{
                                amount0
                                amount1
                                id
                                timestamp
                                pool {{
                                    id
                                }}
                            }}
                            }}
                        }}
                """.format(
                filter, skip
            ), [
                "data",
                "uniswaps",
            ]

        else:
            logging.getLogger(__name__).error(
                "No query found with name {}".format(name)
            )
            raise ValueError("No xtoken query constructor found for: {} ".format(name))

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to int or float or date ...

        Args:
           itm (dict): data to convert
           query_name (str): what to convert
           network
        """
        if query_name == "pools":
            itm["token0"]["decimals"] = int(itm["token0"]["decimals"])
            itm["token1"]["decimals"] = int(itm["token1"]["decimals"])

            # prepare vars
            c0 = 10 ** itm["token0"]["decimals"]
            c1 = 10 ** itm["token1"]["decimals"]

            # convert objects
            itm["createdAt"] = dt.datetime.fromtimestamp(int(itm["createdAt"]))
            itm["price"] = int(itm["price"])
            itm["stakedTokenBalance"][0] = int(itm["stakedTokenBalance"][0]) / c0
            itm["stakedTokenBalance"][1] = int(itm["stakedTokenBalance"][1]) / c1

            itm["bufferTokenBalance"][0] = int(itm["bufferTokenBalance"][0]) / c0
            itm["bufferTokenBalance"][1] = int(itm["bufferTokenBalance"][1]) / c1

            itm["periodFinish"] = dt.datetime.fromtimestamp(int(itm["periodFinish"]))
        elif query_name == "deposits" or query_name == "withdrawals":
            itm["pool"]["token0"]["decimals"] = int(itm["pool"]["token0"]["decimals"])
            itm["pool"]["token1"]["decimals"] = int(itm["pool"]["token1"]["decimals"])

            # prepare vars
            c0 = 10 ** itm["pool"]["token0"]["decimals"]
            c1 = 10 ** itm["pool"]["token1"]["decimals"]

            # convert objects
            itm["timestamp"] = int(itm["timestamp"])
            itm["datetime"] = dt.datetime.fromtimestamp(int(itm["timestamp"]))
            itm["amount0"] = int(itm["amount0"]) / c0
            itm["amount1"] = int(itm["amount1"]) / c1
        elif query_name == "collects":
            itm["pool"]["token0"]["decimals"] = int(itm["pool"]["token0"]["decimals"])
            itm["pool"]["token1"]["decimals"] = int(itm["pool"]["token1"]["decimals"])

            # prepare vars
            c0 = 10 ** itm["pool"]["token0"]["decimals"]
            c1 = 10 ** itm["pool"]["token1"]["decimals"]

            # convert objects
            itm["timestamp"] = int(itm["timestamp"])
            itm["datetime"] = dt.datetime.fromtimestamp(int(itm["timestamp"]))
            itm["token0Fee"] = int(itm["token0Fee"]) / c0
            itm["token1Fee"] = int(itm["token1Fee"]) / c1
        elif query_name == "uniswaps":
            pass

        else:
            logging.getLogger(__name__).error(
                "No converter found with name {}".format(query_name)
            )
            raise ValueError(
                "No xtoken query converter found for: {} ".format(query_name)
            )

        # retrun result
        return itm

    def _url_constructor(self, network, query_name: str = ""):
        return self._URLS[network]


class uniswapv3_scraper(thegraph_scraper_helper):
    def init_URLS(self):
        self._URLS = {
            "ethereum": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
            "polygon": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
            "optimism": "https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis",
            "arbitrum": "https://api.thegraph.com/subgraphs/name/ianlapham/arbitrum-minimal",
            "celo": "https://api.thegraph.com/subgraphs/name/jesse-sawa/uniswap-celo",
            "binance": "https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-bsc",
        }

    def _query_constructor(self, skip: int, name: str, filter: str):
        """Create query

        Args:
           name (str): query function name at thegraph
           skip (int): pagination var
           filter (str): filter composed with filter func
        """

        if name == "pools":
            return """{{pools({}, skip: {}) {{
                        id
                        token1Price
                        token0Price
                        feeTier
                        token0 {{
                            name
                            id
                            decimals
                            symbol
                            }}
                        token1 {{
                            decimals
                            id
                            name
                            symbol
                            }}
                        totalValueLockedUSD
                        totalValueLockedToken1
                        totalValueLockedToken0
                        totalValueLockedETH
                        totalValueLockedUSDUntracked
                        untrackedVolumeUSD
                        volumeToken0
                        volumeToken1
                        volumeUSD
                        txCount
                        liquidityProviderCount
                        liquidity
                        createdAtTimestamp
                        createdAtBlockNumber
                        feeGrowthGlobal0X128
                        feeGrowthGlobal1X128
                        tick

                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "pools",
            ]

        elif name == "ticks":
            return """{{ ticks({}, skip: {}) {{
                        id
                        tickIdx
                        feeGrowthOutside0X128
                        feeGrowthOutside1X128
                        collectedFeesToken0
                        collectedFeesToken1
                        collectedFeesUSD
                        createdAtBlockNumber
                        createdAtTimestamp
                        feesUSD
                        liquidityGross
                        liquidityNet
                        liquidityProviderCount
                        poolAddress
                        price0
                        price1
                        untrackedVolumeUSD
                        volumeToken0
                        volumeToken1
                        volumeUSD
                        pool {{
                            id
                            token0 {{
                                id
                                decimals
                                symbol
                            }}
                            token1 {{
                                id
                                decimals
                                symbol
                            }}
                        }}
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "ticks",
            ]

        elif name == "positions":
            return """{{ positions({}, skip: {}) {{
                        id
                        owner
                        liquidity
                        collectedFeesToken0
                        collectedFeesToken1
                        depositedToken0
                        depositedToken1
                        feeGrowthInside0LastX128
                        feeGrowthInside1LastX128
                        withdrawnToken0
                        withdrawnToken1
                        pool {{
                            id
                            feeGrowthGlobal0X128
                            feeGrowthGlobal1X128
                            observationIndex
                            sqrtPrice
                            tick
                            feeTier
                            token0 {{
                                decimals
                                id
                                symbol
                            }}
                            token1 {{
                                decimals
                                id
                                symbol
                            }}
                        }}
                        tickLower {{
                            id
                            tickIdx
                            feeGrowthOutside0X128
                            feeGrowthOutside1X128
                            liquidityGross
                            liquidityNet
                        }}
                        tickUpper {{
                            id
                            tickIdx
                            feeGrowthOutside0X128
                            feeGrowthOutside1X128
                            liquidityGross
                            liquidityNet
                        }}
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "positions",
            ]

        elif name == "tokens":
            return """{{ tokens({}, skip: {}) {{
                        decimals
                        derivedETH
                        feesUSD
                        id
                        name
                        poolCount
                        symbol
                        totalSupply
                        totalValueLocked
                        totalValueLockedUSD
                        totalValueLockedUSDUntracked
                        txCount
                        untrackedVolumeUSD
                        volume
                        volumeUSD
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "tokens",
            ]

        else:
            logging.getLogger(__name__).error(
                "No univ3 query found with name {}".format(name)
            )
            raise ValueError("No univ3 query constructor found for: {} ".format(name))

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to int or float or date ...

        Args:
           itm (dict): data to convert
           query_name (str): what to convert
           network
        """
        if query_name == "pools":
            itm["token0"]["decimals"] = int(itm["token0"]["decimals"])
            itm["token1"]["decimals"] = int(itm["token1"]["decimals"])
            # prepare vars
            c0 = 10 ** itm["token0"]["decimals"]
            c1 = 10 ** itm["token1"]["decimals"]

            # convert objects
            itm["token0Price"] = float(itm["token0Price"])
            itm["token1Price"] = float(itm["token1Price"])
            itm["feeTier"] = int(itm["feeTier"])  # /1000000

            itm["totalValueLockedUSD"] = float(itm["totalValueLockedUSD"])
            itm["totalValueLockedToken0"] = float(itm["totalValueLockedToken0"])
            itm["totalValueLockedToken1"] = float(itm["totalValueLockedToken1"])
            itm["totalValueLockedETH"] = float(itm["totalValueLockedETH"])
            itm["totalValueLockedUSDUntracked"] = float(
                itm["totalValueLockedUSDUntracked"]
            )
            itm["untrackedVolumeUSD"] = float(itm["untrackedVolumeUSD"])

            itm["volumeToken0"] = float(itm["volumeToken0"])
            itm["volumeToken1"] = float(itm["volumeToken1"])
            itm["volumeUSD"] = float(itm["volumeUSD"])
            itm["txCount"] = int(itm["txCount"])
            itm["liquidityProviderCount"] = int(itm["liquidityProviderCount"])
            itm["liquidity"] = int(itm["liquidity"])
            itm["createdAtTimestamp"] = dt.datetime.fromtimestamp(
                int(itm["createdAtTimestamp"])
            )
            itm["createdAtBlockNumber"] = int(itm["createdAtBlockNumber"])

            itm["feeGrowthGlobal0X128"] = int(itm["feeGrowthGlobal0X128"])
            itm["feeGrowthGlobal1X128"] = int(itm["feeGrowthGlobal1X128"])
            itm["tick"] = int(itm["tick"])

        elif query_name == "ticks":
            itm["pool"]["token0"]["decimals"] = int(itm["pool"]["token0"]["decimals"])
            itm["pool"]["token1"]["decimals"] = int(itm["pool"]["token1"]["decimals"])
            # prepare vars
            c0 = 10 ** itm["pool"]["token0"]["decimals"]
            c1 = 10 ** itm["pool"]["token1"]["decimals"]

            itm["feeGrowthOutside0X128"] = int(itm["feeGrowthOutside0X128"])
            itm["feeGrowthOutside1X128"] = int(itm["feeGrowthOutside1X128"])
            itm["collectedFeesToken0"] = int(itm["collectedFeesToken0"]) / c0
            itm["collectedFeesToken1"] = int(itm["collectedFeesToken1"]) / c1
            itm["collectedFeesUSD"] = float(itm["collectedFeesUSD"])
            itm["createdAtBlockNumber"] = int(itm["createdAtBlockNumber"])
            itm["createdAtTimestamp"] = dt.datetime.fromtimestamp(
                int(itm["createdAtTimestamp"])
            )
            itm["feesUSD"] = float(itm["feesUSD"])
            itm["liquidityGross"] = int(itm["liquidityGross"])
            itm["liquidityNet"] = int(itm["liquidityNet"])
            itm["liquidityProviderCount"] = int(itm["liquidityProviderCount"])
            itm["price0"] = float(itm["price0"])
            itm["price1"] = float(itm["price1"])
            itm["untrackedVolumeUSD"] = float(itm["untrackedVolumeUSD"])
            itm["volumeToken0"] = int(itm["volumeToken0"]) / c0
            itm["volumeToken1"] = int(itm["volumeToken1"]) / c1
            itm["volumeUSD"] = float(itm["volumeUSD"])

        elif query_name == "positions":
            itm["pool"]["token0"]["decimals"] = int(itm["pool"]["token0"]["decimals"])
            itm["pool"]["token1"]["decimals"] = int(itm["pool"]["token1"]["decimals"])

            # prepare vars
            c0 = 10 ** itm["pool"]["token0"]["decimals"]
            c1 = 10 ** itm["pool"]["token1"]["decimals"]

            itm["liquidity"] = int(itm["liquidity"])

            itm["collectedFeesToken0"] = float(itm["collectedFeesToken0"])
            itm["collectedFeesToken1"] = float(itm["collectedFeesToken1"])

            itm["depositedToken0"] = float(itm["depositedToken0"])
            itm["depositedToken1"] = float(itm["depositedToken1"])

            itm["feeGrowthInside0LastX128"] = int(itm["feeGrowthInside0LastX128"])
            itm["feeGrowthInside1LastX128"] = int(itm["feeGrowthInside1LastX128"])

            itm["withdrawnToken0"] = float(itm["withdrawnToken0"])
            itm["withdrawnToken1"] = float(itm["withdrawnToken1"])

            itm["pool"]["feeTier"] = int(itm["pool"]["feeTier"])
            itm["pool"]["feeGrowthGlobal0X128"] = int(
                itm["pool"]["feeGrowthGlobal0X128"]
            )
            itm["pool"]["feeGrowthGlobal1X128"] = int(
                itm["pool"]["feeGrowthGlobal1X128"]
            )
            itm["pool"]["observationIndex"] = int(itm["pool"]["observationIndex"])
            itm["pool"]["sqrtPrice"] = int(itm["pool"]["sqrtPrice"])
            itm["pool"]["tick"] = int(itm["pool"]["tick"])

            itm["tickLower"]["tickIdx"] = int(itm["tickLower"]["tickIdx"])
            itm["tickLower"]["feeGrowthOutside0X128"] = int(
                itm["tickLower"]["feeGrowthOutside0X128"]
            )
            itm["tickLower"]["feeGrowthOutside1X128"] = int(
                itm["tickLower"]["feeGrowthOutside1X128"]
            )
            itm["tickLower"]["liquidityGross"] = int(itm["tickLower"]["liquidityGross"])
            itm["tickLower"]["liquidityNet"] = int(itm["tickLower"]["liquidityNet"])

            itm["tickUpper"]["tickIdx"] = int(itm["tickUpper"]["tickIdx"])
            itm["tickUpper"]["feeGrowthOutside0X128"] = int(
                itm["tickUpper"]["feeGrowthOutside0X128"]
            )
            itm["tickUpper"]["feeGrowthOutside1X128"] = int(
                itm["tickUpper"]["feeGrowthOutside1X128"]
            )
            itm["tickUpper"]["liquidityGross"] = int(itm["tickUpper"]["liquidityGross"])
            itm["tickUpper"]["liquidityNet"] = int(itm["tickUpper"]["liquidityNet"])

        elif query_name == "tokens":
            itm["decimals"] = int(itm["decimals"])
            itm["derivedETH"] = float(itm["derivedETH"])
            itm["feesUSD"] = float(itm["feesUSD"])
            itm["poolCount"] = int(itm["poolCount"])
            itm["totalSupply"] = int(itm["totalSupply"])
            itm["totalValueLocked"] = float(itm["totalValueLocked"])
            itm["totalValueLockedUSD"] = float(itm["totalValueLockedUSD"])
            itm["totalValueLockedUSDUntracked"] = float(
                itm["totalValueLockedUSDUntracked"]
            )
            itm["txCount"] = int(itm["txCount"])
            itm["untrackedVolumeUSD"] = float(itm["untrackedVolumeUSD"])
            itm["volume"] = float(itm["volume"])
            itm["volumeUSD"] = float(itm["volumeUSD"])

        else:
            logging.getLogger(__name__).error(
                "No univ3 converter found with name {}".format(query_name)
            )
            raise ValueError(
                "No univ3 query converter found for: {} ".format(query_name)
            )

        # retrun result
        return itm

    def _url_constructor(self, network, query_name: str = ""):
        return self._URLS[network]


class algebrav3_scraper(thegraph_scraper_helper):
    def _query_constructor(self, skip: int, name: str, filter: str):
        """Create query

        Args:
           name (str): query function name at thegraph
           skip (int): pagination var
           filter (str): filter composed with filter func
        """

        if name == "pools":
            return """{{pools({}, skip: {}) {{
                        id
                        token1Price
                        token0Price
                        fee
                        token0 {{
                            name
                            id
                            decimals
                            symbol
                            }}
                        token1 {{
                            decimals
                            id
                            name
                            symbol
                            }}
                        totalValueLockedUSD
                        totalValueLockedToken1
                        totalValueLockedToken0
                        totalValueLockedMatic
                        totalValueLockedUSDUntracked
                        untrackedVolumeUSD
                        volumeToken0
                        volumeToken1
                        volumeUSD
                        txCount
                        liquidityProviderCount
                        liquidity
                        createdAtTimestamp
                        createdAtBlockNumber
                        feeGrowthGlobal0X128
                        feeGrowthGlobal1X128
                        tick

                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "pools",
            ]

        elif name == "ticks":
            return """{{ ticks({}, skip: {}) {{
                        id
                        tickIdx
                        feeGrowthOutside0X128
                        feeGrowthOutside1X128
                        collectedFeesToken0
                        collectedFeesToken1
                        collectedFeesUSD
                        createdAtBlockNumber
                        createdAtTimestamp
                        feesUSD
                        liquidityGross
                        liquidityNet
                        liquidityProviderCount
                        poolAddress
                        price0
                        price1
                        untrackedVolumeUSD
                        volumeToken0
                        volumeToken1
                        volumeUSD
                        pool {{
                            id
                            token0 {{
                                id
                                decimals
                                symbol
                            }}
                            token1 {{
                                id
                                decimals
                                symbol
                            }}
                        }}
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "ticks",
            ]

        elif name == "tokens":
            return """{{ tokens({}, skip: {}) {{
                        decimals
                        derivedMatic
                        feesUSD
                        id
                        name
                        poolCount
                        symbol
                        totalSupply
                        totalValueLocked
                        totalValueLockedUSD
                        totalValueLockedUSDUntracked
                        txCount
                        untrackedVolumeUSD
                        volume
                        volumeUSD
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "tokens",
            ]

        else:
            logging.getLogger(__name__).error(
                f"No algebra query found with name {name}"
            )
            raise ValueError(f"No algebra query constructor found for: {name} ")

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to int or float or date ...

        Args:
           itm (dict): data to convert
           query_name (str): what to convert
           network
        """
        if query_name == "pools":
            itm["token0"]["decimals"] = int(itm["token0"]["decimals"])
            itm["token1"]["decimals"] = int(itm["token1"]["decimals"])
            # prepare vars
            c0 = 10 ** itm["token0"]["decimals"]
            c1 = 10 ** itm["token1"]["decimals"]

            # convert objects
            itm["token0Price"] = float(itm["token0Price"])
            itm["token1Price"] = float(itm["token1Price"])
            itm["fee"] = int(itm["fee"])  # /1000000

            itm["totalValueLockedUSD"] = float(itm["totalValueLockedUSD"])
            itm["totalValueLockedToken0"] = float(itm["totalValueLockedToken0"])
            itm["totalValueLockedToken1"] = float(itm["totalValueLockedToken1"])
            itm["totalValueLockedMatic"] = float(itm["totalValueLockedMatic"])
            itm["totalValueLockedUSDUntracked"] = float(
                itm["totalValueLockedUSDUntracked"]
            )
            itm["untrackedVolumeUSD"] = float(itm["untrackedVolumeUSD"])

            itm["volumeToken0"] = float(itm["volumeToken0"])
            itm["volumeToken1"] = float(itm["volumeToken1"])
            itm["volumeUSD"] = float(itm["volumeUSD"])
            itm["txCount"] = int(itm["txCount"])
            itm["liquidityProviderCount"] = int(itm["liquidityProviderCount"])
            itm["liquidity"] = int(itm["liquidity"])
            itm["createdAtTimestamp"] = dt.datetime.fromtimestamp(
                int(itm["createdAtTimestamp"])
            )
            itm["createdAtBlockNumber"] = int(itm["createdAtBlockNumber"])

            itm["feeGrowthGlobal0X128"] = int(itm["feeGrowthGlobal0X128"])
            itm["feeGrowthGlobal1X128"] = int(itm["feeGrowthGlobal1X128"])
            itm["tick"] = int(itm["tick"])

        elif query_name == "ticks":
            itm["pool"]["token0"]["decimals"] = int(itm["pool"]["token0"]["decimals"])
            itm["pool"]["token1"]["decimals"] = int(itm["pool"]["token1"]["decimals"])
            # prepare vars
            c0 = 10 ** itm["pool"]["token0"]["decimals"]
            c1 = 10 ** itm["pool"]["token1"]["decimals"]

            itm["feeGrowthOutside0X128"] = int(itm["feeGrowthOutside0X128"])
            itm["feeGrowthOutside1X128"] = int(itm["feeGrowthOutside1X128"])
            itm["collectedFeesToken0"] = int(itm["collectedFeesToken0"]) / c0
            itm["collectedFeesToken1"] = int(itm["collectedFeesToken1"]) / c1
            itm["collectedFeesUSD"] = float(itm["collectedFeesUSD"])
            itm["createdAtBlockNumber"] = int(itm["createdAtBlockNumber"])
            itm["createdAtTimestamp"] = dt.datetime.fromtimestamp(
                int(itm["createdAtTimestamp"])
            )
            itm["feesUSD"] = float(itm["feesUSD"])
            itm["liquidityGross"] = int(itm["liquidityGross"])
            itm["liquidityNet"] = int(itm["liquidityNet"])
            itm["liquidityProviderCount"] = int(itm["liquidityProviderCount"])
            itm["price0"] = float(itm["price0"])
            itm["price1"] = float(itm["price1"])
            itm["untrackedVolumeUSD"] = float(itm["untrackedVolumeUSD"])
            itm["volumeToken0"] = int(itm["volumeToken0"]) / c0
            itm["volumeToken1"] = int(itm["volumeToken1"]) / c1
            itm["volumeUSD"] = float(itm["volumeUSD"])

        elif query_name == "positions":
            itm["pool"]["token0"]["decimals"] = int(itm["pool"]["token0"]["decimals"])
            itm["pool"]["token1"]["decimals"] = int(itm["pool"]["token1"]["decimals"])

            # prepare vars
            c0 = 10 ** itm["pool"]["token0"]["decimals"]
            c1 = 10 ** itm["pool"]["token1"]["decimals"]

            itm["liquidity"] = int(itm["liquidity"])

            itm["collectedFeesToken0"] = float(itm["collectedFeesToken0"])
            itm["collectedFeesToken1"] = float(itm["collectedFeesToken1"])

            itm["depositedToken0"] = float(itm["depositedToken0"])
            itm["depositedToken1"] = float(itm["depositedToken1"])

            itm["feeGrowthInside0LastX128"] = int(itm["feeGrowthInside0LastX128"])
            itm["feeGrowthInside1LastX128"] = int(itm["feeGrowthInside1LastX128"])

            itm["withdrawnToken0"] = float(itm["withdrawnToken0"])
            itm["withdrawnToken1"] = float(itm["withdrawnToken1"])

            itm["pool"]["fee"] = int(itm["pool"]["fee"])
            itm["pool"]["feeGrowthGlobal0X128"] = int(
                itm["pool"]["feeGrowthGlobal0X128"]
            )
            itm["pool"]["feeGrowthGlobal1X128"] = int(
                itm["pool"]["feeGrowthGlobal1X128"]
            )
            itm["pool"]["observationIndex"] = int(itm["pool"]["observationIndex"])
            itm["pool"]["sqrtPrice"] = int(itm["pool"]["sqrtPrice"])
            itm["pool"]["tick"] = int(itm["pool"]["tick"])

            itm["tickLower"]["tickIdx"] = int(itm["tickLower"]["tickIdx"])
            itm["tickLower"]["feeGrowthOutside0X128"] = int(
                itm["tickLower"]["feeGrowthOutside0X128"]
            )
            itm["tickLower"]["feeGrowthOutside1X128"] = int(
                itm["tickLower"]["feeGrowthOutside1X128"]
            )
            itm["tickLower"]["liquidityGross"] = int(itm["tickLower"]["liquidityGross"])
            itm["tickLower"]["liquidityNet"] = int(itm["tickLower"]["liquidityNet"])

            itm["tickUpper"]["tickIdx"] = int(itm["tickUpper"]["tickIdx"])
            itm["tickUpper"]["feeGrowthOutside0X128"] = int(
                itm["tickUpper"]["feeGrowthOutside0X128"]
            )
            itm["tickUpper"]["feeGrowthOutside1X128"] = int(
                itm["tickUpper"]["feeGrowthOutside1X128"]
            )
            itm["tickUpper"]["liquidityGross"] = int(itm["tickUpper"]["liquidityGross"])
            itm["tickUpper"]["liquidityNet"] = int(itm["tickUpper"]["liquidityNet"])

        elif query_name == "tokens":
            itm["decimals"] = int(itm["decimals"])
            itm["derivedMatic"] = float(itm["derivedMatic"])
            itm["feesUSD"] = float(itm["feesUSD"])
            itm["poolCount"] = int(itm["poolCount"])
            itm["totalSupply"] = int(itm["totalSupply"])
            itm["totalValueLocked"] = float(itm["totalValueLocked"])
            itm["totalValueLockedUSD"] = float(itm["totalValueLockedUSD"])
            itm["totalValueLockedUSDUntracked"] = float(
                itm["totalValueLockedUSDUntracked"]
            )
            itm["txCount"] = int(itm["txCount"])
            itm["untrackedVolumeUSD"] = float(itm["untrackedVolumeUSD"])
            itm["volume"] = float(itm["volume"])
            itm["volumeUSD"] = float(itm["volumeUSD"])

        else:
            logging.getLogger(__name__).error(
                "No algebra converter found with name {}".format(query_name)
            )
            raise ValueError(
                "No algebra query converter found for: {} ".format(query_name)
            )

        # retrun result
        return itm

    def _url_constructor(self, network, query_name: str = ""):
        return self._URLS[network]


class quickswap_scraper(algebrav3_scraper):
    def init_URLS(self):
        self._URLS = {
            "polygon": "https://api.thegraph.com/subgraphs/name/sameepsi/quickswap-v3",
        }
        self._URLS_loc = {
            "polygon": "https://api.thegraph.com/subgraphs/name/l0c4t0r/hype-pool-quickswap-polygon",
        }

    def _query_constructor(self, skip: int, name: str, filter: str):
        """Create query

        Args:
           name (str): query function name at thegraph
           skip (int): pagination var
           filter (str): filter composed with filter func
        """

        if name == "hypervisors_loc":
            return """{{ hypervisors({}, skip: {}) {{
                           id
                            pool {{
                            currentTick
                            feeGrowthGlobal0X128
                            feeGrowthGlobal1X128
                            }}
                            basePosition {{
                            liquidity
                            tokensOwed0
                            tokensOwed1
                            feeGrowthInside0X128
                            feeGrowthInside1X128
                            tickLower {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            tickUpper {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            }}
                            limitPosition {{
                            liquidity
                            tokensOwed0
                            tokensOwed1
                            feeGrowthInside0X128
                            feeGrowthInside1X128
                            tickLower {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            tickUpper {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            }}
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "hypervisors",
            ]

        else:
            return super()._query_constructor(skip=skip, name=name, filter=filter)

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to int or float or date ...

        Args:
           itm (dict): data to convert
           query_name (str): what to convert
           network
        """
        if query_name != "hypervisors_loc":
            return super()._converter(itm=itm, query_name=query_name, network=network)

        itm["pool"]["currentTick"] = int(itm["pool"]["currentTick"])
        itm["pool"]["feeGrowthGlobal0X128"] = int(itm["pool"]["feeGrowthGlobal0X128"])
        itm["pool"]["feeGrowthGlobal1X128"] = int(itm["pool"]["feeGrowthGlobal1X128"])

        itm["basePosition"]["liquidity"] = int(itm["basePosition"]["liquidity"])
        self._extracted_from__converter_21(itm, "basePosition")
        itm["limitPosition"]["liquidity"] = int(itm["limitPosition"]["liquidity"])
        self._extracted_from__converter_21(itm, "limitPosition")
        # retrun result
        return itm

    # TODO Rename this here and in `_converter`
    def _extracted_from__converter_21(self, itm, arg1):
        # itm["basePosition"]["tokensOwed0"] = itm["basePosition"]["tokensOwed0"]
        # itm["basePosition"]["tokensOwed1"] = itm["basePosition"]["tokensOwed0"]
        itm[arg1]["feeGrowthInside0X128"] = int(itm[arg1]["feeGrowthInside0X128"])
        itm[arg1]["feeGrowthInside1X128"] = int(itm[arg1]["feeGrowthInside1X128"])
        itm[arg1]["tickLower"]["tickIdx"] = int(itm[arg1]["tickLower"]["tickIdx"])
        itm[arg1]["tickLower"]["feeGrowthOutside0X128"] = int(
            itm[arg1]["tickLower"]["feeGrowthOutside0X128"]
        )

        itm[arg1]["tickLower"]["feeGrowthOutside1X128"] = int(
            itm[arg1]["tickLower"]["feeGrowthOutside1X128"]
        )

        itm[arg1]["tickUpper"]["tickIdx"] = int(itm[arg1]["tickUpper"]["tickIdx"])
        itm[arg1]["tickUpper"]["feeGrowthOutside0X128"] = int(
            itm[arg1]["tickUpper"]["feeGrowthOutside0X128"]
        )

        itm[arg1]["tickUpper"]["feeGrowthOutside1X128"] = int(
            itm[arg1]["tickUpper"]["feeGrowthOutside1X128"]
        )

    def _url_constructor(self, network, query_name: str = ""):
        if "_loc" in query_name:
            return self._URLS_loc[network]
        else:
            return self._URLS[network]


class zyberswap_scraper(algebrav3_scraper):
    def init_URLS(self):
        self._URLS = {
            "arbitrum": "https://api.thegraph.com/subgraphs/name/iliaazhel/zyberswap-info",
        }
        self._URLS_loc = {
            "arbitrum": "https://api.thegraph.com/subgraphs/name/l0c4t0r/hype-pool-zyberswap-arbitrum",
        }

    def _query_constructor(self, skip: int, name: str, filter: str):
        """Create query

        Args:
           name (str): query function name at thegraph
           skip (int): pagination var
           filter (str): filter composed with filter func
        """

        if name == "hypervisors_loc":
            return """{{ hypervisors({}, skip: {}) {{
                           id
                            pool {{
                            currentTick
                            feeGrowthGlobal0X128
                            feeGrowthGlobal1X128
                            }}
                            basePosition {{
                            liquidity
                            tokensOwed0
                            tokensOwed1
                            feeGrowthInside0X128
                            feeGrowthInside1X128
                            tickLower {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            tickUpper {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            }}
                            limitPosition {{
                            liquidity
                            tokensOwed0
                            tokensOwed1
                            feeGrowthInside0X128
                            feeGrowthInside1X128
                            tickLower {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            tickUpper {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            }}
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "hypervisors",
            ]

        else:
            return super()._query_constructor(skip=skip, name=name, filter=filter)

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to int or float or date ...

        Args:
           itm (dict): data to convert
           query_name (str): what to convert
           network
        """
        if query_name != "hypervisors_loc":
            return super()._converter(itm=itm, query_name=query_name, network=network)

        itm["pool"]["currentTick"] = int(itm["pool"]["currentTick"])
        itm["pool"]["feeGrowthGlobal0X128"] = int(itm["pool"]["feeGrowthGlobal0X128"])
        itm["pool"]["feeGrowthGlobal1X128"] = int(itm["pool"]["feeGrowthGlobal1X128"])

        itm["basePosition"]["liquidity"] = int(itm["basePosition"]["liquidity"])
        self._extracted_from__converter_21(itm, "basePosition")
        itm["limitPosition"]["liquidity"] = int(itm["limitPosition"]["liquidity"])
        self._extracted_from__converter_21(itm, "limitPosition")
        # retrun result
        return itm

    # TODO Rename this here and in `_converter`
    def _extracted_from__converter_21(self, itm, arg1):
        # itm["basePosition"]["tokensOwed0"] = itm["basePosition"]["tokensOwed0"]
        # itm["basePosition"]["tokensOwed1"] = itm["basePosition"]["tokensOwed0"]
        itm[arg1]["feeGrowthInside0X128"] = int(itm[arg1]["feeGrowthInside0X128"])
        itm[arg1]["feeGrowthInside1X128"] = int(itm[arg1]["feeGrowthInside1X128"])
        itm[arg1]["tickLower"]["tickIdx"] = int(itm[arg1]["tickLower"]["tickIdx"])
        itm[arg1]["tickLower"]["feeGrowthOutside0X128"] = int(
            itm[arg1]["tickLower"]["feeGrowthOutside0X128"]
        )

        itm[arg1]["tickLower"]["feeGrowthOutside1X128"] = int(
            itm[arg1]["tickLower"]["feeGrowthOutside1X128"]
        )

        itm[arg1]["tickUpper"]["tickIdx"] = int(itm[arg1]["tickUpper"]["tickIdx"])
        itm[arg1]["tickUpper"]["feeGrowthOutside0X128"] = int(
            itm[arg1]["tickUpper"]["feeGrowthOutside0X128"]
        )

        itm[arg1]["tickUpper"]["feeGrowthOutside1X128"] = int(
            itm[arg1]["tickUpper"]["feeGrowthOutside1X128"]
        )

    def _url_constructor(self, network, query_name: str = ""):
        if "_loc" in query_name:
            return self._URLS_loc[network]
        else:
            return self._URLS[network]


class thena_scraper(algebrav3_scraper):
    def init_URLS(self):
        self._URLS = {
            "binance": "https://api.thegraph.com/subgraphs/name/iliaazhel/thena-info",
        }
        self._URLS_loc = {
            "binance": "https://api.thegraph.com/subgraphs/name/l0c4t0r/hype-pool-thena-bsc",
        }

    def _query_constructor(self, skip: int, name: str, filter: str):
        """Create query

        Args:
           name (str): query function name at thegraph
           skip (int): pagination var
           filter (str): filter composed with filter func
        """

        if name == "hypervisors_loc":
            return """{{ hypervisors({}, skip: {}) {{
                           id
                            pool {{
                            currentTick
                            feeGrowthGlobal0X128
                            feeGrowthGlobal1X128
                            }}
                            basePosition {{
                            liquidity
                            tokensOwed0
                            tokensOwed1
                            feeGrowthInside0X128
                            feeGrowthInside1X128
                            tickLower {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            tickUpper {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            }}
                            limitPosition {{
                            liquidity
                            tokensOwed0
                            tokensOwed1
                            feeGrowthInside0X128
                            feeGrowthInside1X128
                            tickLower {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            tickUpper {{
                                tickIdx
                                feeGrowthOutside0X128
                                feeGrowthOutside1X128
                            }}
                            }}
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "hypervisors",
            ]

        else:
            return super()._query_constructor(skip=skip, name=name, filter=filter)

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to int or float or date ...

        Args:
           itm (dict): data to convert
           query_name (str): what to convert
           network
        """
        if query_name != "hypervisors_loc":
            return super()._converter(itm=itm, query_name=query_name, network=network)

        itm["pool"]["currentTick"] = int(itm["pool"]["currentTick"])
        itm["pool"]["feeGrowthGlobal0X128"] = int(itm["pool"]["feeGrowthGlobal0X128"])
        itm["pool"]["feeGrowthGlobal1X128"] = int(itm["pool"]["feeGrowthGlobal1X128"])

        itm["basePosition"]["liquidity"] = int(itm["basePosition"]["liquidity"])
        self._extracted_from__converter_21(itm, "basePosition")
        itm["limitPosition"]["liquidity"] = int(itm["limitPosition"]["liquidity"])
        self._extracted_from__converter_21(itm, "limitPosition")
        # retrun result
        return itm

    # TODO Rename this here and in `_converter`
    def _extracted_from__converter_21(self, itm, arg1):
        # itm["basePosition"]["tokensOwed0"] = itm["basePosition"]["tokensOwed0"]
        # itm["basePosition"]["tokensOwed1"] = itm["basePosition"]["tokensOwed0"]
        itm[arg1]["feeGrowthInside0X128"] = int(itm[arg1]["feeGrowthInside0X128"])
        itm[arg1]["feeGrowthInside1X128"] = int(itm[arg1]["feeGrowthInside1X128"])
        itm[arg1]["tickLower"]["tickIdx"] = int(itm[arg1]["tickLower"]["tickIdx"])
        itm[arg1]["tickLower"]["feeGrowthOutside0X128"] = int(
            itm[arg1]["tickLower"]["feeGrowthOutside0X128"]
        )

        itm[arg1]["tickLower"]["feeGrowthOutside1X128"] = int(
            itm[arg1]["tickLower"]["feeGrowthOutside1X128"]
        )

        itm[arg1]["tickUpper"]["tickIdx"] = int(itm[arg1]["tickUpper"]["tickIdx"])
        itm[arg1]["tickUpper"]["feeGrowthOutside0X128"] = int(
            itm[arg1]["tickUpper"]["feeGrowthOutside0X128"]
        )

        itm[arg1]["tickUpper"]["feeGrowthOutside1X128"] = int(
            itm[arg1]["tickUpper"]["feeGrowthOutside1X128"]
        )

    def _url_constructor(self, network, query_name: str = ""):
        if "_loc" in query_name:
            return self._URLS_loc[network]
        else:
            return self._URLS[network]


class blocks_scraper(thegraph_scraper_helper):
    def init_URLS(self):
        self._URLS = {
            "ethereum": "https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks",
            "polygon": "https://api.thegraph.com/subgraphs/name/sameepsi/maticblocks",
            # "optimism": "https://api.thegraph.com/subgraphs/name/beethovenxfi/optimism-blocks/",
            "arbitrum": "https://api.thegraph.com/subgraphs/name/edoapp/arbitrum-blocks",
            # "celo":"",
            "binance": "https://api.thegraph.com/subgraphs/name/emkay860/blocks",  # "https://api.thegraph.com/subgraphs/name/iliaazhel/bsc-blocks"
            "avalanche": "https://api.thegraph.com/subgraphs/name/iliaazhel/avalanche-blocks",
        }

    def _query_constructor(self, skip: int, name: str, filter: str):
        """Create query

        Args:
           name (str): query function name at thegraph
           skip (int): pagination var
           filter (str): filter composed with filter func
        """

        if name == "blocks":
            return """{{blocks({}, skip: {}) {{
                            author
                            difficulty
                            gasLimit
                            gasUsed
                            id
                            number
                            parentHash
                            receiptsRoot
                            size
                            stateRoot
                            timestamp
                            totalDifficulty
                            unclesHash
                            transactionsRoot
                    }}
                    }}""".format(
                filter, skip
            ), [
                "data",
                "blocks",
            ]

        logging.getLogger(__name__).error(f"No block query found with name {name}")
        raise ValueError(f"No block query constructor found for: {name} ")

    def _converter(self, itm: dict, query_name: str, network: str):
        """Convert string data received from thegraph to int or float or date ...

        Args:
           itm (dict): data to convert
           query_name (str): what to convert
           network
        """
        if query_name == "blocks":
            self._extracted_from__converter_11(itm)
        else:
            logging.getLogger(__name__).error(
                f"No block converter found with name {query_name}"
            )

            raise ValueError(f"No block query converter found for: {query_name} ")

        # retrun result
        return itm

    # TODO Rename this here and in `_converter`
    def _extracted_from__converter_11(self, itm):
        itm["difficulty"] = int(itm["difficulty"])
        itm["gasLimit"] = int(itm["gasLimit"])
        itm["gasUsed"] = int(itm["gasUsed"])
        itm["number"] = int(itm["number"])
        itm["size"] = int(itm["size"])
        itm["timestamp"] = int(itm["timestamp"])
        itm["totalDifficulty"] = int(itm["totalDifficulty"])

    def _url_constructor(self, network, query_name: str = ""):
        return self._URLS[network]
