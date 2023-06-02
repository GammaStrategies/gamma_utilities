from enum import Enum


class Chain(str, Enum):
    #      ( value , id , API url, API name, subgraph name, database name, fantasy_name )
    ARBITRUM = ("arbitrum", 42161, "arbitrum", "Arbitrum")
    CELO = ("celo", 42220, "celo", "Celo")
    ETHEREUM = ("ethereum", 1, "ethereum", "Ethereum")
    OPTIMISM = ("optimism", 10, "optimism", "Optimism")
    POLYGON = ("polygon", 137, "polygon", "Polygon")
    BSC = ("bsc", 56, "binance", "Binance Chain")
    POLYGON_ZKEVM = (
        "polygon_zkevm",
        1101,
        "polygon_zkevm",
        "Polygon zkEVM",
    )
    AVALANCHE = ("avalanche", 43114, "avalanche", "Avalanche")
    FANTOM = ("fantom", 250, "fantom", "Fantom")
    MOONBEAM = ("moonbeam", 1287, "moonbeam", "Moonbeam")

    # extra properties
    id: int
    database_name: str
    fantasy_name: str

    def __new__(
        self,
        value: str,
        id: int,
        database_name: str | None = None,
        fantasy_name: str | None = None,
    ):
        """_summary_

        Args:
            value (str): _description_
            id (int): _description_
            database_name (str | None, optional): . Defaults to value.
            fantasy_name (str | None, optional): . Defaults to value.

        Returns:
            _type_: _description_
        """
        obj = str.__new__(self, value)
        obj._value_ = value
        obj.id = id
        # optional properties
        obj.database_name = database_name or value.lower()
        obj.fantasy_name = fantasy_name or value.lower()
        return obj


class Protocol(str, Enum):
    #            ( value , url, fantasy_name )
    GAMMA = ("gamma", "Gamma Strategies")

    ALGEBRAv3 = ("algebrav3", "UniswapV3")
    UNISWAPv3 = ("uniswapv3", "AlgebraV3")

    QUICKSWAP = ("quickswap", "QuickSwap")
    UNISWAP = ("uniswap", "Uniswap")
    ZYBERSWAP = ("zyberswap", "Zyberswap")
    THENA = ("thena", "Thena")
    GLACIER = ("glacier", "Glacier")
    SPIRITSWAP = ("spiritswap", "SpiritSwap")
    CAMELOT = ("camelot", "Camelot")
    RETRO = ("retro", "Retro")
    STELLASWAP = ("stellaswap", "Stellaswap")
    BEAMSWAP = ("beamswap", "Beamswap")
    RAMSES = ("ramses", "Ramses")
    VEZARD = ("vezard", "veZard")

    # extra properties
    database_name: str
    fantasy_name: str

    def __new__(
        self,
        value: str,
        database_name: str | None = None,
        fantasy_name: str | None = None,
    ):
        """

        Args:
            value (_type_): chain name
            id (_type_): chain id

        Returns:
            : Chain
        """
        obj = str.__new__(self, value)
        obj._value_ = value
        # optional properties
        obj.database_name = database_name or value.lower()
        obj.fantasy_name = fantasy_name or value.lower()
        return obj


class databaseSource(str, Enum):
    """Data source for the prices saved in database"""

    THEGRAPH = "thegraph"
    COINGECKO = "coingecko"
    GECKOTERMINAL = "geckoterminal"
    ONCHAIN = "onchain"
    CACHE = "cache"
    MANUAL = "manual"
    AVERAGE = "average"

    # TODO: implement database_name and ...


class queueItemType(str, Enum):
    """Type of the queue database item"""

    REWARD_STATUS = "reward_status"  # database collection is called "rewards_status"
    REWARD_STATIC = "reward_static"  # database collection is called "rewards_static"
    HYPERVISOR_STATUS = "hypervisor_status"
    HYPERVISOR_STATIC = "hypervisor_static"
    PRICE = "price"
    BLOCK = "block"

    # TODO: implement database_name and ...
