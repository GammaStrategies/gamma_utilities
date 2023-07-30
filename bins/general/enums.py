from enum import Enum


class Chain(str, Enum):
    #      ( value , id , database_name, fantasy_name )
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
    MOONBEAM = ("moonbeam", 1284, "moonbeam", "Moonbeam")

    MANTLE = ("mantle", 5000, "mantle", "Mantle")

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
    #  ( value , database_name, fantasy_name )
    GAMMA = ("gamma", "gamma", "Gamma Strategies")

    ALGEBRAv3 = ("algebrav3", "algebrav3", "AlgebraV3")
    UNISWAPv3 = ("uniswapv3", "uniswapv3", "UniswapV3")

    PANCAKESWAP = ("pancakeswap", "pancakeswap", "Pancakeswap")  # univ3 mod

    BEAMSWAP = ("beamswap", "beamswap", "Beamswap")  # univ3 mod
    CAMELOT = ("camelot", "camelot", "Camelot")  # algebra mods
    QUICKSWAP = ("quickswap", "quickswap", "QuickSwap")
    ZYBERSWAP = ("zyberswap", "zyberswap", "Zyberswap")
    THENA = ("thena", "thena", "Thena")
    GLACIER = ("glacier", "glacier", "Glacier")
    SPIRITSWAP = ("spiritswap", "spiritswap", "SpiritSwap")
    SUSHI = ("sushi", "sushi", "Sushi")
    RETRO = ("retro", "retro", "Retro")
    STELLASWAP = ("stellaswap", "stellaswap", "Stellaswap")

    RAMSES = ("ramses", "ramses", "Ramses")
    VEZARD = ("vezard", "vezard", "veZard")
    EQUILIBRE = ("equilibre", "equilibre", "Equilibre")

    ASCENT = ("ascent", "ascent", "Ascent")

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


class ProtocolVersion(str, Enum):
    #  ( value , protocol, database_name, fantasy_name )
    GAMMA_v1 = ("gamma_v1", Protocol.GAMMA, "gamma_v1", "Gamma v1")
    GAMMA_v2 = ("gamma_v2", Protocol.GAMMA, "gamma_v2", "Gamma v2")

    ALGEBRAv3_v1 = ("algebrav3_v1", Protocol.ALGEBRAv3, "algebrav3_v1", "AlgebraV3 v1")
    ALGEBRAv3_v2 = ("algebrav3_v2", Protocol.ALGEBRAv3, "algebrav3_v2", "AlgebraV3 v2")

    UNISWAPv3 = ("uniswap_v3", Protocol.UNISWAPv3, "uniswap_v3", "Uniswap v3")
    UNISWAPv4 = ("uniswap_v4", Protocol.UNISWAPv3, "uniswap_v4", "Uniswap v4")

    PANCAKESWAP_v1 = (
        "pancakeswap_v1",
        Protocol.PANCAKESWAP,
        "pancakeswap_v1",
        "Pancakeswap v1",
    )  # univ3 mod
    BEAMSWAP_v1 = (
        "beamswap_v1",
        Protocol.BEAMSWAP,
        "beamswap_v1",
        "Beamswap v1",
    )  # univ3 mod
    CAMELOT_v1 = (
        "camelot_v1",
        Protocol.CAMELOT,
        "camelot_v1",
        "Camelot v1",
    )  # algebra mods
    QUICKSWAP_v1 = ("quickswap_v1", Protocol.QUICKSWAP, "quickswap_v1", "QuickSwap v1")
    ZYBERSWAP_v1 = ("zyberswap_v1", Protocol.ZYBERSWAP, "zyberswap_v1", "Zyberswap v1")
    THENA_v1 = ("thena_v1", Protocol.THENA, "thena_v1", "Thena v1")
    GLACIER_v1 = ("glacier_v1", Protocol.GLACIER, "glacier_v1", "Glacier v1")
    SPIRITSWAP_v1 = (
        "spiritswap_v1",
        Protocol.SPIRITSWAP,
        "spiritswap_v1",
        "SpiritSwap v1",
    )
    SUSHI_v1 = ("sushi_v1", Protocol.SUSHI, "sushi_v1", "Sushi v1")
    RETRO_v1 = ("retro_v1", Protocol.RETRO, "retro_v1", "Retro v1")
    STELLASWAP_v1 = (
        "stellaswap_v1",
        Protocol.STELLASWAP,
        "stellaswap_v1",
        "Stellaswap v1",
    )

    RAMSES_v1 = ("ramses_v1", Protocol.RAMSES, "ramses_v1", "Ramses v1")
    VEZARD_v1 = ("vezard_v1", Protocol.VEZARD, "vezard_v1", "veZard v1")
    EQUILIBRE_v1 = ("equilibre_v1", Protocol.EQUILIBRE, "equilibre_v1", "Equilibre v1")

    # extra properties
    protocol: Protocol
    database_name: str
    fantasy_name: str

    def __new__(
        self,
        value: str,
        protocol: Protocol,
        database_name: str | None = None,
        fantasy_name: str | None = None,
    ):
        obj = str.__new__(self, value)
        obj._value_ = value
        obj.protocol = protocol
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
    OPERATION = "operation"

    # TODO: implement database_name and ...


class rewarderType(str, Enum):
    GAMMA_masterchef_v1 = "gamma_masterchef_v1"
    GAMMA_masterchef_v2 = "gamma_masterchef_v2"

    ZYBERSWAP_masterchef_v1 = "zyberswap_masterchef_v1"
    ZYBERSWAP_masterchef_v1_rewarder = "zyberswap_masterchef_v1_rewarder"
    THENA_gauge_v2 = "thena_gauge_v2"
    THENA_voter_v3 = "thena_voter_v3"
    BEAMSWAP_masterchef_v2 = "beamswap_masterchef_v2"
    BEAMSWAP_masterchef_v2_rewarder = "beamswap_masterchef_v2_rewarder"

    ANGLE_MERKLE = "angle_merkle"

    RAMSES_v2 = "ramses_v2"


class Family_type(str, Enum):
    #      ( value , type , database_name, fantasy_name )

    REGISTRY_HYPERVISOR = ("hypervisor", "hypervisor", "Hypervisors registry")

    REGISTRY_MASTERCHEF = ("masterchef", "masterchef", "Masterchefs registry")
    REGISTRY_REWARDER = ("rewarder", "rewarder", "Rewarders registry")
    CHAIN = ("chain", "chain", "Chain")
    PROTOCOL = ("protocol", "protocol", "Protocol")
    DEX = ("dex", "dex", "Dex")

    # extra properties
    database_name: str
    fantasy_name: str

    def __new__(
        self,
        value: str,
        database_name: str | None = None,
        fantasy_name: str | None = None,
    ):
        obj = str.__new__(self, value)
        obj._value_ = value
        # optional properties
        obj.database_name = database_name or value.lower()
        obj.fantasy_name = fantasy_name or value.lower()
        return obj


# HELPERS
def text_to_chain(text: str) -> Chain:
    """Text to Chain conversion

    Args:
        text (str): what to find

    Returns:
        Chain:
    """
    for chain in Chain:
        if text.lower() in [
            chain.value.lower(),
            chain.database_name.lower(),
            chain.fantasy_name.lower(),
        ]:
            return chain

    if text.lower() == "polygon-zkevm":
        return Chain.POLYGON_ZKEVM
    elif text.lower() == "mainnet":
        return Chain.ETHEREUM
    raise ValueError(f"Chain with text {text} not found")


def text_to_protocol(text: str) -> Protocol:
    """Text to Protocol conversion

    Args:
        text (str): what to find

    Returns:
        Protocol:
    """
    for protocol in Protocol:
        if text.lower() in [
            protocol.value.lower(),
            protocol.database_name.lower(),
            protocol.fantasy_name.lower(),
        ]:
            return protocol
    raise ValueError(f"Protocol with text {text} not found")
