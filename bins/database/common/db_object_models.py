from dataclasses import dataclass, field, asdict, InitVar
from ...database.common.db_general_models import (
    tool_mongodb_general,
    tool_database_id,
)

from datetime import datetime


@dataclass
class token(tool_mongodb_general, tool_database_id):
    address: str
    symbol: int
    chain: str
    position: int  # token pool position

    def create_id(self) -> str:
        return f"{self.chain}_{self.address}"

    def _fill_fromDict(self, data: dict):
        """fill class fields from dictionary

        Args:
            data (dict): _description_

        Returns:
            _type_: _description_
        """
        self.id_ = data["id"]
        self.symbol = data["symbol"]
        self.chain = data["chain"]
        self.position = data["position"]


# not used
@dataclass
class pool(tool_mongodb_general, tool_database_id):
    address: str
    chain: str
    fee: int
    tokens: list[token]

    def create_id(self) -> str:
        return f"{self.chain}_{self.address}"

    def _fill_fromDict(self, data: dict):
        """create class from dictionary

        Args:
            data (dict): _description_

        Returns:
            _type_: _description_
        """
        self.address = data["address"]
        self.chain = data["chain"]
        self.fee = data["fee"]
        self.tokens = [
            token(x["address"], x["symbol"], x["chain"], x["position"])
            for x in data["tokens"]
        ]


# GLOBAL DATABASE
@dataclass
class block(tool_mongodb_general, tool_database_id):
    """id: <network>_<block_number>
    network:
    block:
    timestamp:
    """

    network: str
    block: int
    timestamp: datetime.timestamp

    def create_id(self) -> str:
        return f"{self.network}_{self.block}"


@dataclass
class usd_price(tool_mongodb_general, tool_database_id):
    """id: <network>_<block_number>_<address>
    network:
    block:
    address:
    price:
    """

    network: str
    block: int
    address: str
    price_usd: float

    def create_id(self) -> str:
        return f"{self.network}_{self.block}_{self.address}"


@dataclass
class static(tool_mongodb_general, tool_database_id):
    network: str
    address: str  # hypervisor id
    created: datetime.timestamp
    name: str
    pool: pool

    def create_id(self) -> str:
        return f"{self.network}_{self.address}"


# not used
@dataclass
class status(tool_mongodb_general, tool_database_id):
    network: str
    block: int
    address: str  # hypervisor id
    qtty_token0: float  # token qtty   (this is tvl = deployed_qtty + owed fees + parked_qtty )
    qtty_token1: float  #
    deployed_token0: float  # tokens deployed into pool
    deployed_token1: float  #
    parked_token0: float  # tokens sitting in hype contract ( sleeping )
    parked_token1: float  #
    supply: float  # total Suply

    def create_id(self) -> str:
        return f"{self.network}_{self.block}_{self.address}"


# not used
class root_price:
    pass
