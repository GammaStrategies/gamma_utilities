from dataclasses import dataclass, field, asdict, InitVar

from ...database.common.db_general_models import (
    tool_mongodb_general,
    tool_database_id,
)
from ...database.common.db_object_models import root_price


@dataclass
class root_operation(tool_mongodb_general, tool_database_id):
    operation_type: str
    network: str
    timestamp: str
    transactionHash: str
    block: int
    blockHash: str

    def create_id(self) -> str:
        return f"{self.network}_{self.transactionHash}"


@dataclass
class deposit_operation(root_operation, root_price):
    operation_type = "deposit"
    originator: str  #  deposit address originator
    shares: int  #  LP token qtty in return for deposited

    qtty_token0: int  #  token 0 qtty deposited
    qtty_token1: int

    qtty_total_in_usd: float  #  total qtty deposited
    qtty_total_in_token0: int  # total deposited qtty
    qtty_total_in_token1: int


@dataclass
class withdraw_operation(deposit_operation):
    operation_type = "withdraw"


@dataclass
class rebalance_operation(root_operation):
    operation_type = "rebalance"
    upperTick: int
    lowerTick: int


@dataclass
class fee_operation(root_operation, root_price):
    operation_type = "fee"
    gross_token0: int
    gross_token1: int
    gross_total_in_usd: float


@dataclass
class transfer_operation(root_operation, root_price):
    operation_type = "transfer"
    address: str
    source: str
    destination: str
    qtty: int
