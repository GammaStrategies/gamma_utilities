from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import logging

from .general import dict_to_object, token_group


# def convert_object_to_dict(obj, key: str | None = None, filter: callable = None):
#     """Object to dict converter

#     Args:
#         obj ():
#         key (str, optional): . Defaults to None.

#     Returns:
#         dict | list | str | int | float | bool ...:
#     """
#     if isinstance(obj, dict):
#         data = {}
#         for k, v in obj.items():
#             data[k] = convert_object_to_dict(obj=v, key=key, filter=filter)
#         return data
#     elif hasattr(obj, "_ast"):
#         return convert_object_to_dict(obj=obj._ast(), key=key, filter=filter)
#     elif hasattr(obj, "__iter__") and not isinstance(obj, str):
#         return [convert_object_to_dict(obj=v, key=key, filter=filter) for v in obj]
#     elif hasattr(obj, "__dict__"):
#         data = dict(
#             [
#                 (key, convert_object_to_dict(obj=value, key=key, filter=filter))
#                 for key, value in obj.__dict__.items()
#                 if not callable(value) and not key.startswith("_")
#             ]
#         )
#         if key is not None and hasattr(obj, "__class__"):
#             data[key] = obj.__class__.__name__
#         return data
#     # ------------------ #
#     # custom conversions #
#     # ------------------ #
#     else:
#         if isinstance(obj, datetime):
#             return obj.strftime("%Y-%m-%d %H:%M:%S%z")
#         elif filter:
#             # apply filter
#             return filter(obj, key)

#         return obj
# def filter_mongodb(obj, key: str | None = None):
#     """Object to dict filter for mongodb objects"""

#     # convert any int [not timestamp nor block] to avoid mongoDB 8bit errors
#     if isinstance(obj, int) and key not in ["timestamp", "block"]:
#         return str(obj)


@dataclass
class time:
    block: int
    timestamp: int

    def to_dict(self) -> dict:
        return {"block": self.block, "timestamp": self.timestamp}


@dataclass
class token:
    address: str
    decimals: int
    time: time
    symbol: str | None = None
    # hypervisor status
    totalSupply: int | None = None

    def to_dict(self) -> dict:
        # create dict
        result = {
            "address": self.address,
            "decimals": self.decimals,
        }
        # add time
        result.update(self.time.to_dict())

        # add optionals when available
        if self.symbol:
            result["symbol"] = self.symbol
        if self.totalSupply:
            result["totalSupply"] = self.totalSupply

        # return dict
        return result


@dataclass
class fees:
    uncollected: token_group


@dataclass
class position:
    liquidity: int
    qtty: token_group
    lowerTick: int
    upperTick: int
    fees: fees


@dataclass
class positions:
    base: position | None = None
    limit: position | None = None


# address
# name
# time
#   block
#   timestamp
# positions
#   base
#     liquidity
#     qtty
#       token0
#       token1
#     lowerTick
#     upperTick
#     fees
#       uncollected
#          token0
#          token1
#   limit
#    ...
# aggregatedPositions
#   totalAmounts
#     token0
#     token1
#   totalFees
#     uncollected
#       token0
#       token1

#
# token
#   decimals
#   symbol
#   totalSupply
# limits
#   maxTotalSupply
#   maxDeposit0
#   maxDeposit1
# total
#   token0
#   token1
# pool
#   ...
# fee
# protocol | dex


class hypervisor_status(dict_to_object):
    def post_init(self):
        pass
