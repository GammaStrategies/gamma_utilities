from copy import copy, deepcopy
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import logging

from bins.formulas.fees import calculate_gamma_fee
from bins.general.enums import text_to_protocol

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
class time_object:
    block: int
    timestamp: int

    def to_dict(self) -> dict:
        return {"block": self.block, "timestamp": self.timestamp}


@dataclass
class timeframe_object:
    ini: time_object
    end: time_object

    def to_dict(self) -> dict:
        return {"ini": self.ini.to_dict(), "end": self.end.to_dict()}


@dataclass
class token:
    address: str
    decimals: int
    time: time_object
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


def transformer_hypervisor_status(value, key: str):
    # convert bson objectID
    if isinstance(value, str):
        # check if string is float or int
        try:
            return int(value)
        except Exception:
            try:
                return float(value)
            except Exception:
                pass
        # check if string is objectID
        if key in ["qtty_token0", "qtty_token1"]:
            return float(value)
        if key in ["decimals", "block", "timestamp"]:
            return int(value)

        if key == "dex":
            # convert dex to protocol
            return text_to_protocol(value)

        # is actually a string
        return value
    else:
        return value


def transformer_clean_all(value, key: str):
    """All values go to zero of ''"""

    # convert bson objectID
    if isinstance(value, str):
        return ""
    elif isinstance(value, float):
        return 0.0
    elif isinstance(value, Decimal):
        return Decimal("0")
    else:
        return value


class hypervisor_status(dict_to_object):
    def post_init(self):
        pass

    def get_share_price(self, token0_price: Decimal, token1_price: Decimal) -> float:
        """Return share price, including uncollected fees
            (excluding gamma uncollected fees)

        Returns:
            float: share price
        """
        # convert to decimal when float
        if isinstance(token0_price, float):
            token0_price = Decimal(str(token0_price))
        if isinstance(token1_price, float):
            token1_price = Decimal(str(token1_price))

        _lp_underlyingValue = self.get_underlying_value(inDecimal=True)
        _totalSupply = self.get_totalSupply_inDecimal()
        return (
            (_lp_underlyingValue.token0 * token0_price)
            + (_lp_underlyingValue.token1 * token1_price)
        ) / _totalSupply

    # inDecimal format
    def get_totalSupply_inDecimal(self) -> Decimal:
        return Decimal(str(self.totalSupply)) / (10**self.pool.token0.decimals)

    # fees
    def get_protocol_fee(self) -> float:
        """Return Gamma's fee protocol percentage over accrued fees by the positions

        Returns:
            float: gamma fee percentage
        """
        return calculate_gamma_fee(
            fee_rate=self.fee, protocol=text_to_protocol(self.dex)
        )

    def get_fees_uncollected(
        self, inDecimal: bool = True
    ) -> tuple[token_group, token_group]:
        """Return Gamma and LPs fees splited from uncollected fees

        Returns:
            tuple[gamma_fees, lp_fees]: fees split between Gamma and LPs
        """

        # gamma protocolFee
        _gamma_feeProtocol = self.get_protocol_fee()
        _gamma_fees = token_group(
            token0=self.fees_uncollected.qtty_token0 * _gamma_feeProtocol,
            token1=self.fees_uncollected.qtty_token1 * _gamma_feeProtocol,
        )

        # LPs fee
        _lp_fees = token_group(
            token0=self.fees_uncollected.qtty_token0 - _gamma_fees.token0,
            token1=self.fees_uncollected.qtty_token1 - _gamma_fees.token1,
        )

        # check that fees sum are equal to uncollected fees
        if (
            _gamma_fees.token0 + _gamma_fees.token1 + _lp_fees.token0 + _lp_fees.token1
            != self.fees_uncollected.qtty_token0 + self.fees_uncollected.qtty_token1
        ):
            logging.getLogger(__name__).error(
                f" Fees sum error: {_gamma_fees.token0 + _gamma_fees.token1 + _lp_fees.token0 + _lp_fees.token1} != {self.fees_uncollected.qtty_token0 + self.fees_uncollected.qtty_token1}"
            )

        if inDecimal:
            # convert to decimals
            _gamma_fees.token0 = (
                Decimal(str(_gamma_fees.token0)) / 10**self.pool.token0.decimals
            )
            _gamma_fees.token1 = (
                Decimal(str(_gamma_fees.token1)) / 10**self.pool.token1.decimals
            )
            _lp_fees.token0 = (
                Decimal(str(_lp_fees.token0)) / 10**self.pool.token0.decimals
            )
            _lp_fees.token1 = (
                Decimal(str(_lp_fees.token1)) / 10**self.pool.token1.decimals
            )

        return _gamma_fees, _lp_fees

    def get_underlying_value(self, inDecimal: bool = True) -> token_group:
        """LPs underlying value, uncollected fees included

        Args:
            inDecimal (bool, optional): . Defaults to True.

        Returns:
            token_group: hypervisor underlying value qtty
        """

        # get uncollected fees
        _gamma_uncollected_fees, _lp_uncollected_fees = self.get_fees_uncollected(
            inDecimal=False
        )

        # get totalAmounts
        _totalAmounts = token_group(
            token0=self.totalAmounts.total0 + _lp_uncollected_fees.token0,
            token1=self.totalAmounts.total1 + _lp_uncollected_fees.token1,
        )

        if inDecimal:
            # convert to decimals
            _totalAmounts.token0 = (
                Decimal(str(_totalAmounts.token0)) / 10**self.pool.token0.decimals
            )
            _totalAmounts.token1 = (
                Decimal(str(_totalAmounts.token1)) / 10**self.pool.token1.decimals
            )

        return _totalAmounts

    def pre_subtraction(self, key: str, value: any):
        """Be aware that nested variables will not be considered hypervisor status so
        will never go thru this function--> use dict_object pre_subtraction instead

        """
        # call super pre_subtraction function
        return super().pre_subtraction(key=key, value=value)
