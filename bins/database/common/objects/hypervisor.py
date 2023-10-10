from copy import copy, deepcopy
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import logging

from bins.formulas.checks import toUint256


from ....formulas.full_math import mulDiv
from ....formulas.fees import calculate_gamma_fee, convert_feeProtocol
from ....general.enums import Chain, Protocol, text_to_protocol
from .general import dict_to_object, token_group_object


@dataclass
class time_object:
    block: int
    timestamp: int

    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp)

    def to_dict(self) -> dict:
        return {"block": self.block, "timestamp": self.timestamp}


@dataclass
class timeframe_object:
    ini: time_object
    end: time_object

    def to_dict(self) -> dict:
        return {"ini": self.ini.to_dict(), "end": self.end.to_dict()}


@dataclass
class token_object:
    address: str
    decimals: int
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
class fee_growth_object:
    index: int
    feeGrowthGlobalX128: int
    feeGrowthOutsideLowerX128: int
    feeGrowthOutsideUpperX128: int
    feeGrowthInsideLastX128: int


@dataclass
class fees_object:
    # all fees collected from current position
    collected_lp: token_group_object
    collected_gamma: token_group_object

    # fees uncollected ( not yet collected from current position)
    # total uncollected fees = lp + gamma
    uncollected_lp: token_group_object
    uncollected_gamma: token_group_object

    # fee growth data to calculate fees
    fee_growth: list[fee_growth_object]


@dataclass
class position_object:
    name: str
    liquidity: int
    qtty: token_group_object
    lowerTick: int
    upperTick: int

    fees: fees_object


def transformer_hypervisor_status(value, key: str):
    # convert bson objectID
    if isinstance(value, str):
        # check if string is objectID
        if key in ["qtty_token0", "qtty_token1"]:
            _tmp = float(value)
            if _tmp.is_integer():
                return toUint256(int(_tmp))
            else:
                return _tmp
        if key in ["decimals", "block", "timestamp"]:
            return int(value)

        if key == "dex":
            # convert dex to protocol
            return text_to_protocol(value)

        # check if string is float or int
        try:
            return int(value)
        except Exception:
            try:
                return float(value)
            except Exception:
                pass

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


class hypervisor_status_object(dict_to_object):
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

        _share_price = (
            (_lp_underlyingValue.token0 * token0_price)
            + (_lp_underlyingValue.token1 * token1_price)
        ) / _totalSupply

        # check if outlier:  >10**18 and _totalSupply > 1
        if _share_price > 10**18 and _totalSupply > 1:
            raise ValueError(
                f" Share price outlier: {_share_price}: totalSupply:{_totalSupply} t0:{token0_price} t1:{token1_price}"
            )

        return _share_price

    # inDecimal format
    def get_totalSupply_inDecimal(self) -> Decimal:
        return Decimal(str(self.totalSupply)) / (10**self.pool.token0.decimals)

    # fees
    def get_gamma_fee(self) -> int:
        """Return Gamma's fee protocol percentage over accrued fees by the positions

        Returns:
            float: gamma fee percentage
        """
        return calculate_gamma_fee(
            fee_rate=self.fee, protocol=text_to_protocol(self.dex)
        )

    def get_dex_protocol_fee(self) -> tuple[int, int]:
        """AMM feeProtocol. The dex fee charged on every swap, not collected by LPs

        Returns:
            tuple[int, int]]: feeProtocol0, feeProtocol1
        """
        # calculate protocol fees
        if hasattr(self.pool, "globalState"):
            protocol_fee_0_raw = self.pool.globalState.communityFeeToken0
            protocol_fee_1_raw = self.pool.globalState.communityFeeToken1
        else:
            # convert from 8 decimals
            protocol_fee_0_raw = self.pool.slot0.feeProtocol % 16
            protocol_fee_1_raw = self.pool.slot0.feeProtocol >> 4

        protocol_fee_0, protocol_fee_1 = convert_feeProtocol(
            feeProtocol0=protocol_fee_0_raw,
            feeProtocol1=protocol_fee_1_raw,
            hypervisor_protocol=self.dex,
            pool_protocol=self.pool.dex,
        )

        return protocol_fee_0, protocol_fee_1

    def get_pool_fee(self) -> tuple[float, float]:
        """Pool fee charged on every swap

        Returns:
            tuple[float, float]: pool fee
        """
        protocol = text_to_protocol(self.pool.dex)
        fee_tier0 = 0
        fee_tier1 = 0

        if protocol == Protocol.CAMELOT:
            try:
                # Camelot:  (pool.globalState().feeZto + pool.globalState().feeOtz)
                fee_tier0 = int(self.pool.globalState.feeZto)
                fee_tier1 = int(self.pool.globalState.feeOtz)
            except Exception as e:
                logging.getLogger(__name__).exception(f" getting pool fee   {e}")

        elif protocol == Protocol.RAMSES:
            # Ramses:  pool.currentFee()
            try:
                # 'currentFee' in here is actualy the 'fee' field
                fee_tier0 = int(self.pool.fee)
                fee_tier1 = int(self.pool.fee)

            except Exception as e:
                logging.getLogger(__name__).exception(f" getting pool fee   {e}")

        elif protocol in [Protocol.QUICKSWAP, Protocol.THENA, Protocol.ALGEBRAv3]:
            # QuickSwap + StellaSwap (Algebra V1):  pool.globalState().fee
            try:
                fee_tier0 = int(self.pool.globalState.fee)
                fee_tier1 = int(self.pool.globalState.fee)
            except Exception as e:
                logging.getLogger(__name__).exception(f" {e}")
        else:
            # Uniswap: pool.fee()
            try:
                fee_tier0 = int(self.pool.fee)
                fee_tier1 = int(self.pool.fee)
            except Exception as e:
                logging.getLogger(__name__).exception(f" {e}")

        fee_tier0 /= 1000000
        fee_tier1 /= 1000000

        return fee_tier0, fee_tier1

    def get_fees_uncollected(
        self, inDecimal: bool = True
    ) -> tuple[token_group_object, token_group_object]:
        """Return Gamma and LPs fees splited from uncollected fees

        Returns:
            tuple[gamma_fees, lp_fees]: fees split between Gamma and LPs
        """

        # check if the object has already the fees calculated ( some old db data may not contain this info)
        if hasattr(self.fees_uncollected, "gamma_qtty_token0"):
            _gamma_fees = token_group_object(
                token0=self.fees_uncollected.gamma_qtty_token0,
                token1=self.fees_uncollected.gamma_qtty_token1,
            )
            _lp_fees = token_group_object(
                token0=self.fees_uncollected.lps_qtty_token0,
                token1=self.fees_uncollected.lps_qtty_token1,
            )
        else:
            # calculate

            # gamma protocolFee
            _gamma_feeProtocol = self.get_gamma_fee()
            # gamma will take fees only if the uncollected fee qtty is greater than the minimum divisible unit per token
            # at least 2
            _gamma_fees = token_group_object(
                token0=mulDiv(
                    int(self.fees_uncollected.qtty_token0), _gamma_feeProtocol, 100
                ),
                token1=mulDiv(
                    int(self.fees_uncollected.qtty_token1), _gamma_feeProtocol, 100
                ),
            )

            # LPs fee
            _lp_fees = token_group_object(
                token0=int(self.fees_uncollected.qtty_token0) - _gamma_fees.token0,
                token1=int(self.fees_uncollected.qtty_token1) - _gamma_fees.token1,
            )

            # check that fees sum are equal to uncollected fees
            if (
                _gamma_fees.token0
                + _gamma_fees.token1
                + _lp_fees.token0
                + _lp_fees.token1
                != int(self.fees_uncollected.qtty_token0)
                + int(self.fees_uncollected.qtty_token1)
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

    def get_fees_collected(
        self, inDecimal: bool = True
    ) -> tuple[token_group_object, token_group_object]:
        """Fees collected by the current position in its all time lifespan

        Args:
            inDecimal (bool, optional): . Defaults to True.

        Returns:
            tuple[token_group_object, token_group_object]:
        """

        # check if the object has already the fees calculated
        if hasattr(self, "fees_collected"):
            _gamma_fees = token_group_object(
                token0=self.fees_collected.gamma_qtty_token0,
                token1=self.fees_collected.gamma_qtty_token1,
            )
            _lp_fees = token_group_object(
                token0=self.fees_collected.lps_qtty_token0,
                token1=self.fees_collected.lps_qtty_token1,
            )
        else:
            # TODO: calculate
            raise NotImplementedError(
                " fees collected in hype db object is not implemented yet, and its difficult to calculate"
            )
            return None, None

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

    def calculate_gross_fees(
        self,
        collected_fees0: Decimal,
        collected_fees1: Decimal,
    ) -> tuple[Decimal, Decimal]:
        """Calculate gross fees from any suppllied collected fees

        Args:
            collected_fees0 :
            collected_fees1 :

        Returns:
            tuple[float, float]: grossFees_0, grossFees_1
        """
        # calculate gross fees
        protocol_fee_0, protocol_fee_1 = self.get_dex_protocol_fee()

        if protocol_fee_0 < 100:
            grossFees_0 = collected_fees0 / Decimal(str(1 - (protocol_fee_0 / 100)))
        else:
            grossFees_0 = collected_fees0

        if protocol_fee_1 < 100:
            grossFees_1 = collected_fees1 / Decimal(str(1 - (protocol_fee_1 / 100)))
        else:
            grossFees_1 = collected_fees1

        return grossFees_0, grossFees_1

    def calculate_gross_volume(
        self, gross_fees0: Decimal, gross_fees1: Decimal
    ) -> tuple[Decimal, Decimal]:
        """Calculate gross fees volume using gross fees / the pool fee tier

        Args:
            gross_fees0 :
            gross_fees1 :

        Returns:
            tuple[float, float]: grossFees_0, grossFees_1
        """
        fee_tier_0, fee_tier_1 = self.get_pool_fee()
        return gross_fees0 / Decimal(str(fee_tier_0)), gross_fees1 / Decimal(
            str(fee_tier_1)
        )

    def get_underlying_value(self, inDecimal: bool = True) -> token_group_object:
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
        _totalAmounts = token_group_object(
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

    def get_inRange_liquidity(self) -> int:
        """Return liquidity in range of current tick

        Returns:
            int: liquidity in range
        """
        # get current tick
        current_tick = self.currentTick

        # get Gamma's inRange liquidity
        gamma_liquidity_in_range = 0
        if int(self.baseUpper) >= current_tick and int(self.baseLower) <= current_tick:
            gamma_liquidity_in_range += int(self.basePosition.liquidity)
        if (
            int(self.limitUpper) >= current_tick
            and int(self.limitLower) <= current_tick
        ):
            gamma_liquidity_in_range += int(self.limitPosition.liquidity)

        return gamma_liquidity_in_range

    def pre_subtraction(self, key: str, value: any):
        """Be aware that nested variables will not be considered hypervisor status so
        will never go thru this function--> use dict_object pre_subtraction instead

        """
        # call super pre_subtraction function
        return super().pre_subtraction(key=key, value=value)

    # def get_basePosition_data(self):

    #     if hasattr(self, "basePosition_data"):
    #         feeGrowthInside0LastX128 = self.basePosition_data.feeGrowthInside0LastX128
    #         feeGrowthInside1LastX128 = self.basePosition_data.feeGrowthInside1LastX128
    #     else:
    #         # calculate


############################################################################


@dataclass
class pool_database_object:
    address: str
    fee: int

    chain: Chain
    protocol: Protocol
    # dex

    time: time_object
    # block: int
    # timestamp: int

    tickSpacing: int

    tokens: list[token_object]

    protocolFees: list

    feeGrowthGlobal0X128: int
    feeGrowthGlobal1X128: int
    liquidity: int
    maxLiquidityPerTick: int

    slot0: dict

    sqrtPriceX96: int
    tick: int

    # {"sqrtPriceX96": "1947807860919824762980545924",
    # "tick": "-74117",
    # "observationIndex": "7",
    # "observationCardinality": "8",
    # "observationCardinalityNext": "8",
    # "feeProtocol": NumberInt(0),
    # "unlocked": true,}


@dataclass
class hypervisor_database_object:
    # _id:ObjectId
    id: str

    chain: Chain
    protocol: Protocol
    # dex

    # token_info: is the LP token information
    token_info: token_object
    # address: str
    # name: str
    # symbol: str
    # decimals: int
    # totalSupply: int

    maxTotalSupply: int
    deposit0Max: int
    deposit1Max: int

    time: time_object
    # block: int
    # timestamp: int

    positions: list[position_object]
    # baseLower / limitLower / baseUpper / limitUpper:int
    # basePosition / limitPosition : {
    #     liquidity:int
    #     amount0:int
    #     amount1:int

    # totalAmounts
    #   "total0": "96899826009063741615533",
    #   "total1": "58274707216027826624",

    fee: int

    pool: pool_database_object

    # po = {
    #     "fees_uncollected": {"qtty_token0": "0.0", "qtty_token1": "0.0"},
    #     "qtty_depoloyed": {
    #         "qtty_token0": "87894155111268607852544",
    #         "qtty_token1": "52895224729826058320",
    #         "fees_owed_token1": "7175975284136330",
    #         "fees_owed_token0": "2295857379051732021",
    #     },
    #     "tvl": {
    #         "parked_token0": "9003375040416080851840",
    #         "parked_token1": "5372306510917633682",
    #         "deployed_token0": "87894155111268607852544",
    #         "deployed_token1": "52895224729826058320",
    #         "fees_owed_token0": "2295857379051732021",
    #         "fees_owed_token1": "7175975284136330",
    #         "tvl_token0": "96899826009063740436405",
    #         "tvl_token1": "58274707216027828332",
    #     },
    # }
