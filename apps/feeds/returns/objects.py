from dataclasses import dataclass
from decimal import Decimal
import logging

from datetime import datetime, timezone
from bins.database.common.database_ids import create_id_hypervisor_returns
from bins.database.common.objects.hypervisor import (
    hypervisor_status_object,
    transformer_hypervisor_status,
)
from bins.database.helpers import get_price_from_db
from bins.errors.general import ProcessingError
from bins.general.enums import Chain, error_identity, text_to_chain, text_to_protocol


@dataclass
class time_location:
    timestamp: int = None
    block: int = None

    @property
    def datetime(self) -> datetime:
        """UTC datetime from timestamp
        Can return None when timestamp is None"""
        return (
            datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
            if self.timestamp
            else None
        )

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "block": self.block,
        }


@dataclass
class period_timeframe:
    ini: time_location = None
    end: time_location = None

    @property
    def seconds(self) -> int:
        return self.end.timestamp - self.ini.timestamp

    @property
    def blocks(self) -> int:
        return self.end.block - self.ini.block

    def to_dict(self) -> dict:
        return {
            "ini": self.ini.to_dict(),
            "end": self.end.to_dict(),
            "seconds": self.seconds,
            "blocks": self.blocks,
        }

    def from_dict(self, item: dict):
        self.ini = time_location(
            timestamp=item["ini"]["timestamp"], block=item["ini"]["block"]
        )
        self.end = time_location(
            timestamp=item["end"]["timestamp"], block=item["end"]["block"]
        )


# TODO: merge token_group with database object
@dataclass
class token_group:
    token0: Decimal = None
    token1: Decimal = None

    def to_dict(self) -> dict:
        return {
            "token0": self.token0,
            "token1": self.token1,
        }

    def from_dict(self, item: dict):
        self.token0 = item["token0"]
        self.token1 = item["token1"]


@dataclass
class underlying_value:
    qtty: token_group = None
    details: dict = None

    def to_dict(self) -> dict:
        return {
            "qtty": self.qtty.to_dict(),
            "details": self.details or {},
        }

    def from_dict(self, item: dict):
        self.qtty = token_group()
        self.qtty.from_dict(item["qtty"])
        self.details = item["details"]


@dataclass
class qtty_usd_yield:
    qtty: token_group = None
    period_yield: Decimal = None

    def to_dict(self) -> dict:
        result = {}
        if self.qtty:
            result["qtty"] = self.qtty.to_dict()
        result["period_yield"] = self.period_yield
        return result

    def from_dict(self, item: dict):
        self.qtty = token_group()
        self.qtty.from_dict(item["qtty"])
        self.period_yield = item["period_yield"]


@dataclass
class rewards_group:
    usd: Decimal = None
    period_yield: Decimal = None
    details: list = None

    def to_dict(self) -> dict:
        return {
            "usd": self.usd,
            "period_yield": self.period_yield,
            "details": self.details or [],
        }

    def from_dict(self, item: dict):
        self.usd = item["usd"]
        self.period_yield = item["period_yield"]
        self.details = item["details"]


@dataclass
class status_group:
    prices: token_group = None
    underlying: underlying_value = None
    supply: Decimal = None

    def to_dict(self) -> dict:
        return {
            "prices": self.prices.to_dict(),
            "underlying": self.underlying.to_dict(),
            "supply": self.supply,
        }

    def from_dict(self, item: dict):
        self.prices = token_group()
        self.prices.from_dict(item["prices"])
        self.underlying = underlying_value()
        self.underlying.from_dict(item["underlying"])
        self.supply = item["supply"]


@dataclass
class period_status:
    ini: status_group = None
    end: status_group = None

    @property
    def supply_difference(self) -> Decimal:
        """Returns the difference in supply between end and ini

        Returns:
            Decimal:
        """
        return self.end.supply - self.ini.supply

    def to_dict(self) -> dict:
        return {
            "ini": self.ini.to_dict(),
            "end": self.end.to_dict(),
        }

    def from_dict(self, item: dict):
        self.ini = status_group()
        self.ini.from_dict(item["ini"])
        self.end = status_group()
        self.end.from_dict(item["end"])


@dataclass
class period_yield_data:
    """This class contains all the data needed to calculate the yield of a period
    for a given hypervisor.

        # The 'returns' object is a calculation of fees, rewards and impermanent yield over a period of time
        # comprehended between two consecutive hypervisor operations affecting its composition ( one of rebalance, zeroBurn, deposit, withdraw).
        # So, when a 'rebalance/...' happens, the return period starts and when the next 'rebalance/...' happen, the return period ends at that block -1
    """

    # hypervisor address
    address: str = None

    # period timeframe
    timeframe: period_timeframe = None

    # initial and end hypervisor snapshots
    status: period_status = None

    # fees collected during the period ( LPing ) using uncollected fees.
    # this is LPs fees ( not including gamma fees )
    fees: qtty_usd_yield = None
    fees_gamma: qtty_usd_yield = None

    # fees collected by the pool during the period ( calculated using fees+fees_gamma and pool.fee).
    # gross_fees: qtty_usd_yield = None

    # rewards collected during the period ( LPing ) using uncollected fees. This is not accurate when rewards do not include the "extra" info ( absolute qtty of rewards)
    rewards: rewards_group = None

    # fees collected right during block creation, reseting uncollected fees to zero. This is not used for yield calculation, but useful for analysis.
    fees_collected_within: qtty_usd_yield = None

    # Divergence Loss/Gain due to rebalance between hypervisor periods: ( hypervisor period 0 end block - hypervisor period 1 ini block )
    rebalance_divergence: token_group = None

    @property
    def id(self) -> str:
        return create_id_hypervisor_returns(
            hypervisor_address=self.address,
            ini_block=self.timeframe.ini.block,
            end_block=self.timeframe.end.block,
        )

    @property
    def period_blocks_qtty(self) -> int:
        return self.timeframe.blocks

    @property
    def period_seconds(self) -> int:
        return self.timeframe.seconds

    @property
    def period_days(self) -> float:
        return self.period_seconds / (24 * 60 * 60)

    @property
    def ini_underlying_usd(self) -> float:
        t0 = t1 = 0
        try:
            t0 = self.status.ini.underlying.qtty.token0 * self.status.ini.prices.token0
        except:
            pass
        try:
            t1 = self.status.ini.underlying.qtty.token1 * self.status.ini.prices.token1
        except:
            pass
        return t0 + t1

    @property
    def end_underlying_usd(self) -> float:
        t0 = t1 = 0
        try:
            t0 = self.status.end.underlying.qtty.token0 * self.status.end.prices.token0
        except:
            pass
        try:
            t1 = self.status.end.underlying.qtty.token1 * self.status.end.prices.token1
        except:
            pass
        return t0 + t1

    # LP FEES
    @property
    def period_fees_usd(self) -> float:
        """fees aquired during the period ( LPing ) using uncollected fees
            (using end prices)

        Returns:
            float:
        """
        t0 = t1 = 0
        try:
            t0 = self.fees.qtty.token0 * self.status.end.prices.token0
        except:
            pass
        try:
            t1 = self.fees.qtty.token1 * self.status.end.prices.token1
        except:
            pass
        return t0 + t1

    @property
    def period_impermanent_usd(self) -> float:
        """Impermanent divergence represents the value change in market prices and pool token weights

        Returns:
            float:
        """
        return self.period_impermanent_token0_usd + self.period_impermanent_token1_usd

    @property
    def period_impermanent_token0(self) -> float:
        """Impermanent divergence represents the value change in market prices and pool token weights

        Returns:
            float:
        """
        try:
            return (
                self.status.end.underlying.qtty.token0
                - self.status.ini.underlying.qtty.token0
                - self.fees.qtty.token0
            )
        except:
            return Decimal("0")

    @property
    def period_impermanent_token0_usd(self) -> float:
        """Impermanent token0 divergence represents the value change in market prices and pool token weights, converted to usd using end prices
            including rebalance divergence
        Returns:
            float:
        """
        return (self.period_impermanent_token0) * self.status.end.prices.token0

    @property
    def period_impermanent_token1(self) -> float:
        """Impermanent divergence represents the value change in market prices and pool token weights

        Returns:
            float:
        """
        try:
            return (
                self.status.end.underlying.qtty.token1
                - self.status.ini.underlying.qtty.token1
                - self.fees.qtty.token1
            )
        except:
            return Decimal("0")

    @property
    def period_impermanent_token1_usd(self) -> float:
        """Impermanent token1 divergence represents the value change in market prices and pool token weights, converted to usd using end prices
            including rebalance divergence
        Returns:
            float:
        """
        return (self.period_impermanent_token1) * self.status.end.prices.token1

    @property
    def period_impermanent_percentage_yield(self) -> float:
        """Impermanent divergence represents the value change in market prices and pool token weights

        Returns:
            float: _description_
        """
        return (
            self.period_impermanent_usd / self.ini_underlying_usd
            if self.ini_underlying_usd
            else 0
        )

    # price change
    @property
    def period_price_change_token0(self) -> float:
        """end price usd / ini price usd

        Returns:
            float:
        """
        return (
            (self.status.end.prices.token0 - self.status.ini.prices.token0)
            / self.status.ini.prices.token0
            if self.status.ini.prices.token0
            else 0
        )

    @property
    def period_price_change_token1(self) -> float:
        """end price usd / ini price usd

        Returns:
            float:
        """
        return (
            (self.status.end.prices.token1 - self.status.ini.prices.token1)
            / self.status.ini.prices.token1
            if self.status.ini.prices.token1
            else 0
        )

    @property
    def period_price_change_usd(self) -> float:
        """end price usd / ini price usd

        Returns:
            float:
        """
        return (
            (
                self.status.end.prices.token0
                + self.status.end.prices.token1
                - self.status.ini.prices.token0
                - self.status.ini.prices.token1
            )
            / (self.status.ini.prices.token0 + self.status.ini.prices.token1)
            if (self.status.ini.prices.token0 + self.status.ini.prices.token1) > 0
            else 0
        )

    @property
    def price_per_share(self) -> float:
        """Returns the price per share at the end of the period

        Returns:
            float:
        """
        return (
            self.end_underlying_usd / self.status.end.supply
            if self.status.end.supply
            else 0
        )

    @property
    def price_per_share_at_ini(self) -> float:
        """Returns the price per share at the ini of the period

        Returns:
            float:
        """
        return (
            self.ini_underlying_usd / self.status.ini.supply
            if self.status.ini.supply
            else 0
        )

    @property
    def fees_per_share(self) -> float:
        """Return the fees_per_share collected during the period

        Returns:
            float:
        """
        return (
            self.period_fees_usd / self.status.end.supply
            if self.status.end.supply
            else 0
        )

    @property
    def fees_per_share_percentage_yield(self) -> float:
        """Return the fees_per_share collected during the period
            ( as a percentage of the price per share at the beginning of the period)

        Returns:
            float:
        """
        try:
            return self.fees_per_share / self.price_per_share_at_ini
        except:
            pass
        return 0

    @property
    def impermanent_per_share(self) -> float:
        """Return the difference between the price per share at the end of the period and the price per share at the beginning of the period
        and subtract the fees_per_share collected during the period
        """
        return (
            (
                self.end_underlying_usd / self.status.end.supply
                if self.status.end.supply
                else 0
            )
            - (
                self.ini_underlying_usd / self.status.ini.supply
                if self.status.ini.supply
                else 0
            )
            - self.fees_per_share
        )

    @property
    def impermanent_per_share_percentage_yield(self) -> float:
        """Return the difference between the price per share at the end of the period and the price per share at the beginning of the period
        and subtract the fees_per_share collected during the period
        """
        try:
            return self.impermanent_per_share / self.price_per_share_at_ini
        except:
            pass
        return 0

    @property
    def rewards_per_share(self) -> float:
        """Return the rewards_per_share collected during the period

        Returns:
            float:
        """
        try:
            return self.rewards.usd / self.status.end.supply
        except:
            return 0

    @property
    def rewards_per_share_percentage_yield(self) -> float:
        """Return the rewards_per_share collected during the period

        Returns:
            float:
        """
        try:
            return self.rewards.period_yield / self.price_per_share_at_ini
        except:
            return 0

    # init
    def reset_status(self):
        self.status = period_status(
            ini=status_group(
                prices=token_group(),
                underlying=underlying_value(qtty=token_group()),
            ),
            end=status_group(
                prices=token_group(),
                underlying=underlying_value(qtty=token_group()),
            ),
        )

    def reset_fees(self):
        # lps fees
        self.fees = qtty_usd_yield(qtty=token_group())
        # gamma fees
        self.fees_gamma = qtty_usd_yield(qtty=token_group())

    def reset_rewards(self):
        self.rewards = rewards_group()

    def reset_rebalance_divergence(self):
        self.rebalance_divergence = token_group(
            token1=Decimal("0"), token0=Decimal("0")
        )

    # check
    def check_inconsistencies(
        self,
        hype_differences: hypervisor_status_object,
        network: str | None = None,
    ):
        # SUPPLY DIFFERENCE ##########################################################
        # check if supply at ini and end is the same
        supply_diff = hype_differences.get_totalSupply_inDecimal()
        if abs(supply_diff) > Decimal("0.000000999"):
            # do not process already indentified errors
            if network == Chain.ETHEREUM.database_name and self.address in [
                "0xf0a9f5c64f80fa390a46b298791dab9e2bb29bca"  # old visor
            ]:
                logging.getLogger(__name__).warning(
                    f" Hypervisor supply at START differ {supply_diff:,.5%} from END, but hype is identified as old known problem. Skipping."
                )
                return

            # raise error to rescrape
            raise ProcessingError(
                chain=text_to_chain(network),
                item={
                    "hypervisor_address": self.address,
                    "dex": hype_differences.dex.database_name,
                    "ini_block": self.timeframe.ini.block,
                    "end_block": self.timeframe.end.block,
                    "supply_difference": supply_diff,
                    "ini_supply": self.status.ini.supply,
                    "end_supply": self.status.end.supply,
                },
                identity=error_identity.SUPPLY_DIFFERENCE,
                action="rescrape",
                message=f" Hypervisor supply at START differ {supply_diff:,.5%} from END, meaning there are missing operations in between. Rescrape.",
            )
        # ###########################################################################

        # PRICES ####################################################################
        # check if prices are set or can be set from database
        if not network and (
            not self.status.ini.prices.token0
            or not self.status.ini.prices.token1
            or not self.status.end.prices.token0
            or not self.status.end.prices.token1
        ):
            raise Exception(
                " Either network variable must be defined or the token prices have to be previously provided using set_prices function "
            )
        # ###########################################################################

    def check_fees(
        self, hypervisor_symbol: str, hypervisor_name: str, dex: str, network: str
    ):
        # control var
        _ignore = False
        # when negative fees, quantify ( 0.0001 negative fee should not be a huge problem)
        if self.fees.qtty.token0 > Decimal("-0.000001") and self.fees.qtty.token0 < 0:
            logging.getLogger(__name__).warning(
                f" Although hypervisor {self.address} has negative fee growth on token0, they are small enough to be zeroed. Fees: {self.fees.qtty.token0}"
            )
            self.fees.qtty.token0 = Decimal("0")
            _ignore = True

        if self.fees.qtty.token1 > Decimal("-0.000001") and self.fees.qtty.token1 < 0:
            logging.getLogger(__name__).warning(
                f" Although hypervisor {self.address} has negative fee growth on token1, they are small enough to be zeroed. Fees:  {self.fees.qtty.token1}"
            )
            self.fees.qtty.token1 = Decimal("0")
            _ignore = True

        # exit when ignore
        if _ignore:
            return

        # check for positive fee growth
        if self.fees.qtty.token0 < 0 or self.fees.qtty.token1 < 0:
            # raise error to rescrape
            raise ProcessingError(
                chain=text_to_chain(network),
                item={
                    "hypervisor_address": self.address,
                    "hypervisor_symbol": hypervisor_symbol,
                    "hypervisor_name": hypervisor_name,
                    "dex": dex,
                    "ini_block": self.timeframe.ini.block,
                    "end_block": self.timeframe.end.block,
                    "fees_token0": self.fees.qtty.token0 + self.fees_gamma.qtty.token0,
                    "fees_token1": self.fees.qtty.token1 + self.fees_gamma.qtty.token1,
                    "description": " Check if it is related to the old visor contract.",
                },
                identity=error_identity.NEGATIVE_FEES,
                action="rescrape",
                message=f" Fees growth can't be negative and they are [0:{self.fees.qtty.token0} 1:{self.fees.qtty.token1}] for {network} hypervisor {self.address} ini block {self.timeframe.ini.block} end block {self.timeframe.end.block}.",
            )

    # SETUP FUNCTIONS
    def set_prices(
        self, token0_price: Decimal, token1_price: Decimal, position: str = "ini"
    ):
        """fill prices for a given position ( ini or end )

        Args:
            token0_price (Decimal):
            token1_price (Decimal):
            position (str, optional): . Defaults to "ini".
        """
        # create a new status object if needed
        if not self.status:
            self.reset_status()
        # fill prices
        if position == "ini":
            self.status.ini.prices = token_group(
                token0=token0_price, token1=token1_price
            )
        elif position == "end":
            self.status.end.prices = token_group(
                token0=token0_price, token1=token1_price
            )
        else:
            raise ValueError(f"position {position} not valid")

    def set_rebalance_divergence(self, token0: Decimal | int, token1: Decimal | int):
        """Save rebalance divergence for this period

        Args:
            token0 (Decimal | int):
            token1 (Decimal | int):
        """
        # convert to Decimal if needed
        if isinstance(token0, int):
            token0 = Decimal(str(token0))
        if isinstance(token1, int):
            token1 = Decimal(str(token1))
        self.rebalance_divergence = token_group(token0=token0, token1=token1)

    def fill_from_hypervisors_data(
        self,
        ini_hype: dict,
        end_hype: dict,
        network: str | None = None,
    ):
        """fill this object data using the hypervisors data provided in the arguments

        Args:
            ini_hype (dict):
            end_hype (dict):
            network (str | None, optional): . Defaults to None.

        """
        # convert hypervisors to objects for easier manipulation
        _ini_hype = hypervisor_status_object(
            transformer=transformer_hypervisor_status, **ini_hype
        )
        _end_hype = hypervisor_status_object(
            transformer=transformer_hypervisor_status, **end_hype
        )
        # calc. hype differences
        hype_differences = _end_hype - _ini_hype

        # setup basic object info
        self.address = end_hype["address"]
        # new timeframe
        self.timeframe = period_timeframe(
            ini=time_location(timestamp=_ini_hype.timestamp, block=_ini_hype.block),
            end=time_location(timestamp=_end_hype.timestamp, block=_end_hype.block),
        )
        # new status
        if not self.status:
            self.reset_status()
        # new fees
        if not self.fees:
            self.reset_fees()
        # new rewards
        if not self.rewards:
            self.reset_rewards()
        # new rebalance divergence
        if not self.rebalance_divergence:
            self.reset_rebalance_divergence()

        # set supply
        self.status.ini.supply = _ini_hype.get_totalSupply_inDecimal()
        self.status.end.supply = _end_hype.get_totalSupply_inDecimal()

        # check inconsistencies
        self.check_inconsistencies(hype_differences=hype_differences, network=network)

        # fill missing prices from database
        if network:
            if not self.status.ini.prices.token0:
                # get token prices at ini and end blocks from database
                self.status.ini.prices.token0 = Decimal(
                    str(
                        get_price_from_db(
                            network=network,
                            block=_ini_hype.block,
                            token_address=_ini_hype.pool.token0.address,
                        )
                    )
                )
            if not self.status.end.prices.token0:
                self.status.end.prices.token0 = Decimal(
                    str(
                        get_price_from_db(
                            network=network,
                            block=_end_hype.block,
                            token_address=_end_hype.pool.token0.address,
                        )
                    )
                )
            if not self.status.ini.prices.token1:
                self.status.ini.prices.token1 = Decimal(
                    str(
                        get_price_from_db(
                            network=network,
                            block=_ini_hype.block,
                            token_address=_ini_hype.pool.token1.address,
                        )
                    )
                )
            if not self.status.end.prices.token1:
                self.status.end.prices.token1 = Decimal(
                    str(
                        get_price_from_db(
                            network=network,
                            block=_end_hype.block,
                            token_address=_end_hype.pool.token1.address,
                        )
                    )
                )

        # # calculate this period's fees accrued: using uncollected fees
        (
            uncollected_fees_gamma,
            uncollected_fees_lps,
        ) = hype_differences.get_fees_uncollected()
        # gamma fees
        self.fees_gamma.qtty.token0 = uncollected_fees_gamma.token0
        self.fees_gamma.qtty.token1 = uncollected_fees_gamma.token1
        # lps fees
        self.fees.qtty.token0 = uncollected_fees_lps.token0
        self.fees.qtty.token1 = uncollected_fees_lps.token1

        # check fees
        self.check_fees(
            hypervisor_symbol=hype_differences.symbol,
            hypervisor_name=hype_differences.name,
            dex=hype_differences.dex.database_name,
            network=network,
        )

        # get collected fees within the period, if any
        if _ini_hype.operations:
            # initialize control vars
            lastperiod_fees_token0_collected = Decimal("0")
            lastperiod_fees_token1_collected = Decimal("0")
            for operation in _ini_hype.operations:
                if operation.topic in ["rebalance", "zeroBurn"]:
                    # add collected fees to control vars
                    lastperiod_fees_token0_collected += Decimal(
                        str(
                            int(operation.qtty_token0) / (10**operation.decimals_token0)
                        )
                    )
                    lastperiod_fees_token1_collected += Decimal(
                        str(
                            int(operation.qtty_token1) / (10**operation.decimals_token1)
                        )
                    )

            # set collected fees
            self.fees_collected_within = qtty_usd_yield(
                qtty=token_group(
                    token0=lastperiod_fees_token0_collected,
                    token1=lastperiod_fees_token1_collected,
                )
            )

        # get underlying qtty: LP's underlying qtty only ( so uncollected Gamma fees are not included in the underlying qtty)
        _underlying_ini = _ini_hype.get_underlying_value(inDecimal=True)
        _underlying_end = _end_hype.get_underlying_value(inDecimal=True)
        # initial underlying ( including fees uncollected )
        self.status.ini.underlying.qtty.token0 = _underlying_ini.token0
        self.status.ini.underlying.qtty.token1 = _underlying_ini.token1
        # end underlying ( including fees uncollected ) : can differ from ini tvl when asset prices or weights change
        self.status.end.underlying.qtty.token0 = _underlying_end.token0
        self.status.end.underlying.qtty.token1 = _underlying_end.token1

        # Yield percentage
        self.fees.period_yield = (
            (self.period_fees_usd / self.ini_underlying_usd)
            if self.ini_underlying_usd
            else 0
        )
        # gamma yield ( vs initial underlying value )
        self.fees_gamma.period_yield = (
            (
                self.fees_gamma.qtty.token0 * self.status.end.prices.token0
                + self.fees_gamma.qtty.token1 * self.status.end.prices.token1
            )
            / self.ini_underlying_usd
            if self.ini_underlying_usd
            else 0
        )

    def fill_from_rewards_data(self, ini_rewards: list[dict], end_rewards: list[dict]):
        """fill period rewards data from rewards data

        Args:
            ini_rewards (list[dict]): Should have same block and timestamp
            end_rewards (list[dict]): Should have same block and timestamp
        """

        # exit when no rewards to process
        if not ini_rewards and not end_rewards:
            logging.getLogger(__name__).debug(
                f" No rewards to process for {self.address}."
            )
            return

        # process rewards using rewardToken
        # when no ini rewards but end, use end rewards at ini block (  self.timeframe.ini.block or timestamp  )
        # when no end rewards but ini, use ini rewards at end block (  self.timeframe.end.block or timestamp  )

        # group ini and end rewards by rewardToken + rewarder_address
        # can happen that no ini rewards are found but end only...
        grouped_rewards = {}  # {tokenAddress_rewarderAddress: { ini: } }
        for end_reward in end_rewards:
            # create key
            _dictkey = f"{end_reward['rewardToken']}_{end_reward['rewarder_address']}"
            # create basic struct
            grouped_rewards[_dictkey] = {"ini": None, "end": end_reward}
            for ini_reward in ini_rewards:
                if (
                    ini_reward["rewarder_address"] == end_reward["rewarder_address"]
                    and ini_reward["rewardToken"] == end_reward["rewardToken"]
                ):
                    grouped_rewards[_dictkey]["ini"] = ini_reward
                    break
        for ini_reward in ini_rewards:
            # create key
            _dictkey = f"{ini_reward['rewardToken']}_{ini_reward['rewarder_address']}"
            # create basic struct
            if not _dictkey in grouped_rewards:
                grouped_rewards[_dictkey] = {"ini": ini_reward, "end": None}
            else:
                grouped_rewards[_dictkey]["ini"] = ini_reward

        # init rewards
        self.rewards = rewards_group(
            usd=Decimal("0"), period_yield=Decimal("0"), details=[]
        )
        # init cumulative rewards
        yield_cumulative = Decimal("0")
        total_period_seconds = 0

        # process grouped rewards
        for item in grouped_rewards.values():
            if not item["ini"]:
                # no ini rewards found for this item
                #  create a dummy ini reward with end characteristics
                logging.getLogger(__name__).error(
                    f" No initial rewards found for {item}. Using a dummy ini reward instead with the same end characteristics."
                )
                item["ini"] = {}
                item["ini"]["timestamp"] = (
                    item["end"]["timestamp"] - self.period_seconds
                )
                item["ini"]["rewards_perSecond"] = item["end"]["rewards_perSecond"]
                item["ini"]["rewardToken_decimals"] = item["end"][
                    "rewardToken_decimals"
                ]
                item["ini"]["total_hypervisorToken_qtty"] = item["end"][
                    "total_hypervisorToken_qtty"
                ]
                item["ini"]["hypervisor_share_price_usd"] = item["end"][
                    "hypervisor_share_price_usd"
                ]
                # continue
            if not item["end"]:
                #  create a dummy end reward with ini characteristics
                logging.getLogger(__name__).error(
                    f" No end rewards found for {item}. Using a dummy end reward instead with the same ini characteristics."
                )
                item["end"] = {}
                item["end"]["timestamp"] = (
                    item["ini"]["timestamp"] + self.period_seconds
                )
                item["end"]["rewardToken_symbol"] = item["ini"]["rewardToken_symbol"]
                item["end"]["rewardToken"] = item["ini"]["rewardToken"]
                item["end"]["rewards_perSecond"] = item["ini"]["rewards_perSecond"]
                item["end"]["rewardToken_decimals"] = item["ini"][
                    "rewardToken_decimals"
                ]
                item["end"]["rewardToken_price_usd"] = item["ini"][
                    "rewardToken_price_usd"
                ]
                item["end"]["total_hypervisorToken_qtty"] = item["ini"][
                    "total_hypervisorToken_qtty"
                ]
                item["end"]["hypervisor_share_price_usd"] = item["ini"][
                    "hypervisor_share_price_usd"
                ]
                # continue

            # seconds passed
            period_seconds = item["end"]["timestamp"] - item["ini"]["timestamp"]
            # check if period differs from expected
            if abs(period_seconds - self.period_seconds):
                raise ValueError(
                    f" Rewards period differs from expected. Expected: {self.period_seconds} seconds. Found: {period_seconds} seconds."
                )
            # add seconds to total
            total_period_seconds += period_seconds

            # calculate rewards qtty
            _period_rewards_qtty = 0
            _ini_rewards_qtty = 0
            _end_rewards_qtty = 0

            ##########################################################
            # rewards can't be xpresses in absolut terms without looking into claims ( per user ).
            # But we can get a MAX/MIN rewards qtty for the period looking at the rewardsPerSecond
            # so that:
            #   MAX REWARDS QTTY = (end_rewards_perSecond * period_seconds - 1) + ini_end_rewards_perSecond
            #   MIN REWARDS QTTY = (ini_end_rewards_perSecond * period_seconds - 1) + end_end_rewards_perSecond
            #  AVERAGE REWARDS QTTY = (ini_end_rewards_perSecond + end_end_rewards_perSecond) / 2
            #
            #  rationale behind:
            #  MAX_REWARDS_QTTY = One second after initial snapshot, the reward rate changed to end_rewards_perSecond
            #  MIN_REWARDS_QTTY = One second before end snapshot, the reward rate changed to end_rewards_perSecond
            ##########################################################
            _maximum_rewards_qtty = (
                (float(item["end"]["rewards_perSecond"]) * (period_seconds - 1))
                + float(item["ini"]["rewards_perSecond"])
            ) / (10 ** item["end"]["rewardToken_decimals"])
            _minimum_rewards_qtty = (
                (float(item["ini"]["rewards_perSecond"]) * (period_seconds - 1))
                + float(item["end"]["rewards_perSecond"])
            ) / (10 ** item["end"]["rewardToken_decimals"])
            _period_rewards_qtty = (_maximum_rewards_qtty + _minimum_rewards_qtty) / 2

            # calculate usd value
            _period_rewards_usd = (
                _period_rewards_qtty * item["end"]["rewardToken_price_usd"]
            )

            # add usd value to self
            self.rewards.usd += Decimal(str(_period_rewards_usd))

            total_staked_usd = (
                int(item["ini"]["total_hypervisorToken_qtty"]) / (10**18)
            ) * item["ini"]["hypervisor_share_price_usd"]
            # when there is no staked value, use total supply
            if not total_staked_usd:
                logging.getLogger(__name__).debug(
                    f" No staked value found processing return rewards for {self.address}. Using total supply."
                )
                total_staked_usd = (
                    float(self.status.ini.supply)
                    * item["ini"]["hypervisor_share_price_usd"]
                )

            # add reward detail to self
            self.rewards.details.append(
                {
                    "symbol": item["end"]["rewardToken_symbol"],
                    "address": item["end"]["rewardToken"],
                    "qtty": _period_rewards_qtty,
                    "usd": _period_rewards_usd,
                    "seconds": period_seconds,
                    "period yield": _period_rewards_usd / total_staked_usd,
                }
            )

            # add details to status when possible
            if _ini_rewards_qtty or _end_rewards_qtty:
                # initialize vars

                if self.status and self.status.ini and self.status.end:
                    if not self.status.ini.underlying:
                        self.status.ini.underlying = underlying_value(
                            qtty=token_group()
                        )
                    if not self.status.end.underlying:
                        self.status.end.underlying = underlying_value(
                            qtty=token_group()
                        )

                    if not self.status.ini.underlying.details:
                        self.status.ini.underlying.details = {}
                    if not "rewards" in self.status.ini.underlying.details:
                        self.status.ini.underlying.details["rewards"] = {}
                    if not self.status.end.underlying.details:
                        self.status.end.underlying.details = {}
                    if not "rewards" in self.status.end.underlying.details:
                        self.status.end.underlying.details["rewards"] = {}
                    # add to details
                    for i, qtty in [
                        ("ini", _ini_rewards_qtty),
                        ("end", _end_rewards_qtty),
                    ]:
                        if (
                            not item[i]["rewardToken"]
                            in getattr(self.status, i).underlying.details["rewards"]
                        ):
                            getattr(self.status, i).underlying.details["rewards"][
                                item[i]["rewardToken"]
                            ] = {
                                "qtty": qtty,
                                "usd": qtty * item[i]["rewardToken_price_usd"],
                                "counter": 1,
                            }
                        else:
                            # sum reward qtty and usd and add count
                            getattr(self.status, i).underlying.details["rewards"][
                                item[i]["rewardToken"]
                            ]["qtty"] += qtty
                            getattr(self.status, i).underlying.details["rewards"][
                                item[i]["rewardToken"]
                            ]["usd"] += (qtty * item[i]["rewardToken_price_usd"])
                            getattr(self.status, i).underlying.details["rewards"][
                                item[i]["rewardToken"]
                            ]["counter"] += 1

            # add apr and apy to cumulative
            # add to cumulative yield
            if yield_cumulative:
                yield_cumulative *= 1 + Decimal(
                    str(_period_rewards_usd / total_staked_usd)
                )
            else:
                yield_cumulative = 1 + Decimal(
                    str(_period_rewards_usd / total_staked_usd)
                )

        # Yield percentage: percentage of total value staked at ini

        if yield_cumulative:
            yield_cumulative -= 1

        if self.rewards.usd < 0:
            raise ValueError(f" Rewards usd can't be negative {self.rewards.usd}")

        # CONVERT results to self period
        # We want to xtrapolate to the self.period_seconds
        # xtrapolate rewards for the period
        self.rewards.period_yield = (
            ((yield_cumulative / total_period_seconds) * self.period_seconds)
            if total_period_seconds
            else 0
        )

        # TODO: remove next log line
        logging.getLogger(__name__).debug(
            f" --- Rewards period yield changed from  {yield_cumulative:,.2%}  ->  {self.rewards.period_yield:,.2%}"
        )

    # CONVERTER
    def to_dict(self) -> dict:
        """convert this object to a dictionary
        Returns:
        {
            "id": ,
            "address":,
            "timeframe": {
                "ini": {
                    "timestamp": ,
                    "block": ,
                    },
                "end": {
                    "timestamp": ,
                    "block": ,
                    },
                "seconds": ,
                "blocks": ,
            },
            "status": {
                "ini": {
                    "prices": {
                        "token0": "",
                        "token1": "",
                    },
                    "underlying": {
                        "qtty": {
                            "token0": "",
                            "token1": "",
                        },
                        "details": {},
                        "usd": "",
                    },
                    "supply": "",
                },
                "end": {
                    "prices": {
                        "token0": "",
                        "token1": "",
                    },
                    "underlying_value": {
                        "qtty": {
                            "token0": "",
                            "token1": "",
                        },
                        "usd": "",
                    },
                    "supply": "",
                },
            },
            "fees": {
                "collected":{
                    "protocol": {
                        "qtty": {
                            "token0": "",
                            "token1": "",
                        },
                        "usd": "",
                        "period_yield": "",
                    }
                    "lps": {
                        "qtty": {
                            "token0": "",
                            "token1": "",
                        },
                        "usd": "",
                        "period_yield": "",
                    }
                }
                "uncollected":{
                    "protocol": {}
                    "lps": {}
                }

            },
            "rewards": {
                "usd": "",
                "period_yield": "",
                "details": [],
            },
            "impermanent": {
                "usd": "",
                "percentage_yield": "",
                "qtty": {
                    "token0": "",
                    "token1": "",
                },
                rebalance_divergence: {
                    "token0": "",
                    "token1": "",
                },
            },

            "pool":{
                "gamma_gross_
                    "calculated_gamma_volume_usd": "",
                }

        }
        """
        result = {
            "id": self.id,
            "address": self.address,
            "timeframe": self.timeframe.to_dict(),
            "status": self.status.to_dict(),
            "fees": self.fees.to_dict(),
            "fees_gamma": self.fees_gamma.to_dict(),
            "rewards": self.rewards.to_dict(),
            "fees_collected_within": (
                self.fees_collected_within.to_dict()
                if self.fees_collected_within
                else qtty_usd_yield(
                    qtty=token_group(Decimal("0"), Decimal("0")),
                    period_yield=Decimal("0"),
                ).to_dict()
            ),
        }

        return result

    def from_dict(self, item: dict):
        """fill this object data from a dictionary

        Args:
            item (dict): dictionary with the data
        """
        # address
        self.address = item["address"]
        # timeframe
        self.timeframe = period_timeframe()
        self.timeframe.from_dict(item["timeframe"])
        # status
        self.status = period_status()
        self.status.from_dict(item["status"])
        # fees
        self.fees = qtty_usd_yield()
        self.fees.from_dict(item["fees"])
        # fees_gamma
        self.fees_gamma = qtty_usd_yield()
        self.fees_gamma.from_dict(item["fees_gamma"])
        # rewards
        self.rewards = rewards_group()
        self.rewards.from_dict(item["rewards"])
        # fees_collected_within
        self.fees_collected_within = qtty_usd_yield()
        self.fees_collected_within.from_dict(item["fees_collected_within"])
