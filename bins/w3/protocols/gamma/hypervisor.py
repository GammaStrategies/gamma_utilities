from copy import deepcopy
from decimal import Decimal
from web3 import Web3


from ....config.current import WEB3_CHAIN_IDS  # ,CFG
from ....cache import cache_utilities
from ....general.enums import Protocol
from ..general import (
    bep20,
    bep20_multicall,
    erc20,
    erc20_cached,
    bep20_cached,
    erc20_multicall,
)
from ...helpers.multicaller import build_call, execute_multicall
from ..uniswap.pool import (
    poolv3,
    poolv3_bep20,
    poolv3_bep20_multicall,
    poolv3_cached,
    poolv3_bep20_cached,
    ABI_FILENAME as POOL_ABI_FILENAME,
    ABI_FOLDERNAME as POOL_ABI_FOLDERNAME,
    poolv3_multicall,
)
from ....formulas.fees import calculate_gamma_fee

ABI_FILENAME = "hypervisor_modded"
ABI_FOLDERNAME = "gamma"
DEX_NAME = Protocol.GAMMA.database_name
INMUTABLE_FIELDS = {
    "decimals": True,  # decimals should be always fixed
    "symbol": False,
    "factory": False,
    "fee": False,
    "deposit0Max": False,
    "deposit1Max": False,
    "directDeposit": False,
    "feeRecipient": False,
    "maxTotalSupply": False,
    "name": False,
    "owner": False,
    "tickSpacing": False,
    "token0": True,
    "token1": True,
    "pool": True,
}


class gamma_hypervisor(erc20):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

        self._pool: poolv3 | None = None
        self._token0: erc20 | None = None
        self._token1: erc20 | None = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    def identify_dex_name(self) -> str:
        return DEX_NAME

    def inmutable_fields(self) -> dict[str, bool]:
        """inmutable fields by contract

        Returns:
            dict[str, bool]:  field name: is inmutable?
        """
        return INMUTABLE_FIELDS

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        fixed_fields = self.inmutable_fields()

        # create cache helper
        self._cache = cache_utilities.mutable_property_cache(
            filename=cache_filename,
            folder_name="data/cache/onchain",
            reset=False,
            fixed_fields=fixed_fields,
        )

    # PROPERTIES
    @property
    def baseUpper(self) -> int:
        """baseUpper _summary_

        Returns:
            _type_: 0 int24
        """
        return self.call_function_autoRpc("baseUpper")

    @property
    def baseLower(self) -> int:
        """baseLower _summary_

        Returns:
            _type_: 0 int24
        """
        return self.call_function_autoRpc("baseLower")

    @property
    def currentTick(self) -> int:
        """currentTick _summary_

        Returns:
            int: -78627 int24
        """
        return self.call_function_autoRpc("currentTick")

    @property
    def deposit0Max(self) -> int:
        """deposit0Max _summary_

        Returns:
            float: 1157920892373161954234007913129639935 uint256
        """
        return self.call_function_autoRpc("deposit0Max")

    @property
    def deposit1Max(self) -> int:
        """deposit1Max _summary_

        Returns:
            int: 115792089237 uint256
        """
        return self.call_function_autoRpc("deposit1Max")

    # v1 contracts have no directDeposit
    @property
    def directDeposit(self) -> bool:
        """v1 contracts have no directDeposit function

        Returns:
            bool:
        """
        return self.call_function_autoRpc("directDeposit")

    @property
    def fee(self) -> int:
        """fee

        Returns:
            int: 10 uint8 ( in polygon is uint24)
        """
        return self.call_function_autoRpc("fee")

    # v1 contracts have no feeRecipient
    @property
    def feeRecipient(self) -> str:
        """v1 contracts have no feeRecipient function

        Returns:
            str: address
        """
        return self.call_function_autoRpc("feeRecipient")

    @property
    def getBasePosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993 uint128
               amount0     72329994  uint256
               amount1     565062023318300677907  uint256
               }
        """
        tmp = self.call_function_autoRpc("getBasePosition")
        return {
            "liquidity": tmp[0],
            "amount0": tmp[1],
            "amount1": tmp[2],
        }

    @property
    def getLimitPosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993 uint128
               amount0     72329994 uint256
               amount1     565062023318300677907 uint256
               }
        """
        if tmp := self.call_function_autoRpc("getLimitPosition"):
            return {
                "liquidity": tmp[0],
                "amount0": tmp[1],
                "amount1": tmp[2],
            }
        else:
            raise ValueError(f" getTotalAmounts function call returned None")

    @property
    def getTotalAmounts(self) -> dict:
        """

        Returns:
           _type_: total0   2902086313 uint256
                   total1  565062023318300678136 uint256
        """
        if tmp := self.call_function_autoRpc("getTotalAmounts"):
            return {
                "total0": tmp[0],
                "total1": tmp[1],
            }
        else:
            raise ValueError(f" getTotalAmounts function call returned None")

    @property
    def limitLower(self) -> int:
        """limitLower _summary_

        Returns:
            int: 0 int24
        """
        return self.call_function_autoRpc("limitLower")

    @property
    def limitUpper(self) -> int:
        """limitUpper _summary_

        Returns:
            int: 0 int24
        """
        return self.call_function_autoRpc("limitUpper")

    @property
    def maxTotalSupply(self) -> int:
        """maxTotalSupply _summary_

        Returns:
            int: 0 uint256
        """
        return self.call_function_autoRpc("maxTotalSupply")

    @property
    def name(self) -> str:
        return self.call_function_autoRpc("name")

    def nonces(self, owner: str):
        return self.call_function_autoRpc("nonces", None, Web3.toChecksumAddress(owner))

    @property
    def owner(self) -> str:
        return self.call_function_autoRpc("owner")

    @property
    def pool(self) -> poolv3:
        if self._pool is None:
            self._pool = poolv3(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def tickSpacing(self) -> int:
        """tickSpacing _summary_

        Returns:
            int: 60 int24
        """
        return self.call_function_autoRpc("tickSpacing")

    @property
    def token0(self) -> erc20:
        if self._token0 is None:
            self._token0 = erc20(
                address=self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 is None:
            self._token1 = erc20(
                address=self.call_function_autoRpc("token1"),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def block(self) -> int:
        return self._block

    @block.setter
    def block(self, value):
        self._block = value
        self.pool.block = value
        self.token0.block = value
        self.token1.block = value

    @property
    def custom_rpcType(self) -> str | None:
        """ """
        return self._custom_rpcType

    @custom_rpcType.setter
    def custom_rpcType(self, value: str | None):
        """ """
        if not isinstance(value, str):
            raise ValueError(f"custom_rpcType must be a string")
        self._custom_rpcType = value
        self.pool.custom_rpcType = value
        self.token0.custom_rpcType = value
        self.token1.custom_rpcType = value

    # CUSTOM FUNCTIONS

    def get_gamma_fee(self) -> int:
        """Calculate the gamma fee percentage over accrued fees by the positions

        Returns:
            int: gamma fee percentage ( 0-100 )
        """
        return calculate_gamma_fee(
            fee_rate=self.fee, protocol=Protocol(self.identify_dex_name())
        )

    def get_all_events(self):
        return NotImplementedError("get_all_events not implemented for v1 contracts")
        # return [
        #     event.createFilter(fromBlock=self.block)
        #     for event in self.contract.events
        #     if issubclass(event, TransactionEvent) # only get transaction events
        # ]

    def get_qtty_depoloyed(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Returns:
           dict: {
                   "qtty_token0":0,         # quantity of token 0 deployed in dex
                   "qtty_token1":0,         # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,    # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,    # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """
        # positions
        base = self.pool.get_qtty_depoloyed(
            ownerAddress=self.address,
            tickUpper=self.baseUpper,
            tickLower=self.baseLower,
            inDecimal=inDecimal,
        )
        limit = self.pool.get_qtty_depoloyed(
            ownerAddress=self.address,
            tickUpper=self.limitUpper,
            tickLower=self.limitLower,
            inDecimal=inDecimal,
        )

        # add up
        return {k: base.get(k, 0) + limit.get(k, 0) for k in set(base) & set(limit)}

    def get_fees_uncollected(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Returns:
            dict: {
                    "qtty_token0":0,  # quantity of uncollected token 0
                    "qtty_token1":0,  # quantity of uncollected token 1
                    "gamma_qtty_token0":
                    "gamma_qtty_token1":
                    "lps_qtty_token0":
                    "lps_qtty_token1":
                }
        """
        # positions
        base = self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.baseUpper,
            tickLower=self.baseLower,
            protocolFee=self.get_gamma_fee(),
            inDecimal=inDecimal,
        )
        limit = self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.limitUpper,
            tickLower=self.limitLower,
            protocolFee=self.get_gamma_fee(),
            inDecimal=inDecimal,
        )

        result = {k: base.get(k, 0) + limit.get(k, 0) for k in set(base) & set(limit)}

        # result["base_position"] = base
        # result["limit_position"] = limit

        return result

    def get_fees_collected(self, inDecimal: bool = True) -> dict:
        # positions
        base = self.pool.get_fees_collected(
            ownerAddress=self.address,
            tickUpper=self.baseUpper,
            tickLower=self.baseLower,
            protocolFee=self.get_gamma_fee(),
            inDecimal=inDecimal,
        )
        limit = self.pool.get_fees_collected(
            ownerAddress=self.address,
            tickUpper=self.limitUpper,
            tickLower=self.limitLower,
            protocolFee=self.get_gamma_fee(),
            inDecimal=inDecimal,
        )

        return {k: base.get(k, 0) + limit.get(k, 0) for k in set(base) & set(limit)}

    def get_tvl(self, inDecimal=True) -> dict:
        """get total value locked of both positions
           TVL = deployed + parked + owed

        Returns:
           dict: {" tvl_token0": ,      (int or Decimal) sum of below's token 0 (total)
                   "tvl_token1": ,      (int or Decimal)
                   "deployed_token0": , (int or Decimal) quantity of token 0 LPing
                   "deployed_token1": , (int or Decimal)
                   "fees_owed_token0": ,(int or Decimal) fees owed to the position by dex
                   "fees_owed_token1": ,(int or Decimal)
                   "parked_token0": ,   (int or Decimal) quantity of token 0 parked at contract (not deployed)
                   "parked_token1": ,   (int or Decimal)
                   }
        """
        # get deployed fees as int ( force no decimals)
        deployed = self.get_qtty_depoloyed(inDecimal=False)

        result = {"parked_token0": self.pool.token0.balanceOf(self.address)}

        result["parked_token1"] = self.pool.token1.balanceOf(self.address)

        result["deployed_token0"] = deployed["qtty_token0"]
        result["deployed_token1"] = deployed["qtty_token1"]
        result["fees_owed_token0"] = deployed["fees_owed_token0"]
        result["fees_owed_token1"] = deployed["fees_owed_token1"]

        # sumup
        result["tvl_token0"] = (
            result["deployed_token0"]
            + result["fees_owed_token0"]
            + result["parked_token0"]
        )
        result["tvl_token1"] = (
            result["deployed_token1"]
            + result["fees_owed_token1"]
            + result["parked_token1"]
        )

        if inDecimal:
            # convert to decimal
            for key in result:
                if "token0" in key:
                    result[key] = Decimal(result[key]) / Decimal(
                        10**self.token0.decimals
                    )
                elif "token1" in key:
                    result[key] = Decimal(result[key]) / Decimal(
                        10**self.token1.decimals
                    )
                else:
                    raise ValueError(f"Cant convert '{key}' field to decimal")

        return result.copy()

    # Convert

    def as_dict(
        self, convert_bint=False, static_mode: bool = False, minimal: bool = False
    ) -> dict:
        """as_dict

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.
            minimal (bool, optional): only get the minimal indispensable fields ( 44 calls + errors) vs (82+errors)

            # multicall 82->1 call

        Returns:
            dict:
        """
        result = super().as_dict(convert_bint=convert_bint, minimal=minimal)

        if not minimal:
            # not minimal
            result["name"] = self.name
            result["fee"] = self.fee
            result["deposit0Max"] = (
                str(self.deposit0Max) if convert_bint else self.deposit0Max
            )
            result["deposit1Max"] = (
                str(self.deposit1Max) if convert_bint else self.deposit1Max
            )

        result["pool"] = self.pool.as_dict(
            convert_bint=convert_bint, static_mode=static_mode
        )

        # identify hypervisor dex
        result["dex"] = self.identify_dex_name()

        # result["directDeposit"] = self.directDeposit  # not working

        # only return when static mode is off
        if not static_mode:
            self._as_dict_not_static_items(convert_bint, result, minimal)
        else:
            # save feeRecipient only in static mode
            try:
                # fee recipient
                result["feeRecipient"] = self.feeRecipient.lower()
            except Exception as e:
                # this hype version may not have feeRecipient func
                pass
        return result

    def _as_dict_not_static_items(self, convert_bint, result, minimal: bool = False):
        # cached variables
        _baseLower = self.baseLower
        _baseUpper = self.baseUpper
        _limitLower = self.limitLower
        _limitUpper = self.limitUpper

        result["baseLower"] = str(_baseLower) if convert_bint else _baseLower
        result["baseUpper"] = str(_baseUpper) if convert_bint else _baseUpper
        result["currentTick"] = (
            str(self.currentTick) if convert_bint else self.currentTick
        )

        result["limitLower"] = str(_limitLower) if convert_bint else _limitLower
        result["limitUpper"] = str(_limitUpper) if convert_bint else _limitUpper

        # getTotalAmounts
        result["totalAmounts"] = self.getTotalAmounts
        if convert_bint:
            result["totalAmounts"]["total0"] = str(result["totalAmounts"]["total0"])
            result["totalAmounts"]["total1"] = str(result["totalAmounts"]["total1"])

        # uncollected fees
        result["fees_uncollected"] = self.get_fees_uncollected(
            inDecimal=(not convert_bint)
        )
        if convert_bint:
            for k in result["fees_uncollected"].keys():
                result["fees_uncollected"][k] = str(result["fees_uncollected"][k])

        # positions
        result["basePosition"] = self.getBasePosition
        if convert_bint:
            self._as_dict_convert_helper(result, "basePosition")
        result["limitPosition"] = self.getLimitPosition
        if convert_bint:
            self._as_dict_convert_helper(result, "limitPosition")

        # not minimal
        if not minimal:
            result["maxTotalSupply"] = (
                str(self.maxTotalSupply) if convert_bint else self.maxTotalSupply
            )

            # TVL
            result["tvl"] = self.get_tvl(inDecimal=(not convert_bint))
            if convert_bint:
                for k in result["tvl"].keys():
                    result["tvl"][k] = str(result["tvl"][k])

            # Deployed
            result["qtty_depoloyed"] = self.get_qtty_depoloyed(
                inDecimal=(not convert_bint)
            )
            if convert_bint:
                for k in result["qtty_depoloyed"].keys():
                    result["qtty_depoloyed"][k] = str(result["qtty_depoloyed"][k])

            # collected fees
            result["fees_collected"] = self.get_fees_collected(
                inDecimal=(not convert_bint)
            )
            if convert_bint:
                for k in result["fees_collected"].keys():
                    result["fees_collected"][k] = str(result["fees_collected"][k])
            # spacing
            result["tickSpacing"] = (
                str(self.tickSpacing) if convert_bint else self.tickSpacing
            )

            # positions specifics
            result["basePosition_data"] = self.pool.position(
                ownerAddress=Web3.toChecksumAddress(self.address.lower()),
                tickLower=_baseLower,
                tickUpper=_baseUpper,
            )
            if convert_bint:
                self._as_dict_convert_helper_positions(
                    result=result,
                    position_key="basePosition_data",
                    exclue_conversion=["lastLiquidityAddTimestamp"],
                )
            result["basePosition_ticksLower"] = self.pool.ticks(_baseLower)
            if convert_bint:
                self._as_dict_convert_helper_positions(
                    result=result,
                    position_key="basePosition_ticksLower",
                    exclue_conversion=[""],
                )
            result["basePosition_ticksUpper"] = self.pool.ticks(_baseUpper)
            if convert_bint:
                self._as_dict_convert_helper_positions(
                    result=result,
                    position_key="basePosition_ticksUpper",
                    exclue_conversion=[""],
                )

            result["limitPosition_data"] = self.pool.position(
                ownerAddress=Web3.toChecksumAddress(self.address.lower()),
                tickLower=_limitLower,
                tickUpper=_limitUpper,
            )
            if convert_bint:
                self._as_dict_convert_helper_positions(
                    result=result,
                    position_key="limitPosition_data",
                    exclue_conversion=["lastLiquidityAddTimestamp"],
                )
            result["limitPosition_ticksLower"] = self.pool.ticks(_limitLower)
            if convert_bint:
                self._as_dict_convert_helper_positions(
                    result=result,
                    position_key="limitPosition_ticksLower",
                    exclue_conversion=[""],
                )

            result["limitPosition_ticksUpper"] = self.pool.ticks(_limitUpper)
            if convert_bint:
                self._as_dict_convert_helper_positions(
                    result=result,
                    position_key="limitPosition_ticksUpper",
                    exclue_conversion=[""],
                )

    def _as_dict_convert_helper(self, result, arg1):
        result[arg1]["liquidity"] = str(result[arg1]["liquidity"])
        result[arg1]["amount0"] = str(result[arg1]["amount0"])
        result[arg1]["amount1"] = str(result[arg1]["amount1"])

    def _as_dict_convert_helper_positions(
        self, result: dict, position_key: str, exclue_conversion: list[str] = []
    ):
        # convert to string all fields but lastLiquidityAddTimestamp
        for k in result[position_key].keys():
            # do not convert boolean
            if isinstance(result[position_key][k], bool):
                continue

            if k not in exclue_conversion:
                result[position_key][k] = str(result[position_key][k])

    # dict from multicall
    def _get_dict_from_multicall(self) -> dict:
        """Only this object and its super() will be returned as dict ( no pools nor objects within this)
             When any of the function is not returned ( for any reason ), its key will not appear in the result.
            All functions defined in the abi without "inputs" will be returned here
        Returns:
            dict:
        """
        #
        result = super()._get_dict_from_multicall()

        # build fixed objects like (pool, token0, token1..)
        self._build_fixed_objects_from_multicall_result(multicall_result=result)
        # format list vars ( like getTotalAmounts...)
        result = self._convert_formats_multicall_result(multicall_result=result)
        return result

    def _build_fixed_objects_from_multicall_result(self, multicall_result: dict):
        # build fixed objects like (pool, token0, token1..)
        try:
            if self._pool is None:
                self._pool = poolv3(
                    address=multicall_result["pool"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass
        try:
            if self._token0 is None:
                self._token0 = erc20(
                    address=multicall_result["token0"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass
        try:
            if self._token1 is None:
                self._token1 = erc20(
                    address=multicall_result["token1"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass

    def _convert_formats_multicall_result(self, multicall_result: dict) -> dict:
        # format list vars ( like getTotalAmounts...)
        try:
            multicall_result["getTotalAmounts"] = {
                "total0": multicall_result["getTotalAmounts"][0],
                "total1": multicall_result["getTotalAmounts"][1],
            }
        except:
            pass
        try:
            multicall_result["getLimitPosition"] = {
                "liquidity": multicall_result["getLimitPosition"][0],
                "amount0": multicall_result["getLimitPosition"][1],
                "amount1": multicall_result["getLimitPosition"][2],
            }
        except:
            pass
        try:
            multicall_result["getBasePosition"] = {
                "liquidity": multicall_result["getBasePosition"][0],
                "amount0": multicall_result["getBasePosition"][1],
                "amount1": multicall_result["getBasePosition"][2],
            }
        except:
            pass

        return multicall_result


class gamma_hypervisor_cached(erc20_cached, gamma_hypervisor):
    SAVE2FILE = True

    # PROPERTIES
    @property
    def baseLower(self) -> int:
        prop_name = "baseLower"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def baseUpper(self) -> int:
        prop_name = "baseUpper"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def currentTick(self) -> int:
        prop_name = "currentTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def deposit0Max(self) -> int:
        prop_name = "deposit0Max"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def deposit1Max(self) -> int:
        prop_name = "deposit1Max"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def directDeposit(self) -> bool:
        prop_name = "directDeposit"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def fee(self) -> int:
        prop_name = "fee"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def feeRecipient(self) -> str:
        prop_name = "feeRecipient"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getBasePosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993
               amount0     72329994
               amount1     565062023318300677907
               }
        """
        prop_name = "getBasePosition"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result.copy()

    @property
    def getLimitPosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993
               amount0     72329994
               amount1     565062023318300677907
               }
        """
        prop_name = "getLimitPosition"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result.copy()

    @property
    def getTotalAmounts(self) -> dict:
        """_

        Returns:
           _type_: total0   2902086313
                   total1  565062023318300678136
        """
        prop_name = "getTotalAmounts"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )

        return result.copy()

    @property
    def limitLower(self) -> int:
        prop_name = "limitLower"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def limitUpper(self) -> int:
        prop_name = "limitUpper"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def maxTotalSupply(self) -> int:
        prop_name = "maxTotalSupply"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def name(self) -> str:
        prop_name = "name"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def owner(self) -> str:
        prop_name = "owner"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def pool(self) -> poolv3_cached:
        if self._pool is None:
            # check if cached
            prop_name = "pool"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            self._pool = poolv3_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def tickSpacing(self) -> int:
        prop_name = "tickSpacing"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def token0(self) -> erc20_cached:
        if self._token0 is None:
            # check if cached
            prop_name = "token0"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            self._token0 = erc20_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20_cached:
        if self._token1 is None:
            # check if cached
            prop_name = "token1"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            self._token1 = erc20_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def witelistedAddress(self) -> str:
        prop_name = "witelistedAddress"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result


class gamma_hypervisor_multicall(gamma_hypervisor):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
        known_data: dict | None = None,
        pool_abi_filename: str = "",
        pool_abi_path: str = "",
    ):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"
        self._pool_abi_filename = pool_abi_filename or POOL_ABI_FILENAME
        self._pool_abi_path = (
            pool_abi_path or f"{self.abi_root_path}/{POOL_ABI_FOLDERNAME}"
        )

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        if known_data:
            self._fill_from_known_data(known_data=known_data)

    # PROPERTIES
    @property
    def name(self) -> str:
        return self._name

    @property
    def totalSupply(self) -> int:
        return self._totalSupply

    @property
    def symbol(self) -> int:
        return self._symbol

    @property
    def decimals(self) -> int:
        return self._decimals

    @property
    def baseLower(self) -> int:
        return self._baseLower

    @property
    def baseUpper(self) -> int:
        return self._baseUpper

    @property
    def currentTick(self) -> int:
        return self._currentTick

    @property
    def deposit0Max(self) -> int:
        return self._deposit0Max

    @property
    def deposit1Max(self) -> int:
        return self._deposit1Max

    @property
    def directDeposit(self) -> bool:
        return self._directDeposit

    @property
    def fee(self) -> int:
        return self._fee

    @property
    def feeRecipient(self) -> str:
        return self._feeRecipient

    @property
    def getBasePosition(self) -> dict:
        return deepcopy(self._getBasePosition)

    @property
    def getLimitPosition(self) -> dict:
        return deepcopy(self._getLimitPosition)

    @property
    def getTotalAmounts(self) -> dict:
        return deepcopy(self._getTotalAmounts)

    @property
    def limitLower(self) -> int:
        return self._limitLower

    @property
    def limitUpper(self) -> int:
        return self._limitUpper

    @property
    def maxTotalSupply(self) -> int:
        return self._maxTotalSupply

    @property
    def name(self) -> str:
        return self._name

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def pool(self) -> poolv3_multicall:
        return self._pool

    @property
    def tickSpacing(self) -> int:
        return self._tickSpacing

    @property
    def token0(self) -> erc20_multicall:
        return self._token0

    @property
    def token1(self) -> erc20_multicall:
        return self._token1

    @property
    def witelistedAddress(self) -> str:
        return self._witelistedAddress

    #################

    @property
    def custom_rpcType(self) -> str | None:
        """ """
        return self._custom_rpcType

    @custom_rpcType.setter
    def custom_rpcType(self, value: str | None):
        """ """
        if not isinstance(value, str):
            raise ValueError(f"custom_rpcType must be a string")
        try:
            self._custom_rpcType = value
            self.pool.custom_rpcType = value
            self.token0.custom_rpcType = value
            self.token1.custom_rpcType = value
        except:
            pass

    def fill_with_multicall(
        self,
        pool_address: str | None = None,
        token0_address: str | None = None,
        token1_address: str | None = None,
    ):
        data = execute_multicall(
            network=self._network,
            block=self.block,
            hypervisor_address=self._address,
            pool_address=pool_address,
            token0_address=token0_address,
            token1_address=token1_address,
            hypervisor_abi_filename=self._abi_filename,
            hypervisor_abi_path=self._abi_path,
            pool_abi_filename=self._pool_abi_filename,
            pool_abi_path=self._pool_abi_path,
            convert_bint=False,
        )

        # fill addresses
        if pool_address:
            data["pool"]["address"] = pool_address
        if token0_address:
            data["token0"]["address"] = token0_address
        if token1_address:
            data["token1"]["address"] = token1_address

        self._fill_from_known_data(known_data=data)

    def _fill_from_known_data(self, known_data: dict):
        # ERC20
        self._name = known_data["name"]
        self._symbol = known_data["symbol"]
        self._decimals = known_data["decimals"]
        self._totalSupply = known_data["totalSupply"]
        #
        self._baseLower = known_data["baseLower"]
        self._baseUpper = known_data["baseUpper"]
        self._currentTick = known_data["currentTick"]
        self._deposit0Max = known_data["deposit0Max"]
        self._deposit1Max = known_data["deposit1Max"]
        self._directDeposit = known_data.get("directDeposit", None)
        self._fee = known_data["fee"]
        self._feeRecipient = known_data.get("feeRecipient", None)
        self._getBasePosition = {
            "liquidity": known_data["getBasePosition"][0],
            "amount0": known_data["getBasePosition"][1],
            "amount1": known_data["getBasePosition"][2],
        }
        self._getLimitPosition = {
            "liquidity": known_data["getLimitPosition"][0],
            "amount0": known_data["getLimitPosition"][1],
            "amount1": known_data["getLimitPosition"][2],
        }
        self._getTotalAmounts = {
            "total0": known_data["getTotalAmounts"][0],
            "total1": known_data["getTotalAmounts"][1],
        }
        self._limitLower = known_data["limitLower"]
        self._limitUpper = known_data["limitUpper"]
        self._maxTotalSupply = known_data["maxTotalSupply"]
        self._name = known_data["name"]
        self._owner = known_data["owner"]
        self._tickSpacing = known_data["tickSpacing"]
        self._whitelistedAddress = known_data.get("whitelistedAddress", None)

        # add tokens to pool
        known_data["pool"]["token0"] = known_data["token0"]
        known_data["pool"]["token1"] = known_data["token1"]

        # create objects
        self._fill_from_known_data_objects(known_data=known_data)

    def _fill_from_known_data_objects(self, known_data: dict):
        self._pool = poolv3_multicall(
            address=known_data["pool"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["pool"],
        )
        self._token0 = erc20_multicall(
            address=known_data["token0"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token0"],
        )
        self._token1 = erc20_multicall(
            address=known_data["token1"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token1"],
        )


class gamma_hypervisor_bep20(bep20, gamma_hypervisor):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

        self._pool: poolv3_bep20 | None = None
        self._token0: bep20 | None = None
        self._token1: bep20 | None = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def pool(self) -> poolv3_bep20:
        if self._pool is None:
            self._pool = poolv3_bep20(
                address=self.call_function_autoRpc("pool"),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def token0(self) -> bep20:
        if self._token0 is None:
            self._token0 = bep20(
                address=self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> bep20:
        if self._token1 is None:
            self._token1 = bep20(
                address=self.call_function_autoRpc("token1"),
                network=self._network,
                block=self.block,
            )
        return self._token1

    def _build_fixed_objects_from_multicall_result(self, multicall_result: dict):
        # build fixed objects like (pool, token0, token1..)
        try:
            if self._pool is None:
                self._pool = poolv3_bep20(
                    address=multicall_result["pool"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass
        try:
            if self._token0 is None:
                self._token0 = bep20(
                    address=multicall_result["token0"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass
        try:
            if self._token1 is None:
                self._token1 = bep20(
                    address=multicall_result["token1"],
                    network=self._network,
                    block=self.block,
                )
        except:
            pass


class gamma_hypervisor_bep20_cached(bep20_cached, gamma_hypervisor_bep20):
    SAVE2FILE = True

    # PROPERTIES
    @property
    def baseLower(self) -> int:
        prop_name = "baseLower"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def baseUpper(self) -> int:
        prop_name = "baseUpper"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def currentTick(self) -> int:
        prop_name = "currentTick"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def deposit0Max(self) -> int:
        prop_name = "deposit0Max"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def deposit1Max(self) -> int:
        prop_name = "deposit1Max"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def directDeposit(self) -> bool:
        prop_name = "directDeposit"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def fee(self) -> int:
        prop_name = "fee"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def feeRecipient(self) -> str:
        prop_name = "feeRecipient"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def getBasePosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993
               amount0     72329994
               amount1     565062023318300677907
               }
        """
        prop_name = "getBasePosition"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result.copy()

    @property
    def getLimitPosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993
               amount0     72329994
               amount1     565062023318300677907
               }
        """
        prop_name = "getLimitPosition"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result.copy()

    @property
    def getTotalAmounts(self) -> dict:
        """_

        Returns:
           _type_: total0   2902086313
                   total1  565062023318300678136
        """
        prop_name = "getTotalAmounts"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )

        return result.copy()

    @property
    def limitLower(self) -> int:
        prop_name = "limitLower"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def limitUpper(self) -> int:
        prop_name = "limitUpper"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def maxTotalSupply(self) -> int:
        prop_name = "maxTotalSupply"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def name(self) -> str:
        prop_name = "name"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def owner(self) -> str:
        prop_name = "owner"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def pool(self) -> poolv3_bep20_cached:
        if self._pool is None:
            # check if cached
            prop_name = "pool"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            self._pool = poolv3_bep20_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def tickSpacing(self) -> int:
        prop_name = "tickSpacing"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result

    @property
    def token0(self) -> bep20_cached:
        if self._token0 is None:
            # check if cached
            prop_name = "token0"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            self._token0 = bep20_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> bep20_cached:
        if self._token1 is None:
            # check if cached
            prop_name = "token1"
            result = self._cache.get_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
            )
            if result is None:
                result = self.call_function_autoRpc(prop_name)
                self._cache.add_data(
                    chain_id=self._chain_id,
                    address=self.address,
                    block=self.block,
                    key=prop_name,
                    data=result,
                    save2file=self.SAVE2FILE,
                )
            self._token1 = bep20_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def witelistedAddress(self) -> str:
        prop_name = "witelistedAddress"
        result = self._cache.get_data(
            chain_id=self._chain_id,
            address=self.address,
            block=self.block,
            key=prop_name,
        )
        if result is None:
            result = getattr(super(), prop_name)
            self._cache.add_data(
                chain_id=self._chain_id,
                address=self.address,
                block=self.block,
                key=prop_name,
                data=result,
                save2file=self.SAVE2FILE,
            )
        return result


class gamma_hypervisor_bep20_multicall(gamma_hypervisor_multicall):
    @property
    def pool(self) -> poolv3_bep20_multicall:
        return self._pool

    @property
    def token0(self) -> bep20_multicall:
        return self._token0

    @property
    def token1(self) -> bep20_multicall:
        return self._token1

    def _fill_from_known_data_objects(self, known_data: dict):
        self._pool = poolv3_bep20_multicall(
            address=known_data["pool"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["pool"],
        )
        self._token0 = bep20_multicall(
            address=known_data["token0"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token0"],
        )
        self._token1 = bep20_multicall(
            address=known_data["token1"]["address"],
            network=self._network,
            block=self.block,
            timestamp=self._timestamp,
            known_data=known_data["token1"],
        )
