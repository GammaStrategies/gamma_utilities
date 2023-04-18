import contextlib
import logging
import sys
import math

from decimal import Decimal
from web3 import Web3
from web3.contract import ContractEvent

from bins.configuration import CONFIGURATION, WEB3_CHAIN_IDS
from bins.cache import cache_utilities
from bins.w3.onchain_utilities.basic import web3wrap, erc20, erc20_cached
from bins.w3.onchain_utilities.exchanges import (
    univ3_pool,
    univ3_pool_cached,
    algebrav3_pool,
    algebrav3_pool_cached,
)


class gamma_hypervisor(erc20):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "hypervisor"
        self._abi_path = abi_path or "data/abi/gamma"

        self._pool: univ3_pool | None = None
        self._token0: erc20 | None = None
        self._token1: erc20 | None = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # SETUP
    def setup_cache(self):
        # define network
        if self._network in WEB3_CHAIN_IDS:
            self._chain_id = WEB3_CHAIN_IDS[self._network]
        else:
            self._chain_id = self.w3.eth.chain_id

        # made up a descriptive cahce file name
        cache_filename = f"{self._chain_id}_{self.address.lower()}"

        fixed_fields = {
            "decimals": False,
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
        }

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
        return self._contract.functions.baseUpper().call(block_identifier=self.block)

    @property
    def baseLower(self) -> int:
        """baseLower _summary_

        Returns:
            _type_: 0 int24
        """
        return self._contract.functions.baseLower().call(block_identifier=self.block)

    @property
    def currentTick(self) -> int:
        """currentTick _summary_

        Returns:
            int: -78627 int24
        """
        return self._contract.functions.currentTick().call(block_identifier=self.block)

    @property
    def deposit0Max(self) -> int:
        """deposit0Max _summary_

        Returns:
            float: 1157920892373161954234007913129639935 uint256
        """
        return self._contract.functions.deposit0Max().call(block_identifier=self.block)

    @property
    def deposit1Max(self) -> int:
        """deposit1Max _summary_

        Returns:
            int: 115792089237 uint256
        """
        return self._contract.functions.deposit1Max().call(block_identifier=self.block)

    # v1 contracts have no directDeposit
    @property
    def directDeposit(self) -> bool:
        """v1 contracts have no directDeposit function

        Returns:
            bool:
        """
        return self._contract.functions.directDeposit().call(
            block_identifier=self.block
        )

    @property
    def fee(self) -> int:
        """fee _summary_

        Returns:
            int: 10 uint8
        """
        return self._contract.functions.fee().call(block_identifier=self.block)

    # v1 contracts have no feeRecipient
    @property
    def feeRecipient(self) -> str:
        """v1 contracts have no feeRecipient function

        Returns:
            str: address
        """
        return self._contract.functions.feeRecipient().call(block_identifier=self.block)

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
        tmp = self._contract.functions.getBasePosition().call(
            block_identifier=self.block
        )
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
        tmp = self._contract.functions.getLimitPosition().call(
            block_identifier=self.block
        )
        return {
            "liquidity": tmp[0],
            "amount0": tmp[1],
            "amount1": tmp[2],
        }

    @property
    def getTotalAmounts(self) -> dict:
        """

        Returns:
           _type_: total0   2902086313 uint256
                   total1  565062023318300678136 uint256
        """
        tmp = self._contract.functions.getTotalAmounts().call(
            block_identifier=self.block
        )
        return {
            "total0": tmp[0],
            "total1": tmp[1],
        }

    @property
    def limitLower(self) -> int:
        """limitLower _summary_

        Returns:
            int: 0 int24
        """
        return self._contract.functions.limitLower().call(block_identifier=self.block)

    @property
    def limitUpper(self) -> int:
        """limitUpper _summary_

        Returns:
            int: 0 int24
        """
        return self._contract.functions.limitUpper().call(block_identifier=self.block)

    @property
    def maxTotalSupply(self) -> int:
        """maxTotalSupply _summary_

        Returns:
            int: 0 uint256
        """
        return self._contract.functions.maxTotalSupply().call(
            block_identifier=self.block
        )

    @property
    def name(self) -> str:
        return self._contract.functions.name().call(block_identifier=self.block)

    def nonces(self, owner: str):
        return self._contract.functions.nonces()(Web3.toChecksumAddress(owner)).call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pool(self) -> univ3_pool:
        if self._pool is None:
            self._pool = univ3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
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
        return self._contract.functions.tickSpacing().call(block_identifier=self.block)

    @property
    def token0(self) -> erc20:
        if self._token0 is None:
            self._token0 = erc20(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 is None:
            self._token1 = erc20(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
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

    # CUSTOM FUNCTIONS
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
                }
        """
        # positions
        base = self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.baseUpper,
            tickLower=self.baseLower,
            inDecimal=inDecimal,
        )
        limit = self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.limitUpper,
            tickLower=self.limitLower,
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

    def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.

        Returns:
            dict:
        """
        result = super().as_dict(convert_bint=convert_bint)

        result["name"] = self.name
        result["pool"] = self.pool.as_dict(
            convert_bint=convert_bint, static_mode=static_mode
        )

        result["fee"] = self.fee

        # identify hypervisor dex
        result["dex"] = self.identify_dex_name()

        result["deposit0Max"] = (
            str(self.deposit0Max) if convert_bint else self.deposit0Max
        )

        result["deposit1Max"] = (
            str(self.deposit1Max) if convert_bint else self.deposit1Max
        )

        # result["directDeposit"] = self.directDeposit  # not working

        # only return when static mode is off
        if not static_mode:
            self._as_dict_not_static_items(convert_bint, result)
        return result

    def _as_dict_not_static_items(self, convert_bint, result):
        result["baseLower"] = str(self.baseLower) if convert_bint else self.baseLower
        result["baseUpper"] = str(self.baseUpper) if convert_bint else self.baseUpper
        result["currentTick"] = (
            str(self.currentTick) if convert_bint else self.currentTick
        )

        result["limitLower"] = str(self.limitLower) if convert_bint else self.limitLower

        result["limitUpper"] = str(self.limitUpper) if convert_bint else self.limitUpper

        # getTotalAmounts
        result["totalAmounts"] = self.getTotalAmounts
        if convert_bint:
            result["totalAmounts"]["total0"] = str(result["totalAmounts"]["total0"])
            result["totalAmounts"]["total1"] = str(result["totalAmounts"]["total1"])

        result["maxTotalSupply"] = (
            str(self.maxTotalSupply) if convert_bint else self.maxTotalSupply
        )

        # TVL
        result["tvl"] = self.get_tvl(inDecimal=(not convert_bint))
        if convert_bint:
            for k in result["tvl"].keys():
                result["tvl"][k] = str(result["tvl"][k])

        # Deployed
        result["qtty_depoloyed"] = self.get_qtty_depoloyed(inDecimal=(not convert_bint))
        if convert_bint:
            for k in result["qtty_depoloyed"].keys():
                result["qtty_depoloyed"][k] = str(result["qtty_depoloyed"][k])

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
        result["tickSpacing"] = (
            str(self.tickSpacing) if convert_bint else self.tickSpacing
        )

    def _as_dict_convert_helper(self, result, arg1):
        result[arg1]["liquidity"] = str(result[arg1]["liquidity"])
        result[arg1]["amount0"] = str(result[arg1]["amount0"])
        result[arg1]["amount1"] = str(result[arg1]["amount1"])


class gamma_hypervisor_algebra(gamma_hypervisor):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "data/abi/gamma"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    @property
    def pool(self) -> algebrav3_pool:
        if self._pool is None:
            self._pool = algebrav3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool


class gamma_hypervisor_quickswap(gamma_hypervisor_algebra):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "data/abi/gamma"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )


class gamma_hypervisor_zyberswap(gamma_hypervisor_algebra):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "data/abi/gamma"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )


class gamma_hypervisor_thena(gamma_hypervisor_algebra):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "data/abi/gamma"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    @property
    def pool(self) -> algebrav3_pool:
        if self._pool is None:
            self._pool = algebrav3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
                abi_filename="albebrav3pool_thena",
            )
        return self._pool


# cached classes


class gamma_hypervisor_cached(gamma_hypervisor):
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
    def pool(self) -> str:
        if self._pool is None:
            self._pool = univ3_pool_cached(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
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
    def token0(self) -> erc20:
        if self._token0 is None:
            self._token0 = erc20_cached(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 is None:
            self._token1 = erc20_cached(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
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


class gamma_hypervisor_algebra_cached(gamma_hypervisor_algebra):
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
    def pool(self) -> str:
        if self._pool is None:
            self._pool = algebrav3_pool_cached(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
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
    def token0(self) -> erc20:
        if self._token0 is None:
            self._token0 = erc20_cached(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 is None:
            self._token1 = erc20_cached(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
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


class gamma_hypervisor_quickswap_cached(gamma_hypervisor_algebra_cached):
    SAVE2FILE = True


class gamma_hypervisor_zyberswap_cached(gamma_hypervisor_algebra_cached):
    SAVE2FILE = True


class gamma_hypervisor_thena_cached(gamma_hypervisor_algebra_cached):
    SAVE2FILE = True

    @property
    def pool(self) -> str:
        if self._pool is None:
            self._pool = algebrav3_pool_cached(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
                abi_filename="albebrav3pool_thena",
            )
        return self._pool


# registries


class gamma_hypervisor_registry(web3wrap):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "registry"
        self._abi_path = abi_path or "data/abi/gamma/ethereum"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # implement harcoded erroneous addresses to reduce web3 calls
    __blacklist_addresses = {
        "ethereum": [
            "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599".lower()
        ],  # address:index
        "polygon": [
            "0xa9782a2c9c3fb83937f14cdfac9a6d23946c9255".lower(),
            "0xfb0bc232CD11dBe804B489860c470B7f9cc80D9F".lower(),
        ],
        "optimism": ["0xc7722271281Aa6D5D027fC9B21989BE99424834f".lower()],
        "arbitrum": ["0x38f81e638f9e268e8417F2Ff76C270597fa077A0".lower()],
    }

    @property
    def counter(self) -> int:
        """number of hypervisors indexed, initial being 0  and end the counter value

        Returns:
            int: positions of hypervisors in registry
        """
        return self._contract.functions.counter().call(block_identifier=self.block)

    def hypeByIndex(self, index: int) -> tuple[str, int]:
        """Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
        return self._contract.functions.hypeByIndex(index).call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    def registry(self, index: int) -> str:
        return self._contract.functions.registry(index).call(
            block_identifier=self.block
        )

    def registryMap(self, address: str) -> int:
        return self._contract.functions.registryMap(
            Web3.toChecksumAddress(address)
        ).call(block_identifier=self.block)

    # CUSTOM FUNCTIONS
    def get_hypervisors_generator(self) -> gamma_hypervisor:
        """Retrieve hypervisors from registry

        Returns:
           gamma_hypervisor
        """
        total_qtty = self.counter + 1  # index positions ini=0 end=counter
        for i in range(total_qtty):
            try:
                hypervisor_id, idx = self.hypeByIndex(index=i)

                # filter blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and hypervisor_id.lower()
                    in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                # build hypervisor
                hypervisor = gamma_hypervisor(
                    address=hypervisor_id,
                    network=self._network,
                    block=self.block,
                )
                # check this is actually an hypervisor (erroneous addresses exist like "ethereum":{"0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"})
                hypervisor.getTotalAmounts  # test func

                # return correct hypervisor
                yield hypervisor
            except Exception:
                logging.getLogger(__name__).warning(
                    f" Hypervisor registry returned the address {hypervisor_id} and may not be an hypervisor ( at web3 chain id: {self._chain_id} )"
                )

    def get_hypervisors_addresses(self) -> list[str]:
        """Retrieve hypervisors all addresses from registry

        Returns:
           list of addresses
        """

        total_qtty = self.counter + 1  # index positions ini=0 end=counter

        result = []
        for i in range(total_qtty):
            # executiuon reverted:  arbitrum and mainnet have diff ways of indexing (+1 or 0)
            with contextlib.suppress(Exception):
                hypervisor_id, idx = self.hypeByIndex(index=i)

                # filter erroneous and blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and hypervisor_id.lower()
                    in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                result.append(hypervisor_id)

        return result


# rewarders
class masterchef_rewarder(web3wrap):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "masterchef_rewarder"
        self._abi_path = abi_path or "data/abi/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    @property
    def acc_token_precision(self) -> int:
        return self._contract.functions.ACC_TOKEN_PRECISION().call(
            block_identifier=self.block
        )

    @property
    def masterchef_v2(self) -> str:
        return self._contract.functions.MASTERCHEF_V2().call(
            block_identifier=self.block
        )

    @property
    def funder(self) -> str:
        return self._contract.functions.funder().call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pendingOwner(self) -> str:
        return self._contract.functions.pendingOwner().call(block_identifier=self.block)

    def pendingToken(self, pid: int, user: str) -> int:
        return self._contract.functions.pendingToken(pid, user).call(
            block_identifier=self.block
        )

    def pendingTokens(self, pid: int, user: str, input: int) -> tuple[list, list]:
        # rewardTokens address[], rewardAmounts uint256[]
        return self._contract.functions.pendingTokens(pid, user, input).call(
            block_identifier=self.block
        )

    def poolIds(self, input: int) -> int:
        return self._contract.functions.poolIds(input).call(block_identifier=self.block)

    def poolInfo(self, input: int) -> tuple[int, int, int]:
        """_summary_

        Args:
            input (int): _description_

        Returns:
            tuple[int, int, int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
                accSushiPerShare — accumulated SUSHI per share, times 1e12.
                lastRewardBlock — number of block, when the reward in the pool was the last time calculated
                allocPoint — allocation points assigned to the pool. SUSHI to distribute per block per pool = SUSHI per block * pool.allocPoint / totalAllocPoint
        """
        return self._contract.functions.poolInfo(input).call(
            block_identifier=self.block
        )

    @property
    def poolLength(self) -> int:
        return self._contract.functions.poolLength().call(block_identifier=self.block)

    @property
    def rewardPerSecond(self) -> int:
        return self._contract.functions.rewardPerSecond().call(
            block_identifier=self.block
        )

    @property
    def rewardToken(self) -> str:
        return self._contract.functions.rewardToken().call(block_identifier=self.block)

    @property
    def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        return self._contract.functions.totalAllocPoint().call(
            block_identifier=self.block
        )

    def userInfo(self, pid: int, user: str) -> tuple[int, int]:
        """_summary_

        Args:
            pid (int): pool index
            user (str): user address

        Returns:
            tuple[int, int]: amount uint256, rewardDebt uint256
                    amount — how many Liquid Provider (LP) tokens the user has supplied
                    rewardDebt — the amount of SUSHI entitled to the user

        """
        return self._contract.functions.userInfo(pid, user).call(
            block_identifier=self.block
        )

    # CUSTOM
    def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.

        Returns:
            dict:
        """
        result = super().as_dict(convert_bint=convert_bint)

        result["token_precision"] = (
            str(self.acc_token_precision) if convert_bint else self.acc_token_precision
        )
        result["masterchef_address"] = (self.masterchef_v2).lower()
        result["owner"] = (self.owner).lower()
        result["pendingOwner"] = (self.pendingOwner).lower()

        result["poolLength"] = self.poolLength

        result["rewardPerSecond"] = (
            str(self.rewardPerSecond) if convert_bint else self.rewardPerSecond
        )
        result["rewardToken"] = (self.rewardToken).lower()

        result["totalAllocPoint"] = (
            str(self.totalAllocPoint) if convert_bint else self.totalAllocPoint
        )

        # only return when static mode is off
        if not static_mode:
            pass

        return result


# rewarder registry
class masterchef_v1(web3wrap):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "masterchef_v1"
        self._abi_path = abi_path or "data/abi/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    @property
    def sushi(self) -> str:
        """The SUSHI token contract address

        Returns:
            str: token address
        """
        return self._contract.functions.SUSHI().call(block_identifier=self.block)

    def getRewarder(self, pid: int, rid: int) -> str:
        """Retrieve rewarder address from masterchef

        Args:
            pid (int): The index of the pool
            rid (int): The index of the rewarder

        Returns:
            str: address
        """
        return self._contract.functions.getRewarder(pid, rid).call(
            block_identifier=self.block
        )

    def lpToken(self, pid: int) -> str:
        """Retrieve lp token address (hypervisor) from masterchef

        Args:
            index (int): index of the pool ( same of rewarder )

        Returns:
            str:  hypervisor address ( LP token)
        """
        return self._contract.functions.lpToken(pid).call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pendingOwner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pendingSushi(self, pid: int, user: str) -> int:
        """pending SUSHI reward for a given user

        Args:
            pid (int): The index of the pool
            user (str):  address

        Returns:
            int: _description_
        """
        return self._contract.functions.pendingSushi(pid, user).call(
            block_identifier=self.block
        )

    def poolInfo(
        self,
    ) -> tuple[int, int, int]:
        """_summary_

        Returns:
            tuple[int,int,int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
        """
        return self._contract.functions.poolInfo().call(block_identifier=self.block)

    @property
    def poolLength(self) -> int:
        """Returns the number of MCV2 pools
        Returns:
            int:
        """
        return self._contract.functions.poolLength().call(block_identifier=self.block)


class zyberchef_v1(web3wrap):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "zyberchef_v1"
        self._abi_path = abi_path or "data/abi/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # TODO: https://arbiscan.io/address/0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd#readContract


# masterchef registry ( registry of the "rewarders registry")
class masterChef_registry(web3wrap):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "masterchef_registry_v1"
        self._abi_path = abi_path or "data/abi/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # implement harcoded erroneous addresses to reduce web3 calls
    __blacklist_addresses = {}

    @property
    def counter(self) -> int:
        """number of hypervisors indexed, initial being 0  and end the counter value-1

        Returns:
            int: positions of hypervisors in registry
        """
        return self._contract.functions.counter().call(block_identifier=self.block)

    def hypeByIndex(self, index: int) -> tuple[str, int]:
        """Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
        return self._contract.functions.hypeByIndex(index).call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    def registry(self, index: int) -> str:
        return self._contract.functions.registry(index).call(
            block_identifier=self.block
        )

    def registryMap(self, address: str) -> int:
        return self._contract.functions.registryMap(
            Web3.toChecksumAddress(address)
        ).call(block_identifier=self.block)

    # CUSTOM FUNCTIONS
    def get_masterchef_generator(self) -> masterchef_v1:
        """Retrieve masterchef contracts from registry

        Returns:
           masterchefV2 contract
        """
        total_qtty = self.counter + 1  # index positions ini=0 end=counter
        for i in range(total_qtty):
            try:
                address, idx = self.hypeByIndex(index=i)

                # filter blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and address.lower() in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                yield masterchef_v1(
                    address=address,
                    network=self._network,
                    block=self.block,
                )

            except Exception:
                logging.getLogger(__name__).warning(
                    f" Masterchef registry returned the address {address} and may not be a masterchef contract ( at web3 chain id: {self._chain_id} )"
                )

    def get_masterchef_addresses(self) -> list[str]:
        """Retrieve masterchef addresses from registry

        Returns:
           list of addresses
        """

        total_qtty = self.counter + 1  # index positions ini=0 end=counter

        result = []
        for i in range(total_qtty):
            # executiuon reverted:  arbitrum and mainnet have diff ways of indexing (+1 or 0)
            with contextlib.suppress(Exception):
                address, idx = self.hypeByIndex(index=i)

                # filter erroneous and blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and address.lower() in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                result.append(address)

        return result


#
#
#
# TODO: decimals n stuff
class arrakis_hypervisor(erc20):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
    ):
        self._abi_filename = abi_filename or "gunipool"
        self._abi_path = abi_path or "data/abi/arrakis"

        self._pool: univ3_pool = None

        self._token0: erc20 = None
        self._token1: erc20 = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
        )

    # PROPERTIES
    @property
    def gelatoBalance0(self) -> int:
        return self._contract.functions.gelatoBalance0().call(
            block_identifier=self.block
        )

    @property
    def gelatoBalance1(self) -> int:
        return self._contract.functions.gelatoBalance1().call(
            block_identifier=self.block
        )

    @property
    def gelatoFeeBPS(self) -> int:
        return self._contract.functions.gelatoFeeBPS().call(block_identifier=self.block)

    @property
    def gelatoRebalanceBPS(self) -> int:
        return self._contract.functions.gelatoRebalanceBPS().call(
            block_identifier=self.block
        )

    @property
    def gelatoSlippageBPS(self) -> int:
        return self._contract.functions.gelatoSlippageBPS().call(
            block_identifier=self.block
        )

    @property
    def gelatoSlippageInterval(self) -> int:
        return self._contract.functions.gelatoSlippageInterval().call(
            block_identifier=self.block
        )

    @property
    def gelatoWithdrawBPS(self) -> int:
        return self._contract.functions.gelatoWithdrawBPS().call(
            block_identifier=self.block
        )

    def getMintAmounts(self, amount0Max, amount1Max) -> dict:
        """
        Args:
           amount0Max (_type_):
           amount1Max (_type_):

        Returns:
           dict: amount0 uint256, amount1 uint256, mintAmount uint256
        """

        tmp = self._contract.functions.getMintAmounts(amount0Max, amount1Max).call(
            block_identifier=self.block
        )
        return {"amount0": tmp[0], "amount1": tmp[1], "mintAmount": tmp[2]}

    @property
    def getPositionID(self) -> str:
        return self._contract.functions.getPositionID().call(
            block_identifier=self.block
        )

    @property
    def getUnderlyingBalances(self) -> dict:
        """getUnderlyingBalances _summary_

        Returns:
           dict: amount0Current: current total underlying balance of token0
                   amount1Current: current total underlying balance of token1
        """
        tmp = self._contract.functions.getUnderlyingBalances().call(
            block_identifier=self.block
        )
        return {
            "amount0Current": tmp[0],
            "amount1Current": tmp[1],
        }

    def getUnderlyingBalancesAtPrice(self, sqrtRatioX96) -> dict:
        """

        Returns:
           dict: amount0Current: current total underlying balance of token0 at price
                 amount1Current: current total underlying balance of token1 at price
        """
        tmp = self._contract.functions.getUnderlyingBalancesAtPrice(sqrtRatioX96).call(
            block_identifier=self.block
        )
        return {
            "amount0Current": tmp[0],
            "amount1Current": tmp[1],
        }

    @property
    def lowerTick(self) -> int:
        return self._contract.functions.lowerTick().call(block_identifier=self.block)

    @property
    def manager(self) -> str:
        return self._contract.functions.manager().call(block_identifier=self.block)

    @property
    def managerBalance0(self) -> int:
        return self._contract.functions.managerBalance0().call(
            block_identifier=self.block
        )

    @property
    def managerBalance1(self) -> int:
        return self._contract.functions.managerBalance1().call(
            block_identifier=self.block
        )

    @property
    def managerFeeBPS(self) -> int:
        return self._contract.functions.managerFeeBPS().call(
            block_identifier=self.block
        )

    @property
    def managerTreasury(self) -> str:
        return self._contract.functions.managerTreasury().call(
            block_identifier=self.block
        )

    @property
    def name(self) -> str:
        return self._contract.functions.name().call(block_identifier=self.block)

    @property
    def pool(self) -> str:
        if self._pool is None:
            self._pool = univ3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def token0(self) -> erc20:
        if self._token0 is None:
            self._token0 = erc20(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 is None:
            self._token1 = erc20(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def upperTick(self) -> int:
        return self._contract.functions.upperTick().call(block_identifier=self.block)

    @property
    def version(self) -> str:
        return self._contract.functions.version().call(block_identifier=self.block)

    # CUSTOM PROPERTIES
    @property
    def block(self):
        """ """
        return self._block

    @block.setter
    def block(self, value):
        self._block = value
        self.pool.block = value
        self.token0.block = value
        self.token1.block = value

    # CUSTOM FUNCTIONS
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
        # position
        return self.pool.get_qtty_depoloyed(
            ownerAddress=self.address,
            tickUpper=self.upperTick,
            tickLower=self.lowerTick,
            inDecimal=inDecimal,
        )

    def get_fees_uncollected(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Returns:
            dict: {
                    "qtty_token0":0,  # quantity of uncollected token 0
                    "qtty_token1":0,  # quantity of uncollected token 1
                }
        """
        # positions
        return self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.upperTick,
            tickLower=self.lowerTick,
            inDecimal=inDecimal,
        )

    def get_tvl(self) -> dict:
        """get total value locked of both positions
           TVL = deployed + parked + owed

        Returns:
           dict: {" tvl_token0": ,      (float) Total quantity locked of token 0
                   "tvl_token1": ,      (float) Total quantity locked of token 1
                   "deployed_token0": , (float)
                   "deployed_token1": , (float)
                   "fees_owed_token0": ,(float)
                   "fees_owed_token1": ,(float)
                   "parked_token0": ,   (float) quantity of token 0 parked at contract (not deployed)
                   "parked_token1": ,   (float)  quantity of token 1 parked at contract (not deployed)
                   }
        """
        # get deployed fees as int
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

        # transform everythin to deicmal
        for key in result:
            if "token0" in key:
                result[key] /= 10**self.token0.decimals
            elif "token1" in key:
                result[key] /= 10**self.token1.decimals
            else:
                raise ValueError(f"Cant convert '{key}' field to decimal")

        return result


class arrakis_hypervisor_cached(arrakis_hypervisor):
    SAVE2FILE = True

    # PROPERTIES
    @property
    def gelatoBalance0(self) -> int:
        prop_name = "gelatoBalance0"
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
    def gelatoBalance1(self) -> int:
        prop_name = "gelatoBalance1"
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
    def gelatoFeeBPS(self) -> int:
        prop_name = "gelatoFeeBPS"
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
    def gelatoRebalanceBPS(self) -> int:
        prop_name = "gelatoRebalanceBPS"
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
    def gelatoSlippageBPS(self) -> int:
        prop_name = "gelatoSlippageBPS"
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
    def gelatoSlippageInterval(self) -> int:
        prop_name = "gelatoSlippageInterval"
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
    def gelatoWithdrawBPS(self) -> int:
        prop_name = "gelatoWithdrawBPS"
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
    def getPositionID(self) -> str:
        prop_name = "getPositionID"
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
    def getUnderlyingBalances(self) -> dict:
        """getUnderlyingBalances _summary_

        Returns:
           dict: amount0Current: current total underlying balance of token0
                   amount1Current: current total underlying balance of token1
        """
        prop_name = "getUnderlyingBalances"
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
    def lowerTick(self) -> int:
        prop_name = "lowerTick"
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
    def manager(self) -> str:
        prop_name = "manager"
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
    def managerBalance0(self) -> int:
        prop_name = "managerBalance0"
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
    def managerBalance1(self) -> int:
        prop_name = "managerBalance1"
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
    def managerFeeBPS(self) -> int:
        prop_name = "managerFeeBPS"
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
    def managerTreasury(self) -> str:
        prop_name = "managerTreasury"
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
    def upperTick(self) -> int:
        prop_name = "upperTick"
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
    def version(self) -> str:
        prop_name = "version"
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
    def token0(self) -> erc20:
        if self._token0 is None:
            self._token0 = erc20_cached(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 is None:
            self._token1 = erc20_cached(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def pool(self) -> str:
        if self._pool is None:
            self._pool = univ3_pool_cached(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool
