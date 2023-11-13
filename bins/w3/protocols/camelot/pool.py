from copy import deepcopy
from web3 import Web3

from bins.errors.general import ProcessingError
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import algebra
from ..general import erc20_cached


ABI_FILENAME = "camelot_pool"
ABI_FOLDERNAME = "camelot"
DEX_NAME = Protocol.CAMELOT.database_name

# ABI_FILENAMES = {
#     "0xb7Dd20F3FBF4dB42Fd85C839ac0241D09F72955f".lower(): "camelot_pool_old"
# }


class pool(algebra.pool.poolv3):
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

    @property
    def communityFeeLastTimestamp(self) -> int:
        return self.call_function_autoRpc("communityFeeLastTimestamp")

    @property
    def communityVault(self) -> str:
        # TODO: communityVault object
        return self.call_function_autoRpc("communityVault")

    @property
    def getCommunityFeePending(self) -> tuple[int, int]:
        """The amounts of token0 and token1 that will be sent to the vault

        Returns:
            tuple[int,int]: token0,token1
        """
        return self.call_function_autoRpc("getCommunityFeePending")

    def getTimepoints(self, secondsAgo: int):
        raise NotImplementedError(" No get Timepoints in camelot")

    @property
    def getReserves(self) -> tuple[int, int]:
        """The amounts of token0 and token1 currently held in reserves

        Returns:
            tuple[int,int]: token0,token1
        """
        return self.call_function_autoRpc("getReserves")

    @property
    def globalState(self) -> dict:
        """

        Returns:
           dict:    uint160 price; // The square root of the current price in Q64.96 format
                    int24 tick; // The current tick
                    uint16 feeZto; // The current fee for ZtO swap in hundredths of a bip, i.e. 1e-6
                    uint16 feeOtz; // The current fee for OtZ swap in hundredths of a bip, i.e. 1e-6
                    uint16 timepointIndex; // The index of the last written timepoint
                    uint8 communityFee; // The community fee represented as a percent of all collected fee in thousandths (1e-3)
                    bool unlocked; // True if the contract is unlocked, otherwise - false
        """
        if tmp := self.call_function_autoRpc("globalState"):
            return {
                "sqrtPriceX96": tmp[0],
                "tick": tmp[1],
                "fee": tmp[2],
                "timepointIndex": tmp[4],
                "communityFeeToken0": tmp[5],
                "communityFeeToken1": tmp[5],
                "unlocked": tmp[6],
                # special
                "feeZto": tmp[2],
                "feeOtz": tmp[3],
            }
        else:
            raise ProcessingError(
                chain=text_to_chain(self._network),
                item={
                    "pool_address": self.address,
                    "block": self.block,
                    "object": "pool.globalState",
                },
                identity=error_identity.RETURN_NONE,
                action="",
                message=f" globalState function of {self.address} at block {self.block} returned none. (Check contract creation block)",
            )


class pool_cached(pool):
    SAVE2FILE = True

    # PROPERTIES

    @property
    def activeIncentive(self) -> str:
        prop_name = "activeIncentive"
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
    def dataStorageOperator(self) -> algebra.pool.dataStorageOperator_cached:
        """ """
        if self._dataStorage is None:
            # check if cached
            prop_name = "dataStorageOperator"
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
            self._dataStorage = algebra.pool.dataStorageOperator_cached(
                address=result,
                network=self._network,
                block=self.block,
            )
        return self._dataStorage

    @property
    def factory(self) -> str:
        prop_name = "factory"
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
    def globalState(self) -> dict:
        prop_name = "globalState"
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
    def liquidity(self) -> int:
        prop_name = "liquidity"
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
    def liquidityCooldown(self) -> int:
        prop_name = "liquidityCooldown"
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
    def maxLiquidityPerTick(self) -> int:
        prop_name = "maxLiquidityPerTick"
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
        """The first of the two tokens of the pool, sorted by address

        Returns:
           erc20:
        """
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
    def feeGrowthGlobal0X128(self) -> int:
        prop_name = "feeGrowthGlobal0X128"
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
    def feeGrowthGlobal1X128(self) -> int:
        prop_name = "feeGrowthGlobal1X128"
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


class pool_multicall(algebra.pool.poolv3_multicall):
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
    ):
        self._abi_filename = abi_filename or ABI_FILENAME
        self._abi_path = abi_path or f"{self.abi_root_path}/{ABI_FOLDERNAME}"

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

    def identify_dex_name(self) -> str:
        return DEX_NAME

    @property
    def communityFeeLastTimestamp(self) -> int:
        return self._communityFeeLastTimestamp

    @property
    def communityVault(self) -> str:
        return self._communityVault

    @property
    def getCommunityFeePending(self) -> tuple[int, int]:
        """The amounts of token0 and token1 that will be sent to the vault

        Returns:
            tuple[int,int]: token0,token1
        """
        return deepcopy(self._getCommunityFeePending)

    @property
    def getReserves(self) -> tuple[int, int]:
        """The amounts of token0 and token1 currently held in reserves

        Returns:
            tuple[int,int]: token0,token1
        """
        return deepcopy(self._getReserves)

    def _fill_from_known_data(self, known_data: dict):
        self._factory = known_data["factory"]
        self._communityFeeLastTimestamp = known_data["communityFeeLastTimestamp"]
        self._communityVault = known_data["communityVault"]
        self._getCommunityFeePending = known_data["getCommunityFeePending"]
        self._getReserves = known_data["getReserves"]
        self._feeGrowthGlobal0X128 = known_data["totalFeeGrowth0Token"]
        self._feeGrowthGlobal1X128 = known_data["totalFeeGrowth1Token"]
        self._liquidity = known_data["liquidity"]
        self._maxLiquidityPerTick = known_data["maxLiquidityPerTick"]
        # self._protocolFees = known_data["protocolFees"]
        self._tickSpacing = known_data["tickSpacing"]
        self._activeIncentive = known_data["activeIncentive"]
        self._liquidityCooldown = known_data["liquidityCooldown"]
        self._globalState = {
            "sqrtPriceX96": known_data["globalState"][0],
            "tick": known_data["globalState"][1],
            "fee": known_data["globalState"][2],
            "timepointIndex": known_data["globalState"][4],
            "communityFeeToken0": known_data["globalState"][5],
            "communityFeeToken1": known_data["globalState"][5],
            "unlocked": known_data["globalState"][6],
            # special
            "feeZto": known_data["globalState"][2],
            "feeOtz": known_data["globalState"][3],
        }

        self._fill_from_known_data_objects(known_data=known_data)
