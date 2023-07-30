from web3 import Web3
from ....general.enums import Protocol
from .. import algebra
from ..general import erc20_cached


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
        self._abi_filename = abi_filename or "camelot_pool"
        self._abi_path = abi_path or f"{self.abi_root_path}/camelot"

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
        return Protocol.CAMELOT.database_name

    @property
    def comunityFeeLastTimestamp(self) -> int:
        return self.call_function_autoRpc("comunityFeeLastTimestamp")

    @property
    def comunityVault(self) -> str:
        # TODO: comunityVault object
        return self.call_function_autoRpc("comunityVault")

    @property
    def getComunityFeePending(self) -> tuple[int, int]:
        """The amounts of token0 and token1 that will be sent to the vault

        Returns:
            tuple[int,int]: token0,token1
        """
        return self.call_function_autoRpc("getComunityFeePending")

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
            raise ValueError(f" globalState function call returned None")


# TODO: simplify class with inheritance
class pool_cached(pool):
    SAVE2FILE = True

    def identify_dex_name(self) -> str:
        return Protocol.CAMELOT.database_name

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
            self._dataStorage = algebra.pool.dataStorageOperator_cached(
                address=self.call_function_autoRpc("dataStorageOperator"),
                # address=self._contract.functions.dataStorageOperator().call(
                #     block_identifier=self.block
                # ),
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
            self._token0 = erc20_cached(
                address=self.call_function_autoRpc("token0"),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20_cached:
        if self._token1 is None:
            self._token1 = erc20_cached(
                address=self.call_function_autoRpc("token1"),
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
