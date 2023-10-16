from web3 import Web3

from bins.errors.general import ProcessingError
from ....general.enums import Protocol, error_identity, text_to_chain
from .. import algebra
from ..general import erc20_cached


ABI_FILENAME = "lynex_pool"
ABI_FOLDERNAME = "lynex"
DEX_NAME = Protocol.LYNEX.database_name


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
    def globalState(self) -> dict:
        """The globalState structure in the pool stores many values but requires only one slot
            and is exposed as a single method to save gas when accessed externally.

        Returns:
           dict:    uint160 price; The current price of the pool as a sqrt(dToken1/dToken0) Q64.96 value
                    int24 tick; The current tick of the pool, i.e. according to the last tick transition that was run.
                                This value may not always be equal to SqrtTickMath.getTickAtSqrtRatio(price) if the price is on a tick boundary;
                    uint16 fee; The last pool fee value in hundredths of a bip, i.e. 1e-6
                    uint16 timepointIndex; The index of the last written timepoint
                    uint16 communityFeeToken0;  The community fee percentage of the swap fee in thousandths (1e-3) for token0
                    uint16 communityFeeToken1; The community fee percentage of the swap fee in thousandths (1e-3) for token1
                    bool unlocked; Whether the pool is currently locked to reentrancy
        """
        if tmp := self.call_function_autoRpc("globalState"):
            return {
                "sqrtPriceX96": tmp[0],
                "tick": tmp[1],
                "fee": tmp[2],
                "timepointIndex": tmp[3],
                "communityFeeToken0": tmp[4],
                "communityFeeToken1": tmp[5],
                "unlocked": tmp[6],
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


class pool_cached(pool, algebra.pool.poolv3_cached):
    SAVE2FILE = True

    # PROPERTIES

    @property
    def globalState(self) -> dict:
        """The globalState structure in the pool stores many values but requires only one slot
            and is exposed as a single method to save gas when accessed externally.

        Returns:
           dict:    uint160 price; The current price of the pool as a sqrt(dToken1/dToken0) Q64.96 value
                    int24 tick; The current tick of the pool, i.e. according to the last tick transition that was run.
                                This value may not always be equal to SqrtTickMath.getTickAtSqrtRatio(price) if the price is on a tick boundary;
                    uint16 fee; The last pool fee value in hundredths of a bip, i.e. 1e-6
                    uint16 timepointIndex; The index of the last written timepoint
                    uint16 communityFeeToken0;  The community fee percentage of the swap fee in thousandths (1e-3) for token0
                    uint16 communityFeeToken1; The community fee percentage of the swap fee in thousandths (1e-3) for token1
                    bool unlocked; Whether the pool is currently locked to reentrancy
        """
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
