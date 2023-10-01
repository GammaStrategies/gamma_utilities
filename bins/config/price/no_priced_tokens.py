from dataclasses import dataclass
import logging
from bins.configuration import CONFIGURATION
from bins.general.enums import Chain
from bins.w3.builders import build_erc20_helper


@dataclass
class NoPricedToken_item:
    token_address: str
    chain: Chain
    block: int
    timestamp: int = None


@dataclass
class NoPricedToken_conversion:
    original: NoPricedToken_item
    converted: NoPricedToken_item
    conversion_rate: float


### CONFIGURE HERE THE PROCESSES TO GET THE RATE OF TOKENS THAT ARE NOT PRICED IN THE POOL
def xgamma(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    gamma_token = "0x6bea7cfef803d1e3d5f7c0103f7ded065644e197".lower()
    xgamma_token = "0x26805021988F1a45dC708B5FB75Fc75F21747D8c".lower()

    if address.lower() == xgamma_token:
        # get balanceOf Gamma token
        try:
            gamma_contract = build_erc20_helper(
                chain=chain, address=gamma_token, block=block
            )
            xgamma_contract = build_erc20_helper(
                chain=chain, address=xgamma_token, block=block
            )
            gamma_staked = gamma_contract.balanceOf(address=address)
            xgamma_totalSupply = xgamma_contract.totalSupply

            return NoPricedToken_conversion(
                original=NoPricedToken_item(
                    token_address=xgamma_token, chain=chain, block=block
                ),
                converted=NoPricedToken_item(
                    token_address=gamma_token, chain=chain, block=block
                ),
                conversion_rate=gamma_staked / xgamma_totalSupply,
            )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Can't get xGamma token price. Error: {e} "
            )

    return None


def xram(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    # xRam is a buy option of RAM with 30% price penalty when selling back to RAM previous to 90 days
    ram_token = "0xaaa6c1e32c55a7bfa8066a6fae9b42650f262418".lower()
    xram_token = "0xaaa1ee8dc1864ae49185c368e8c64dd780a50fb7".lower()

    if address.lower() == xram_token:
        return NoPricedToken_conversion(
            original=NoPricedToken_item(
                token_address=xram_token, chain=chain, block=block
            ),
            converted=NoPricedToken_item(
                token_address=ram_token, chain=chain, block=block
            ),
            conversion_rate=0.7,
        )


def oretro(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    # oRETRO is a call option token that is used as the emission token for the Retro protocol
    # The discount rate is subject to change and based on market conditions.
    retro_token = "0xbfa35599c7aebb0dace9b5aa3ca5f2a79624d8eb".lower()
    oretro_token = "0x3a29cab2e124919d14a6f735b6033a3aad2b260f".lower()

    if address.lower() == oretro_token:
        # get the discount rate from the contract
        erc20 = build_erc20_helper(
            chain=chain,
            address=oretro_token,
            abi_filename="oretro",
            abi_path=(CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi")
            + "/retro",
            block=block,
        )
        discount_rate = erc20.call_function_autoRpc("discount")
        conversion_rate = (100 - discount_rate) / 100

        return NoPricedToken_conversion(
            original=NoPricedToken_item(
                token_address=oretro_token, chain=chain, block=block
            ),
            converted=NoPricedToken_item(
                token_address=retro_token, chain=chain, block=block
            ),
            conversion_rate=conversion_rate,
        )


def angle(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    # Angle token in optimism has no pool yet but it doen in ethereum
    angle_ethereum_token = "0x31429d1856ad1377a8a0079410b297e1a9e214c2".lower()
    angle_optimism_token = "0x58441e37255b09f9f545e9dc957f1c41658ff665".lower()
    angle_polygon_token = "0x900f717ea076e1e7a484ad9dd2db81ceec60ebf1".lower()

    # Only return when in OPTIMISM
    if chain == Chain.OPTIMISM and address.lower() == angle_optimism_token:
        try:
            # change the block from optimism to ethereum using timestamps
            _optimism_timestamp = build_erc20_helper(
                chain=chain, address=angle_optimism_token, block=block
            )._timestamp
            _ethereum_block = build_erc20_helper(
                chain=Chain.ETHEREUM,
                address=angle_ethereum_token,
                timestamp=_optimism_timestamp,
            ).blockNumberFromTimestamp(timestamp=_optimism_timestamp)

            if _ethereum_block:
                return NoPricedToken_conversion(
                    original=NoPricedToken_item(
                        token_address=angle_optimism_token,
                        chain=chain,
                        block=block,
                        timestamp=_optimism_timestamp,
                    ),
                    converted=NoPricedToken_item(
                        token_address=angle_ethereum_token,
                        chain=Chain.ETHEREUM,
                        block=_ethereum_block,
                        timestamp=_optimism_timestamp,
                    ),
                    conversion_rate=1,
                )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Can't build Angle NoPricedToken conversion object. Error: {e} "
            )

    elif chain == Chain.POLYGON and address.lower() == angle_polygon_token:
        try:
            # change the block from optimism to ethereum using timestamps
            _polygon_timestamp = build_erc20_helper(
                chain=chain, address=angle_polygon_token, block=block
            )._timestamp
            _ethereum_block = build_erc20_helper(
                chain=Chain.ETHEREUM,
                address=angle_ethereum_token,
                timestamp=_polygon_timestamp,
            ).blockNumberFromTimestamp(timestamp=_polygon_timestamp)

            if _ethereum_block:
                return NoPricedToken_conversion(
                    original=NoPricedToken_item(
                        token_address=angle_polygon_token,
                        chain=chain,
                        block=block,
                        timestamp=_polygon_timestamp,
                    ),
                    converted=NoPricedToken_item(
                        token_address=angle_ethereum_token,
                        chain=Chain.ETHEREUM,
                        block=_ethereum_block,
                        timestamp=_polygon_timestamp,
                    ),
                    conversion_rate=1,
                )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Can't build Angle NoPricedToken conversion object. Error: {e} "
            )


# ADD HERE THE TOKENS THAT ARE NOT PRICED IN ANY POOL
TOKEN_ADDRESS_CONVERSION = {
    Chain.ETHEREUM: {
        # xGamma--Gamma
        "0x26805021988F1a45dC708B5FB75Fc75F21747D8c".lower(): xgamma,
    },
    Chain.ARBITRUM: {
        # xRAM--RAM
        "0xaaa1ee8dc1864ae49185c368e8c64dd780a50fb7".lower(): xram,
    },
    Chain.POLYGON: {
        # oRETRO--RETRO
        "0x3a29cab2e124919d14a6f735b6033a3aad2b260f".lower(): oretro,
        # ANGLE
        "0x900f717ea076e1e7a484ad9dd2db81ceec60ebf1".lower(): angle,
    },
    Chain.OPTIMISM: {
        # ANGLE
        "0x58441e37255b09f9f545e9dc957f1c41658ff665".lower(): angle,
    },
}


def no_priced_token_conversions(
    chain: Chain, address: str, block: int
) -> NoPricedToken_conversion:
    """returnds the price of a token in a specific chain that is not priced for some reason ( no public pool) but has a conversion rate to another token

    Args:
        chain (Chain):
        address (str):
    """
    if callme := TOKEN_ADDRESS_CONVERSION.get(chain, {}).get(address.lower(), None):
        return callme(chain, address, block)

    return None
