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
    # Only return when in ARBITRUM
    # if chain != Chain.ARBITRUM:
    #     return None

    # xRam is a buy option of RAM with 30% price penalty when selling back to RAM previous to 90 days
    ram_token = "0xaaa6c1e32c55a7bfa8066a6fae9b42650f262418".lower()
    xram_token = "0xaaa1ee8dc1864ae49185c368e8c64dd780a50fb7".lower()

    if address.lower() == xram_token:
        conversion_rate = None
        try:
            # get the discount rate from the contract
            erc20 = build_erc20_helper(
                chain=chain,
                address=xram_token,
                abi_filename="xRam",
                abi_path=(
                    CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
                )
                + "/ramses",
                block=block,
            )
            _precision = erc20.call_function_autoRpc("PRECISION")
            _discount_rate = 0
            try:
                _discount_rate = erc20.call_function_autoRpc("exitRatio")
            except Exception as e:
                pass
            if not _discount_rate:
                try:
                    _discount_rate = erc20.call_function_autoRpc("discount")
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Can't get xRam token price discount.... Error: {e} . Fallback to 60%"
                    )
                    _discount_rate = 60

            conversion_rate = _discount_rate / _precision
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Can't get xRam token price. Error: {e} . Fallback to 50%"
            )
            conversion_rate = 0.5

        #
        return NoPricedToken_conversion(
            original=NoPricedToken_item(
                token_address=xram_token, chain=chain, block=block
            ),
            converted=NoPricedToken_item(
                token_address=ram_token, chain=chain, block=block
            ),
            conversion_rate=conversion_rate,
        )


def xphar(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:

    # xPhar is a buy option of PHAR with 30% price penalty when selling back to RAM previous to 90 days
    phar_token = "0xaaab9d12a30504559b0c5a9a5977fee4a6081c6b".lower()
    xphar_token = "0xaaae58986b24e422740c8f22b3efb80bcbd68159".lower()

    if address.lower() == xphar_token:
        conversion_rate = None
        try:
            # get the discount rate from the contract
            erc20 = build_erc20_helper(
                chain=chain,
                address=xphar_token,
                abi_filename="xPhar",
                abi_path=(
                    CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
                )
                + "/pharaoh",
                block=block,
            )
            _precision = erc20.call_function_autoRpc("PRECISION")
            _discount_rate = 0
            try:
                _discount_rate = erc20.call_function_autoRpc("exitRatio")
            except Exception as e:
                pass
            if not _discount_rate:
                try:
                    _discount_rate = erc20.call_function_autoRpc("discount")
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Can't get xPhar token price discount.... Error: {e} . Fallback to 60%"
                    )
                    _discount_rate = 50

            conversion_rate = _discount_rate / _precision
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Can't get xPhar token price. Error: {e} . Fallback to 50%"
            )
            conversion_rate = 0.5

        #
        return NoPricedToken_conversion(
            original=NoPricedToken_item(
                token_address=xphar_token, chain=chain, block=block
            ),
            converted=NoPricedToken_item(
                token_address=phar_token, chain=chain, block=block
            ),
            conversion_rate=conversion_rate,
        )


def xcleo(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:

    # xCleo is a buy option of CLEO with 50% price penalty when selling back previous to X days
    cleo_token = "0xc1e0c8c30f251a07a894609616580ad2ceb547f2".lower()
    xcleo_token = "0xAAAE58986b24e422740C8F22B3efB80BCbD68159".lower()

    if address.lower() == xcleo_token:
        conversion_rate = None
        try:
            # get the discount rate from the contract
            erc20 = build_erc20_helper(
                chain=chain,
                address=xcleo_token,
                abi_filename="xCleo",
                abi_path=(
                    CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
                )
                + "/cleopatra",
                block=block,
            )
            _precision = erc20.call_function_autoRpc("PRECISION")
            _discount_rate = 0
            try:
                _discount_rate = erc20.call_function_autoRpc("exitRatio")
            except Exception as e:
                pass
            if not _discount_rate:
                try:
                    _discount_rate = erc20.call_function_autoRpc("discount")
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Can't get xCleo token price discount.... Error: {e} . Fallback to 50%"
                    )
                    _discount_rate = 50

            conversion_rate = _discount_rate / _precision
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Can't get xCleo token price. Error: {e} . Fallback to 50%"
            )
            conversion_rate = 0.5

        #
        return NoPricedToken_conversion(
            original=NoPricedToken_item(
                token_address=xcleo_token, chain=chain, block=block
            ),
            converted=NoPricedToken_item(
                token_address=cleo_token, chain=chain, block=block
            ),
            conversion_rate=conversion_rate,
        )


def oretro(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    # oRETRO is a call option token that is used as the emission token for the Retro protocol
    # The discount rate is subject to change and based on market conditions.
    retro_token = "0xbfa35599c7aebb0dace9b5aa3ca5f2a79624d8eb".lower()
    oretro_token = "0x3a29cab2e124919d14a6f735b6033a3aad2b260f".lower()

    if address.lower() == oretro_token:
        # the contract was deployed in block 45556777, so no data before that
        if chain == Chain.POLYGON and block > 0 and block < 45556777:
            logging.getLogger(__name__).warning(
                f" No data for {chain.fantasy_name} oRETRO before block 45556777"
            )
            return None

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


def csushi(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    csushi_ethereum = "0x4b0181102a0112a2ef11abee5563bb4a3176c9d7".lower()
    csushi_polygon = "0x26aa9b3d8a49a2ed849ac66ea9aa37ee36bc6b24".lower()
    # cToken compound.finance

    # we convert to ethereum
    if chain != Chain.ETHEREUM:
        try:
            _ethereum_block = None

            # Polygon
            if chain == Chain.POLYGON and address.lower() == csushi_polygon:
                # change the block from optimism to ethereum using timestamps
                _timestamp = build_erc20_helper(
                    chain=chain, address=csushi_polygon, block=block
                )._timestamp
                _ethereum_block = build_erc20_helper(
                    chain=Chain.ETHEREUM,
                    address=csushi_ethereum,
                    timestamp=_timestamp,
                ).blockNumberFromTimestamp(timestamp=_timestamp)

            # return if found
            if _ethereum_block:
                return NoPricedToken_conversion(
                    original=NoPricedToken_item(
                        token_address=csushi_polygon,
                        chain=chain,
                        block=block,
                        timestamp=_timestamp,
                    ),
                    converted=NoPricedToken_item(
                        token_address=csushi_ethereum,
                        chain=Chain.ETHEREUM,
                        block=_ethereum_block,
                        timestamp=_timestamp,
                    ),
                    conversion_rate=1,
                )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Can't build cSushi NoPricedToken conversion object. Error: {e} "
            )


def xgrail(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    # xGrail is a buy option of GRAIL with 50% price penalty when selling back to GRAIL previous to 15 days, and from there it decreases linearly to 0% at 6 months
    #   Minimum duration - 15 days (50% GRAIL output)
    #   Maximum duration - 6 months (100% GRAIL output)
    #   Linearly decreasing from 50% to 0% over 6 months
    # For the shake of simplicity we will use a constant 50% price penalty
    grail_token = "0x3d9907f9a368ad0a51be60f7da3b97cf940982d8".lower()
    xgrail_token = "0x3caae25ee616f2c8e13c74da0813402eae3f496b".lower()

    if address.lower() == xgrail_token and chain == Chain.ARBITRUM:
        return NoPricedToken_conversion(
            original=NoPricedToken_item(
                token_address=xgrail_token, chain=chain, block=block
            ),
            converted=NoPricedToken_item(
                token_address=grail_token, chain=chain, block=block
            ),
            conversion_rate=0.5,
        )


def olynx(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    # call option token that is used as the emission token for the Lynex dex
    # The discount rate is subject to change and based on market conditions.

    olynx_token = "0x63349BA5E1F71252eCD56E8F950D1A518B400b60".lower()

    if address.lower() == olynx_token:
        # check block and chain
        if chain == Chain.LINEA and block > 0 and block < 2207782:
            logging.getLogger(__name__).warning(
                f" No data for {chain.fantasy_name} oLYNEX before block 2207782"
            )
            return None

        # get the underlying token and discount rate from the contract
        erc20 = build_erc20_helper(
            chain=chain,
            address=olynx_token,
            abi_filename="optionTokenv3",
            abi_path=(CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi")
            + "/lynex",
            block=block,
        )
        # get the token address to convert to
        underlying_token = erc20.call_function_autoRpc("UNDERLYING_TOKEN")
        if not underlying_token:
            logging.getLogger(__name__).error(
                f" Can't get the underlying token for oLYNEX. Using LYNEX token as fallback"
            )
            underlying_token = "0x1a51b19ce03dbe0cb44c1528e34a7edd7771e9af".lower()

        discount_rate = erc20.call_function_autoRpc("discount")
        conversion_rate = (100 - discount_rate) / 100

        return NoPricedToken_conversion(
            original=NoPricedToken_item(
                token_address=olynx_token, chain=chain, block=block
            ),
            converted=NoPricedToken_item(
                token_address=underlying_token.lower(), chain=chain, block=block
            ),
            conversion_rate=conversion_rate,
        )


def oath(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    # https://www.oath.eco/oath-v2-token-migration-guide/
    # convert OATHv1 to OATHv2 at same price
    if chain == Chain.ARBITRUM:
        oathv1 = "0xa1150db5105987cec5fd092273d1e3cbb22b378b".lower()
        oathv2 = "0x00e1724885473B63bCE08a9f0a52F35b0979e35A".lower()
    elif chain == Chain.OPTIMISM:
        oathv1 = "0x39fde572a18448f8139b7788099f0a0740f51205".lower()
        oathv2 = "0x00e1724885473B63bCE08a9f0a52F35b0979e35A".lower()
    elif chain == Chain.ETHEREUM:
        oathv1 = "0x6f9c26fa731c7ea4139fa669962cf8f1ce6c8b0b".lower()
        oathv2 = "0xd20523b39fAF1D6e9023a4D6085f87B7b0DE7926".lower()
    elif chain == Chain.POLYGON:
        oathv1 = "0xc2c52ff5134596f5ff1b1204d3304228f2432836".lower()
        oathv2 = "0x7c603C3C0C97a565cf202c94AB5298bF8510f7dc".lower()
    elif chain == Chain.AVALANCHE:
        oathv1 = "0x2c69095d81305f1e3c6ed372336d407231624cea".lower()
        oathv2 = "0xAD090976CE846935DCfF1dEd852668beeD912916".lower()
    elif chain == Chain.BSC:
        oathv1 = "0xd3c6ceedd1cc7bd4304f72b011d53441d631e662".lower()
        oathv2 = "0x73f4C95AF5C2892253c068850B8C9a753636f58d".lower()
    else:
        return None

    if address.lower() == oathv1:
        return NoPricedToken_conversion(
            original=NoPricedToken_item(token_address=oathv1, chain=chain, block=block),
            converted=NoPricedToken_item(
                token_address=oathv2, chain=chain, block=block
            ),
            conversion_rate=1,
        )


# ####
def esPLS(chain: Chain, address: str, block: int) -> NoPricedToken_conversion:
    # Escrowed PLS: Plutus DAO token not convertable (as far as known now)
    return None


# ADD HERE THE TOKENS THAT ARE NOT PRICED IN ANY POOL
TOKEN_ADDRESS_CONVERSION = {
    Chain.ETHEREUM: {
        # xGamma--Gamma
        "0x26805021988F1a45dC708B5FB75Fc75F21747D8c".lower(): xgamma,
        # OATHv1--OATHv2
        "0x6f9c26fa731c7ea4139fa669962cf8f1ce6c8b0b".lower(): oath,
    },
    Chain.ARBITRUM: {
        # xRAM--RAM
        "0xaaa1ee8dc1864ae49185c368e8c64dd780a50fb7".lower(): xram,
        # xGRAIL--GRAIL
        "0x3caae25ee616f2c8e13c74da0813402eae3f496b".lower(): xgrail,
        # OATHv1--OATHv2
        "0xa1150db5105987cec5fd092273d1e3cbb22b378b".lower(): oath,
    },
    Chain.POLYGON: {
        # oRETRO--RETRO
        "0x3a29cab2e124919d14a6f735b6033a3aad2b260f".lower(): oretro,
        # ANGLE -> ethereum
        "0x900f717ea076e1e7a484ad9dd2db81ceec60ebf1".lower(): angle,
        # CSUSHI (cToken) -> ethereum
        "0x26aa9b3d8a49a2ed849ac66ea9aa37ee36bc6b24".lower(): csushi,
        # OATHv1--OATHv2
        "0xc2c52ff5134596f5ff1b1204d3304228f2432836".lower(): oath,
    },
    Chain.OPTIMISM: {
        # ANGLE -> ethereum
        "0x58441e37255b09f9f545e9dc957f1c41658ff665".lower(): angle,
        # OATHv1--OATHv2
        "0x39fde572a18448f8139b7788099f0a0740f51205".lower(): oath,
    },
    Chain.AVALANCHE: {
        # xPHAR--PHAR
        "0xaaae58986b24e422740c8f22b3efb80bcbd68159".lower(): xphar,
        # OATHv1--OATHv2
        "0x2c69095d81305f1e3c6ed372336d407231624cea".lower(): oath,
    },
    Chain.LINEA: {
        # oLYNEX--LYNEX(or configured token)
        "0x63349BA5E1F71252eCD56E8F950D1A518B400b60".lower(): olynx,
    },
    Chain.MANTLE: {
        # xCLEO--CLEO
        "0xAAAE58986b24e422740C8F22B3efB80BCbD68159".lower(): xcleo,
    },
    Chain.BSC: {
        # OATHv1--OATHv2
        "0xd3c6ceedd1cc7bd4304f72b011d53441d631e662".lower(): oath,
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
