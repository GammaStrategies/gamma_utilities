from dataclasses import dataclass
import logging
from bins.general.enums import Chain
from bins.w3.builders import build_erc20_helper


@dataclass
class NoPricedToken_conversion:
    original_token_address: str
    converted_token_address: str
    conversion_rate: float


### CONFIGURE HERE THE PROCESSES TO GET THE RATE OF TOKENS THAT ARE NOT PRICED IN THE POOL
def xgamma(chain: Chain, address: str) -> NoPricedToken_conversion:
    gamma_token = "0x6bea7cfef803d1e3d5f7c0103f7ded065644e197".lower()
    xgamma_token = "0x26805021988F1a45dC708B5FB75Fc75F21747D8c".lower()

    # get balanceOf Gamma token
    try:
        gamma_contract = build_erc20_helper(chain=chain, address=gamma_token)
        xgamma_contract = build_erc20_helper(chain=chain, address=xgamma_token)
        gamma_staked = gamma_contract.balanceOf(address=address)
        xgamma_totalSupply = xgamma_contract.totalSupply

        return NoPricedToken_conversion(
            original_token_address=xgamma_token,
            converted_token_address=gamma_token,
            conversion_rate=gamma_staked / xgamma_totalSupply,
        )
    except Exception as e:
        logging.getLogger(__name__).error(f" Can't get xGamma token price. Error: {e} ")

    return None


def xram(chain: Chain, address: str) -> NoPricedToken_conversion:
    # xRam is a buy option of RAM with 30% price penalty when selling back to RAM previous to 90 days
    ram_token = "0xaaa6c1e32c55a7bfa8066a6fae9b42650f262418".lower()
    xram_token = "0xaaa1ee8dc1864ae49185c368e8c64dd780a50fb7".lower()

    return NoPricedToken_conversion(
        original_token_address=xram_token,
        converted_token_address=ram_token,
        conversion_rate=0.7,
    )


# ADD HERE THE TOKENS THAT ARE NOT PRICED IN ANY POOL
TOKEN_ADDRESS_CONVERSION = {
    Chain.ETHEREUM: {
        # xGamma--Gamma
        "0x26805021988F1a45dC708B5FB75Fc75F21747D8c".lower(): xgamma,
    },
    Chain.ARBITRUM: {
        # xRAM--RAM
        "0xaaa1ee8dc1864ae49185c368e8c64dd780a50fb7".lower(): xram
    },
}


def no_priced_token_conversions(chain: Chain, address: str) -> NoPricedToken_conversion:
    """returnds the price of a token in a specific chain that is not priced for some reason ( no public pool) but has a conversion rate to another token

    Args:
        chain (Chain):
        address (str):
    """
    if callme := TOKEN_ADDRESS_CONVERSION.get(chain, {}).get(address.lower(), None):
        return callme(chain, address)

    return None
