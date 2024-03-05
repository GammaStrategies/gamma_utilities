import os
from .general.enums import Chain, Protocol

from .general.general_utilities import (
    load_configuration,
    check_configuration_file,
)
from .general.command_line import parse_commandLine_args
from .log import log_helper

CONFIGURATION = {}

# convert command line arguments to dict variables
cml_parameters = parse_commandLine_args()


# load configuration
CONFIGURATION = (
    load_configuration(cfg_name=cml_parameters.config)
    if cml_parameters.config
    else load_configuration()
)

# check configuration
check_configuration_file(CONFIGURATION)

# add cml_parameters into loaded config ( this is used later on to load again the config file to be able to update on-the-fly vars)
if "_custom_" not in CONFIGURATION.keys():
    CONFIGURATION["_custom_"] = {}
CONFIGURATION["_custom_"]["cml_parameters"] = cml_parameters

# add log subfolder if set
if CONFIGURATION["_custom_"]["cml_parameters"].log_subfolder:
    CONFIGURATION["logs"]["save_path"] = os.path.join(
        CONFIGURATION["logs"]["save_path"],
        CONFIGURATION["_custom_"]["cml_parameters"].log_subfolder,
    )

# setup logging
log_helper.setup_logging(customconf=CONFIGURATION)


#### ADD STATIC CONFIG HERE ####

STATIC_REGISTRY_ADDRESSES = {
    "ethereum": {
        "hypervisors": {
            "uniswapv3": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946".lower(),
            "pancakeswap": "0x2E18B825b049c4994370b0DB6C35d0100295b96C".lower(),
        },
        "MasterChefV2Registry": {},
        "feeDistributors": [
            "0x07432C021f0A65857a3Ab608600B9FEABF568EA0".lower(),
            "0x8451122f06616baff7feb10afc2c4f4132fc4709".lower(),
        ],
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0x0E632a15EbCBa463151B5367B4fCF91313e389a6".lower(),
        },
    },
    "polygon": {
        "hypervisors": {
            "uniswapv3": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055".lower(),
            "quickswap": "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd".lower(),
            "retro": "0xcac19d43c9558753d7535978a370055614ce832e".lower(),
            "sushi": "0x97686103b3e7238ca6c2c439146b30adbd84a593".lower(),
            "ascent": "0x7b9c2f68f16c3618bb45616fb98d83f94fd7062e".lower(),
            # "zero": "".lower(),
        },
        "MasterChefRegistry": "0x135B02F8b110Fe2Dd8B6a5e2892Ee781264c2fbe".lower(),
        "MasterChefV2Registry": {
            "uniswapv3": "0x02C8D3FCE5f072688e156F503Bd5C7396328613A".lower(),
            "quickswap": "0x62cD3612233B2F918BBf0d17B9Eda3005b84e16f".lower(),
            "retro": "0x838f6c0189cd8fd831355b31d71b03373480ab83".lower(),
            "sushi": "0x73cb7b82e43759b637e1eb833b6c2711f3e45dca".lower(),
        },
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0x9418D0aa02fCE40804aBF77bb81a1CcBeB91eaFC".lower(),
        },
        "uniproxy": {
            "retro": "0x923fe416408b1db7c72e5c8fd3a82bc06ff74621".lower(),
        },
    },
    "optimism": {
        "hypervisors": {
            "uniswapv3": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599".lower(),
        },
        "MasterChefV2Registry": {
            "uniswapv3": "0x81d9bF667205662bfa729C790F67D97D54EA391C".lower(),
        },
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0xc2c7a0d9a9e0467090281c3a4f28D40504d08FB4".lower(),
        },
    },
    "arbitrum": {
        "hypervisors": {
            "uniswapv3": "0x66CD859053c458688044d816117D5Bdf42A56813".lower(),
            "zyberswap": "0x37595FCaF29E4fBAc0f7C1863E3dF2Fe6e2247e9".lower(),
            "camelot": "0xa216C2b6554A0293f69A1555dd22f4b7e60Fe907".lower(),
            "sushi": "0x0f867f14b39a5892a39841a03ba573426de4b1d0".lower(),
            "ramses": "0x34ffbd9db6b9bd8b095a0d156de69a2ad2944666".lower(),
            "pancakeswap": "0x166cd995f9301590e381c488ffd4f18c3ca38a27".lower(),
        },
        "MasterChefV2Registry": {
            "camelot": "0x26da8473AaA54e8c7835fA5fdd1599eB4c144d31".lower(),
            "pancakeswap": "0xde0a8b0b323e7c7fB7a11Dd727af6E33970728A9".lower(),
            "sushi": "0x5f0589ae3ff36bcd1d7a5b1e5287b1ed65f1a934".lower(),
            "ramses": "0x9dfdf32ae82c7e8ebc156ea28e6637b120e00d12".lower(),
        },
        "zyberswap_v1_masterchefs": [
            "0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd".lower(),
        ],
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0xA86CC1ae2D94C6ED2aB3bF68fB128c2825673267".lower(),
        },
        "camelot_nft": {
            "master": "0x55401A4F396b3655f66bf6948A1A4DC61Dfc21f4".lower(),
            "nftPoolFactory": "0x6dB1EF0dF42e30acF139A70C1Ed0B7E6c51dBf6d".lower(),
            "nitroPoolFactory": "0xe0a6b372Ac6AF4B37c7F3a989Fe5d5b194c24569".lower(),
        },
        "uniproxy": {
            "sushi": "0x530071b0373ab3029cad32e0c19b75253e231b69".lower(),
            "ramses": "0xc6dd68b546d696d5a31837b05065a151d6b6f892".lower(),
            "camelot": "0xdeF431652F824E04B678a49ce84267f560884470".lower(),
        },
        # important to identify ultimate depositor
        "deposit_proxies": [
            # Camelot spNFT Gamma uniproxy helper
            "0x851b3Fb3c3178Cd3FBAa0CdaAe0175Efa15a30f1".lower(),
        ],
    },
    "celo": {
        "hypervisors": {
            "uniswapv3": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569".lower(),
        },
        "MasterChefV2Registry": {},
    },
    "binance": {
        "hypervisors": {
            "uniswapv3": "0x0b4645179C1b668464Df01362fC6219a7ab3234c".lower(),
            "thena": "0xd4bcFC023736Db5617E5638748E127581d5929bd".lower(),
            "pancakeswap": "0x8d7884666C056555fDD423Cd37846968040651b3".lower(),
        },
        "MasterChefV2Registry": {},
    },
    "polygon_zkevm": {
        "hypervisors": {
            "quickswap": "0xD08B593eb3460B7aa5Ce76fFB0A3c5c938fd89b8".lower(),
        },
        "MasterChefV2Registry": {
            "quickswap": "0x5b8F58a33808222d1fF93C919D330cfA5c8e1B7d".lower(),
        },
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0xC16B81Af351BA9e64C1a069E3Ab18c244A1E3049".lower(),
        },
        "uniproxy": {
            "quickswap": "0x66CD859053c458688044d816117D5Bdf42A56813".lower(),
        },
    },
    "fantom": {
        "hypervisors": {
            "spiritswap": "0xf874d4957861e193aec9937223062679c14f9aca".lower(),
        },
        "MasterChefV2Registry": {
            "spiritswap": "0xf5bfa20f4a77933fee0c7bb7f39e7642a070d599".lower(),
        },
        "uniProxy": {
            "spiritswap": "0x6002d7714e8038f2058e8162b0b86c0b19c31908".lower(),
        },
    },
    "moonbeam": {
        "hypervisors": {
            "stellaswap": "0x6002d7714e8038f2058e8162b0b86c0b19c31908".lower(),
            "beamswap": "0xb7dfc304d9cd88d98a262ce5b6a39bb9d6611063".lower(),
            "uniswapv3": "0x688cb9492bd2C72016f1765d813B2D713aa1F4C7".lower(),
        },
        "MasterChefV2Registry": {
            "stellaswap": "0xd08b593eb3460b7aa5ce76ffb0a3c5c938fd89b8".lower(),
            "beamswap": "0x1cc4ee0cb063e9db36e51f5d67218ff1f8dbfa0f".lower(),
            "uniswapv3": "0x562c87e31952ed591e3e12522e80850c7d504dc1".lower(),
        },
        "uniproxy": {
            "beamswap": "0x1e2d8f84605d32a2cbf302e30bfd2387badf35dd".lower(),
            "stellaswap": "0x34c14dCc974f827e0aa8E7a3EB0Db2431d09068E".lower(),
        },
    },
    "avalanche": {
        "hypervisors": {
            "glacier": "0x3FE6F25DA67DC6AD2a5117a691f9951eA14d6f15".lower(),
            "pharaoh": "0x71ea9545ED7f8662a8B461d7cb0899745E3fb3E6".lower(),
        },
        "MasterChefV2Registry": {
            "glacier": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599".lower(),
        },
        "uniproxy": {
            "glacier": "0xE64c62244c48F9d0AA70d411432b825e2F8B05B0".lower(),
        },
    },
    "base": {
        "hypervisors": {
            "synthswap": "0x1e86a593e55215957c4755f1be19a229af3286f6".lower(),
            "sushi": "0x6d5c54F535b073B9C2206Baf721Af2856E5cD683".lower(),
            "basex": "0xB24DC81f8Be7284C76C7cF865b803807B3C2EF55".lower(),
            "baseswap": "0x8118C33513feC13f8cf488CCb4509190650F0e92".lower(),
            "swapbased": "0x39ce2eB762e7bFe19b6AD4D5bA384c67CE4051f0".lower(),
            "pancakeswap": "0x339685503dD534D27ce4a064314c2E5c7144aa92".lower(),
            "thick": "0xf1DF4F17e34Ba710DfFC487F73f1e19476E815a6".lower(),
        },
        "synthswap_v1_masterchefs": [
            "0xef153cb7bfc04c657cb7f582c7411556320098b9".lower(),
        ],
        "MasterChefV2Registry": {
            "pancakeswap": "0x0087cA4844cAE94B1c51dEc0f9434A6F92006Af9".lower(),
            "baseswap": "0x5981B43b74bb6DaA13a3cd97dddF4C52c9f0fC5F".lower(),
            "swapbased": "0x1dE56231bE01e4d91A085e68E573e4aDaf78693A".lower(),
            "thick": "0xD8266D45bB9fB3456b2b953535480dB7e4f95179".lower(),
        },
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0xC16B81Af351BA9e64C1a069E3Ab18c244A1E3049".lower(),
        },
        "uniproxy": {
            "synthswap": "0x38f61169D8bcc08cE303401A13332259F557B35f".lower(),
            "sushi": "0xc40F63879630dFF5b69dd6d287f7735E65e90702".lower(),
            "basex": "0x77a9BB1de36f0c431aaD0b1D63496F6D94256e7c".lower(),
        },
    },
    "mantle": {
        "hypervisors": {
            "fusionx": "0x683292172E2175bd08e3927a5e72FC301b161300".lower(),
        },
        "MasterChefV2Registry": {
            "fusionx": "0xC27DDd78FC49875Fe6F844B72bbf31DFBB099881".lower(),
        },
        "uniproxy": {
            "fusionx": "0xFc13Ebe7FEB9595D70195E9168aA7F3acE153621".lower(),
        },
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0xC16B81Af351BA9e64C1a069E3Ab18c244A1E3049".lower(),
        },
    },
    "linea": {
        "hypervisors": {
            "lynex": "0xC27DDd78FC49875Fe6F844B72bbf31DFBB099881".lower(),
        },
        "uniproxy": {
            "lynex": "0xFc13Ebe7FEB9595D70195E9168aA7F3acE153621".lower(),
        },
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0x5adDc89785D75C86aB939E9e15bfBBb7Fc086A87".lower(),
        },
    },
    "rollux": {
        "hypervisors": {
            "pegasys": "0x683292172E2175bd08e3927a5e72FC301b161300".lower(),
        },
        "MasterChefV2Registry": {
            "pegasys": "0xC27DDd78FC49875Fe6F844B72bbf31DFBB099881".lower(),
        },
    },
    "kava": {
        "hypervisors": {
            "kinetix": "0x683292172E2175bd08e3927a5e72FC301b161300".lower(),
        },
        "MasterChefV2Registry": {
            "kinetix": "0xC27DDd78FC49875Fe6F844B72bbf31DFBB099881".lower(),
        },
    },
    "opbnb": {
        "hypervisors": {
            "pancakeswap": "0x683292172E2175bd08e3927a5e72FC301b161300".lower(),
            "thena": "0xf14Fb95d6E7E1ab5fCdFfF7Ab203a84b9361E6FC".lower(),
        },
        "MasterChefV2Registry": {
            "pancakeswap": "0x38f61169D8bcc08cE303401A13332259F557B35f".lower(),
        },
        "uniproxy": {
            "pancakeswap": "0x1E86A593E55215957C4755f1BE19a229AF3286f6".lower(),
            "thena": "0xca8759814695516C34168BBedd86290964D37adA".lower(),
        },
    },
    "manta": {
        "hypervisors": {
            "aperture": "0x683292172e2175bd08e3927a5e72fc301b161300".lower(),
            "quickswap": "0x8a9570ec97534277Ade6e46d100939FbCE4968f0".lower(),
        },
        "MasterChefV2Registry": {
            "aperture": "0xc27ddd78fc49875fe6f844b72bbf31dfbb099881".lower(),
            "quickswap": "0xD6Cc4a33dA7557A629e819c68fB805ddB225f517".lower(),
        },
    },
    "metis": {
        "hypervisors": {
            "hercules": "0xFc13Ebe7FEB9595D70195E9168aA7F3acE153621".lower(),
        },
        "MasterChefV2Registry": {
            "hercules": "0x38f61169D8bcc08cE303401A13332259F557B35f".lower(),
        },
        "uniproxy": {
            "hercules": "0x5aCcDa2325CcfE8660522C7d5c3A885cDcF04501".lower(),
        },
    },
    "gnosis": {
        "hypervisors": {
            "swapr": "0x683292172E2175bd08e3927a5e72FC301b161300".lower(),
        },
        "MasterChefV2Registry": {},
        "uniproxy": {
            "swapr": "0x38f61169d8bcc08ce303401a13332259f557b35f".lower(),
        },
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0xFD0DFC837Fe7ED19B23df589b6F6Da5a775F99E0".lower(),
        },
    },
}


KNOWN_VALID_MASTERCHEFS = {
    "polygon": {
        "uniswapv3": ["0x570d60a60baa356d47fda3017a190a48537fcd7d"],
        "quickswap": [
            "0x20ec0d06f447d550fc6edee42121bc8c1817b97d",
            "0x68678cf174695fc2d27bd312df67a3984364ffdd",
        ],
    },
    "optimism": {
        "uniswapv3": ["0xc7846d1bc4d8bcf7c45a7c998b77ce9b3c904365"],
    },
}


#### STATIC PRICE ORACLES PATH ####


USDC_TOKEN_ADDRESSES = {
    Chain.ARBITRUM: [
        "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8".lower(),  # Bridged USDC (USDC.e)
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831".lower(),  # USD Coin (USDC)
    ],
    Chain.AVALANCHE: ["0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e".lower()],
    Chain.BASE: [
        "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".lower(),
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca".lower(),
    ],
    Chain.BSC: ["0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d".lower()],
    Chain.CELO: [
        "0x765DE816845861e75A25fCA122bb6898B8B1282a".lower(),
        "0x37f750b7cc259a2f741af45294f6a16572cf5cad".lower(),
    ],
    Chain.ETHEREUM: ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".lower()],
    Chain.FANTOM: ["0x04068da6c83afcfa0e13ba15a6696662335d5b75".lower()],
    Chain.LINEA: ["0x176211869ca2b568f2a7d4ee941e073a821ee1ff".lower()],
    Chain.MANTLE: ["0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9".lower()],
    Chain.MOONBEAM: [
        "0x931715fee2d06333043d11f658c8ce934ac61d0c".lower(),  # USD Coin (Wormhole) (USDC)
        "0x818ec0a7fe18ff94269904fced6ae3dae6d6dc0b".lower(),  # Multichain Wrapped USDC
        "0x8f552a71efe5eefc207bf75485b356a0b3f01ec9".lower(),  # USD Coin (USDC)],
    ],
    Chain.OPTIMISM: [
        "0x7f5c764cbc14f9669b88837ca1490cca17c31607".lower(),  # USD Coin (Bridged from Ethereum) (USDC.e)
        "0x0b2c639c533813f4aa9d7837caf62653d097ff85".lower(),  # USD Coin (USDC)
    ],
    Chain.POLYGON: [
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower(),
        "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359".lower(),
    ],
    Chain.POLYGON_ZKEVM: ["0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035".lower()],
    Chain.ROLLUX: ["0x368433cac2a0b8d76e64681a9835502a1f2a8a30".lower()],
    Chain.OPBNB: ["0x9e5aac1ba1a2e6aed6b32689dfcf62a509ca96f3".lower()],
    Chain.KAVA: [
        "0xfa9343c3897324496a05fc75abed6bac29f8a40f".lower(),
        "0x919c1c267bc06a7039e03fcc2ef738525769109c".lower(),  # usdt
    ],
    Chain.METIS: ["0xea32a96608495e54156ae48931a7c20f0dcc1a21".lower()],
    Chain.MANTA: ["0xb73603c5d87fa094b7314c74ace2e64d165016fb".lower()],
    Chain.GNOSIS: ["0xddafbb505ad214d7b80b1f830fccc89b60fb7a83".lower()],
}


# exclude list of token addresses
TOKEN_ADDRESS_EXCLUDE = {
    Chain.ETHEREUM: {
        "0x8d652c6d4a8f3db96cd866c1a9220b1447f29898".lower(): "AglaMerkl",
    },
    Chain.POLYGON: {
        "0xd8ef817FFb926370dCaAb8F758DDb99b03591A5e".lower(): "AglaMerkl",
        "0x74a2cac1c16cf1b47c176001bd5442029e1ddf3c".lower(): "testtoken",
    },
    Chain.ARBITRUM: {
        "0xe0688a2fe90d0f93f17f273235031062a210d691".lower(): "AglaMerkl",
    },
    Chain.OPTIMISM: {
        "0x41a65aae5d1c8437288d5a29b4d049897572758e".lower(): "MockAngleReward (aglaMerkl)",
    },
    Chain.AVALANCHE: {
        "0xAAA940c5CF5ECa01931bD361F71E288485d3187A".lower(): "Escrowed Token (xToken) pharaoh",
    },
}


# fixed priced tokens: token address and its price when block is lower than <block> variable -> ( use gamma's pool creation block)
# this is usefull on new tokens not yet listed on any DEX/AMM
TOKEN_ADDRESS_FIXED_PRICE = {
    Chain.ARBITRUM: {
        "0xaaa6c1e32c55a7bfa8066a6fae9b42650f262418".lower(): {
            "block": 124485017,
            "price": 0.03,
            "symbol": "OATH",
        },
    },
}
