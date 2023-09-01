from bins.general.enums import Chain, Protocol

#### STATIC DEFAULTS ####
WEB3_CHAIN_IDS = {chain.database_name: chain.id for chain in Chain}


STATIC_REGISTRY_ADDRESSES = {
    "ethereum": {
        "hypervisors": {
            "uniswapv3": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946".lower(),
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
        },
        "MasterChefV2Registry": {
            "camelot": "0x26da8473AaA54e8c7835fA5fdd1599eB4c144d31".lower(),
        },
        "zyberswap_v1_masterchefs": [
            "0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd".lower(),
        ],
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0xA86CC1ae2D94C6ED2aB3bF68fB128c2825673267".lower(),
        },
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
    },
    "fantom": {
        "hypervisors": {
            "spiritswap": "0xf874d4957861e193aec9937223062679c14f9aca".lower(),
        },
        "MasterChefV2Registry": {
            "spiritswap": "0xf5bfa20f4a77933fee0c7bb7f39e7642a070d599".lower(),
        },
    },
    "moonbeam": {
        "hypervisors": {
            "stellaswap": "0x6002d7714e8038f2058e8162b0b86c0b19c31908".lower(),
            "beamswap": "0xb7dfc304d9cd88d98a262ce5b6a39bb9d6611063".lower(),
        },
        "MasterChefV2Registry": {
            "stellaswap": "0xd08b593eb3460b7aa5ce76ffb0a3c5c938fd89b8".lower(),
            "beamswap": "0x1cc4ee0cb063e9db36e51f5d67218ff1f8dbfa0f".lower(),
        },
    },
    "avalanche": {
        "hypervisors": {
            "glacier": "0x3FE6F25DA67DC6AD2a5117a691f9951eA14d6f15".lower(),
        },
        "MasterChefV2Registry": {
            "glacier": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599".lower(),
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


DEX_POOLS = {
    Chain.ETHEREUM: {
        "USDC_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640".lower(),
        },
        "WETH_RPL": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xe42318ea3b998e8355a3da364eb9d48ec725eb45".lower(),
        },
        "GAMMA_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x4006bed7bf103d70a1c6b7f1cef4ad059193dc25".lower(),
        },
        "AXL_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x5b0d2536f0c970b8d9cbf3959460fb97ce808ade".lower(),
        },
        "RAW_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xcde473286561d9b876bead3ac7cc38040f738d3f".lower(),
            "token0": "0xb41f289d699c5e79a51cb29595c203cfae85f32a".lower(),
            "token1": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".lower(),
        },
    },
    Chain.OPTIMISM: {
        "WETH_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x85149247691df622eaf1a8bd0cafd40bc45154a9".lower(),
        },
        "WETH_OP": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x68f5c0a2de713a54991e01858fd27a3832401849".lower(),
        },
    },
    Chain.POLYGON: {
        "WMATIC_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xae81fac689a1b4b1e06e7ef4a2ab4cd8ac0a087d".lower(),
        },
        "WMATIC_QI": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x5cd94ead61fea43886feec3c95b1e9d7284fdef3".lower(),
        },
        "WMATIC_QUICK": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x9f1a8caf3c8e94e43aa64922d67dff4dc3e88a42".lower(),
        },
        "WMATIC_DQUICK": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xb8d00c66accdc01e78fd7957bf24050162916ae2".lower(),
        },
        "WMATIC_GHST": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x80deece4befd9f27d2df88064cf75f080d3ce1b2".lower(),
        },
        "WMATIC_ANKR": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x2f2dd65339226df7441097a710aba0f493879579".lower(),
        },
        "USDC_DAVOS": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xfb0bc232cd11dbe804b489860c470b7f9cc80d9f".lower(),
        },
        "USDC_GIDDY": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x65c30f39b880bdd9616280450c4b41cc74b438b7".lower(),
        },
        "WMATIC_LCD": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xd9c2c978915b907df04972cb3f577126fe55143c".lower(),
        },
        "WOMBAT_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xaf835698673655e9910de8398df6c5238f5d3aeb".lower(),
        },
        "USDC_FIS": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x2877703a3ba3e712d684d22bd6d60cc0031d84e8".lower(),
        },
        "SD_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x5d0acfa39a0fca603147f1c14e53f46be76984bc".lower(),
        },
        "USDC_DAI": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xe7e0eb9f6bcccfe847fdf62a3628319a092f11a2".lower(),
        },
        "USDC_axlPEPE": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x27c30be7bf776e31e2cbbb9fe6db18d86f09da01".lower(),
            "token0": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower(),
            "token1": "0x8bae3f5eb10f39663e57be19741fd9ccef0e113a".lower(),
        },
        "oRETRO_RETRO": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x387FBcE5E2933Bd3a7243D0be2aAC8fD9Ab3D55d".lower(),
            "token0": "0x3a29cab2e124919d14a6f735b6033a3aad2b260f".lower(),
            "token1": "0xbfa35599c7aebb0dace9b5aa3ca5f2a79624d8eb".lower(),
        },
    },
    Chain.POLYGON_ZKEVM: {
        "WETH_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xc44ad482f24fd750caeba387d2726d8653f8c4bb".lower(),
        },
        "QUICK_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x1247b70c4b41890e8c1836e88dd0c8e3b23dd60e".lower(),
        },
        "WETH_MATIC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xb73abfb5a2c89f4038baa476ff3a7942a021c196".lower(),
        },
        "WETH_WBTC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xfc4a3a7dc6b62bd2ea595b106392f5e006083b83".lower(),
            "token0": "0x4f9a0e7fd2bf6067db6994cf12e4495df938e6e9".lower(),
            "token1": "0xea034fb02eb1808c2cc3adbc15f447b93cbe08e1".lower(),
        },
        "USDC_DAI": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x68cc0516162b423930cd8448a2a00310e841e7f5".lower(),
            "token0": "0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035".lower(),  # USDC
            "token1": "0xc5015b9d9161dca7e18e32f6f25c4ad850731fd4".lower(),  # DAI
        },
        "USDT_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x9591b8a30c3a52256ea93e98da49ee43afa136a8".lower(),
            "token0": "0x1e4a5963abfd975d8c9021ce480b42188849d41d".lower(),  # USDT
            "token1": "0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035".lower(),  # USDC
        },
    },
    Chain.BSC: {
        "THE_WBNB": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x51bd5e6d3da9064d59bcaa5a76776560ab42ceb8".lower(),
        },
        "THE_USDT": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x98a0004b8e9fe161369528a2e07de56c15a27d76".lower(),
        },
        "USDT_WBNB": {
            "protocol": Protocol.PANCAKESWAP,
            "address": "0x36696169c63e42cd08ce11f5deebbcebae652050".lower(),
            "token0": "0x55d398326f99059ff775485246999027b3197955".lower(),
            "token1": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c".lower(),
        },
        "USDT_USDC": {
            "protocol": Protocol.PANCAKESWAP,
            "address": "0x92b7807bf19b7dddf89b706143896d05228f3121".lower(),
            "token0": "0x55d398326f99059ff775485246999027b3197955".lower(),
            "token1": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d".lower(),
        },
    },
    Chain.AVALANCHE: {},
    Chain.ARBITRUM: {
        "DAI_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xf0428617433652c9dc6d1093a42adfbf30d29f74".lower(),
        },
        "USDT_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x8c9d230d45d6cfee39a6680fb7cb7e8de7ea8e71".lower(),
            "token0": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9".lower(),
            "token1": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8".lower(),
        },
        "WETH_USDT": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x641c00a822e8b671738d32a431a4fb6074e5c79d".lower(),
            "token0": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(),
            "token1": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9".lower(),
        },
    },
    Chain.MOONBEAM: {},
}


DEX_POOLS_PRICE_PATHS = {
    Chain.ETHEREUM: {
        # GAMMA
        "0x6bea7cfef803d1e3d5f7c0103f7ded065644e197".lower(): [
            (DEX_POOLS[Chain.ETHEREUM]["GAMMA_WETH"], 1),
            (
                DEX_POOLS[Chain.ETHEREUM]["USDC_WETH"],
                0,
            ),
        ],
        # RPL
        "0xd33526068d116ce69f19a9ee46f0bd304f21a51f".lower(): [
            (
                DEX_POOLS[Chain.ETHEREUM]["WETH_RPL"],
                0,
            ),
            (
                DEX_POOLS[Chain.ETHEREUM]["USDC_WETH"],
                0,
            ),
        ],
        # AXL
        "0x467719ad09025fcc6cf6f8311755809d45a5e5f3".lower(): [
            (DEX_POOLS[Chain.ETHEREUM]["AXL_USDC"], 1)
        ],
    },
    Chain.OPTIMISM: {
        # OP
        "0x4200000000000000000000000000000000000042".lower(): [
            (DEX_POOLS[Chain.OPTIMISM]["WETH_OP"], 0),
            (DEX_POOLS[Chain.OPTIMISM]["WETH_USDC"], 1),
        ],
        # MOCK-OPT
        "0x601e471de750cdce1d5a2b8e6e671409c8eb2367".lower(): [
            (DEX_POOLS[Chain.OPTIMISM]["WETH_OP"], 0),
            (DEX_POOLS[Chain.OPTIMISM]["WETH_USDC"], 1),
        ],
    },
    Chain.POLYGON: {
        # USDC
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower(): [],
        # WMATIC
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1)
        ],
        # QI
        "0x580a84c73811e1839f75d86d75d88cca0c241ff4".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_QI"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # QUICK
        "0xb5c064f955d8e7f38fe0460c556a72987494ee17".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_QUICK"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # dQUICK
        "0x958d208cdf087843e9ad98d23823d32e17d723a1".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_DQUICK"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # GHST
        "0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_GHST"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # ANKR
        "0x101a023270368c0d50bffb62780f4afd4ea79c35".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_ANKR"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # DAVOS
        "0xec38621e72d86775a89c7422746de1f52bba5320".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_DAVOS"], 0)
        ],
        # GIDDY
        "0x67eb41a14c0fe5cd701fc9d5a3d6597a72f641a6".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_GIDDY"], 0)
        ],
        # LCD
        "0xc2a45fe7d40bcac8369371b08419ddafd3131b4a".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_LCD"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # WOMBAT
        "0x0c9c7712c83b3c70e7c5e11100d33d9401bdf9dd".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WOMBAT_USDC"], 1),
        ],
        # FIS
        "0x7a7b94f18ef6ad056cda648588181cda84800f94".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_FIS"], 0),
        ],
        # SD
        "0x1d734a02ef1e1f5886e66b0673b71af5b53ffa94".lower(): [
            (DEX_POOLS[Chain.POLYGON]["SD_USDC"], 1),
        ],
        # DAI
        "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_DAI"], 0),
        ],
        # axlPEPE
        "0x8bae3f5eb10f39663e57be19741fd9ccef0e113a".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_axlPEPE"], 0),
        ],
    },
    Chain.POLYGON_ZKEVM: {
        # WMATIC
        "0xa2036f0538221a77a3937f1379699f44945018d0".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_MATIC"], 0),
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_USDC"], 1),
        ],
        # QUICK
        "0x68286607a1d43602d880d349187c3c48c0fd05e6".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["QUICK_USDC"], 1),
        ],
        # WETH
        "0x4f9a0e7fd2bf6067db6994cf12e4495df938e6e9".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_USDC"], 1),
        ],
        # WBTC
        "0xea034fb02eb1808c2cc3adbc15f447b93cbe08e1".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_WBTC"], 0),
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_USDC"], 1),
        ],
        # DAI
        "0xc5015b9d9161dca7e18e32f6f25c4ad850731fd4".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["USDC_DAI"], 0),
        ],
        # USDT
        "0x1e4a5963abfd975d8c9021ce480b42188849d41d".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["USDT_USDC"], 1),
        ],
    },
    Chain.BSC: {
        # THE
        "0xf4c8e32eadec4bfe97e0f595add0f4450a863a11".lower(): [
            (DEX_POOLS[Chain.BSC]["THE_USDT"], 0),
        ],
        # WBNB
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c".lower(): [
            (DEX_POOLS[Chain.BSC]["USDT_WBNB"], 0),
            (DEX_POOLS[Chain.BSC]["USDT_USDC"], 0),
        ],
    },
    Chain.AVALANCHE: {},
    Chain.ARBITRUM: {
        # DAI
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1".lower(): [
            (DEX_POOLS[Chain.ARBITRUM]["DAI_USDC"], 1),
        ],
        # USDT
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9".lower(): [
            (DEX_POOLS[Chain.ARBITRUM]["USDT_USDC"], 1),
        ],
    },
    Chain.MOONBEAM: {},
}


USDC_TOKEN_ADDRESSES = {
    Chain.ETHEREUM: ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".lower()],
    Chain.OPTIMISM: ["0x7f5c764cbc14f9669b88837ca1490cca17c31607".lower()],
    Chain.POLYGON: ["0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower()],
    Chain.POLYGON_ZKEVM: ["0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035".lower()],
    Chain.BSC: ["0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d".lower()],
    Chain.AVALANCHE: ["0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e".lower()],
    Chain.ARBITRUM: ["0xff970a61a04b1ca14834a43f5de4533ebddb5cc8".lower()],
    Chain.MOONBEAM: ["0x931715fee2d06333043d11f658c8ce934ac61d0c".lower()],
    #    Chain.CELO:[],
}

# token without price that need manual conversion to get its value. Specify the address of the corresponding token like, XGAMMA--GAMMA
TOKEN_ADDRESS_CONVERSION = {
    Chain.ETHEREUM: {
        # xGamma--Gamma
        "0x26805021988F1a45dC708B5FB75Fc75F21747D8c".lower(): "0x6bea7cfef803d1e3d5f7c0103f7ded065644e197".lower(),
    },
    Chain.ARBITRUM: {
        # xRAM--RAM
        "0xaaa1ee8dc1864ae49185c368e8c64dd780a50fb7".lower(): "0xaaa6c1e32c55a7bfa8066a6fae9b42650f262418".lower()
    },
}


# exclude list of token addresses
TOKEN_ADDRESS_EXCLUDE = {
    Chain.POLYGON: {
        "0xd8ef817FFb926370dCaAb8F758DDb99b03591A5e".lower(): "AnglaMerkl",
    },
    Chain.ARBITRUM: {
        "0xe0688a2fe90d0f93f17f273235031062a210d691".lower(): "AnglaMerkl",
    },
}
