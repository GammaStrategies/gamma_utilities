from bins.general.enums import Chain

# https://docs.chain.link/data-feeds/price-feeds/addresses/
CHAINLINK_USD_PRICE_FEEDS = {
    Chain.ETHEREUM: {
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".lower(): {
            "token": "WETH",
            "address_feed": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419".lower(),
            "creation_block": 10606501,
        },
        "0xdac17f958d2ee523a2206206994597c13d831ec7".lower(): {
            "token": "USDT",
            "address_feed": "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D".lower(),
            "creation_block": 11870289,
        },
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".lower(): {
            "token": "USDC",
            "address_feed": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6".lower(),
            "creation_block": 11869355,
        },
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599".lower(): {
            "token": "WBTC",
            "address_feed": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c".lower(),
            "creation_block": 10606501,
        },
        "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984".lower(): {
            "token": "UNI",
            "address_feed": "0x553303d460EE0afB37EdFf9bE42922D8FF63220e".lower(),
            "creation_block": 11317271,
        },
        "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9".lower(): {
            "token": "AAVE",
            "address_feed": "0x547a514d5e3769680Ce22B2361c10Ea13619e8a9".lower(),
            "creation_block": 11179893,
        },
    },
    Chain.ARBITRUM: {
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(): {
            "token": "WETH",
            "address_feed": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612".lower(),
            "creation_block": 101490,
        },
    },
    Chain.BASE: {
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca".lower(): {
            "token": "USDbC",
            "address_feed": "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B".lower(),
            "creation_block": 2093500,
        },
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".lower(): {
            "token": "USDC",
            "address_feed": "0x7e860098F58bBFC8648a4311b374B1D669a2bc6B".lower(),
            "creation_block": 2093500,
        },
        "0x4200000000000000000000000000000000000006".lower(): {
            "token": "WETH",
            "address_feed": "0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70".lower(),
            "creation_block": 2092862,
        },
        "0x50c5725949a6f0c72e6c4a641f24049a917db0cb".lower(): {
            "token": "DAI",
            "address_feed": "0x591e79239a7d679378eC8c847e5038150364C78F".lower(),
            "creation_block": 2105150,
        },
    },
    Chain.AVALANCHE: {
        "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7".lower(): {
            "token": "WAVAX",
            "address_feed": "0x0A77230d17318075983913bC2145DB16C7366156".lower(),
            "creation_block": 2655715,
        },
        "0x152b9d0fdc40c096757f570a51e494bd4b943e50".lower(): {
            "token": "BTC.b",
            "address_feed": "0x86442E3a98558357d46E6182F4b262f76c4fa26F".lower(),
            "creation_block": 22835709,
        },
        "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab".lower(): {
            "token": "WETH.b",
            "address_feed": "0x976B3D034E162d8bD72D6b9C989d545b839003b0".lower(),
            "creation_block": 2656574,
        },
        "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7".lower(): {
            "token": "USDt",
            "address_feed": "0xEBE676ee90Fe1112671f19b6B7459bC678B67e8a".lower(),
            "creation_block": 2657390,
        },
        "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e".lower(): {
            "token": "USDC",
            "address_feed": "0xF096872672F44d6EBA71458D74fe67F9a77a23B9".lower(),
            "creation_block": 2713394,
        },
        "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664".lower(): {
            "token": "USDC.e",
            "address_feed": "0xF096872672F44d6EBA71458D74fe67F9a77a23B9".lower(),
            "creation_block": 2713394,
        },
    },
    Chain.GNOSIS: {
        "0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1".lower(): {
            "token": "WETH",
            "address_feed": "0xa767f745331D267c7751297D982b050c93985627".lower(),
        },
        "0x6c76971f98945ae98dd7d4dfca8711ebea946ea6".lower(): {
            "token": "stETH",
            "address_feed": "0x229e486Ee0D35b7A9f668d10a1e6029eEE6B77E0".lower(),
        },
        "0x4ecaba5870353805a9f068101a40e0f32ed605c6".lower(): {
            "token": "USDT",
            "address_feed": "0x68811D7DF835B1c33e6EEae8E7C141eF48d48cc7".lower(),
        },
        "0x9c58bacc331c9aa871afd802db6379a98e80cedb".lower(): {
            "token": "GNO",
            "address_feed": "0x22441d81416430A54336aB28765abd31a792Ad37".lower(),
        },
    },
    Chain.METIS: {
        "0xea32a96608495e54156ae48931a7c20f0dcc1a21".lower(): {
            "token": "m.USDC",
            "address_feed": "0x663855969c85F3BE415807250414Ca9129533a5f".lower(),
        },
        "0xbb06dca3ae6887fabf931640f67cab3e3a16f4dc".lower(): {
            "token": "m.USDT",
            "address_feed": "0xbb06dca3ae6887fabf931640f67cab3e3a16f4dc".lower(),
        },
        "0x420000000000000000000000000000000000000a".lower(): {
            "token": "WETH",
            "address_feed": "0x3BBe70e2F96c87aEce7F67A2b0178052f62E37fE".lower(),
        },
        "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000".lower(): {
            "token": "METIS",
            "address_feed": "0xD4a5Bb03B5D66d9bf81507379302Ac2C2DFDFa6D".lower(),
            # in monitoring state
        },
        "0x75cb093e4d61d2a2e65d8e0bbb01de8d89b53481".lower(): {
            "token": "WMETIS",
            "address_feed": "0xD4a5Bb03B5D66d9bf81507379302Ac2C2DFDFa6D".lower(),
            # in monitoring state
        },
    },
    Chain.LINEA: {
        "0x3aab2285ddcddad8edf438c1bab47e1a9d05a9b4".lower(): {
            "token": "WBTC",
            "address_feed": "0x7A99092816C8BD5ec8ba229e3a6E6Da1E628E1F9".lower(),
        },
        "0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f".lower(): {
            "token": "WETH",
            "address_feed": "0x3c6Cd9Cc7c7a4c2Cf5a82734CD249D7D593354dA".lower(),
        },
        "0x176211869ca2b568f2a7d4ee941e073a821ee1ff".lower(): {
            "token": "USDC",
            "address_feed": "0xAADAa473C1bDF7317ec07c915680Af29DeBfdCb5".lower(),
        },
        "0xa219439258ca9da29e9cc4ce5596924745e12b93".lower(): {
            "token": "USDT",
            "address_feed": "0xefCA2bbe0EdD0E22b2e0d2F8248E99F4bEf4A7dB".lower(),
        },
        "0xb5bedd42000b71fdde22d3ee8a79bd49a568fc8f".lower(): {
            "token": "wstETH",
            "address_feed": "0x8eCE1AbA32716FdDe8D6482bfd88E9a0ee01f565".lower(),
        },
    },
}
