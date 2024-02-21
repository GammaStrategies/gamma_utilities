from bins.general.enums import Chain

# https://docs.chain.link/data-feeds/price-feeds/addresses/
CHAINLINK_USD_PRICE_FEEDS = {
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
}