from bins.general.enums import Chain

# https://docs.chain.link/data-feeds/price-feeds/addresses/
CHAINLINK_USD_PRICE_FEEDS = {
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
    }
}
