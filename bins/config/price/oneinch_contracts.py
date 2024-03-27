from bins.general.enums import Chain

# https://github.com/1inch/spot-price-aggregator
ONEINCH_SPOT_PRICE_CONTRACTS = {
    Chain.ETHEREUM: {
        "oracle": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8".lower(),
        "connectors": [
            "0x0000000000000000000000000000000000000000".lower(),  # ETH
            "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower(),  # WETH
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(),  # USDC
            "0x6B175474E89094C44Da98b954EedeAC495271d0F".lower(),  # DAI
            "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower(),  # USDT
            "0xFFfFfFffFFfffFFfFFfFFFFFffFFFffffFfFFFfF".lower(),  # NONE
            "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599".lower(),  # WBTC
            "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490".lower(),  # 3CRV
        ],
    },
    Chain.BSC: {
        "oracle": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8".lower(),
    },
    Chain.POLYGON: {
        "oracle": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8".lower(),
    },
    Chain.OPTIMISM: {
        "oracle": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8".lower(),
    },
    Chain.ARBITRUM: {
        "oracle": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8".lower(),
    },
    Chain.AVALANCHE: {
        "oracle": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8".lower(),
    },
    Chain.GNOSIS: {
        "oracle": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8".lower(),
    },
    Chain.FANTOM: {
        "oracle": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8".lower(),
    },
    Chain.BASE: {
        "oracle": "0x0AdDd25a91563696D8567Df78D5A01C9a991F9B8".lower(),
    },
}
