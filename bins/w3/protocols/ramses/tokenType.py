# https://github.com/RamsesExchange/Ramses-API/blob/1a8de09704e2b1d1d631d0d7acdb0e2c9c794306/cl/constants/tokenType.py
# https://github.com/RamsesExchange/Ramses-API/blob/master/cl/constants/tokenType.py
# Not using enum because it's not json serializable
Token_Type = {
    "LSD": -1,
    "NEAD": -2,
    "OTHERS": 0,
    "WETH": 1,
    "RAM": 1,
    "LOOSE_STABLE": 2,
    "STABLE": 3,
}

token_type_dict = {
    "gDAI": Token_Type["LOOSE_STABLE"],
    "LUSD": Token_Type["LOOSE_STABLE"],
    "ERN": Token_Type["LOOSE_STABLE"],
    "DOLA": Token_Type["LOOSE_STABLE"],
    "MAI": Token_Type["LOOSE_STABLE"],
    "GRAI": Token_Type["LOOSE_STABLE"],
    "jEUR": Token_Type["LOOSE_STABLE"],
    "USDC": Token_Type["STABLE"],
    "USDC.e": Token_Type["STABLE"],
    "USDT": Token_Type["STABLE"],
    "FRAX": Token_Type["STABLE"],
    "DAI": Token_Type["STABLE"],
    "frxETH": Token_Type["LSD"],
    "stETH": Token_Type["LSD"],
    "wstETH": Token_Type["LSD"],
    "neadRAM": Token_Type["NEAD"],
}

weth_address = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower()
ram_address = "0xAAA6C1E32C55A7Bfa8066A6FAE9b42650F262418".lower()
