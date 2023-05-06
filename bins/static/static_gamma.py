from attr import dataclass


static_gamma_data = {
    "ethereum": {
        "hypervisor_registry": [
            {
                "address": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946",
                "start_block": 13659998,
            }
        ],
        "fee_distributor": [
            {
                "address": "0x07432C021f0A65857a3Ab608600B9FEABF568EA0",
                "start_block": 13129847,
            },
            {
                "address": "0x8451122f06616baff7feb10afc2c4f4132fc4709",
                "start_block": 13129208,
            },
        ],
    },
    "polygon": {
        "hypervisor_registry": [
            {
                "address": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055",
                "start_block": 25305922,
            }
        ]
    },
    "optimism": {
        "hypervisor_registry": [
            {
                "address": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599",
                "start_block": 6538026,
            }
        ]
    },
    "arbitrum": {
        "hypervisor_registry": [
            {
                "address": "0x66CD859053c458688044d816117D5Bdf42A56813",
                "start_block": 10617223,
            }
        ]
    },
    "celo": {
        "hypervisor_registry": [
            {
                "address": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569",
                "start_block": 14032548,
            }
        ]
    },
}


@dataclass
class dex:
    name: str


@dataclass
class fee_distributor:
    address: str
    start_block: int


@dataclass
class rewards_registry:
    address: str
    start_block: int


@dataclass
class rewarder:
    type: str
    address: str
    start_block: int


@dataclass
class hypervisor:
    address: str
    network: str
    dex: str
    start_block: int
    rewarders: list[rewarder]


@dataclass
class network:
    name: str
    hypervisors: list[hypervisor]
    fee_distributors: list[fee_distributor]
    reward_registries: list[rewards_registry]
    hypervisor_registry: str


STATIC_W3_ADDRESSES = {
    "ethereum": {
        "hypervisors": {
            "uniswapv3": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946",
        },
        "rewards": {},
    },
    "polygon": {
        "hypervisors": {
            "uniswapv3": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055",
            "quickswap": "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd",
        },
        "rewards": {},
    },
    "optimism": {
        "hypervisors": {
            "uniswapv3": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599",
        },
        "rewards": {},
    },
    "arbitrum": {
        "hypervisors": {
            "uniswapv3": "0x66CD859053c458688044d816117D5Bdf42A56813",
        },
        "rewards": {},
    },
    "celo": {
        "hypervisors": {
            "uniswapv3": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569",
        },
        "rewards": {},
    },
}


HYPERVISOR_REGISTRIES = {
    "uniswapv3": {
        "ethereum": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946",
        "polygon": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055",
        "optimism": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599",
        "arbitrum": "0x66CD859053c458688044d816117D5Bdf42A56813",
        "celo": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569",
    },
    "quickswap": {
        "polygon": "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd",
    },
}
