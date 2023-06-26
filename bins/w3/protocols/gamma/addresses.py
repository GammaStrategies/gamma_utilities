from bins.general.enums import (
    Chain,
    Protocol,
    rewarderType,
    Family_type,
    ProtocolVersion,
)


# HYPERVISOR_REGISTRY_ADDRESSES = {
#     Chain.ETHEREUM: {
#         Protocol.UNISWAPv3: "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946".lower(),
#     },
#     Chain.POLYGON: {
#         Protocol.UNISWAPv3: "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055".lower(),
#         Protocol.QUICKSWAP: "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd".lower(),
#         Protocol.RETRO: "0xcac19d43c9558753d7535978a370055614ce832e".lower(),
#         Protocol.SUSHI: "0x97686103b3e7238ca6c2c439146b30adbd84a593".lower(),
#     },
#     Chain.OPTIMISM: {
#         Protocol.UNISWAPv3: "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599".lower(),
#     },
#     Chain.ARBITRUM: {
#         Protocol.UNISWAPv3: "0x66CD859053c458688044d816117D5Bdf42A56813".lower(),
#         Protocol.ZYBERSWAP: "0x37595FCaF29E4fBAc0f7C1863E3dF2Fe6e2247e9".lower(),
#         Protocol.CAMELOT: "0xa216C2b6554A0293f69A1555dd22f4b7e60Fe907".lower(),
#         Protocol.SUSHI: "0x0f867f14b39a5892a39841a03ba573426de4b1d0".lower(),
#     },
#     Chain.CELO: {
#         Protocol.UNISWAPv3: "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569".lower(),
#     },
#     Chain.BSC: {
#         Protocol.UNISWAPv3: "0x0b4645179C1b668464Df01362fC6219a7ab3234c".lower(),
#         Protocol.THENA: "0xd4bcFC023736Db5617E5638748E127581d5929bd".lower(),
#     },
#     Chain.POLYGON_ZKEVM: {
#         Protocol.QUICKSWAP: "0xD08B593eb3460B7aa5Ce76fFB0A3c5c938fd89b8".lower(),
#     },
#     Chain.FANTOM: {
#         Protocol.SPIRITSWAP: "0xf874d4957861e193aec9937223062679c14f9aca".lower(),
#     },
#     Chain.MOONBEAM: {
#         Protocol.STELLASWAP: "0x6002d7714e8038f2058e8162b0b86c0b19c31908".lower(),
#         Protocol.BEAMSWAP: "0xb7dfc304d9cd88d98a262ce5b6a39bb9d6611063".lower(),
#     },
#     Chain.AVALANCHE: {},
# }


ADDRESSES = {
    Chain.ETHEREUM: {
        Protocol.UNISWAPv3: {
            Family_type.REGISTRY_HYPERVISOR: {
                # type : list of addresses
                ProtocolVersion.GAMMA_v2: [],
            },
            Family_type.REGISTRY_REWARDER: {},
        },
    },
    Chain.POLYGON: {},
    Chain.OPTIMISM: {},
    Chain.ARBITRUM: {},
    Chain.CELO: {},
    Chain.BSC: {},
    Chain.POLYGON_ZKEVM: {},
    Chain.FANTOM: {},
    Chain.MOONBEAM: {},
    Chain.AVALANCHE: {},
}


(
    [
        # HYPERVISOR REGISTRY ADDRESSES
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.ETHEREUM,
            "protocol": Protocol.UNISWAPv3,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.POLYGON,
            "protocol": Protocol.UNISWAPv3,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.POLYGON,
            "protocol": Protocol.QUICKSWAP,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.POLYGON,
            "protocol": Protocol.RETRO,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0xcac19d43c9558753d7535978a370055614ce832e".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.POLYGON,
            "protocol": Protocol.SUSHI,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x97686103b3e7238ca6c2c439146b30adbd84a593".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.OPTIMISM,
            "protocol": Protocol.UNISWAPv3,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.ARBITRUM,
            "protocol": Protocol.UNISWAPv3,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x66CD859053c458688044d816117D5Bdf42A56813".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.ARBITRUM,
            "protocol": Protocol.ZYBERSWAP,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x37595FCaF29E4fBAc0f7C1863E3dF2Fe6e2247e9".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.ARBITRUM,
            "protocol": Protocol.CAMELOT,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0xa216C2b6554A0293f69A1555dd22f4b7e60Fe907".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.ARBITRUM,
            "protocol": Protocol.SUSHI,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x0f867f14b39a5892a39841a03ba573426de4b1d0".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.CELO,
            "protocol": Protocol.UNISWAPv3,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.BSC,
            "protocol": Protocol.UNISWAPv3,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x0b4645179C1b668464Df01362fC6219a7ab3234c".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.BSC,
            "protocol": Protocol.THENA,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0xd4bcFC023736Db5617E5638748E127581d5929bd".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.POLYGON_ZKEVM,
            "protocol": Protocol.QUICKSWAP,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0xD08B593eb3460B7aa5Ce76fFB0A3c5c938fd89b8".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.FANTOM,
            "protocol": Protocol.SPIRITSWAP,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0xf874d4957861e193aec9937223062679c14f9aca".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.MOONBEAM,
            "protocol": Protocol.STELLASWAP,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x6002d7714e8038f2058e8162b0b86c0b19c31908".lower(),
        },
        {
            "item": Family_type.REGISTRY_HYPERVISOR,
            "chain": Chain.MOONBEAM,
            "protocol": Protocol.BEAMSWAP,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0xb7dfc304d9cd88d98a262ce5b6a39bb9d6611063".lower(),
        },
    ]
    + [
        # REWARDER REGISTRY ADDRESSES
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.POLYGON,
            "protocol": Protocol.UNISWAPv3,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0x02C8D3FCE5f072688e156F503Bd5C7396328613A".lower(),
        },
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.POLYGON,
            "protocol": Protocol.QUICKSWAP,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0x62cD3612233B2F918BBf0d17B9Eda3005b84e16f".lower(),
        },
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.POLYGON,
            "protocol": Protocol.RETRO,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0x838f6c0189cd8fd831355b31d71b03373480ab83".lower(),
        },
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.POLYGON,
            "protocol": Protocol.SUSHI,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0x73cb7b82e43759b637e1eb833b6c2711f3e45dca".lower(),
        },
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.OPTIMISM,
            "protocol": Protocol.UNISWAPv3,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0x81d9bF667205662bfa729C790F67D97D54EA391C".lower(),
        },
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.ARBITRUM,
            "protocol": Protocol.CAMELOT,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0x26da8473AaA54e8c7835fA5fdd1599eB4c144d31".lower(),
        },
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.POLYGON_ZKEVM,
            "protocol": Protocol.QUICKSWAP,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0x5b8F58a33808222d1fF93C919D330cfA5c8e1B7d".lower(),
        },
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.FANTOM,
            "protocol": Protocol.SPIRITSWAP,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0xf5bfa20f4a77933fee0c7bb7f39e7642a070d599".lower(),
        },
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.MOONBEAM,
            "protocol": Protocol.STELLASWAP,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0xd08b593eb3460b7aa5ce76ffb0a3c5c938fd89b8".lower(),
        },
        {
            "item": Family_type.REGISTRY_REWARDER,
            "chain": Chain.MOONBEAM,
            "protocol": Protocol.BEAMSWAP,
            "version": rewarderType.GAMMA_masterchef_v2,
            "address": "0x1cc4ee0cb063e9db36e51f5d67218ff1f8dbfa0f".lower(),
        },
    ]
    + [
        # FEE DISTRIBUTOR ADDRESSES
        {
            "item": Family_type.FEE_DISTRIBUTOR,
            "chain": Chain.ETHEREUM,
            "protocol": Protocol.GAMMA,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x07432C021f0A65857a3Ab608600B9FEABF568EA0".lower(),
        },
        {
            "item": Family_type.FEE_DISTRIBUTOR,
            "chain": Chain.ETHEREUM,
            "protocol": Protocol.GAMMA,
            "version": ProtocolVersion.GAMMA_v2,
            "address": "0x8451122f06616baff7feb10afc2c4f4132fc4709".lower(),
        },
    ]
    + [
        # MASTERCHEF REGISTRY ADDRESSES
    ]
)
REWARDER_REGISTRY_ADDRESSES = {
    Chain.ETHEREUM: {},
    Chain.POLYGON: {
        Protocol.UNISWAPv3: "0x02C8D3FCE5f072688e156F503Bd5C7396328613A".lower(),
        Protocol.QUICKSWAP: "0x62cD3612233B2F918BBf0d17B9Eda3005b84e16f".lower(),
        Protocol.RETRO: "0x838f6c0189cd8fd831355b31d71b03373480ab83".lower(),
        Protocol.SUSHI: "0x73cb7b82e43759b637e1eb833b6c2711f3e45dca".lower(),
    },
    Chain.OPTIMISM: {
        Protocol.UNISWAPv3: "0x81d9bF667205662bfa729C790F67D97D54EA391C".lower(),
    },
    Chain.ARBITRUM: {
        Protocol.CAMELOT: "0x26da8473AaA54e8c7835fA5fdd1599eB4c144d31".lower(),
    },
    Chain.CELO: {},
    Chain.BSC: {},
    Chain.POLYGON_ZKEVM: {
        Protocol.QUICKSWAP: "0x5b8F58a33808222d1fF93C919D330cfA5c8e1B7d".lower(),
    },
    Chain.FANTOM: {
        Protocol.SPIRITSWAP: "0xf5bfa20f4a77933fee0c7bb7f39e7642a070d599".lower(),
    },
    Chain.MOONBEAM: {
        Protocol.STELLASWAP: "0xd08b593eb3460b7aa5ce76ffb0a3c5c938fd89b8".lower(),
        Protocol.BEAMSWAP: "0x1cc4ee0cb063e9db36e51f5d67218ff1f8dbfa0f".lower(),
    },
}

FEE_DISTRIBUTORS = {
    Chain.ETHEREUM: [
        "0x07432C021f0A65857a3Ab608600B9FEABF568EA0".lower(),
        "0x8451122f06616baff7feb10afc2c4f4132fc4709".lower(),
    ],
    Chain.POLYGON: [],
    Chain.OPTIMISM: [],
    Chain.ARBITRUM: [],
    Chain.CELO: [],
    Chain.BSC: [],
    Chain.POLYGON_ZKEVM: [],
    Chain.FANTOM: [],
    Chain.MOONBEAM: [],
    Chain.AVALANCHE: [],
}

# registry of masterchefs
MASTERCHEF_REGISTRY_ADDRESSES = {
    Chain.POLYGON: [
        "0x135B02F8b110Fe2Dd8B6a5e2892Ee781264c2fbe".lower(),
    ],
}


STATIC_REGISTRY_ADDRESSES = {
    "ethereum": {
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0x0E632a15EbCBa463151B5367B4fCF91313e389a6".lower(),
        },
    },
    "polygon": {
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0x9418D0aa02fCE40804aBF77bb81a1CcBeB91eaFC".lower(),
        },
    },
    "optimism": {
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0xc2c7a0d9a9e0467090281c3a4f28D40504d08FB4".lower(),
        },
    },
    "arbitrum": {
        "zyberswap_v1_masterchefs": [
            "0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd".lower(),
        ],
        "angle_merkl": {
            "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
            "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
            "coreMerkl": "0xA86CC1ae2D94C6ED2aB3bF68fB128c2825673267".lower(),
        },
    },
}
