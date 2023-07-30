from ....general.enums import (
    Chain,
    Family_type,
    Protocol,
    ProtocolVersion,
    rewarderType,
)


ADDRESSES = {
    Chain.ETHEREUM: {
        Family_type.REGISTRY_REWARDER: {
            rewarderType.ANGLE_MERKLE: {
                "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
                "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
                "coreMerkl": "0x0E632a15EbCBa463151B5367B4fCF91313e389a6".lower(),
            },
        },
    },
    Chain.POLYGON: {
        Family_type.REGISTRY_REWARDER: {
            rewarderType.ANGLE_MERKLE: {
                "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
                "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
                "coreMerkl": "0x9418D0aa02fCE40804aBF77bb81a1CcBeB91eaFC".lower(),
            },
        },
    },
    Chain.OPTIMISM: {
        Family_type.REGISTRY_REWARDER: {
            rewarderType.ANGLE_MERKLE: {
                "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
                "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
                "coreMerkl": "0xc2c7a0d9a9e0467090281c3a4f28D40504d08FB4".lower(),
            },
        },
    },
    Chain.ARBITRUM: {
        Family_type.REGISTRY_REWARDER: {
            rewarderType.ANGLE_MERKLE: {
                "distributor": "0x3Ef3D8bA38EBe18DB133cEc108f4D14CE00Dd9Ae".lower(),
                "distributionCreator": "0x8BB4C975Ff3c250e0ceEA271728547f3802B36Fd".lower(),
                "coreMerkl": "0xA86CC1ae2D94C6ED2aB3bF68fB128c2825673267".lower(),
            },
        },
    },
}
