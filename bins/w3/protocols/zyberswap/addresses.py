from ....general.enums import Chain, Family_type, Protocol, rewarderType


ADDRESSES = {
    Chain.ARBITRUM: {
        Family_type.REGISTRY_HYPERVISOR: {
            Protocol.ZYBERSWAP: "0x37595FCaF29E4fBAc0f7C1863E3dF2Fe6e2247e9".lower(),
        },
        Family_type.REGISTRY_REWARDER: {
            rewarderType.ZYBERSWAP_masterchef_v1: "0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd".lower(),
        },
    },
}
