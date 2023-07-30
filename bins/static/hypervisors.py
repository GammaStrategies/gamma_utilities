from ..static.static_gamma import hypervisor, rewarder

#######################################################
__network = "arbitrum"
__dex = "zyberswap"
#######################################################
tmp = hypervisor(
    address="0x35ea99ab62bcf7992136558e94fb97c7807fcd6a",
    network=__network,
    dex=__dex,
    start_block=67703892,
    rewarders=[],
)
tmp.rewarders.append(
    rewarder(
        type="zyberswap_masterchef_v1",
        address="0x9BA666165867E916Ee7Ed3a3aE6C19415C2fBDDD".lower(),
        start_block=54769965,
    )
)
