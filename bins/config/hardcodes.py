from bins.configuration import CONFIGURATION
from bins.general.enums import Chain

## Some Gamma contracts have unit24 fee function outputs (old univ3 contracts)
## load special abi for any specified chain/address here defined
GAMMA_HYPERVISOR_ABIS = {
    Chain.ETHEREUM: {
        "0x93acb12ae1effb3426220c20c6d408eeaae59d72": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xa1c739fa2fdfdd7049e385d60d4921ef7226daa5": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x35abccd8e577607275647edab08c537fa32cc65e": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x9a98bffabc0abf291d6811c034e239e916bbcec0": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x716bd8a7f8a44b010969a1825ae5658e7a18630d": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x6c8116abe5c5f2c39553c6f4217840e71462539c": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x97491b65c9c8e8754b5c55ed208ff490b2ee6190": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x407e99b20d61f245426031df872966953909e9d3": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xc92ff322c8a18e38b46393dbcc8a7c5691586497": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x18d3284d9eff64fc97b64ab2b871738e684aa151": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x6e67bb258b6485b688cbb526c868d4428b634cf1": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x09b8d86c6275e707155cdb5963cf611a432ccb21": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x53a4512bbe5083695d8e890789fe1cf6f5686d52": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x0d3fbebfdd96940952618598a5f012de7240c552": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x0624eb9691d99178d0d2bd76c72f1dbb4db05286": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x33682bfc1d94480a0e3de0a565180b182b71d485": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xd8dbdb77305898365d7ba6dd438f2663f7d4e409": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x4564a37c88e3b13d3a0c08832dcf88278997e6fe": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xb60f4ac1514be672b2ec35a023f4c89373d3a4ef": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x4f7997158d66ca31d9734674fdcd12cc74e503a7": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xbb9b86a75ca3115caab045e2af17b0bba483acbc": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x0407c810546f1dc007f01a80e65983072d5c6dfa": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
    },
    Chain.POLYGON: {
        "0x9c3b8d3d977ba1d58848565149cb5ac1689dfa5b": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x9ca70521bf8a7f7345dfe893d117c4414cae9151": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xf874d4957861e193aec9937223062679c14f9aca": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x8b6e73f17b613ce189be413f5dc435139f5fd45c": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x6002d7714e8038f2058e8162b0b86c0b19c31908": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xd08b593eb3460b7aa5ce76ffb0a3c5c938fd89b8": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xf5bfa20f4a77933fee0c7bb7f39e7642a070d599": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xfa4bf5c7d995642f908318275e816dc023924ad7": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x0f548d7ad1a0cb30d1872b8c18894484d76e1569": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xed354a827d99992d9cdada809449985cb73b8bb1": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x8cacde53d63fda23a8f802653eeef931c8528cac": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xd4bcfc023736db5617e5638748e127581d5929bd": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x6b3d98406779ddca311e6c43553773207b506fa6": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x8d6d90f2e7a20d5ec355287c37e3f20de50b8349": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x8891450de400229a58eb23457a7984c6b461beda": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xde8edc067b079b3965fde36d11aa834287f9b663": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xe64c62244c48f9d0aa70d411432b825e2f8b05b0": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x3fe6f25da67dc6ad2a5117a691f9951ea14d6f15": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xd1c24a1eaf4b6978ea4152634be62e947dfca142": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
    },
    Chain.OPTIMISM: {
        "0x8b6e73f17b613ce189be413f5dc435139f5fd45c": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x2d6b26f430f261b77d14c495585116aa579b7217": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x0f548d7ad1a0cb30d1872b8c18894484d76e1569": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x0b4645179c1b668464df01362fc6219a7ab3234c": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x3f848eefd234da0b1b98ea876fe2ee86a10773ca": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x66cd859053c458688044d816117d5bdf42a56813": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
    },
}
