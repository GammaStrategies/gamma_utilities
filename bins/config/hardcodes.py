from bins.configuration import CONFIGURATION
from bins.general.enums import Chain, Protocol


## Special abis filename/path reference for the specified chain/address here defined
# Often, a few ABIs used at any protocol, for any reason, are different from the standard ones. This is a list of those ABIs.


## Some Gamma contracts have unit24 fee function outputs (old univ3 contracts)
SPECIAL_HYPERVISOR_ABIS = {
    Chain.ETHEREUM: {
        "0x65bc5c6a2630a87c2b494f36148e338dd76c054f": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xf0a9f5c64f80fa390a46b298791dab9e2bb29bca": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xa9782a2c9c3fb83937f14cdfac9a6d23946c9255": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xe065ff6a26f286ddb0e823920caaecd1fcd57ba1": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
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
        "0xe14dbb7d054ff1ff5c0cd6adac9f8f26bc7b8945": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x0ec4a47065bf52e1874d2491d4deeed3c638c75f": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x24fe0b138d9b10a7f0502e213212ee6648926ecb": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x1b56860eaf38f27b99d2b0d8ffac86b0f1173f1a": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x33412fef1af035d6dba8b2f9b33b022e4c31dbb4": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x23c85dca3d19b31f14aeea19beac32c2cb2ffc72": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x3cca05926af387f1ab4cd45ce8975d31f0469927": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x5230371a6d5311b1d7dd30c0f5474c2ef0a24661": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x586880065937a0b1b9541723619b75739df8ef13": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x3f805de0ec508bf7311baaa617015809be9ce953": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x388a3938fb6c9c6cb0415946dd5d026f7d98e22c": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x7d89593e1f327f06579faef15e88aadfae51713a": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x7f92463e24b2ea1f7267aceed3ad68f7a956d2d8": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x705b3acaf102404cfdd5e4a60535e4e70091273c": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x85cbed523459b7f6f81c11e710df969703a8a70c": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x9196617815d95853945cd8f5e4e0bb88fdfe0281": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x85a5326f08c44ec673e4bfc666b737f7f3dc6b37": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x8cd73cb1e1fa35628e36b8c543c5f825cd4e77f1": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xa625ea468a4c70f13f9a756ffac3d0d250a5c276": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xb542f4cb10b7913307e3ed432acd9bf2e709f5fa": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xae29f871c9a4cda7ad2c8dff7193c2a0fe3d0c05": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xc14e7ec60699a39cfd59bae06168afc2c76f32ac": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xc86b1e7fa86834cac1468937cdd53ba3ccbc1153": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xb666bfdb553a1aff4042c1e4f39e43852ba9731d": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xeaa629224b3ab2b42b42fddb53a1b51351acbe4f": {
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
        "0xd930ab15c8078ebae4ac8da1098a81583603f7ce": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xf19f91d7889668a533f14d076adc187be781a458": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0xf6eeca73646ea6a5c878814e6508e87facc7927c": {
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
        "0xfe9d3a1e865b9eaea9cd1d9d9073cc520be240c5": {
            "file": "hypervisor_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/gamma",
        },
        "0x34b95494c3c2732aa82e1e56be57074fee7a2b28": {
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


## Special Algebra contract ABIs ( duplicate entry key for pool address and hype address in order to work with multicall classes)
# ADD also all hypervisor addresses with the same pool, so that multicall classes can work as xpected.
SPECIAL_POOL_ABIS = {
    Chain.ARBITRUM: {
        # camelot pool different from standard
        "0xb7Dd20F3FBF4dB42Fd85C839ac0241D09F72955f".lower(): {
            "file": "camelot_pool_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/camelot",
        },
        # add HYPERVISOR for 0xb7Dd20F3FBF4dB42Fd85C839ac0241D09F72955f (so that works with hype multicall classes)
        "0xe8494636e8424c79d8d79dd4bbcd7b56454d1b3d".lower(): {
            "file": "camelot_pool_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/camelot",
        },
        # add HYPERVISOR for 0xb7Dd20F3FBF4dB42Fd85C839ac0241D09F72955f (so that works with hype multicall classes)
        "0x7ccc26A514FCd52A48e17996f6c56de205803159".lower(): {
            "file": "camelot_pool_old",
            "folder": (
                CONFIGURATION.get("data", {}).get("abi_path", None) or "data/abi"
            )
            + "/camelot",
        },
    }
}


# Hypervisor returns sometimes, at the beguining of the hype life, at setup, direct deposits ( transfers not deposits) to the pool are used to set its token weights correctly.
# Here we define the hypervisor starting block desired ( being a block after the fixing ratio transfers are done).

#  <Chain>:{
#      <hypervisor address>: <starting block>
#  }
HYPERVISOR_RETURNS_FORCED_INI_BLOCKS = {
    Chain.AVALANCHE: {
        "0xfa81e2922b084ab260f7f8abd1d455d1235688d0".lower(): 41465089,
        "0xce8f3d036a7d2860c1dbb35a392e1c505feac4f3".lower(): 41467795,
        "0x08c0fe331e82b9e1e3d72bd7dd9ab4a730a84481".lower(): 41469930,
        "0x19ccd73473252db5d3f290bffdf7db45dc7849ca".lower(): 41470754,
    },
}


# Implemented but not launched Chains like (currently opbnb) have no hype events thus operations gathering always takes longer time to complete ( because the initial point is the same block).
# We can define here a block ( now) we know thare is no operations before.).
# This only affects the operations gathering process.
HYPERVISOR_NO_OPERATIONS_BEFORE = {
    Chain.OPBNB: 16097091,
}


# Gamma fee Revenue is not always the exact amounts transfered to the feeRecipients.
# Sometimes, special agreements are made where Gamma collects 100% of a 'gross revenue' value but gets only a percentage of that.
# Here we define the fee in a by chain dex basis.
# chain:{ dex(protocol database name): fee multiplier }
REVENUE_FEE_OVERRIDE = {
    Chain.ARBITRUM: {
        Protocol.CAMELOT.database_name: 0.623529,
    },
    Chain.POLYGON: {Protocol.QUICKSWAP.database_name: 0.5},
    Chain.POLYGON_ZKEVM: {
        Protocol.QUICKSWAP.database_name: 0.5,
        Protocol.QUICKSWAP_UNISWAP.database_name: 0.5,
    },
    Chain.MANTA: {Protocol.QUICKSWAP.database_name: 0.5},
    Chain.BASE: {Protocol.THICK.database_name: 0.2, Protocol.BASEX.database_name: 0.2},
    Chain.LINEA: {Protocol.LYNEX.database_name: 0.2},
    Chain.ASTAR_ZKEVM: {Protocol.QUICKSWAP.database_name: 0.5},
    Chain.IMMUTABLE_ZKEVM: {Protocol.QUICKSWAP.database_name: 0.5},
    Chain.BLAST: {Protocol.BLASTER.database_name: 0.2},
}
