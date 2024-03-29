from bins.general.enums import Chain, Protocol


#### STATIC PRICE ORACLES PATH ####

DEX_POOLS = {
    Chain.ETHEREUM: {
        "USDC_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640".lower(),
            "min_block": 12376729,
        },
        "WETH_RPL": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xe42318ea3b998e8355a3da364eb9d48ec725eb45".lower(),
            "min_block": 13598687,
        },
        "GAMMA_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x4006bed7bf103d70a1c6b7f1cef4ad059193dc25".lower(),
            "min_block": 13869973,
        },
        "AXL_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x5b0d2536f0c970b8d9cbf3959460fb97ce808ade".lower(),
            "min_block": 16242904,
        },
        "RAW_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xcde473286561d9b876bead3ac7cc38040f738d3f".lower(),
            "token0": "0xb41f289d699c5e79a51cb29595c203cfae85f32a".lower(),
            "token1": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".lower(),
            "min_block": 13856851,
        },
        "BABEL_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xe2de090153403b0f0401142d5394da897630dcb7".lower(),
            "token0": "0xf4dc48d260c93ad6a96c5ce563e70ca578987c74".lower(),
            "token1": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".lower(),
            "min_block": 13682876,
        },
        "renBTC-WETH":{
            "protocol": Protocol.UNISWAPv3,
            "address": "0x3cb75142bdae2bef3eb119affb288aeb0599dc4f".lower(),
            "token0": "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d".lower(),
            "token1": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".lower(),
            "min_block": 12377251,
        },
    },
    Chain.OPTIMISM: {
        "WETH_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x85149247691df622eaf1a8bd0cafd40bc45154a9".lower(),
            "min_block": 1000000,
        },
        "WETH_OP": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x68f5c0a2de713a54991e01858fd27a3832401849".lower(),
            "min_block": 6516111,
        },
        "OATH_ERN": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xcf215e62a2e0c840d52ed4edad7d8f0881159da4".lower(),
            "token0": "0x00e1724885473b63bce08a9f0a52f35b0979e35a".lower(),
            "token1": "0xc5b001dc33727f8f26880b184090d3e252470d45".lower(),
            "min_block": 111688323,
        },
        "ERN_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x4ce4a1a593ea9f2e6b2c05016a00a2d300c9ffd8".lower(),
            "token0": "0xc5b001dc33727f8f26880b184090d3e252470d45".lower(),  # ERN
            "token1": "0x7f5c764cbc14f9669b88837ca1490cca17c31607".lower(),
            "min_block": 88991910,
        },
    },
    Chain.POLYGON: {
        "WMATIC_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xae81fac689a1b4b1e06e7ef4a2ab4cd8ac0a087d".lower(),
            "min_block": 32611263,
        },
        "WMATIC_QI": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x5cd94ead61fea43886feec3c95b1e9d7284fdef3".lower(),
            "min_block": 32986128,
        },
        "WMATIC_QUICK": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x9f1a8caf3c8e94e43aa64922d67dff4dc3e88a42".lower(),
            "min_block": 0,
        },
        "WMATIC_DQUICK": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xb8d00c66accdc01e78fd7957bf24050162916ae2".lower(),
            "min_block": 32975647,
        },
        "WMATIC_GHST": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x80deece4befd9f27d2df88064cf75f080d3ce1b2".lower(),
            "min_block": 39917089,
        },
        "WMATIC_ANKR": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x2f2dd65339226df7441097a710aba0f493879579".lower(),
            "min_block": 39917089,
        },
        "USDC_DAVOS": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xfb0bc232cd11dbe804b489860c470b7f9cc80d9f".lower(),
            "min_block": 38555352,
        },
        "USDC_GIDDY": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x65c30f39b880bdd9616280450c4b41cc74b438b7".lower(),
            "min_block": 39290163,
        },
        "WMATIC_LCD": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xd9c2c978915b907df04972cb3f577126fe55143c".lower(),
            "min_block": 35886526,
        },
        "WOMBAT_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xaf835698673655e9910de8398df6c5238f5d3aeb".lower(),
            "min_block": 33761652,
        },
        "USDC_FIS": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x2877703a3ba3e712d684d22bd6d60cc0031d84e8".lower(),
            "min_block": 42318209,
        },
        "SD_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x5d0acfa39a0fca603147f1c14e53f46be76984bc".lower(),
            "min_block": 34836803,
        },
        "USDC_DAI": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xe7e0eb9f6bcccfe847fdf62a3628319a092f11a2".lower(),
            "min_block": 33025326,
        },
        "USDC_axlPEPE": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x27c30be7bf776e31e2cbbb9fe6db18d86f09da01".lower(),
            "token0": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower(),
            "token1": "0x8bae3f5eb10f39663e57be19741fd9ccef0e113a".lower(),
            "min_block": 42536080,
        },
        "oRETRO_RETRO": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x387FBcE5E2933Bd3a7243D0be2aAC8fD9Ab3D55d".lower(),
            "token0": "0x3a29cab2e124919d14a6f735b6033a3aad2b260f".lower(),
            "token1": "0xbfa35599c7aebb0dace9b5aa3ca5f2a79624d8eb".lower(),
            "min_block": 45795073,
        },
        "liveRETRO_RETRO": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x6333bb8b6f1dda6f929d70edeb9e31c8148dc9ef".lower(),
            "token0": "0xcaaf554900e33ae5dbc66ae9f8adc3049b7d31db".lower(),
            "token1": "0xbfa35599c7aebb0dace9b5aa3ca5f2a79624d8eb".lower(),
            "min_block": 47307879,
        },
        "RETRO_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xc7d8b9c270d0e31a6a0cf4496fe019766be42e15".lower(),
            "token0": "0xbfa35599c7aebb0dace9b5aa3ca5f2a79624d8eb".lower(),
            "token1": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower(),
            "min_block": 45554364,
        },
        "XOC_WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x4c493eea376d57d69a4e6d55ef048068e65f1765".lower(),
            "token0": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619".lower(),
            "token1": "0xa411c9aa00e020e4f88bc19996d29c5b7adb4acf".lower(),
            "min_block": 41482316,
        },
        "VEXT_USDT": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x37db450ead1aefad6c38fbefca616f8f5c0cfa23".lower(),
            "token0": "0x27842334c55c01ddfe81bf687425f906816c5141".lower(),
            "token1": "0xc2132d05d31c914a87c6611c10748aeb04b58e8f".lower(),
            "min_block": 47168504,
        },
        "WETH_CONE": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x1923114924fb259858519256c3b5860e91932dd9".lower(),
            "token0": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619".lower(),
            "token1": "0xba777ae3a3c91fcd83ef85bfe65410592bdd0f7c".lower(),
            "min_block": 42902919,
        },
        "USDC-EUROe": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x50ac505af93eff6a650531ab001206cfa213bb85".lower(),
            "token0": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower(),
            "token1": "0x820802fa8a99901f52e39acd21177b0be6ee2974".lower(),
            "min_block": 40602008,
        },
        # "USDC-SLING": {
        #     "protocol": Protocol.QUICKSWAP,
        #     "address": "0x01e0d3b51b8a005951d8ff6b0ad147a7faead9d5".lower(),
        #     "token0": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359".lower(),
        #     "token1": "0xfc9fa9771145fbb98d15c8c2cc94b837a56d554c".lower(),
        #     "min_block": 49137121,
        # },
        # "SLING-WETH": {
        #     "protocol": Protocol.UNISWAPv3,
        #     "address": "0xad318065c39ad984da234000be9c8a1c5557f5e7".lower(),
        #     "token0": "0xfc9fa9771145fbb98d15c8c2cc94b837a56d554c".lower(),
        #     "token1": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619".lower(),
        #     "min_block": 49786925,
        # },
    },
    Chain.POLYGON_ZKEVM: {
        "WETH_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xc44ad482f24fd750caeba387d2726d8653f8c4bb".lower(),
            "min_block": 5000000,
        },
        "QUICK_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x1247b70c4b41890e8c1836e88dd0c8e3b23dd60e".lower(),
            "min_block": 5000000,
        },
        "WETH_MATIC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xb73abfb5a2c89f4038baa476ff3a7942a021c196".lower(),
            "min_block": 5000000,
        },
        "WETH_WBTC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0xfc4a3a7dc6b62bd2ea595b106392f5e006083b83".lower(),
            "token0": "0x4f9a0e7fd2bf6067db6994cf12e4495df938e6e9".lower(),
            "token1": "0xea034fb02eb1808c2cc3adbc15f447b93cbe08e1".lower(),
            "min_block": 5000000,
        },
        "USDC_DAI": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x68cc0516162b423930cd8448a2a00310e841e7f5".lower(),
            "token0": "0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035".lower(),  # USDC
            "token1": "0xc5015b9d9161dca7e18e32f6f25c4ad850731fd4".lower(),  # DAI
            "min_block": 5000000,
        },
        "USDT_USDC": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x9591b8a30c3a52256ea93e98da49ee43afa136a8".lower(),
            "token0": "0x1e4a5963abfd975d8c9021ce480b42188849d41d".lower(),  # USDT
            "token1": "0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035".lower(),  # USDC
            "min_block": 5000000,
        },
    },
    Chain.BSC: {
        "THE_WBNB": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x51bd5e6d3da9064d59bcaa5a76776560ab42ceb8".lower(),
            "min_block": 27057711,
        },
        "THE_USDT": {
            "protocol": Protocol.ALGEBRAv3,
            "address": "0x98a0004b8e9fe161369528a2e07de56c15a27d76".lower(),
            "min_block": 27317923,
        },
        "USDT_WBNB": {
            "protocol": Protocol.PANCAKESWAP,
            "address": "0x36696169c63e42cd08ce11f5deebbcebae652050".lower(),
            "token0": "0x55d398326f99059ff775485246999027b3197955".lower(),
            "token1": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c".lower(),
            "min_block": 26970848,
        },
        "USDT_USDC": {
            "protocol": Protocol.PANCAKESWAP,
            "address": "0x92b7807bf19b7dddf89b706143896d05228f3121".lower(),
            "token0": "0x55d398326f99059ff775485246999027b3197955".lower(),
            "token1": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d".lower(),
            "min_block": 26972126,
        },
        # "ETSa-BUSD":{
        #     "protocol": Protocol.THENA,
        #     "address": "0xb23a34cc853e47d8bfd148baf066b6f1541571c9".lower(),
        #     "token0": "0x5b852898cd47d2be1d77d30377b3642290f5ec75".lower(),
        #     "token1": "0xe9e7cea3dedca5984780bafc599bd69add087d56".lower(),
        #     "min_block": ,
        # },
    },
    Chain.AVALANCHE: {
        "PHAR_WAVAX": {
            "protocol": Protocol.PHARAOH,
            "address": "0xc1f141909cea52f5315c19c1121cef1dc86f4926".lower(),
            "token0": "0xaaab9d12a30504559b0c5a9a5977fee4a6081c6b".lower(),  # PHAR
            "token1": "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7".lower(),  # WAVAX
            "min_block": 39001510,
        },
        "WAVAX_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xfae3f424a0a47706811521e3ee268f00cfb5c45e".lower(),
            "token0": "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7".lower(),  # WAVAX
            "token1": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e".lower(),  # USDC
            "min_block": 32516926,
        },
    },
    Chain.ARBITRUM: {
        "DAI_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xf0428617433652c9dc6d1093a42adfbf30d29f74".lower(),
            "token0": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1".lower(),
            "token1": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8".lower(),
            "min_block": 65214341,
        },
        "USDT_USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x8c9d230d45d6cfee39a6680fb7cb7e8de7ea8e71".lower(),
            "token0": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9".lower(),
            "token1": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8".lower(),
            "min_block": 64173428,
        },
        "WETH_USDT": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x641c00a822e8b671738d32a431a4fb6074e5c79d".lower(),
            "token0": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(),
            "token1": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9".lower(),
            "min_block": 302082,
        },
        # "GRAI-LUSD": {
        #     "protocol": Protocol.UNISWAPv3,
        #     "address": "0x3df1094722c7368e26a2e7c57c91a0289f6fa732".lower(),
        #     "token0": "0x894134a25a5fac1c2c26f1d8fbf05111a3cb9487".lower(),
        #     "token1": "0x93b346b6bc2548da6a1e7d98e9a421b42541425b".lower(),
        #     "min_block": 113253265,
        # },
        "LUSD-USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x1557fdfda61f135baf1a1682eebaa086a0fcab6e".lower(),
            "token0": "0x93b346b6bc2548da6a1e7d98e9a421b42541425b".lower(),
            "token1": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8".lower(),
            "min_block": 20148926,
        },
        "GRAI-LUSD": {
            "protocol": Protocol.RAMSES,
            "address": "0x92e305a63646e76bdd3681f7ece7529cd4e8ed5b".lower(),
            "token0": "0x894134a25a5fac1c2c26f1d8fbf05111a3cb9487".lower(),
            "token1": "0x93b346b6bc2548da6a1e7d98e9a421b42541425b".lower(),
            "min_block": 106262769,
        },
        "ARB-USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xcda53b1f66614552f834ceef361a8d12a0b8dad8".lower(),
            "token0": "0x912ce59144191c1204e64559fe8253a0e49e6548".lower(),
            "token1": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8".lower(),
            "min_block": 71876737,
        },
        "OATH-ERN": {
            "protocol": Protocol.RAMSES,
            "address": "0x07b6699a5163e076498ed7511b7d4778e3949a31".lower(),
            "token0": "0xa1150db5105987cec5fd092273d1e3cbb22b378b".lower(),
            "token1": "0xa334884bf6b0a066d553d19e507315e839409e62".lower(),
            "min_block": 103382549,
        },
        "CAKE-WETH": {
            "protocol": Protocol.PANCAKESWAP,
            "address": "0xf5fac36c2429e1cf84d4abacdb18477ef32589c9".lower(),
            "token0": "0x1b896893dfc86bb67cf57767298b9073d2c1ba2c".lower(),
            "token1": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(),
            "min_block": 122887421,
        },
        "WETH-USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443".lower(),
            "token0": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(),
            "token1": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8".lower(),
            "min_block": 100909,
        },
        "pxETH-WETH": {
            "protocol": Protocol.CAMELOT,
            "address": "0x1d9e8e0a4b445ceff9b8673ed5b219ccb38a4117".lower(),
            "token0": "0x300d2c875c6fb8ce4bf5480b4d34b7c9ea8a33a4".lower(),
            "token1": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".lower(),
            "min_block": 187054712,
        },
    },
    Chain.MOONBEAM: {
        "stDOT-xcDOT": {
            "protocol": Protocol.STELLASWAP,
            "address": "0xd9d1064e32704bdd540f90d3a9ecaf037748b966".lower(),
            "token0": "0xbc7e02c4178a7df7d3e564323a5c359dc96c4db4".lower(),  # stDOT
            "token1": "0xffffffff1fcacbd218edc0eba20fc2308c778080".lower(),  # xcDOT
            "min_block": 4712455,
        },
        "xcDOT-xcBNC": {
            "protocol": Protocol.STELLASWAP,
            "address": "0x33c465cfd6932e70a3664593f87616ba9166b0a7".lower(),
            "token0": "0xffffffff1fcacbd218edc0eba20fc2308c778080".lower(),  # xcDOT
            "token1": "0xffffffff7cc06abdf7201b350a1265c62c8601d2".lower(),  # xcBNC
            "min_block": 4625410,
        },
        "xcDOT-WGLMR": {
            "protocol": Protocol.STELLASWAP,
            "address": "0xb13b281503f6ec8a837ae1a21e86a9cae368fcc5".lower(),
            "token0": "0xffffffff1fcacbd218edc0eba20fc2308c778080".lower(),  # xcDOT
            "token1": "0xacc15dc74880c9944775448304b263d191c6077f".lower(),  # WGLMR
            "min_block": 2772018,
        },
        "stDOT-WGLMR": {
            "protocol": Protocol.STELLASWAP,
            "address": "0xac6ccde03b940ebcea55115b3f573cb93cfc96c0".lower(),
            "token0": "0xbc7e02c4178a7df7d3e564323a5c359dc96c4db4".lower(),  # stDOT
            "token1": "0xacc15dc74880c9944775448304b263d191c6077f".lower(),  # WGLMR
            "min_block": 4716768,
        },
        "USDC-WGLMR_STELLA": {
            "protocol": Protocol.STELLASWAP,
            "address": "0xab8c35164a8e3ef302d18da953923ea31f0fe393".lower(),
            "token0": "0x931715fee2d06333043d11f658c8ce934ac61d0c".lower(),  # USDC
            "token1": "0xacc15dc74880c9944775448304b263d191c6077f".lower(),  # WGLMR
            "min_block": 2650227,
        },
        "USDC-WGLMR_BEAM": {
            "protocol": Protocol.BEAMSWAP,
            "address": "0xf7e2f39624aad83ad235a090be89b5fa861c29b8".lower(),
            "token0": "0x931715fee2d06333043d11f658c8ce934ac61d0c".lower(),  # USDC
            "token1": "0xacc15dc74880c9944775448304b263d191c6077f".lower(),  # WGLMR
            "min_block": 3582431,
        },
        # "xcvDOT-xcDOT": {
        #     "protocol": Protocol.STELLASWAP,
        #     "address": "0xc75247c065aa9411faa30195bb84078b99f1934d".lower(),
        #     "token0": "0xffffffff15e1b7e3df971dd813bc394deb899abf".lower(),  # xcvDOT
        #     "token1": "0xffffffff1fcacbd218edc0eba20fc2308c778080".lower(),  # xcDOT
        #     "min_block": 4710813,
        # },
        "xcvDOT-xcDOT_BEAM": {
            "protocol": Protocol.BEAMSWAP,
            "address": "0x946583b3801c703dfa042f82f3b9b3a2a9a79393".lower(),
            "token0": "0xffffffff15e1b7e3df971dd813bc394deb899abf".lower(),  # xcvDOT
            "token1": "0xffffffff1fcacbd218edc0eba20fc2308c778080".lower(),  # xcDOT
            "min_block": 4617273,
        },
    },
    Chain.BASE: {
        "TBTC-WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x9fee7385a2979d15277c3467db7d99ef1a2669d7".lower(),
            "token0": "0x236aa50979d5f3de3bd1eeb40e81137f22ab794b".lower(),
            "token1": "0x4200000000000000000000000000000000000006".lower(),
            "min_block": 2392639,
        },
        "SUSHI-USDbC": {
            # sushiswap v3
            "protocol": Protocol.UNISWAPv3,
            "address": "0x82d22f27d97ce9a93eb68b5b1a43792492fa89c9".lower(),
            "token0": "0x7d49a065d17d6d4a55dc13649901fdbb98b2afba".lower(),
            "token1": "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca".lower(),
            "min_block": 3571306,
        },
        "WETH-USDbC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x4c36388be6f416a29c8d8eee81c771ce6be14b18".lower(),
            "token0": "0x4200000000000000000000000000000000000006".lower(),
            "token1": "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca".lower(),
            "min_block": 2112314,
        },
    },
    Chain.CELO: {
        "agEUR-cEUR": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x4b7a4530d56ff55a4dce089d917ede812e543307".lower(),
            "token0": "0xc16b81af351ba9e64c1a069e3ab18c244a1e3049".lower(),
            "token1": "0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73".lower(),
            "min_block": 20363939,
        },
        "agEUR-cUSD": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x1f18cd7d1c7ba0dbe3d9abe0d3ec84ce1ad10066".lower(),
            "token0": "0x765de816845861e75a25fca122bb6898b8b1282a".lower(),
            "token1": "0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73".lower(),
            "min_block": 14030663,
        },
        "WETH-CELO": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xd88d5f9e6c10e6febc9296a454f6c2589b1e8fae".lower(),
            "token0": "0x66803fb87abd4aac3cbb3fad7c3aa01f6f3fb207".lower(),
            "token1": "0x471ece3750da237f93b8e339c536989b8978a438".lower(),
            "min_block": 14001235,
        },
        "CELO-cUSD": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x2d70cbabf4d8e61d5317b62cbe912935fd94e0fe".lower(),
            "token0": "0x471ece3750da237f93b8e339c536989b8978a438".lower(),
            "token1": "0x765de816845861e75a25fca122bb6898b8b1282a".lower(),
            "min_block": 14172424,
        },
        "USDC-cUSD": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xea3fb6e3313a2a90757e4ca3d6749efd0107b0b6".lower(),
            "token0": "0x37f750b7cc259a2f741af45294f6a16572cf5cad".lower(),
            "token1": "0x765de816845861e75a25fca122bb6898b8b1282a".lower(),
            "min_block": 14002457,
        },
        "USDC-WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xb90fe7da36ac89448e6dfd7f2bb1e90a66659977".lower(),
            "token0": "0x37f750b7cc259a2f741af45294f6a16572cf5cad".lower(),  # USDC
            "token1": "0x66803fb87abd4aac3cbb3fad7c3aa01f6f3fb207".lower(),  # WETH
            "min_block": 18491109,
        },
        "cMCO2-CELO": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x112466c8b6e5abe42c78c47eb1b9d40baa3f943c".lower(),
            "token0": "0x32a9fe697a32135bfd313a6ac28792dae4d9979d".lower(),  # cMCO2
            "token1": "0x471ece3750da237f93b8e339c536989b8978a438".lower(),  # CELO
            "min_block": 13997603,
        },
    },
    Chain.MANTA: {
        "USDC-WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xd7f09148eb22686cb5dcbdd0cf27d04123d14c74".lower(),
            "token0": "0xb73603c5d87fa094b7314c74ace2e64d165016fb".lower(),  # USDC
            "token1": "0x0dc808adce2099a9f62aa87d9670745aba741746".lower(),  # WETH
            "min_block": 600000,
        },
        "QUICK-USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xeab583e16df26c670c047260710ed172aade45a0".lower(),
            "token0": "0xe22e3d44ea9fb0a87ea3f7a8f41d869c677f0020".lower(),  # QUICK
            "token1": "0xb73603c5d87fa094b7314c74ace2e64d165016fb".lower(),  # USDC
            "min_block": 600000,
        },
        "STONE-WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xa5101d48355d5d731c2bedd273aa0eb7ed55d0c7".lower(),
            "token0": "0xec901da9c68e90798bbbb74c11406a32a70652c3".lower(),  # STONE
            "token1": "0x0dc808adce2099a9f62aa87d9670745aba741746".lower(),
            "min_block": 600000,
        },
        "WBTC-WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xfc9ffc1c6e0ebf7be3ce93245b309f4d3b593101".lower(),
            "token0": "0x305e88d809c9dc03179554bfbf85ac05ce8f18d6".lower(),  # WBTC
            "token1": "0x0dc808adce2099a9f62aa87d9670745aba741746".lower(),
            "min_block": 600000,
        },
        "wstETH-WETH": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x578ef5f3a3d34949a590ae7719e5bd69a3720865".lower(),
            "token0": "0x2fe3ad97a60eb7c79a976fc18bb5ffd07dd94ba5".lower(),  # wstETH
            "token1": "0x0dc808adce2099a9f62aa87d9670745aba741746".lower(),
            "min_block": 600000,
        },
        "USDC-USDT": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x462609c41ca27bd4240778c53c0908542b59b972".lower(),
            "token0": "0xb73603c5d87fa094b7314c74ace2e64d165016fb".lower(),  # USDC
            "token1": "0xf417f5a458ec102b90352f697d6e2ac3a3d2851f".lower(),  # USDT
            "min_block": 600000,
        },
        "MATIC-USDC": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0xbf11176eb2b7b14aec32acbfe1085bf55798d138".lower(),
            "token0": "0x0f52a51287f9b3894d73df05164d0ee2533ccbb4".lower(),  # MATIC
            "token1": "0xb73603c5d87fa094b7314c74ace2e64d165016fb".lower(),  # USDC
            "min_block": 700000,
        },
        "wUSDM-STONE": {
            "protocol": Protocol.UNISWAPv3,
            "address": "0x1a6378383258a5d8ae40d383200ae29c53e85af9".lower(),
            "token0": "0xbdad407f77f44f7da6684b416b1951eca461fb07".lower(),  # wUSDM
            "token1": "0xec901da9c68e90798bbbb74c11406a32a70652c3".lower(),  #
            "min_block": 700000,
        },
    },
    Chain.GNOSIS: {
        "PNK-WETH": {
            "protocol": Protocol.SWAPR,
            "address": "0x2613cb099c12cecb1bd290fd0ef6833949374165".lower(),
            "token0": "0x37b60f4e9a31a64ccc0024dce7d0fd07eaa0f7b3".lower(),  # PNK
            "token1": "0x6a023ccd1ff6f2045c3309768ead9e68f978f6e1".lower(),  # WETH
            "min_block": 19991544,
        },
    },
    Chain.LINEA: {
        "LYNX-WETH": {
            "protocol": Protocol.LYNEX,
            "address": "0x78b1730c6b7f49e1650093b0cb09f9df51dc831f".lower(),
            "token0": "0x1a51b19ce03dbe0cb44c1528e34a7edd7771e9af".lower(),  # LYNX
            "token1": "0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f".lower(),  # WETH
            "min_block": 2375630,
        },
        "LYNX-USDC": {
            "protocol": Protocol.LYNEX,
            "address": "0xdda5ec5af00ab99dc80c33e08881eb80c027d498".lower(),
            "token0": "0x1a51b19ce03dbe0cb44c1528e34a7edd7771e9af".lower(),  # LYNX
            "token1": "0x176211869ca2b568f2a7d4ee941e073a821ee1ff".lower(),  # USDC
            "min_block": 2207775,
        },
    },
    Chain.MANTLE: {
        "CLEO-WMNT": {
            "protocol": Protocol.CLEOPATRA,
            "address": "0xf7b5113492b5f642075bbcaa02494df8f188cade".lower(),
            "token0": "0xc1e0c8c30f251a07a894609616580ad2ceb547f2".lower(),  # CLEO
            "token1": "0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8".lower(),  # WMNT
            "min_block": 39399843,
        },
        "WETH-WMNT": {
            "protocol": Protocol.FUSIONX,
            "address": "0x47453cb250f705211e7a0de2f9c5d94cfecc8abd".lower(),
            "token0": "0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111".lower(),  # WETH
            "token1": "0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8".lower(),  # WMNT
            "min_block": 6494,
        },
        "USDC-WETH": {
            "protocol": Protocol.FUSIONX,
            "address": "0x01845ec86909006758de0d57957d88da10bf5809".lower(),
            "token0": "0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9".lower(),  # USDC
            "token1": "0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111".lower(),  # WETH
            "min_block": 24671423,
        },
    },
    Chain.OPBNB: {
        "BTCB-WBNB": {
            "protocol": Protocol.PANCAKESWAP,
            "address": "0xde632a1481a8e01700cdf1818e89fb4c2a9acec6".lower(),
            "token0": "0x7c6b91d9be155a6db01f749217d76ff02a7227f2".lower(),  # BTCB
            "token1": "0x4200000000000000000000000000000000000006".lower(),  # WBNB
            "min_block": 4420916,
        },
        "WETH-WBNB": {
            "protocol": Protocol.PANCAKESWAP,
            "address": "0xff00f4e09820dbbe8582f771800732dae7f002bd".lower(),
            "token0": "0xe7798f023fc62146e8aa1b36da45fb70855a77ea".lower(),  # WETH
            "token1": "0x4200000000000000000000000000000000000006".lower(),  # WBNB
            "min_block": 4520463,
        },
        "USDT-WBNB": {
            "protocol": Protocol.PANCAKESWAP,
            "address": "0xc4f981189558682f15f60513158b699354b30204".lower(),
            "token0": "0x9e5aac1ba1a2e6aed6b32689dfcf62a509ca96f3".lower(),  # USDT
            "token1": "0x4200000000000000000000000000000000000006".lower(),  # WBNB
            "min_block": 3352761,
        },
    },
    Chain.ASTAR_ZKEVM: {
        "USDT-USDC": {
            "protocol": Protocol.UNISWAPv3,  # quickswap
            "address": "0x9331b844a67accddcc103b6a84237a589f8c1d96".lower(),
            "token0": "0x1e4a5963abfd975d8c9021ce480b42188849d41d".lower(),  # USDT
            "token1": "0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035".lower(),  # USDC
            "min_block": 281220,
        },
        "wstETH-WETH": {
            "protocol": Protocol.UNISWAPv3,  # quickswap
            "address": "0xca573c6a2d08fb5181f5992a626fa907fa97cc3d".lower(),
            "token0": "0x5d8cff95d7a57c0bf50b30b43c7cc0d52825d4a9".lower(),  # wstETH
            "token1": "0xe9cc37904875b459fa5d0fe37680d36f1ed55e38".lower(),  # WETH
            "min_block": 281505,
        },
        "vASTR-ASTR": {
            "protocol": Protocol.UNISWAPv3,  # quickswap
            "address": "0x5a20cbccfd256100546ca70e368d102ad7bdcdd1".lower(),
            "token0": "0x7746ef546d562b443ae4b4145541a3b1a3d75717".lower(),  # vASTR
            "token1": "0xdf41220c7e322bfef933d85d01821ad277f90172".lower(),  # ASTR
            "min_block": 382080,
        },
        "ASTR-WBTC": {
            "protocol": Protocol.UNISWAPv3,  # quickswap
            "address": "0xdc014394c439e152394e92ab013ece1f91c718d7".lower(),
            "token0": "0xdf41220c7e322bfef933d85d01821ad277f90172".lower(),  # ASTR
            "token1": "0xea034fb02eb1808c2cc3adbc15f447b93cbe08e1".lower(),  # WBTC
            "min_block": 300263,
        },
        "USDC-WETH": {
            "protocol": Protocol.UNISWAPv3,  # quickswap
            "address": "0x47d7b9510ae2835c7c293825641a5427226d34cb".lower(),
            "token0": "0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035".lower(),  #
            "token1": "0xe9cc37904875b459fa5d0fe37680d36f1ed55e38".lower(),  #
            "min_block": 245691,
        },
        "ASTR-USDC": {
            "protocol": Protocol.UNISWAPv3,  # quickswap
            "address": "0x8bcbd2b0240c3e54d803e33ba944fa98267312ff".lower(),
            "token0": "0xdf41220c7e322bfef933d85d01821ad277f90172".lower(),  #
            "token1": "0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035".lower(),  #
            "min_block": 403386,
        },
    },
    Chain.IMMUTABLE_ZKEVM: {
        "GOG-WIMX": {
            "protocol": Protocol.UNISWAPv3,  # quickswap
            "address": "0x6faec9c84d319ef037d0c3ce4a15502f1c1e4306".lower(),
            "token0": "0xb00ed913aaff8280c17bff33cce82fe6d79e85e8".lower(),  # GOG
            "token1": "0x3a0c2ba54d6cbd3121f01b96dfd20e99d1696c9d".lower(),  # WIMX
            "min_block": 3753850,
        },
        "WIMX-USDC": {
            "protocol": Protocol.UNISWAPv3,  # quickswap
            "address": "0x5f5664979ed0f8124e9910f9476b12304ed85e61".lower(),
            "token0": "0x3a0c2ba54d6cbd3121f01b96dfd20e99d1696c9d".lower(),  # WIMX
            "token1": "0x6de8acc0d406837030ce4dd28e7c08c5a96a30d2".lower(),  # USDC
            "min_block": 3772535,
        },
        "ETH-USDC": {
            "protocol": Protocol.UNISWAPv3,  # quickswap
            "address": "0x647638891606f98ea6900d51011f0f24e040ad95".lower(),
            "token0": "0x52a6c53869ce09a731cd772f245b97a4401d3348".lower(),  # ETH
            "token1": "0x6de8acc0d406837030ce4dd28e7c08c5a96a30d2".lower(),  # USDC
            "min_block": 3846867,
        },
    },
}


DEX_POOLS_PRICE_PATHS = {
    Chain.ETHEREUM: {
        # GAMMA
        "0x6bea7cfef803d1e3d5f7c0103f7ded065644e197".lower(): [
            (DEX_POOLS[Chain.ETHEREUM]["GAMMA_WETH"], 1),
            (
                DEX_POOLS[Chain.ETHEREUM]["USDC_WETH"],
                0,
            ),
        ],
        # RPL
        "0xd33526068d116ce69f19a9ee46f0bd304f21a51f".lower(): [
            (
                DEX_POOLS[Chain.ETHEREUM]["WETH_RPL"],
                0,
            ),
            (
                DEX_POOLS[Chain.ETHEREUM]["USDC_WETH"],
                0,
            ),
        ],
        # AXL
        "0x467719ad09025fcc6cf6f8311755809d45a5e5f3".lower(): [
            (DEX_POOLS[Chain.ETHEREUM]["AXL_USDC"], 1)
        ],
        # BABEL
        "0xf4dc48d260c93ad6a96c5ce563e70ca578987c74".lower(): [
            (DEX_POOLS[Chain.ETHEREUM]["BABEL_WETH"], 0),
            (DEX_POOLS[Chain.ETHEREUM]["USDC_WETH"], 0),
        ],
        # renBTC
        "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d".lower(): [
            (DEX_POOLS[Chain.ETHEREUM]["renBTC-WETH"], 0),
            (DEX_POOLS[Chain.ETHEREUM]["USDC_WETH"], 0),
        ],
    },
    Chain.OPTIMISM: {
        # OP
        "0x4200000000000000000000000000000000000042".lower(): [
            (DEX_POOLS[Chain.OPTIMISM]["WETH_OP"], 0),
            (DEX_POOLS[Chain.OPTIMISM]["WETH_USDC"], 1),
        ],
        # MOCK-OPT
        "0x601e471de750cdce1d5a2b8e6e671409c8eb2367".lower(): [
            (DEX_POOLS[Chain.OPTIMISM]["WETH_OP"], 0),
            (DEX_POOLS[Chain.OPTIMISM]["WETH_USDC"], 1),
        ],
        # OATH
        "0x00e1724885473b63bce08a9f0a52f35b0979e35a".lower(): [
            (DEX_POOLS[Chain.OPTIMISM]["OATH_ERN"], 1),
            (DEX_POOLS[Chain.OPTIMISM]["ERN_USDC"], 1),
        ],
    },
    Chain.POLYGON: {
        # USDC
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower(): [],
        # WMATIC
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1)
        ],
        # QI
        "0x580a84c73811e1839f75d86d75d88cca0c241ff4".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_QI"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # QUICK
        "0xb5c064f955d8e7f38fe0460c556a72987494ee17".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_QUICK"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # dQUICK
        "0x958d208cdf087843e9ad98d23823d32e17d723a1".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_DQUICK"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # GHST
        "0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_GHST"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # ANKR
        "0x101a023270368c0d50bffb62780f4afd4ea79c35".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_ANKR"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # DAVOS
        "0xec38621e72d86775a89c7422746de1f52bba5320".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_DAVOS"], 0)
        ],
        # GIDDY
        "0x67eb41a14c0fe5cd701fc9d5a3d6597a72f641a6".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_GIDDY"], 0)
        ],
        # LCD
        "0xc2a45fe7d40bcac8369371b08419ddafd3131b4a".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WMATIC_LCD"], 0),
            (DEX_POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # WOMBAT
        "0x0c9c7712c83b3c70e7c5e11100d33d9401bdf9dd".lower(): [
            (DEX_POOLS[Chain.POLYGON]["WOMBAT_USDC"], 1),
        ],
        # FIS
        "0x7a7b94f18ef6ad056cda648588181cda84800f94".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_FIS"], 0),
        ],
        # SD
        "0x1d734a02ef1e1f5886e66b0673b71af5b53ffa94".lower(): [
            (DEX_POOLS[Chain.POLYGON]["SD_USDC"], 1),
        ],
        # DAI
        "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_DAI"], 0),
        ],
        # axlPEPE
        "0x8bae3f5eb10f39663e57be19741fd9ccef0e113a".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC_axlPEPE"], 0),
        ],
        # EUROe
        "0x820802fa8a99901f52e39acd21177b0be6ee2974".lower(): [
            (DEX_POOLS[Chain.POLYGON]["USDC-EUROe"], 0),
        ],
        # liveRETRO
        "0xcaaf554900e33ae5dbc66ae9f8adc3049b7d31db".lower(): [
            (DEX_POOLS[Chain.POLYGON]["liveRETRO_RETRO"], 0),
            (DEX_POOLS[Chain.POLYGON]["RETRO_USDC"], 0),
        ],
    },
    Chain.POLYGON_ZKEVM: {
        # WMATIC
        "0xa2036f0538221a77a3937f1379699f44945018d0".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_MATIC"], 0),
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_USDC"], 1),
        ],
        # QUICK
        "0x68286607a1d43602d880d349187c3c48c0fd05e6".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["QUICK_USDC"], 1),
        ],
        # WETH
        "0x4f9a0e7fd2bf6067db6994cf12e4495df938e6e9".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_USDC"], 1),
        ],
        # WBTC
        "0xea034fb02eb1808c2cc3adbc15f447b93cbe08e1".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_WBTC"], 0),
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["WETH_USDC"], 1),
        ],
        # DAI
        "0xc5015b9d9161dca7e18e32f6f25c4ad850731fd4".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["USDC_DAI"], 0),
        ],
        # USDT
        "0x1e4a5963abfd975d8c9021ce480b42188849d41d".lower(): [
            (DEX_POOLS[Chain.POLYGON_ZKEVM]["USDT_USDC"], 1),
        ],
    },
    Chain.BSC: {
        # THE
        "0xf4c8e32eadec4bfe97e0f595add0f4450a863a11".lower(): [
            (DEX_POOLS[Chain.BSC]["THE_USDT"], 0),
        ],
        # WBNB
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c".lower(): [
            (DEX_POOLS[Chain.BSC]["USDT_WBNB"], 0),
            (DEX_POOLS[Chain.BSC]["USDT_USDC"], 0),
        ],
    },
    Chain.AVALANCHE: {
        # PHAR
        "0xaaab9d12a30504559b0c5a9a5977fee4a6081c6b".lower(): [
            (DEX_POOLS[Chain.AVALANCHE]["PHAR_WAVAX"], 0),
            (DEX_POOLS[Chain.AVALANCHE]["WAVAX_USDC"], 1),
        ],
        # WAVAX
        "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7".lower(): [
            (DEX_POOLS[Chain.AVALANCHE]["WAVAX_USDC"], 1),
        ],
    },
    Chain.ARBITRUM: {
        # DAI
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1".lower(): [
            (DEX_POOLS[Chain.ARBITRUM]["DAI_USDC"], 1),
        ],
        # USDT
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9".lower(): [
            (DEX_POOLS[Chain.ARBITRUM]["USDT_USDC"], 1),
        ],
        # GRAI
        "0x894134a25a5fac1c2c26f1d8fbf05111a3cb9487".lower(): [
            (DEX_POOLS[Chain.ARBITRUM]["GRAI-LUSD"], 0),
            (DEX_POOLS[Chain.ARBITRUM]["LUSD-USDC"], 1),
        ],
        # ARB
        "0x912ce59144191c1204e64559fe8253a0e49e6548".lower(): [
            (DEX_POOLS[Chain.ARBITRUM]["ARB-USDC"], 1),
        ],
        # CAKE
        "0x1b896893dfc86bb67cf57767298b9073d2c1ba2c".lower(): [
            (DEX_POOLS[Chain.ARBITRUM]["CAKE-WETH"], 1),
            (DEX_POOLS[Chain.ARBITRUM]["WETH-USDC"], 1),
        ],
        # pxETH
        "0x4e0f3385c7f4a0f6e5e2f2d6b1f3a3f0f9c1f6a".lower(): [
            (DEX_POOLS[Chain.ARBITRUM]["pxETH-WETH"], 1),
            (DEX_POOLS[Chain.ARBITRUM]["WETH-USDC"], 1),
        ],
    },
    Chain.MOONBEAM: {
        # xcBNC
        "0xffffffff7cc06abdf7201b350a1265c62c8601d2".lower(): [
            (DEX_POOLS[Chain.MOONBEAM]["xcDOT-xcBNC"], 0),
            (DEX_POOLS[Chain.MOONBEAM]["xcDOT-WGLMR"], 0),
            (DEX_POOLS[Chain.MOONBEAM]["USDC-WGLMR_STELLA"], 0),
        ],
        # xcvDOT
        "0xffffffff15e1b7e3df971dd813bc394deb899abf".lower(): [
            (DEX_POOLS[Chain.MOONBEAM]["xcvDOT-xcDOT_BEAM"], 1),
            (DEX_POOLS[Chain.MOONBEAM]["xcDOT-WGLMR"], 0),
            (DEX_POOLS[Chain.MOONBEAM]["USDC-WGLMR_STELLA"], 0),
        ],
        # xcDOT
        "0xffffffff1fcacbd218edc0eba20fc2308c778080".lower(): [
            (DEX_POOLS[Chain.MOONBEAM]["xcDOT-WGLMR"], 0),
            (DEX_POOLS[Chain.MOONBEAM]["USDC-WGLMR_STELLA"], 0),
        ],
    },
    Chain.BASE: {
        # TBTC
        "0x236aa50979d5f3de3bd1eeb40e81137f22ab794b".lower(): [
            (DEX_POOLS[Chain.BASE]["TBTC-WETH"], 0),
            (DEX_POOLS[Chain.BASE]["WETH-USDbC"], 1),
        ],
    },
    Chain.CELO: {
        # WETH
        "0x66803fb87abd4aac3cbb3fad7c3aa01f6f3fb207".lower(): [
            (DEX_POOLS[Chain.CELO]["WETH-CELO"], 0),
            (DEX_POOLS[Chain.CELO]["CELO-cUSD"], 1),
        ],
        # cMCO2
        "0x32a9fe697a32135bfd313a6ac28792dae4d9979d".lower(): [
            (DEX_POOLS[Chain.CELO]["cMCO2-CELO"], 0),
            (DEX_POOLS[Chain.CELO]["CELO-cUSD"], 1),
        ],
    },
    Chain.MANTA: {
        # WETH
        "0x0dc808adce2099a9f62aa87d9670745aba741746".lower(): [
            (DEX_POOLS[Chain.MANTA]["USDC-WETH"], 1),
        ],
        # QUICK
        "0xe22e3d44ea9fb0a87ea3f7a8f41d869c677f0020".lower(): [
            (DEX_POOLS[Chain.MANTA]["QUICK-USDC"], 0),
        ],
        # STONE
        "0xec901da9c68e90798bbbb74c11406a32a70652c3".lower(): [
            (DEX_POOLS[Chain.MANTA]["STONE-WETH"], 0),
            (DEX_POOLS[Chain.MANTA]["USDC-WETH"], 1),
        ],
        # WBTC
        "0x305e88d809c9dc03179554bfbf85ac05ce8f18d6".lower(): [
            (DEX_POOLS[Chain.MANTA]["WBTC-WETH"], 0),
            (DEX_POOLS[Chain.MANTA]["USDC-WETH"], 1),
        ],
        # wstETH
        "0x2fe3ad97a60eb7c79a976fc18bb5ffd07dd94ba5".lower(): [
            (DEX_POOLS[Chain.MANTA]["wstETH-WETH"], 0),
            (DEX_POOLS[Chain.MANTA]["USDC-WETH"], 1),
        ],
        # USDT
        "0xf417f5a458ec102b90352f697d6e2ac3a3d2851f".lower(): [
            (DEX_POOLS[Chain.MANTA]["USDC-USDT"], 0),
        ],
        # MATIC
        "0x0f52a51287f9b3894d73df05164d0ee2533ccbb4".lower(): [
            (DEX_POOLS[Chain.MANTA]["MATIC-USDC"], 1),
        ],
        # wUSDM
        "0xbdad407f77f44f7da6684b416b1951eca461fb07".lower(): [
            (DEX_POOLS[Chain.MANTA]["wUSDM-STONE"], 0),
            (DEX_POOLS[Chain.MANTA]["STONE-WETH"], 0),
            (DEX_POOLS[Chain.MANTA]["USDC-WETH"], 0),
        ],
    },
    Chain.MANTLE: {
        # WETH
        "0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111".lower(): [
            (DEX_POOLS[Chain.MANTLE]["USDC-WETH"], 0),
        ],
        # CLEO
        "0xc1e0c8c30f251a07a894609616580ad2ceb547f2".lower(): [
            (DEX_POOLS[Chain.MANTLE]["CLEO-WMNT"], 0),
            (DEX_POOLS[Chain.MANTLE]["WETH-WMNT"], 1),
            (DEX_POOLS[Chain.MANTLE]["USDC-WETH"], 0),
        ],
        # WMNT
        "0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8".lower(): [
            (DEX_POOLS[Chain.MANTLE]["WETH-WMNT"], 0),
            (DEX_POOLS[Chain.MANTLE]["USDC-WETH"], 1),
        ],
    },
    Chain.OPBNB: {
        # BTCB
        "0x7c6b91d9be155a6db01f749217d76ff02a7227f2".lower(): [
            (DEX_POOLS[Chain.OPBNB]["BTCB-WBNB"], 0),
            (DEX_POOLS[Chain.OPBNB]["USDT-WBNB"], 1),
        ],
    },
    Chain.ASTAR_ZKEVM: {
        # USDT
        "0x1e4a5963abfd975d8c9021ce480b42188849d41d".lower(): [
            (DEX_POOLS[Chain.ASTAR_ZKEVM]["USDT-USDC"], 1),
        ],
        # wstETH
        "0x5d8cff95d7a57c0bf50b30b43c7cc0d52825d4a9".lower(): [
            (DEX_POOLS[Chain.ASTAR_ZKEVM]["wstETH-WETH"], 0),
            (DEX_POOLS[Chain.ASTAR_ZKEVM]["USDC-WETH"], 0),
        ],
        # vASTR
        "0x7746ef546d562b443ae4b4145541a3b1a3d75717".lower(): [
            (DEX_POOLS[Chain.ASTAR_ZKEVM]["vASTR-ASTR"], 0),
            (DEX_POOLS[Chain.ASTAR_ZKEVM]["ASTR-USDC"], 0),
        ],
        # ASTR
        "0xdf41220c7e322bfef933d85d01821ad277f90172".lower(): [
            (DEX_POOLS[Chain.ASTAR_ZKEVM]["ASTR-USDC"], 0),
        ],
        # WBTC
        "0xea034fb02eb1808c2cc3adbc15f447b93cbe08e1".lower(): [
            (DEX_POOLS[Chain.ASTAR_ZKEVM]["ASTR-WBTC"], 0),
            (DEX_POOLS[Chain.ASTAR_ZKEVM]["ASTR-USDC"], 0),
        ],
        # ETH
        "0xe9cc37904875b459fa5d0fe37680d36f1ed55e38".lower(): [
            (DEX_POOLS[Chain.ASTAR_ZKEVM]["USDC-WETH"], 0),
        ],
    },
    Chain.IMMUTABLE_ZKEVM: {
        # GOG
        "0xb00ed913aaff8280c17bff33cce82fe6d79e85e8".lower(): [
            (DEX_POOLS[Chain.IMMUTABLE_ZKEVM]["GOG-WIMX"], 0),
            (DEX_POOLS[Chain.IMMUTABLE_ZKEVM]["WIMX-USDC"], 1),
        ],
        # WIMX
        "0x3a0c2ba54d6cbd3121f01b96dfd20e99d1696c9d".lower(): [
            (DEX_POOLS[Chain.IMMUTABLE_ZKEVM]["WIMX-USDC"], 1),
        ],
        # ETH
        "0x52a6c53869ce09a731cd772f245b97a4401d3348".lower(): [
            (DEX_POOLS[Chain.IMMUTABLE_ZKEVM]["ETH-USDC"], 1),
        ],
    },
}
