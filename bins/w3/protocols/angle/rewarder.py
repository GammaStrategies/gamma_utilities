import logging
from web3 import Web3
from bins.general.enums import rewarderType
from bins.w3.protocols.gamma.rewarder import gamma_rewarder


class angle_merkle_distributor_v2(gamma_rewarder):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "MerkleRootDistributorV2"
        self._abi_path = abi_path or "data/abi/angle"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    def claimed(self, user: str, token: str) -> int:
        """amount to track claimed amounts

        Args:
            user (str):
            token (str):

        Returns:
            int:
        """
        return self.call_function_autoRpc(
            "claimed", None, Web3.toChecksumAddress(user), Web3.toChecksumAddress(token)
        )

    def operators(self, user: str, operator: str) -> int:
        """authorisation to claim

        Args:
            user (str):
            operator (str):

        Returns:
            int:
        """
        return self.call_function_autoRpc(
            "operators",
            None,
            Web3.toChecksumAddress(user),
            Web3.toChecksumAddress(operator),
        )

    @property
    def treasury(self) -> str:
        """treasury address

        Returns:
            str:
        """
        return self.call_function_autoRpc("treasury", None)

    @property
    def tree(self) -> tuple[str, str]:
        """Root of a Merkle tree which leaves are (address user, address token, uint amount)
            representing an amount of tokens owed to user.
            The Merkle tree is assumed to have only increasing amounts: that is to say if a user can claim 1,
            then after the amount associated in the Merkle tree for this token should be x > 1

        Returns:
            tuple[str,str]:
                    merkleRoot   bytes32 :  0xc6664a8a96012f41af2608204c5a61565949a7d2634681c15dceb8b221e818c5
                    ipfsHash   bytes32 :  0xaea7a60091aabd89bdc3193b3b8becbf9281894f69b6f12285c274e97f40b2bb
        """
        return self.call_function_autoRpc("tree", None)

    def trusted(self, address: str) -> int:
        """Trusted EOAs to update the merkle root

        Args:
            address (str):

        Returns:
            int:
        """
        return self.call_function_autoRpc(
            "trusted", None, Web3.toChecksumAddress(address)
        )

    def whitelist(self, address: str) -> int:
        """Whether or not to enable permissionless claiming

        Args:
            address (str):

        Returns:
            int:
        """
        return self.call_function_autoRpc(
            "whitelist", None, Web3.toChecksumAddress(address)
        )

    # def get_ipfs_cid_v0(self) -> str:
    #     """ Construct IPFS CID v0 from ipfs hash sourced from tree function

    #     Returns:
    #         str:
    #     """

    # from bins.converters.ipfs_bytes import ipfs_bytes_to_cid_v0
    #     return ipfs_bytes_to_cid_v0(self.tree["ipfsHash"])


class angle_merkle_distributor_creator(gamma_rewarder):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "DistributionCreator"
        self._abi_path = abi_path or "data/abi/angle"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def BASE_9(self) -> int:
        """Base for fee computation  (constant)"""
        return self.call_function_autoRpc("BASE_9", None)

    @property
    def EPOCH_DURATION(self) -> int:
        """Epoch duration in seconds (constant)"""
        return self.call_function_autoRpc("EPOCH_DURATION", None)

    @property
    def core(self) -> str:
        """Core contract handling access control"""
        return self.call_function_autoRpc("core", None)

    def distributionList(self, id: int) -> tuple:
        """List of all rewards ever distributed or to be distributed in the contract

        Args:
            tuple:  rewardId bytes32,               Custom data specified by the distributor
                    uniV3Pool address,              Address of the Uniswap V3 pool
                    rewardToken address,            Address of the token to be distributed
                    amount uint256,                 Amount of tokens to be distributed
                    propToken0 uint32,              Proportion of rewards that'll be split among LPs which brought token0 in the pool during the time of the distribution
                    propToken1 uint32,              Proportion of rewards that'll be split among LPs which brought token1 in the pool during the time of the distribution
                    propFees uint32,                Proportion of rewards that'll be split among LPs which accumulated fees during the time of the distribution
                    epochStart uint32,              Timestamp of the start of the distribution
                    numEpoch uint32,                Number of hours for which the distribution should last once it has started
                    isOutOfRangeIncentivized uint32,    Whether out of range liquidity should be incentivized
                    boostedReward uint32,           Multiplier provided by the address boosting reward. In the case of a Curve distribution where veCRV provides a 2.5x boost, this would be equal to 25000
                    boostingAddress address,        Address of the token which dictates who gets boosted rewards or not. This is optional and if the zero address is given no boost will be taken into account
                    additionalData bytes            Custom data specified by the distributor

        Returns:
            int:
        """
        return self.call_function_autoRpc("distributionList", None, id)

    @property
    def distributor(self) -> str:
        """Distributor contract address"""
        return self.call_function_autoRpc("distributor", None)

    def feeRebate(self, address: str) -> int:
        """Maps an address to its fee rebate

        Returns:
            int:
        """
        return self.call_function_autoRpc(
            "feeRebate", None, Web3.toChecksumAddress(address)
        )

    @property
    def feeRecipient(self) -> str:
        """Address to which fees are forwarded"""
        return self.call_function_autoRpc("feeRecipient", None)

    @property
    def fees(self) -> int:
        """Value (in base 10**9) of the fees taken when creating a distribution for a pool which do not have a whitelisted token in it"""
        return self.call_function_autoRpc("fees", None)

    @property
    def getActiveDistributions(self) -> list[dict]:
        """Returns the list of all currently active distributions on pools of supported AMMs

        Returns:
           list[dict]:[
                   [
                       rewardId    0xa922593be6d33b26bfad4d55a35c412b555d99e3bb8552397816a893e9fa4c2d,     -> ID ( rewardId= bytes32(keccak256(abi.encodePacked(msg.sender, senderNonce))) )
                       POOL        0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9,                             -> POOL
                       token       0x31429d1856aD1377A8A0079410B297e1a9e214c2,                             -> token
                       totalAmount    423058392579202828719633,                                            -> totalAmount
                       wrapperContracts    0x3785Ce82be62a342052b9E5431e9D3a839cfB581,                           Vyper_contract
                       wrapperTypes    3,                                                                   Type of the wrappers (...2=Gamma, 3=blacklisted addresses)
                       propToken0    4000,                                                                   propToken0
                       propToken1    2000,                                                                   propToken1
                       propFees    4000,                                                                     propFees
                       epochStart    1685577600,                                                             epochStart
                       numEpoch    168,                                                                      numEpoch
                       isOutOfRangeincentivized    0,                                                        isOutOfRangeincentivized
                       boostedReward    25000,                                                              -> boostedReward
                       boostedAddress    0x52701bFA0599db6db2b2476075D9a2f4Cb77DAe3,                        -> boostedAddress
                       additionalData    0x,
                       pool fee    500,                                                                       pool fee
                       token0 contract    0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8,                         agEUR token contract
                       token0 decim     18,                                                                   agEUR decimals
                       token0 symbol     agEUR,                                                               agEUR symbol
                       token0 balance in pool     958630637523418638910027,                                   agEUR poolBalance
                       token1 contract     0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,                        WETH token contract
                       token1 decim     18,                                                                   WETH decimals
                       token1 symbol    WETH,                                                                 WETH symbol
                       token1 balance in pool    468051842301471649778,                                       WETH poolBalance
                       tokenSymbol    ANGLE,                                                                 -> tokenSymbol
                       tokenDecimals    18                                                                   -> tokenDecimals
                   ], [...],
                   ]
        """

        result = []
        for raw_result in self.call_function_autoRpc("getActiveDistributions", None):
            try:
                result.append(self.convert_distribution_tuple(raw_result))

            except Exception as e:
                logging.getLogger(__name__).exception(
                    f"Error parsing getActiveDistributions: {e}"
                )

        return result

    def getActivePoolDistributions(self, address: str) -> list[dict]:
        """

        Returns:
            list[tuple]:[
                    [
                        rewardId    0xa922593be6d33b26bfad4d55a35c412b555d99e3bb8552397816a893e9fa4c2d,     -> ID ( rewardId= bytes32(keccak256(abi.encodePacked(msg.sender, senderNonce))) )
                        POOL        0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9,                             -> POOL
                        token       0x31429d1856aD1377A8A0079410B297e1a9e214c2,                             -> token
                        totalAmount    423058392579202828719633,                                            -> totalAmount
                        wrapperContracts    0x3785Ce82be62a342052b9E5431e9D3a839cfB581,                           Vyper_contract
                        wrapperTypes    3,
                        propToken0    4000,                                                                   propToken0
                        propToken1    2000,                                                                   propToken1
                        propFees    4000,                                                                     propFees
                        epochStart    1685577600,                                                             epochStart
                        numEpoch    168,                                                                      numEpoch
                        isOutOfRangeincentivized    0,                                                        isOutOfRangeincentivized
                        boostedReward    25000,                                                              -> boostedReward
                        boostedAddress    0x52701bFA0599db6db2b2476075D9a2f4Cb77DAe3,                        -> boostedAddress
                        additionalData    0x,
                        pool fee    500,                                                                       pool fee
                        token0 contract    0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8,                         agEUR token contract
                        token0 decim     18,                                                                   agEUR decimals
                        token0 symbol     agEUR,                                                               agEUR symbol
                        token0 balance in pool     958630637523418638910027,                                   agEUR poolBalance
                        token1 contract     0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,                        WETH token contract
                        token1 decim     18,                                                                   WETH decimals
                        token1 symbol    WETH,                                                                 WETH symbol
                        token1 balance in pool    468051842301471649778,                                       WETH poolBalance
                        tokenSymbol    ANGLE,                                                                 -> tokenSymbol
                        tokenDecimals    18                                                                   -> tokenDecimals
                    ], [...],
                    ]
        """
        result = []
        for raw_result in self.call_function_autoRpc(
            "getActivePoolDistributions", None, Web3.toChecksumAddress(address)
        ):
            try:
                result.append(self.convert_distribution_tuple(raw_result))

            except Exception as e:
                logging.getLogger(__name__).exception(
                    f"Error parsing getActiveDistributions: {e}"
                )

        return result

    @property
    def getAllDistributions(self) -> list[dict]:
        """Returns the list of all distributions ever made or to be done in the future
        Returns:
            list[tuple]:[
                    [
                        rewardId    0xa922593be6d33b26bfad4d55a35c412b555d99e3bb8552397816a893e9fa4c2d,
                        POOL        0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9,
                        token       0x31429d1856aD1377A8A0079410B297e1a9e214c2,
                        totalAmount    423058392579202828719633,
                        wrapperContracts    0x3785Ce82be62a342052b9E5431e9D3a839cfB581,
                        wrapperTypes    3,
                        propToken0    4000,
                        propToken1    2000,
                        propFees    4000,
                        epochStart    1685577600,
                        numEpoch    168,
                        isOutOfRangeincentivized    0,
                        boostedReward    25000,
                        boostedAddress    0x52701bFA0599db6db2b2476075D9a2f4Cb77DAe3,
                        additionalData    0x,
                    ], [...],
                    ]
        """
        result = []
        for raw_result in self.call_function_autoRpc("getAllDistributions", None):
            try:
                result.append(self.convert_distribution_tuple(raw_result))

            except Exception as e:
                logging.getLogger(__name__).exception(
                    f"Error parsing getActiveDistributions: {e}"
                )

        return result

    def getDistributionsAfterEpoch(self, epochStart: int) -> list[tuple]:
        """Returns the list of all distributions that were or will be live after `epochStart` (included)

        Returns:
           list[tuple]:[
                   [
                       rewardId    0xa922593be6d33b26bfad4d55a35c412b555d99e3bb8552397816a893e9fa4c2d,     -> ID ( rewardId= bytes32(keccak256(abi.encodePacked(msg.sender, senderNonce))) )
                       POOL        0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9,                             -> POOL
                       token       0x31429d1856aD1377A8A0079410B297e1a9e214c2,                             -> token
                       totalAmount    423058392579202828719633,                                            -> totalAmount
                        wrapperContracts    0x3785Ce82be62a342052b9E5431e9D3a839cfB581,
                        wrapperTypes    3,
                       propToken0    4000,                                                                   propToken0
                       propToken1    2000,                                                                   propToken1
                       propFees    4000,                                                                     propFees
                       epochStart    1685577600,                                                             epochStart
                       numEpoch    168,                                                                      numEpoch
                       isOutOfRangeincentivized    0,                                                        isOutOfRangeincentivized
                       boostedReward    25000,                                                              -> boostedReward
                       boostedAddress    0x52701bFA0599db6db2b2476075D9a2f4Cb77DAe3,                        -> boostedAddress
                       additionalData    0x,
                       pool fee    500,                                                                       pool fee
                       token0 contract    0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8,                         agEUR token contract
                       token0 decim     18,                                                                   agEUR decimals
                       token0 symbol     agEUR,                                                               agEUR symbol
                       token0 balance in pool     958630637523418638910027,                                   agEUR poolBalance
                       token1 contract     0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,                        WETH token contract
                       token1 decim     18,                                                                   WETH decimals
                       token1 symbol    WETH,                                                                 WETH symbol
                       token1 balance in pool    468051842301471649778,                                       WETH poolBalance
                       tokenSymbol    ANGLE,                                                                 -> tokenSymbol
                       tokenDecimals    18                                                                   -> tokenDecimals
                   ], [...],
                   ]
        """
        return self.call_function_autoRpc(
            "getDistributionsAfterEpoch", None, epochStart
        )

    def getDistributionsBetweenEpochs(
        self, epochStart: int, epochEnd: int
    ) -> list[tuple]:
        """Gets the list of all the distributions that have been active between `epochStart` and `epochEnd` (excluded)
            Conversely, if a distribution starts after `epochStart` and ends before `epochEnd`, it is returned by this function
        Returns:
          list[tuple]:[
                  [
                      rewardId    0xa922593be6d33b26bfad4d55a35c412b555d99e3bb8552397816a893e9fa4c2d,     -> ID ( rewardId= bytes32(keccak256(abi.encodePacked(msg.sender, senderNonce))) )
                      POOL        0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9,                             -> POOL
                      token       0x31429d1856aD1377A8A0079410B297e1a9e214c2,                             -> token
                      totalAmount    423058392579202828719633,                                            -> totalAmount
                       wrapperContracts    0x3785Ce82be62a342052b9E5431e9D3a839cfB581,
                        wrapperTypes    3,
                      propToken0    4000,                                                                   propToken0
                      propToken1    2000,                                                                   propToken1
                      propFees    4000,                                                                     propFees
                      epochStart    1685577600,                                                             epochStart
                      numEpoch    168,                                                                      numEpoch
                      isOutOfRangeincentivized    0,                                                        isOutOfRangeincentivized
                      boostedReward    25000,                                                              -> boostedReward
                      boostedAddress    0x52701bFA0599db6db2b2476075D9a2f4Cb77DAe3,                        -> boostedAddress
                      additionalData    0x,
                      pool fee    500,                                                                       pool fee
                      token0 contract    0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8,                         agEUR token contract
                      token0 decim     18,                                                                   agEUR decimals
                      token0 symbol     agEUR,                                                               agEUR symbol
                      token0 balance in pool     958630637523418638910027,                                   agEUR poolBalance
                      token1 contract     0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,                        WETH token contract
                      token1 decim     18,                                                                   WETH decimals
                      token1 symbol    WETH,                                                                 WETH symbol
                      token1 balance in pool    468051842301471649778,                                       WETH poolBalance
                      tokenSymbol    ANGLE,                                                                 -> tokenSymbol
                      tokenDecimals    18                                                                   -> tokenDecimals
                  ], [...],
                  ]
        """
        return self.call_function_autoRpc(
            "getDistributionsBetweenEpochs", None, epochStart, epochEnd
        )

    def getDistributionsForEpoch(self, epoch: int) -> list[tuple]:
        """Returns the list of all the distributions that were or that are going to be live at a specific epoch

        Returns:
           list[tuple]:[
                   [
                       rewardId    0xa922593be6d33b26bfad4d55a35c412b555d99e3bb8552397816a893e9fa4c2d,     -> ID ( rewardId= bytes32(keccak256(abi.encodePacked(msg.sender, senderNonce))) )
                       POOL        0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9,                             -> POOL
                       token       0x31429d1856aD1377A8A0079410B297e1a9e214c2,                             -> token
                       totalAmount    423058392579202828719633,                                            -> totalAmount
                        wrapperContracts    0x3785Ce82be62a342052b9E5431e9D3a839cfB581,
                        wrapperTypes    3,
                       propToken0    4000,                                                                   propToken0
                       propToken1    2000,                                                                   propToken1
                       propFees    4000,                                                                     propFees
                       epochStart    1685577600,                                                             epochStart
                       numEpoch    168,                                                                      numEpoch
                       isOutOfRangeincentivized    0,                                                        isOutOfRangeincentivized
                       boostedReward    25000,                                                              -> boostedReward
                       boostedAddress    0x52701bFA0599db6db2b2476075D9a2f4Cb77DAe3,                        -> boostedAddress
                       additionalData    0x,
                       pool fee    500,                                                                       pool fee
                       token0 contract    0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8,                         agEUR token contract
                       token0 decim     18,                                                                   agEUR decimals
                       token0 symbol     agEUR,                                                               agEUR symbol
                       token0 balance in pool     958630637523418638910027,                                   agEUR poolBalance
                       token1 contract     0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,                        WETH token contract
                       token1 decim     18,                                                                   WETH decimals
                       token1 symbol    WETH,                                                                 WETH symbol
                       token1 balance in pool    468051842301471649778,                                       WETH poolBalance
                       tokenSymbol    ANGLE,                                                                 -> tokenSymbol
                       tokenDecimals    18                                                                   -> tokenDecimals
                   ], [...],
                   ]
        """
        return self.call_function_autoRpc("getDistributionsForEpoch", None, epoch)

    def getPoolDistributionsAfterEpoch(
        self, pool_address: str, epochStart: int
    ) -> list[tuple]:
        """Returns the list of all distributions that were or will be live after `epochStart` (included) for a specific pool
        Returns:
         list[tuple]:[
                 [
                     rewardId    0xa922593be6d33b26bfad4d55a35c412b555d99e3bb8552397816a893e9fa4c2d,     -> ID ( rewardId= bytes32(keccak256(abi.encodePacked(msg.sender, senderNonce))) )
                     POOL        0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9,                             -> POOL
                     token       0x31429d1856aD1377A8A0079410B297e1a9e214c2,                             -> token
                     totalAmount    423058392579202828719633,                                            -> totalAmount
                     wrapperContracts    0x3785Ce82be62a342052b9E5431e9D3a839cfB581,
                        wrapperTypes    3,
                     propToken0    4000,                                                                   propToken0
                     propToken1    2000,                                                                   propToken1
                     propFees    4000,                                                                     propFees
                     epochStart    1685577600,                                                             epochStart
                     numEpoch    168,                                                                      numEpoch
                     isOutOfRangeincentivized    0,                                                        isOutOfRangeincentivized
                     boostedReward    25000,                                                              -> boostedReward
                     boostedAddress    0x52701bFA0599db6db2b2476075D9a2f4Cb77DAe3,                        -> boostedAddress
                     additionalData    0x,
                     pool fee    500,                                                                       pool fee
                     token0 contract    0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8,                         agEUR token contract
                     token0 decim     18,                                                                   agEUR decimals
                     token0 symbol     agEUR,                                                               agEUR symbol
                     token0 balance in pool     958630637523418638910027,                                   agEUR poolBalance
                     token1 contract     0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,                        WETH token contract
                     token1 decim     18,                                                                   WETH decimals
                     token1 symbol    WETH,                                                                 WETH symbol
                     token1 balance in pool    468051842301471649778,                                       WETH poolBalance
                     tokenSymbol    ANGLE,                                                                 -> tokenSymbol
                     tokenDecimals    18                                                                   -> tokenDecimals
                 ], [...],
                 ]
        """
        return self.call_function_autoRpc(
            "getPoolDistributionsAfterEpoch",
            None,
            Web3.toChecksumAddress(pool_address),
            epochStart,
        )

    def getPoolDistributionsBetweenEpochs(
        self, pool_address: str, epochStart: int, epochEnd: int
    ) -> list[tuple]:
        """Returns the list of all distributions that were or will be live between `epochStart` (included) and `epochEnd` (excluded) for a specific pool
        Returns:
          list[tuple]:[
                  [
                      rewardId    0xa922593be6d33b26bfad4d55a35c412b555d99e3bb8552397816a893e9fa4c2d,     -> ID ( rewardId= bytes32(keccak256(abi.encodePacked(msg.sender, senderNonce))) )
                      POOL        0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9,                             -> POOL
                      token       0x31429d1856aD1377A8A0079410B297e1a9e214c2,                             -> token
                      totalAmount    423058392579202828719633,                                            -> totalAmount
                       wrapperContracts    0x3785Ce82be62a342052b9E5431e9D3a839cfB581,
                        wrapperTypes    3,
                      propToken0    4000,                                                                   propToken0
                      propToken1    2000,                                                                   propToken1
                      propFees    4000,                                                                     propFees
                      epochStart    1685577600,                                                             epochStart
                      numEpoch    168,                                                                      numEpoch
                      isOutOfRangeincentivized    0,                                                        isOutOfRangeincentivized
                      boostedReward    25000,                                                              -> boostedReward
                      boostedAddress    0x52701bFA0599db6db2b2476075D9a2f4Cb77DAe3,                        -> boostedAddress
             getDistributionsBetweenEpochs         token1 balance in pool    468051842301471649778,                                       WETH poolBalance
                      tokenSymbol    ANGLE,                                                                 -> tokenSymbol
                      tokenDecimals    18                                                                   -> tokenDecimals
                  ], [...],
                  ]
        """
        return self.call_function_autoRpc(
            "getPoolDistributionsBetweenEpochs",
            None,
            Web3.toChecksumAddress(pool_address),
            epochStart,
            epochEnd,
        )

    def getPoolDistributionsForEpoch(
        self, pool_address: str, epoch: int
    ) -> list[tuple]:
        """Returns the list of all the distributions that were or that are going to be live at a specific epoch and for a specific pool

        Returns:
           list[tuple]:[
                   [
                       rewardId    0xa922593be6d33b26bfad4d55a35c412b555d99e3bb8552397816a893e9fa4c2d,     -> ID ( rewardId= bytes32(keccak256(abi.encodePacked(msg.sender, senderNonce))) )
                       POOL        0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9,                             -> POOL
                       token       0x31429d1856aD1377A8A0079410B297e1a9e214c2,                             -> token
                       totalAmount    423058392579202828719633,                                            -> totalAmount
                       wrapperContracts    0x3785Ce82be62a342052b9E5431e9D3a839cfB581,
                        wrapperTypes    3,
                       propToken0    4000,                                                                   propToken0
                       propToken1    2000,                                                                   propToken1
                       propFees    4000,                                                                     propFees
                       epochStart    1685577600,                                                             epochStart
                       numEpoch    168,                                                                      numEpoch
                       isOutOfRangeincentivized    0,                                                        isOutOfRangeincentivized
                       boostedReward    25000,                                                              -> boostedReward
                       boostedAddress    0x52701bFA0599db6db2b2476075D9a2f4Cb77DAe3,                        -> boostedAddress
                       additionalData    0x,
                       pool fee    500,                                                                       pool fee
                       token0 contract    0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8,                         agEUR token contract
                       token0 decim     18,                                                                   agEUR decimals
                       token0 symbol     agEUR,                                                               agEUR symbol
                       token0 balance in pool     958630637523418638910027,                                   agEUR poolBalance
                       token1 contract     0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,                        WETH token contract
                       token1 decim     18,                                                                   WETH decimals
                       token1 symbol    WETH,                                                                 WETH symbol
                       token1 balance in pool    468051842301471649778,                                       WETH poolBalance
                       tokenSymbol    ANGLE,                                                                 -> tokenSymbol
                       tokenDecimals    18                                                                   -> tokenDecimals
                   ], [...],
                   ]
        """
        return self.call_function_autoRpc(
            "getPoolDistributionsForEpoch",
            None,
            Web3.toChecksumAddress(pool_address),
            epoch,
        )

    @property
    def getValidRewardTokens(self) -> list[tuple[str, int]]:
        """Returns the list of all the reward tokens supported as well as their minimum amounts
        Returns: list[tuple[token address, minimum amount]]

        """
        return self.call_function_autoRpc("getValidRewardTokens", None)

    def isWhitelistedToken(self, address: str) -> int:
        """token to whether it is whitelisted or not. No fees are to be paid for incentives given on pools with whitelisted tokens
        Returns:

        """
        return self.call_function_autoRpc(
            "isWhitelistedToken", None, Web3.toChecksumAddress(address)
        )

    @property
    def message(self) -> str:
        """Message that needs to be acknowledged by users creating a distribution"""
        return self.call_function_autoRpc("message", None)

    @property
    def messageHash(self) -> str:
        """Hash of the message that needs to be signed by users creating a distribution"""
        return self.call_function_autoRpc("messageHash", None)

    def nonces(self, address: str) -> int:
        """nonce for creating a distribution
        Returns:

        """
        return self.call_function_autoRpc(
            "nonces", None, Web3.toChecksumAddress(address)
        )

    def rewardTokenMinAmounts(self, address: str) -> int:
        """token to the minimum amount that must be sent per epoch for a distribution to be valid if `rewardTokenMinAmounts[token] == 0`, then `token` cannot be used as a reward
        Returns:

        """
        return self.call_function_autoRpc(
            "rewardTokenMinAmounts", None, Web3.toChecksumAddress(address)
        )

    def rewardTokens(self, id: int) -> str:
        """List of all reward tokens that have at some point been accepted
        Returns: address

        """
        return self.call_function_autoRpc("rewardTokens", None, id)

    def userSignatureWhitelist(self, address: str) -> int:
        """ """
        return self.call_function_autoRpc(
            "userSignatureWhitelist", None, Web3.toChecksumAddress(address)
        )

    def userSignatures(self, address: str) -> str:
        """ """
        return self.call_function_autoRpc(
            "userSignatures", None, Web3.toChecksumAddress(address)
        )

    # custom functions

    def convert_distribution_tuple(self, raw_data: tuple) -> dict:
        """Converts the raw data from the getDistributionsBetweenEpochs function into a dictionary"""
        return {
            "rewardId": raw_data[0][0],
            "pool": raw_data[0][1].lower(),
            "token": raw_data[0][2].lower(),
            "totalAmount": raw_data[0][3],
            "wraper_contracts": raw_data[0][4],
            "wraper_type": raw_data[0][5],
            "propToken0": raw_data[0][6],
            "propToken1": raw_data[0][7],
            "propFees": raw_data[0][8],
            "epochStart": raw_data[0][9],
            "numEpoch": raw_data[0][10],
            "isOutOfRangeincentivized": raw_data[0][11],
            "boostedReward": raw_data[0][12],
            "boostedAddress": raw_data[0][13],
            "additionalData": raw_data[0][14],
            "pool_fee": raw_data[1],
            "token0_contract": raw_data[2][0].lower(),
            "token0_decimals": raw_data[2][1],
            "token0_symbol": raw_data[2][2],
            "token0_balance_in_pool": raw_data[2][3],
            "token1_contract": raw_data[3][0].lower(),
            "token1_decimals": raw_data[3][1],
            "token1_symbol": raw_data[3][2],
            "token1_balance_in_pool": raw_data[3][3],
            "token_symbol": raw_data[4],
            "token_decimals": raw_data[5],
        }

    def construct_reward_data(
        self,
        distribution_data: dict,
        hypervisor_address: str,
        total_hypervisorToken_qtty: int | None = None,
        epoch_duration: int | None = None,
        convert_bint: bool = False,
    ) -> dict:
        """

        Args:
            distribution_data (dict): _description_
            hypervisor_address (str): _description_
            total_hypervisorToken_qtty (int | None, optional): zero as default
            epoch_duration (int | None, optional): _description_. Defaults to None.
            convert_bint (bool, optional): _description_. Defaults to False.

        Returns:
            dict:
        """

        # calculate rewards per second
        rewardsPerSec = distribution_data["totalAmount"] / (
            distribution_data["numEpoch"] * (epoch_duration or self.EPOCH_DURATION)
        )
        # total hype qtty
        total_hypervisorToken_qtty = total_hypervisorToken_qtty or 0

        return {
            "block": self.block,
            "timestamp": self._timestamp,
            "hypervisor_address": hypervisor_address.lower(),
            "rewarder_address": self.distributor.lower(),
            "rewarder_type": rewarderType.ANGLE_MERKLE,
            "rewarder_refIds": [],
            "rewarder_registry": self.address.lower(),
            "rewardToken": distribution_data["token"].lower(),
            "rewardToken_symbol": distribution_data["token_symbol"],
            "rewardToken_decimals": distribution_data["token_decimals"],
            "rewards_perSecond": str(rewardsPerSec) if convert_bint else rewardsPerSec,
            "total_hypervisorToken_qtty": str(total_hypervisorToken_qtty)
            if convert_bint
            else total_hypervisorToken_qtty,
        }

    # get all rewards
    def get_rewards(
        self,
        hypervisors_pools: list[tuple[str, str]] | None = None,
        pids: list[int] | None = None,
        convert_bint: bool = False,
    ) -> list[dict]:
        """Search for rewards data

        Args:
            hypervisors_pools (list[tuple[str,str]] | None, optional): list of hypervisor+pool . When defaults to None, all rewards will be returned ( without hype address and may not be related to gamma)
            pids (list[int] | None, optional): pool ids linked to hypervisor/pool. When defaults to None, all pools will be returned.
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
        Returns:
            list[dict]:
                        block: int
                        timestamp: int
                        hypervisor_address: str
                        rewarder_address: str
                        rewarder_type: str
                        rewarder_refIds: list[str]
                        rewardToken: str
                        rewardToken_symbol: str
                        rewardToken_decimals: int
                        rewards_perSecond: int
                        total_hypervisorToken_qtty: int = ZERO!!
        """
        result = []

        # save for later use
        _epoch_duration = self.EPOCH_DURATION
        # roundEpoch -> (epoch / EPOCH_DURATION) * EPOCH_DURATION;
        # is live? -> (distributionEpochStart + distribution.numEpoch * EPOCH_DURATION > roundedEpochStart &&  distributionEpochStart < roundedEpochEnd)

        if hypervisors_pools:
            for hypervisor, pool in hypervisors_pools:
                # get data
                for reward_data in self.getActivePoolDistributions(pool):
                    result.append(
                        self.construct_reward_data(
                            distribution_data=reward_data,
                            hypervisor_address=hypervisor,
                            epoch_duration=_epoch_duration,
                            convert_bint=convert_bint,
                        )
                    )

        else:
            # TODO: get all hypervisors data ... by pid
            raise NotImplementedError("Not implemented yet")

        return result

    def get_reward_calculations(
        self, distribution: dict, _epoch_duration: int | None = None
    ) -> dict:
        """extracts reward paste info from distribution raw data

        Args:
            distribution (dict): dict returned in convert_distribution_tuple def
            _epoch_duration (int | None, optional): supply to avoid innecesary RPC calls. Defaults to None.

        Returns:
            dict: {
                "reward_x_epoch": ,
                "reward_x_second": ,
                "reward_yearly": ,
                "reward_yearly_token0": ,
                "reward_yearly_token1": ,
                "reward_yearly_fees": ,
            }
        """
        reward_x_epoch = distribution["totalAmount"] / (distribution["numEpoch"])

        reward_x_second = reward_x_epoch / (_epoch_duration or self.EPOCH_DURATION)

        reward_yearly = reward_x_second * 3600 * 24 * 365

        reward_yearly_token0 = distribution["propToken0"] * reward_yearly
        reward_yearly_token1 = distribution["propToken1"] * reward_yearly
        reward_yearly_fees = distribution["propFees"] * reward_yearly

        return {
            "reward_x_epoch": reward_x_epoch,
            "reward_x_second": reward_x_second,
            "reward_yearly": reward_yearly,
            "reward_yearly_token0": reward_yearly_token0,
            "reward_yearly_token1": reward_yearly_token1,
            "reward_yearly_fees": reward_yearly_fees,
        }
