import logging
from web3 import Web3
from eth_abi import abi

from bins.w3.helpers.multicaller import build_call_with_abi_part, execute_parse_calls
from ....configuration import TOKEN_ADDRESS_EXCLUDE
from ....general.enums import rewarderType, text_to_chain

from ..gamma.rewarder import gamma_rewarder


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
        self._abi_path = abi_path or f"{self.abi_root_path}/angle"

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
        self._abi_filename = abi_filename or "DistributionCreator_v3"
        self._abi_path = abi_path or f"{self.abi_root_path}/angle"

        # cache hour and base
        self._HOUR = None
        self._BASE_9 = None

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
        if not self._BASE_9:
            self._BASE_9 = self.call_function_autoRpc("BASE_9", None)

        return self._BASE_9

    @property
    def CHAIN_ID(self) -> int:
        """"""
        return self.call_function_autoRpc("CHAIN_ID", None)

    @property
    def HOUR(self) -> int:
        """Hour in seconds (constant)"""
        if not self._HOUR:
            self._HOUR = self.call_function_autoRpc("HOUR", None)
        return self._HOUR

    def _nonces(self, address: str) -> int:
        """nonce for creating a distribution
        Returns:

        """
        return self.call_function_autoRpc(
            "_nonces", None, Web3.toChecksumAddress(address)
        )

    def _campaign(self, id) -> tuple:
        """ """
        return self.call_function_autoRpc("_campaign", None, id)

    def campaignId(self, campaigndata: tuple):
        """ """
        return self.call_function_autoRpc("campaignId", None, campaigndata)

    def campaignList(self, idx: int):
        """ """
        return self.format_campaign(
            campaign_data=self.call_function_autoRpc("campaignList", None, idx)
        )

    def campaignLookup(self, campaignId) -> int:
        """ """
        return self.call_function_autoRpc("campaignLookup", None, campaignId)

    def campaignSpecificFees(self, input) -> int:
        """ """
        return self.call_function_autoRpc("campaignSpecificFees", None, input)

    @property
    def core(self) -> str:
        """Core contract handling access control"""
        return self.call_function_autoRpc("core", None)

    @property
    def defaultFees(self) -> int:
        """Value (in base 10**9) of the fees taken when creating a campaign"""
        return self.call_function_autoRpc("defaultFees", None)

    def distribution(self, id: int) -> tuple:
        """Returns the distribution at a given index converted into a campaign

        Args:
            id (int): index
        """
        result = self.call_function_autoRpc("distribution", None, id)
        return {
            "campaignId": "0x" + result[0].hex(),
            "creator": result[1],
            "rewardToken": result[2],
            "amount": result[3],
            "campaignType": result[4],
            "startTimestamp": result[5],
            "duration": result[6],
            "campaignData": result[7],
        }
        #           campaignData:
        #                 distributionToConvert.uniV3Pool,
        #                 distributionToConvert.propFees, // eg. 6000
        #                 distributionToConvert.propToken0, // eg. 3000
        #                 distributionToConvert.propToken1, // eg. 1000
        #                 distributionToConvert.isOutOfRangeIncentivized, // eg. 0
        #                 distributionToConvert.boostingAddress, // eg. NULL_ADDRESS
        #                 distributionToConvert.boostedReward, // eg. 0
        #                 whitelist, // eg. []
        #                 blacklist, // eg. []
        #                 "0x"

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

    def getCampaignsBetween(self, start: int, end: int, skip: int, first: int):
        """ """
        result = []
        for x in self.call_function_autoRpc(
            "getCampaignsBetween", None, start, end, skip, first
        )[0]:
            try:
                result.append(self.format_campaign(campaign_data=x))
            except Exception as e:
                logging.getLogger(__name__).error(f" Error decoding campaign data: {e}")

        return result

    def getDistributionsBetweenEpochs(
        self, epochStart: int, epochEnd: int, skip: int, first: int
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
            "getDistributionsBetweenEpochs", None, epochStart, epochEnd, skip, first
        )

    @property
    def getValidRewardTokens(self) -> list[tuple[str, int]]:
        """Returns the list of all the reward tokens supported as well as their minimum amounts
        Returns: list[tuple[token address, minimum amount]]

        """
        return self.call_function_autoRpc("getValidRewardTokens", None)

    def getValidRewardTokens2(self, skip: int, first: int) -> list[tuple[str, int]]:
        """Returns the list of all the reward tokens supported as well as their minimum amounts
        Returns: list[tuple[token address, minimum amount]]

        """
        return self.call_function_autoRpc("getValidRewardTokens", None, skip, first)

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

    @property
    def proxiableUUID(self) -> str:
        """"""
        return self.call_function_autoRpc("proxiableUUID", None)

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

    def format_campaign(self, campaign_data: list) -> dict:
        # decode the campaign data field
        try:
            _data_decoded = abi.decode(
                [
                    "address",
                    "uint32",
                    "uint32",
                    "uint32",
                    "uint32",
                    "address",
                    "uint32",
                    "address[]",
                    "address[]",
                    "bytes",
                ],
                campaign_data[7],
            )
            return {
                "campaignId": "0x" + campaign_data[0].hex(),
                "creator": campaign_data[1].lower(),
                "rewardToken": campaign_data[2].lower(),
                "amount": campaign_data[3],
                "campaignType": campaign_data[4],
                "startTimestamp": campaign_data[5],
                "duration": campaign_data[6],
                "campaignData": {
                    "pool": _data_decoded[0].lower(),
                    "propFees": _data_decoded[1],
                    "propToken0": _data_decoded[2],
                    "propToken1": _data_decoded[3],
                    "isOutOfRangeIncentivized": _data_decoded[4],
                    "boostingAddress": _data_decoded[5].lower(),
                    "boostedReward": _data_decoded[6],
                    "whitelist": _data_decoded[7],
                    "blacklist": _data_decoded[8],
                    "extra": _data_decoded[9],
                },
            }
        except Exception as e:
            raise ValueError(
                f" Error decoding id {campaign_data[0].hex()} campaign data: {e}"
            )

    def get_all_distributions(
        self,
        multicall: bool = True,
        max_index: int = 10000,
        max_calls_atOnce: int = 100,
    ) -> list[tuple]:
        result = []

        if multicall:
            # create all calls
            _calls = [
                build_call_with_abi_part(
                    abi_part=self.get_abi_function("distributionList"),
                    inputs_values=[i],
                    address=self.address,
                    object="merkl_distributor",
                )
                for i in range(0, max_index)
            ]

            logging.getLogger(__name__).info(
                f" {self._network}: {len(_calls)} fn calls will be executed for {self._address} angle merkl distributor, meaning that will be splited in {round(len(_calls)/max_calls_atOnce):,.0f} batches of {max_calls_atOnce} function calls, for each web3 call to RPC."
            )

            # execute multicall in batches
            for i in range(0, len(_calls), max_calls_atOnce):
                # get multicall data
                _tmp_multicall_data = execute_parse_calls(
                    network=self._network,
                    block=self.block,
                    calls=_calls[i : i + max_calls_atOnce],
                    convert_bint=False,
                    requireSuccess=False,
                    timestamp=self._timestamp,
                )
                # convert and add temporary data to multicall_result
                _errors = 0
                for itm in _tmp_multicall_data:
                    if not itm["outputs"]:
                        logging.getLogger(__name__).debug(
                            f" Bruteforce index multicall stopped bc no results found in a call."
                        )
                        _errors += max_calls_atOnce
                        break
                    try:
                        result.append(
                            {
                                "rewardId": "0x" + itm["outputs"][0]["value"].hex(),
                                "pool": itm["outputs"][1]["value"],
                                "rewardToken": itm["outputs"][2]["value"],
                                "amount": itm["outputs"][3]["value"],
                                "propToken0": itm["outputs"][4]["value"],
                                "propToken1": itm["outputs"][5]["value"],
                                "propFees": itm["outputs"][6]["value"],
                                "epochStart": itm["outputs"][7]["value"],
                                "numEpoch": itm["outputs"][8]["value"],
                                "isOutOfRangeIncetivized": itm["outputs"][9]["value"],
                                "boostedReward": itm["outputs"][10]["value"],
                                "boostingAddress": itm["outputs"][11]["value"],
                                "additionalData": "0x"
                                + itm["outputs"][12]["value"].hex(),
                            }
                        )
                    except Exception as e:
                        _errors += 1
                        # abort on too many errors
                        if _errors >= max_calls_atOnce * 0.10:
                            logging.getLogger(__name__).debug(
                                f" Bruteforce index multicall stopped bc no more results found in a loop"
                            )
                            break

                if _errors >= max_calls_atOnce * 0.10:
                    break

        else:
            logging.getLogger(__name__).debug(
                f" Getting all distributions from {self.address} using single calls"
            )
            for i in range(0, max_index):
                try:
                    itm = self.distributionList(i)
                    result.append(
                        {
                            "rewardId": "0x" + itm["outputs"][0]["value"].hex(),
                            "pool": itm["outputs"][1]["value"],
                            "rewardToken": itm["outputs"][2]["value"],
                            "amount": itm["outputs"][3]["value"],
                            "propToken0": itm["outputs"][4]["value"],
                            "propToken1": itm["outputs"][5]["value"],
                            "propFees": itm["outputs"][6]["value"],
                            "epochStart": itm["outputs"][7]["value"],
                            "numEpoch": itm["outputs"][8]["value"],
                            "isOutOfRangeIncetivized": itm["outputs"][9]["value"],
                            "boostedReward": itm["outputs"][10]["value"],
                            "boostingAddress": itm["outputs"][11]["value"],
                            "additionalData": "0x" + itm["outputs"][12]["value"].hex(),
                        }
                    )
                    # result += self.distributionList(i)
                except Exception as e:
                    break

        return result

    def get_all_campaigns(
        self,
        multicall: bool = True,
        max_index: int = 10000,
        max_calls_atOnce: int = 100,
    ) -> list[dict]:
        """Get all campaigns

        Args:
            multicall (bool, optional): _description_. Defaults to True.
            max_index (int, optional): _description_. Defaults to 10000.
            max_calls_atOnce (int, optional): _description_. Defaults to 100.

        Returns:
            list[dict]: [ {
                    "campaignId":
                    "creator": str
                    "rewardToken": str
                    "amount": int
                    "campaignType": int
                    "startTimestamp": int
                    "duration": int
                    "campaignData": {
                        "pool": str
                        "propFees": int
                        "propToken0": int
                        "propToken1": int
                        "isOutOfRangeIncentivized":
                        "boostingAddress": str
                        "boostedReward":
                        "whitelist": list[str]
                        "blacklist": list[str]
                        "extra":
                    },
            ]
        """
        result = []

        if multicall:
            # create all calls
            _calls = [
                build_call_with_abi_part(
                    abi_part=self.get_abi_function("campaignList"),
                    inputs_values=[i],
                    address=self.address,
                    object="merkl_distributor",
                )
                for i in range(0, max_index)
            ]

            logging.getLogger(__name__).info(
                f" {self._network}: {len(_calls)} fn calls will be executed for {self._address} angle merkl distributor, meaning that will be splited in {round(len(_calls)/max_calls_atOnce):,.0f} batches of {max_calls_atOnce} function calls, for each web3 call to RPC."
            )

            # execute multicall in batches
            for i in range(0, len(_calls), max_calls_atOnce):
                # get multicall data
                _tmp_multicall_data = execute_parse_calls(
                    network=self._network,
                    block=self.block,
                    calls=_calls[i : i + max_calls_atOnce],
                    convert_bint=False,
                    requireSuccess=False,
                    timestamp=self._timestamp,
                )
                # convert and add temporary data to multicall_result
                _errors = 0
                for itm in _tmp_multicall_data:
                    if not itm["outputs"]:
                        logging.getLogger(__name__).debug(
                            f" Bruteforce index multicall stopped bc no results found in a call."
                        )
                        _errors += max_calls_atOnce
                        break
                    try:
                        result.append(
                            self.format_campaign(
                                campaign_data=[
                                    itm["outputs"][i]["value"] for i in range(8)
                                ]
                            )
                        )
                    except Exception as e:
                        _errors += 1
                        # abort on too many errors
                        if _errors >= max_calls_atOnce * 0.25:
                            logging.getLogger(__name__).debug(
                                f" Bruteforce index multicall stopped bc no more results found in a loop"
                            )
                            break

                if _errors >= max_calls_atOnce * 0.25:
                    break

        else:
            logging.getLogger(__name__).debug(
                f" Getting all campaigns from {self.address} using single calls"
            )
            for i in range(0, max_index):
                try:
                    itm = self.distributionList(i)
                    result.append(
                        self.format_campaign(
                            campaign_data=[itm["outputs"][i]["value"] for i in range(8)]
                        )
                    )
                except Exception as e:
                    break

        return result

    def get_active_campaigns_manual(self, pool_address: str | None = None):
        """Get all active campaigns using a manual loop

        Args:
            pool_address (str | None, optional): pool filter . Defaults to None.

        Yields:
            {
                    "campaignId":
                    "creator": str
                    "rewardToken": str
                    "amount": int
                    "campaignType": int
                    "startTimestamp": int
                    "duration": int
                    "campaignData": {
                        "pool": str
                        "propFees": int
                        "propToken0": int
                        "propToken1": int
                        "isOutOfRangeIncentivized":
                        "boostingAddress": str
                        "boostedReward":
                        "whitelist": list[str]
                        "blacklist": list[str]
                        "extra":
                    }
        """

        for campaign in self.get_all_campaigns():
            # check if address is valid
            if (
                not pool_address
                or campaign["campaignData"]["pool"].lower() == pool_address
            ):
                # check if distribution is still active
                if (
                    campaign["startTimestamp"] + campaign["duration"]
                ) > self._timestamp:
                    yield campaign

    def get_active_campaigns(self, pool_address: str | None = None):
        """Get all active campaigns ( uses getCampaignsBetween)

        Args:
            pool_address (str | None, optional): pool filter . Defaults to None.

        Yields:
            {
                    "campaignId":
                    "creator": str
                    "rewardToken": str
                    "amount": int
                    "campaignType": int
                    "startTimestamp": int
                    "duration": int
                    "campaignData": {
                        "pool": str
                        "propFees": int
                        "propToken0": int
                        "propToken1": int
                        "isOutOfRangeIncentivized":
                        "boostingAddress": str
                        "boostedReward":
                        "whitelist": list[str]
                        "blacklist": list[str]
                        "extra":
                    }
        """

        for campaign in self.getCampaignsBetween(
            start=self._timestamp,
            end=int(self._timestamp + 3600 * 24 * 365),
            skip=0,
            first=10000,
        ):
            # check if address is valid
            if (
                not pool_address
                or campaign["campaignData"]["pool"].lower() == pool_address
            ):
                yield campaign

    def construct_reward_data(
        self,
        campaign_data: dict,
        hypervisor_address: str,
        total_hypervisorToken_qtty: int | None = None,
        convert_bint: bool = False,
    ) -> dict:
        """Will not return token symbol and decimals ! and total hype qtty when not set..

        Args:
            campaign_data (dict):
            hypervisor_address (str): _description_
            total_hypervisorToken_qtty (int | None, optional): zero as default
            epoch_duration (int | None, optional): _description_. Defaults to None.
            convert_bint (bool, optional): _description_. Defaults to False.

        Returns:
            dict:
        """

        # calculate rewards per second
        rewardsPerSec = campaign_data["amount"] / campaign_data["duration"]

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
            "rewardToken": campaign_data["rewardToken"].lower(),
            "rewardToken_symbol": "",
            "rewardToken_decimals": "",
            "rewards_perSecond": str(rewardsPerSec) if convert_bint else rewardsPerSec,
            "total_hypervisorToken_qtty": (
                str(total_hypervisorToken_qtty)
                if convert_bint
                else total_hypervisorToken_qtty
            ),
        }

    def isValid_reward_token(self, reward_address: str) -> bool:
        """Check if reward token address is a valid enabled address

        Args:
            reward_address (str):

        Returns:
            bool: invalid=False
        """
        # check if dummy
        if (
            reward_address.lower()
            in TOKEN_ADDRESS_EXCLUDE.get(text_to_chain(self._network), {}).keys()
        ):
            # return is not valid
            return False

        return True

    def isBlacklisted(self, reward_data: dict, hypervisor_address: str) -> bool:
        try:
            for address in reward_data["campaignData"]["blacklist"]:
                if address.lower() == hypervisor_address.lower():
                    return True
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Error while checking blacklisted address "
            )

        return False

    def _getRoundedEpoch(self, epoch: int, epoch_duration: int | None = None) -> int:
        """Rounds an `epoch` timestamp to the start of the corresponding period"""
        epoch_duration = epoch_duration or self.HOUR
        return (epoch / epoch_duration) * epoch_duration

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

        if hypervisors_pools:

            # create a list of pools
            _pools_list = [x[1] for x in hypervisors_pools]
            # get all active campaigns
            _active_campaigns = [x for x in self.get_active_campaigns()]

            # iterate over hypervisors pools
            for hypervisor, pool_address in hypervisors_pools:
                # get data
                for reward_data in _active_campaigns:
                    # check if pool is valid
                    if reward_data["campaignData"]["pool"].lower() not in _pools_list:
                        continue

                    # check if the token is valid
                    if not self.isValid_reward_token(
                        reward_data["rewardToken"].lower()
                    ):
                        continue

                    # check if the address has been blacklisted ( wrapperType -> 3)
                    if self.isBlacklisted(
                        reward_data=reward_data, hypervisor_address=hypervisor
                    ):
                        logging.getLogger(__name__).warning(
                            f" {hypervisor} is blacklisted for {reward_data['tokenSymbol']} angle Merkl rewards"
                        )
                        continue

                    result.append(
                        self.construct_reward_data(
                            campaign_data=reward_data,
                            hypervisor_address=hypervisor,
                            convert_bint=convert_bint,
                        )
                    )

        else:
            # TODO: get all hypervisors data ... by pid
            raise NotImplementedError("Not implemented yet")

        return result

    def get_reward_calculations(
        self, campaign: dict, _epoch_duration: int | None = None
    ) -> dict:
        """extracts reward paste info from campaign raw data

        Args:
            campaign (dict): dict
            _epoch_duration (int | None, optional): supply to avoid innecesary RPC calls. Defaults to None.

        Returns:
            dict: {
                "reward_x_epoch": ,
                "reward_x_second": ,
                "reward_yearly": ,
                "reward_yearly_token0": ,
                "reward_yearly_token1": ,
                "reward_yearly_fees": ,

                "reward_x_epoch_decimal": ,
                "reward_x_second_decimal": ,
                "reward_yearly_decimal": ,
                "reward_yearly_token0_decimal": ,
                "reward_yearly_token1_decimal": ,
                "reward_yearly_fees_decimal": ,
                }
        """

        if not _epoch_duration:
            _epoch_duration = self.HOUR

        reward_x_second = campaign["amount"] / campaign["duration"]
        reward_x_second_decimal = reward_x_second / (
            10 ** campaign["rewardToken_decimals"]
        )

        epochs = campaign["duration"] // _epoch_duration

        reward_x_epoch = campaign["amount"] / epochs
        reward_x_epoch_decimal = reward_x_epoch / (
            10 ** campaign["rewardToken_decimals"]
        )

        reward_yearly_decimal = reward_x_second_decimal * 3600 * 24 * 365
        reward_yearly = reward_x_second * 3600 * 24 * 365

        reward_yearly_token0_decimal = (
            campaign["campaignData"]["propToken0"] / 10000
        ) * reward_yearly_decimal
        reward_yearly_token0 = (
            campaign["campaignData"]["propToken0"] / 10000
        ) * reward_yearly
        reward_yearly_token1_decimal = (
            campaign["campaignData"]["propToken1"] / 10000
        ) * reward_yearly_decimal
        reward_yearly_token1 = (
            campaign["campaignData"]["propToken1"] / 10000
        ) * reward_yearly

        reward_yearly_fees_decimal = (
            campaign["campaignData"]["propFees"] / 10000
        ) * reward_yearly_decimal
        reward_yearly_fees = (
            campaign["campaignData"]["propFees"] / 10000
        ) * reward_yearly

        return {
            "reward_x_epoch": reward_x_epoch,
            "reward_x_second": reward_x_second,
            "reward_yearly": reward_yearly,
            "reward_yearly_token0": reward_yearly_token0,
            "reward_yearly_token1": reward_yearly_token1,
            "reward_yearly_fees": reward_yearly_fees,
            #
            "reward_x_epoch_decimal": reward_x_epoch_decimal,
            "reward_x_second_decimal": reward_x_second_decimal,
            "reward_yearly_decimal": reward_yearly_decimal,
            "reward_yearly_token0_decimal": reward_yearly_token0_decimal,
            "reward_yearly_token1_decimal": reward_yearly_token1_decimal,
            "reward_yearly_fees_decimal": reward_yearly_fees_decimal,
        }

    def balanceOf_tokens(
        self,
        wallet_address: str,
        token_addresses: list[str],
        block: int | None = None,
        timestamp: int | None = None,
        max_calls_atOnce: int = 100,
    ) -> dict[str, int]:
        """Get balances of tokens

        Args:
            token_addresses (list[str]): list of token addresses

        Returns:
            dict[str, int]: dict with token address as key and balance as value
        """

        if not block:
            block = self.block
        if not timestamp:
            timestamp = self._timestamp

        _calls = [
            build_call_with_abi_part(
                abi_part={
                    "constant": True,
                    "inputs": [{"name": "_owner", "type": "address"}],
                    "name": "balanceOf",
                    "outputs": [{"name": "balance", "type": "uint256"}],
                    "payable": False,
                    "stateMutability": "view",
                    "type": "function",
                },
                inputs_values=[Web3.toChecksumAddress(wallet_address)],
                address=token_address,
                object="balanceOf",
            )
            for token_address in token_addresses
        ]

        logging.getLogger(__name__).debug(
            f" {self._network}: {len(_calls)} fn calls will be executed in multicall to get the balance of {wallet_address} for {len(token_addresses)} tokens."
        )
        max_calls_atOnce = min(max_calls_atOnce, len(_calls))
        result = {}
        # execute multicall in batches
        for i in range(0, len(_calls), max_calls_atOnce):
            # get multicall data
            _tmp_multicall_data = execute_parse_calls(
                network=self._network,
                block=self.block,
                calls=_calls[i : i + max_calls_atOnce],
                convert_bint=False,
                requireSuccess=False,
                timestamp=self._timestamp,
            )
            # convert and add temporary data to multicall_result
            for itm in _tmp_multicall_data:
                try:
                    result[itm["address"].lower()] = itm["outputs"][0]["value"]
                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f"Error while parsing multicall data: {e}"
                    )

        return result
