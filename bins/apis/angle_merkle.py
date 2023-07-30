from ..general.enums import Chain
from ..general.net_utilities import get_request


# TODO: for static    rewarder Address, refIDs, rewarder_registry, rewarder_type, rewards_perSecond,...
# TODO: for status    ...


class angle_merkle_wraper:
    """Wrapper for the angle merkle api   https://api.angle.money/api-docs/"""

    def __init__(self):
        self.discard_token_address_list = {
            Chain.ARBITRUM: ["0xE0688A2FE90d0f93F17f273235031062a210d691".lower()],
        }

    def get_epochs(self, chain: Chain) -> list[dict]:
        """when updates took place ( epochs and timestamps )

        Args:
            chain (Chain):

        Returns:
            list: [{"epoch":467578,"timestamp":1683283164 }, ...]
        """
        # create url
        url = f"{self.build_main_url(chain)}/updates.json"

        return get_request(url=url)

    def get_rewards(self, chain: Chain, epoch: int | None = None) -> dict:
        """accumulated rewards for each address from ini to epoch (amounts given are cumulative)
            ( state of the rewards.json at a given epoch)
        Args:
            chain (Chain):
            epoch (int | None, optional): . Defaults to None.

        Returns:
            dict: {
                "lastUpdateEpoch": int,
                "updateTimestamp": int,
                "updateTxBlockNumber": int,
                "rewards": {
                    <address>: {
                }
            }
        """

        # create url
        url = (
            f"{self.build_main_url(chain)}/backup/rewards_{epoch}.json"
            if epoch
            else f"{self.build_main_url(chain)}/rewards.json"
        )

        return get_request(url=url)

    def build_main_url(self, chain: Chain):
        return f"https://angleprotocol.github.io/merkl-rewards/{chain.id}"

    def get_gamma_rewards(self, chain: Chain, epoch: int | None = None) -> dict:
        # prepare result struct
        result = {}

        # get rewards for epoch
        if rewards_data := self.get_rewards(chain=chain, epoch=epoch):
            # lastUpdateEpoch = rewards_data["lastUpdateEpoch"] # diff exists btween this and pool's lastUpdateEpoch
            updateTimestamp = rewards_data["updateTimestamp"]
            for reward_id, reward_data in rewards_data["rewards"].items():
                # discard tokens in list
                if reward_data["token"].lower() in self.discard_token_address_list.get(
                    chain, []
                ):
                    continue
                # lower case pool address
                pool = reward_data["pool"].lower()

                for holder_address, amount_data in reward_data["holders"].items():
                    # all addresses to lower case
                    holder_address = holder_address.lower()

                    if gamma_amount := amount_data["breakdown"].get("Gamma", 0):
                        # add to result
                        if pool not in result:
                            result[pool] = {
                                "amount": 0,
                                "rewardId": reward_id,
                                "boostedAddress": reward_data["boostedAddress"].lower(),
                                "boostedReward": reward_data["boostedReward"],
                                "token": reward_data["token"].lower(),
                                "tokenDecimals": reward_data["tokenDecimals"],
                                "tokenSymbol": reward_data["tokenSymbol"],
                                "vsTotalAmount": 0,
                                "lastUpdateEpoch": reward_data["lastUpdateEpoch"],
                                "updateTimestamp": updateTimestamp,
                                "users": {},
                            }
                        result[pool]["amount"] += int(gamma_amount)
                        if holder_address not in result[pool]["users"]:
                            result[pool]["users"][holder_address] = {"amount": 0}
                        result[pool]["users"][holder_address]["amount"] += int(
                            gamma_amount
                        )

                        # calculate vsTotalAmount
                        result[pool]["vsTotalAmount"] = result[pool]["amount"] / int(
                            reward_data["totalAmount"]
                        )

        return result

    def get_angle_computed_apr(self, chain: Chain) -> dict:
        """get APR data sourced directly from Angle protocol

        Args:
            chain (Chain):

        Returns:
            dict:
                0xFf5713FdbAD797b81539b5F9766859d4E050a6CC
                    almDetails
                        0	{…}
                        1	{…}
                        2	{…}
                        3
                            address	"0x54110794464cda7c7cc30bfe047ebe4038874812"
                            almLiquidity	0.010626968917940165
                            label	"Gamma 0x54110794464cda7c7cc30bfe047ebe4038874812"
                            origin	2
                            poolBalance0	0.15697740535490154
                            poolBalance1	0.000061113368809844
                    aprs
                        Average APR (rewards / pool TVL)	121.04090552200174
                        SUSHI APR (rewards for SUSHI / SUSHI TVL)	33.45404519112058
                        WETH APR (rewards for WETH / WETH TVL)	87.59176828314386
                        Steer 0xaebbf70504c4d07be4a28cb37e6a1c9401ef00f9	68.57504966218352
                        Gamma 0x566bfd3ad5f6ac8445411dcae5730253d9fede51	54.781002605135
                        Gamma 0xa52ecc4ed16f97c71071a3bd14309e846647d7f0	66.59123309007497
                        Gamma 0x54110794464cda7c7cc30bfe047ebe4038874812	63.29434958057311
                    chainId	137
                    decimalToken0	18
                    decimalToken1	18
                    distributionData
                    0
                        amm	1
                        amount	97
                        breakdown	{}
                        end	1685534400
                        id	"0xb3fc2abc303c70a16ab9d5…7a3d971b024dd34b97e94b1"
                        isBoosted	false
                        isLive	false
                        isMock	false
                        isOutOfRangeIncentivized	false
                        propFees	10
                        propToken0	45
                        propToken1	45
                        start	1684324800
                        token	"0x0b3F868E0BE5597D5DB7fEB59E1CADBb0fdDa50a"
                        tokenSymbol	"SUSHI"
                        unclaimed	0
                        wrappers
                            0	2
                            1	2
                            2	2
                            3	6
                    1	{…}
                    2	{…}
                    3	{…}
                    4	{…}
                    5	{…}
                    6	{…}
                    7	{…}
                    8	{…}
                    9	{…}
                    10	{…}
                    liquidity	27571.184748392985
                    meanAPR	121.04090552200174
                    pool	"0xFf5713FdbAD797b81539b5F9766859d4E050a6CC"
                    poolFee	0.3
                    rewardsPerToken	{}
                    token0	"0x0b3F868E0BE5597D5DB7fEB59E1CADBb0fdDa50a"
                    token0InPool	13150.447872776927
                    token1	"0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619"
                    token1InPool	1.7861113069716428
                    tokenSymbol0	"SUSHI"
                    tokenSymbol1	"WETH"
                    tvl	12963.594190903937
                    userTVL	0
                    userTotalBalance0	0
                    userTotalBalance1	0
        """

        url = f"https://api.angle.money/v1/merkl?chainId={chain.id}"
        return get_request(url=url)

    def get_global_angle_computed_apr(self) -> dict:
        """_summary_

        Returns:
            dict:
                {
                "veANGLE": {
                    "details": {
                        "interests": 0
                    },
                    "value": 0
                },
                "Sushi agEUR/ANGLE LP": {
                    "details": {
                        "min": 10.111399806118301,
                        "max": 25.278499515295753,
                        "fees": 0.35849527649597035
                    },
                    "value": 10.469895082614272,
                    "address": "0xBa625B318483516F7483DD2c4706aC92d44dBB2B"
                },
                "Uni-V3 agEUR/ETH LP": {
                    "details": {
                        "Average APR (rewards / pool TVL)": 32.42676911929763,
                        "(ve)Boosted Average APR": 81.06692279824408,
                        "agEUR APR (rewards for agEUR / agEUR TVL)": 20.983585853035084,
                        "WETH APR (rewards for WETH / WETH TVL)": 16.983407867098425,
                        "Average Arrakis APR": 22.975428754383387,
                        "Average Gamma APR": 21.875379219871817
                    },
                    "value": 81.06692279824408,
                    "address": "0x3785Ce82be62a342052b9E5431e9D3a839cfB581"
                },
                "Uni-V3 agEUR/USDC LP": {
                    "details": {
                        "Average APR (rewards / pool TVL)": 14.979526523659002,
                        "(ve)Boosted Average APR": 37.44881630914751,
                        "agEUR APR (rewards for agEUR / agEUR TVL)": 14.513525687289759,
                        "USDC APR (rewards for USDC / USDC TVL)": 5.1023940837980275,
                        "Average Arrakis APR": 12.918269569922092,
                        "Average Gamma APR": 21.654448263327122,
                        "Average DefiEdge APR": 12.932200225608849
                    },
                    "value": 37.44881630914751,
                    "address": "0xEB7547a8a734b6fdDBB8Ce0C314a9E6485100a3C"
                },
                "Polygon Uni-V3 agEUR/USDC LP": {
                    "details": {
                        "Average APR (rewards / pool TVL)": 16.3867224434811,
                        "(ve)Boosted Average APR": 40.966806108702755,
                        "USDC APR (rewards for USDC / USDC TVL)": 5.629486608777804,
                        "agEUR APR (rewards for agEUR / agEUR TVL)": 15.687629377452058,
                        "Average Arrakis APR": 12.509956666960887,
                        "Average Gamma APR": 15.693977948284209,
                        "Average DefiEdge APR": 15.68762937745865
                    },
                    "value": 40.966806108702755,
                    "address": "0x4EA4C5ca64A3950E53c61d0616DAF92727119093"
                }
            }
        """
        return get_request(url="https://api.angle.money/v1/apr")
