from enum import Enum
from bins.general.enums import Chain
from bins.general.net_utilities import get_request


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
                {
                "pools": {
                    "0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9": {
                        "aprs": {
                            "Average APR (rewards / pool TVL)": 32.42676911929763,
                            "(ve)Boosted Average APR": 81.06692279824408,
                            "agEUR APR (rewards for agEUR / agEUR TVL)": 20.983585853035084,
                            "WETH APR (rewards for WETH / WETH TVL)": 16.983407867098425,
                            "Average Arrakis APR": 22.975428754383387,
                            "Average Gamma APR": 21.875379219871817
                            },
                        "chainId": 1,
                        "decimalToken0": 18,
                        "decimalToken1": 18,
                        "distributionData": [
                            {
                                "amm": 0,
                                "amount": 423058.39257920283,
                                "breakdown": {},
                                "end": 1686182400,
                                "isBoosted": true,
                                "isLive": false,
                                "isMock": false,
                                "isOutOfRangeIncentivized": false,
                                "propFees": 40,
                                "propToken0": 40,
                                "propToken1": 20,
                                "start": 1685577600,
                                "token": "0x31429d1856aD1377A8A0079410B297e1a9e214c2",
                                "tokenSymbol": "ANGLE",
                                "unclaimed": 0,
                                "wrappers": [
                                    0,
                                    2
                                ]
                            },
                            {
                                "amm": 0,
                                "amount": 419923.2944062928,
                                "breakdown": {},
                                "end": 1686787200,
                                "isBoosted": true,
                                "isLive": false,
                                "isMock": false,
                                "isOutOfRangeIncentivized": false,
                                "propFees": 40,
                                "propToken0": 40,
                                "propToken1": 20,
                                "start": 1686182400,
                                "token": "0x31429d1856aD1377A8A0079410B297e1a9e214c2",
                                "tokenSymbol": "ANGLE",
                                "unclaimed": 0,
                                "wrappers": [
                                    0,
                                    2
                                ]
                            },
                            {
                                "amm": 0,
                                "amount": 246210.2636292404,
                                "breakdown": {},
                                "end": 1687392000,
                                "isBoosted": true,
                                "isLive": false,
                                "isMock": false,
                                "isOutOfRangeIncentivized": false,
                                "propFees": 40,
                                "propToken0": 40,
                                "propToken1": 20,
                                "start": 1686787200,
                                "token": "0x31429d1856aD1377A8A0079410B297e1a9e214c2",
                                "tokenSymbol": "ANGLE",
                                "unclaimed": 0,
                                "wrappers": [
                                    0,
                                    2
                                ]
                            },
                            {
                                "amm": 0,
                                "amount": 243975.27074993515,
                                "breakdown": {},
                                "end": 1687996800,
                                "isBoosted": true,
                                "isLive": false,
                                "isMock": false,
                                "isOutOfRangeIncentivized": false,
                                "propFees": 40,
                                "propToken0": 40,
                                "propToken1": 20,
                                "start": 1687392000,
                                "token": "0x31429d1856aD1377A8A0079410B297e1a9e214c2",
                                "tokenSymbol": "ANGLE",
                                "unclaimed": 0,
                                "wrappers": [
                                    0,
                                    2
                                ]
                            },
                            {
                                "amm": 0,
                                "amount": 336642.11093298777,
                                "breakdown": {},
                                "end": 1688601600,
                                "isBoosted": true,
                                "isLive": false,
                                "isMock": false,
                                "isOutOfRangeIncentivized": false,
                                "propFees": 40,
                                "propToken0": 40,
                                "propToken1": 20,
                                "start": 1687996800,
                                "token": "0x31429d1856aD1377A8A0079410B297e1a9e214c2",
                                "tokenSymbol": "ANGLE",
                                "unclaimed": 0,
                                "wrappers": [
                                    0,
                                    2
                                ]
                            },
                            {
                                "amm": 0,
                                "amount": 330279.17457460955,
                                "breakdown": {},
                                "end": 1689206400,
                                "isBoosted": true,
                                "isLive": true,
                                "isMock": false,
                                "isOutOfRangeIncentivized": false,
                                "propFees": 40,
                                "propToken0": 40,
                                "propToken1": 20,
                                "start": 1688601600,
                                "token": "0x31429d1856aD1377A8A0079410B297e1a9e214c2",
                                "tokenSymbol": "ANGLE",
                                "unclaimed": 0,
                                "wrappers": [
                                    0,
                                    2
                                ]
                            }
                        ],
                        "liquidity": 105062.60605084439,
                        "meanAPR": 81.06692279824408,
                        "pool": "0x8dB1b906d47dFc1D84A87fc49bd0522e285b98b9",
                        "poolFee": 0.05,
                        "rewardsPerToken": {},
                        "token0": "0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8",
                        "token0InPool": 921063.6689873504,
                        "token1": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                        "token1InPool": 332.25038995374683,
                        "tokenSymbol0": "agEUR",
                        "tokenSymbol1": "WETH",
                        "tvl": 1639073.2879926525,
                        "userBalances": [
                            {
                                "balance0": 0,
                                "balance1": 0,
                                "origin": 0,
                                "tvl": 0
                            },
                            {
                                "balance0": 0,
                                "balance1": 0,
                                "origin": 2,
                                "tvl": 0
                            },
                            {
                                "balance0": 0,
                                "balance1": 0,
                                "origin": 2,
                                "tvl": 0
                            }
                        ],
                        "userTVL": 0,
                        "userTotalBalance0": 0,
                        "userTotalBalance1": 0
                    },
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
