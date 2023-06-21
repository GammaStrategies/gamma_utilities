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
