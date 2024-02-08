from dataclasses import dataclass


@dataclass
class multifeeDistribution_snapshot:
    id: str = None
    block: int = None
    timestamp: int = None
    # multiFeeDistribution address
    address: str = None
    dex: str = None
    hypervisor_address: str = None
    hypervisor_share_price_usd: float = None
    hypervisor_staked: int = None

    rewardToken: str = None
    rewardToken_symbol: str = None
    rewardToken_decimals: int = None
    rewardToken_price: float = None

    apr: float = None
    apr_baseRewards: float = None
    apr_boostedRewards: float = None

    baseRewards_sinceLastUpdateTime: int = None
    boostedRewards_sinceLastUpdateTime: int = None
    seconds_sinceLastUpdateTime: int = None

    def as_dict(self):
        """convert to

        Returns:
            _type_: _description_
        """
        # TODO: replace manual return with gettattr + check or similar
        result = {}

        if self.id:
            result["id"] = self.id
        else:
            raise ValueError(f" multiFeeDistribution snapshot should have an id")

        if self.block:
            result["block"] = self.block
        if self.timestamp:
            result["timestamp"] = self.timestamp
        if self.address:
            result["address"] = self.address
        if self.dex:
            result["dex"] = self.dex
        if self.hypervisor_address:
            result["hypervisor_address"] = self.hypervisor_address
        if self.hypervisor_share_price_usd:
            result["hypervisor_share_price_usd"] = self.hypervisor_share_price_usd
        if self.hypervisor_staked:
            result["hypervisor_staked"] = self.hypervisor_staked
        if self.rewardToken:
            result["rewardToken"] = self.rewardToken
        if self.rewardToken_symbol:
            result["rewardToken_symbol"] = self.rewardToken_symbol
        if self.rewardToken_decimals:
            result["rewardToken_decimals"] = self.rewardToken_decimals
        if self.rewardToken_price:
            result["rewardToken_price"] = self.rewardToken_price
        if self.apr != None:
            result["apr"] = self.apr

        if self.apr_baseRewards is not None:
            result["apr_baseRewards"] = self.apr_baseRewards
        if self.apr_boostedRewards is not None:
            result["apr_boostedRewards"] = self.apr_boostedRewards
        if self.baseRewards_sinceLastUpdateTime is not None:
            result["baseRewards_sinceLastUpdateTime"] = (
                self.baseRewards_sinceLastUpdateTime
            )
        if self.boostedRewards_sinceLastUpdateTime is not None:
            result["boostedRewards_sinceLastUpdateTime"] = (
                self.boostedRewards_sinceLastUpdateTime
            )
        if self.seconds_sinceLastUpdateTime is not None:
            result["seconds_sinceLastUpdateTime"] = self.seconds_sinceLastUpdateTime

        return result
