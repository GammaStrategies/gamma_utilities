from dataclasses import dataclass


@dataclass
class multifeeDistribution_snapshot:
    id: str = None
    block: int = None
    timestamp: int = None
    address: str = None
    dex: str = None
    hypervisor_address: str = None
    hypervisor_price_x_share: float = None
    rewardToken: str = None
    rewardToken_symbol: str = None
    rewardToken_decimals: int = None
    rewardToken_balance: int = None
    rewardToken_price: float = None
    rewardData: dict = None
    topic: str = None
    total_staked: int = None
    current_period_rewards: dict = None
    last_period_rewards: dict = None
    last_updated_data: dict = None

    def as_dict(self):
        """_summary_

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
        if self.hypervisor_price_x_share:
            result["hypervisor_price_x_share"] = self.hypervisor_price_x_share
        if self.rewardToken:
            result["rewardToken"] = self.rewardToken
        if self.rewardToken_symbol:
            result["rewardToken_symbol"] = self.rewardToken_symbol
        if self.rewardToken_decimals:
            result["rewardToken_decimals"] = self.rewardToken_decimals
        if self.rewardToken_balance:
            result["rewardToken_balance"] = self.rewardToken_balance
        if self.rewardToken_price:
            result["rewardToken_price"] = self.rewardToken_price
        if self.rewardData:
            result["rewardData"] = self.rewardData
        if self.topic:
            result["topic"] = self.topic
        if self.total_staked:
            result["total_staked"] = self.total_staked
        if self.current_period_rewards:
            result["current_period_rewards"] = self.current_period_rewards
        if self.last_period_rewards:
            result["last_period_rewards"] = self.last_period_rewards
        if self.last_updated_data:
            result["last_updated_data"] = self.last_updated_data
        return result


@dataclass
class multifeeDistribution_snapshot_2:
    id: str = None
    block: int = None
    timestamp: int = None
    address: str = None
    dex: str = None
    hypervisor_address: str = None
    hypervisor_price_x_share: float = None
    rewardToken: str = None
    rewardToken_symbol: str = None
    rewardToken_decimals: int = None
    rewardToken_balance: int = None
    rewardToken_price: float = None
    rewardData: dict = None
    topic: str = None
    total_staked: int = None
    current_period_rewards: dict = None
    last_period_rewards: dict = None
    last_updated_data: dict = None

    def as_dict(self):
        """_summary_

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
        if self.hypervisor_price_x_share:
            result["hypervisor_price_x_share"] = self.hypervisor_price_x_share
        if self.rewardToken:
            result["rewardToken"] = self.rewardToken
        if self.rewardToken_symbol:
            result["rewardToken_symbol"] = self.rewardToken_symbol
        if self.rewardToken_decimals:
            result["rewardToken_decimals"] = self.rewardToken_decimals
        if self.rewardToken_balance:
            result["rewardToken_balance"] = self.rewardToken_balance
        if self.rewardToken_price:
            result["rewardToken_price"] = self.rewardToken_price
        if self.rewardData:
            result["rewardData"] = self.rewardData
        if self.topic:
            result["topic"] = self.topic
        if self.total_staked:
            result["total_staked"] = self.total_staked
        if self.current_period_rewards:
            result["current_period_rewards"] = self.current_period_rewards
        if self.last_period_rewards:
            result["last_period_rewards"] = self.last_period_rewards
        if self.last_updated_data:
            result["last_updated_data"] = self.last_updated_data
        return result
