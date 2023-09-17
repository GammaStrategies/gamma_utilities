import time

from dataclasses import dataclass

from bins.database.common.database_ids import create_id_operation, create_id_queue
from bins.general.enums import queueItemType


@dataclass
class QueueItem:
    type: queueItemType
    block: int
    address: str
    data: dict
    processing: float = 0  # timestamp
    id: str | None = None
    creation: float = 0
    _id: str | None = None  # db only
    count: int = 0

    def __post_init__(self):
        # setup id
        self._setup_id()

        # add creation time when object is created for the first time (not when it is loaded from database)
        if self.creation == 0:
            self.creation = time.time()
            # self.count = 0 # not needed because it is set to 0 by default
        else:
            # add a counter to avoid infinite info gathering loops on errors
            self.count += 1

    def _setup_id(self):
        # setup id
        if self.type == queueItemType.REWARD_STATUS:
            # reward status should have rewardToken as id
            if "reward_static" in self.data:
                # TODO: change for a combination of queue id + reward id
                self.id = create_id_queue(
                    type=self.type,
                    block=self.block,
                    hypervisor_address=self.data["reward_static"]["hypervisor_address"],
                    rewarder_address=self.data["reward_static"]["rewarder_address"],
                    rewardToken_address=self.data["reward_static"]["rewardToken"],
                )
            else:
                raise ValueError(
                    f" {self.data} is missing reward_static. using id: {self.id}"
                )
                # self.id = create_id_queue(
                #     type=self.type,
                #     block=self.block,
                #     hypervisor_address=self.data['hypervisor_status']['address'],
                #     )
                # self.id = f"{self.type}_{self.block}_{self.address}_{self.data['hypervisor_status']['address']}"
                # logging.getLogger(__name__).error(
                #     f" {self.data} is missing reward_static. using id: {self.id}"
                # )

        elif self.type == queueItemType.OPERATION:
            # create the basic id
            self.id = create_id_queue(
                type=self.type, block=self.block, hypervisor_address=self.address
            )
            # add operation id
            if "logIndex" in self.data and "transactionHash" in self.data:
                self.id = f"{self.id}_{create_id_operation(logIndex=self.data['logIndex'], transactionHash=self.data['transactionHash'])}"

        elif self.type == queueItemType.LATEST_MULTIFEEDISTRIBUTION:
            # create the basic id
            self.id = create_id_queue(
                type=self.type, block=self.block, hypervisor_address=self.address
            )
            # add operation id
            if "logIndex" in self.data and "transactionHash" in self.data:
                self.id = f"{self.id}_{create_id_operation(logIndex=self.data['logIndex'], transactionHash=self.data['transactionHash'])}"

        else:
            self.id = create_id_queue(
                type=self.type, block=self.block, hypervisor_address=self.address
            )

    @property
    def as_dict(self) -> dict:
        return {
            "type": self.type,
            "block": self.block,
            "address": self.address,
            "processing": self.processing,
            "data": self.data,
            "id": self.id,
            "creation": self.creation,
            "count": self.count,
        }

    @property
    def can_be_processed(self) -> bool:
        """Check if sufficient time has passed since creation

        Returns:
            bool:
        """

        # disable this check
        return True

        if self.count > 20:
            # this item needs to be manually checked
            return False
        elif self.count > 10:
            # 1 day + 2^count seconds
            min_seconds_passed = 60 * 60 * 24
            time_passed = time.time() - self.creation
            tmp_sec_to_pass = 2 ** (self.count) + min_seconds_passed
            return time_passed > tmp_sec_to_pass
        elif self.count > 5:
            time_passed = time.time() - self.creation
            # 1 hour
            return time_passed > 60 * 60

        return True
