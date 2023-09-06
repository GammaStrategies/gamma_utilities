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
        minimum time table:
            count	minutes	    hours       days
                1	5	        0.1
                2	10	        0.2
                3	16	        0.3
                4	22	        0.4
                5	32	        0.5
                6	52	        0.9
                7	106	        1.8
                8	274	        4.6
                9	819	        13.6        0.6
                10	2,603	    43.4        1.8
                11	8,479	    141.3       5.9
                12	27,858	    464.3       19.3
                13	91,799	    1,530.0     63.8
                14	302,792	    5,046.5     210.3
                15	999,057	    16,651.0    693.8
                16	3,296,722	54,945.4    2,289.4
                17	10,879,004	181,316.7   7,554.9
                18	35,900,521	598,342.0   24,931.8
                19	118,471,519	1,974,525.3 82,267.7
                20	390,955,798	6,515,930.0 271,497.9

        Returns:
            bool:
        """

        if self.count > 10:
            return False
        #     time_passed = time.time() - self.creation
        #     calculation = (self.count * 300) + (3.3**self.count)
        #     return self.count == 0 or time_passed >= calculation
        # else:
        return True
