import time

from dataclasses import dataclass
from bins.configuration import CONFIGURATION

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

        # make sure block is an int
        self.block = int(self.block)

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


### Helpers ###


def create_priority_queueItemType() -> list[list[queueItemType]]:
    """This is just all queue items all time in order of priority
        It will process any queueitem as FIFO ( first in first out ). This can have situations were only 1 itemType is processed for a long time, while other items are waiting

    Returns:
        list[list[queueItemType]]:
    """
    # create an ordered list of queue item types
    queue_items_list = CONFIGURATION["_custom_"]["cml_parameters"].queue_types or list(
        queueItemType
    )
    # order by priority
    queue_items_list.sort(key=lambda x: x.order, reverse=False)

    # queue is processed in creation order:
    #   Include for each queue item type the types that need to be processed before it
    types_combination = {
        queueItemType.OPERATION: queue_items_list,
        queueItemType.BLOCK: queue_items_list,
        queueItemType.HYPERVISOR_STATUS: queue_items_list,
        # only do price when price
        queueItemType.PRICE: queue_items_list,
        queueItemType.LATEST_MULTIFEEDISTRIBUTION: queue_items_list,
        queueItemType.REWARD_STATUS: queue_items_list,
        queueItemType.HYPERVISOR_STATIC: queue_items_list,
        queueItemType.REWARD_STATIC: queue_items_list,
        queueItemType.REVENUE_OPERATION: queue_items_list,
    }

    # build a result
    result = []
    for queue_item in queue_items_list:
        if queue_item in types_combination:
            tmp_result = types_combination[queue_item]
            # tmp_result.append(queue_item)
            result.append(tmp_result)

    return result


def create_priority_queueItemType_latestOut() -> list[list[queueItemType]]:
    """Only process latest items when the item turn happens ( not on the other type¡s turn)
        It will process any queueitem as FIFO ( first in first out ) except for the 'latest_multifeedistributor' type, that will be processed once in a loop .
        While only 1 itemType can be processed for a long time, the 'latest_multifeedistributor' types are always processed, no mater what.

    Returns:
        list[list[queueItemType]]: _description_
    """
    # create an ordered list of queue item types
    queue_items_list = CONFIGURATION["_custom_"]["cml_parameters"].queue_types or list(
        queueItemType
    )
    # order by priority
    queue_items_list.sort(key=lambda x: x.order, reverse=False)

    # create a list without latest type
    queue_items_list_withoutLatest = queue_items_list.copy()
    if queueItemType.LATEST_MULTIFEEDISTRIBUTION in queue_items_list_withoutLatest:
        queue_items_list_withoutLatest.remove(queueItemType.LATEST_MULTIFEEDISTRIBUTION)

    # queue is processed in creation order:
    #   Include for each queue item type the types that need to be processed before it
    types_combination = {
        queueItemType.OPERATION: queue_items_list_withoutLatest,
        queueItemType.BLOCK: queue_items_list_withoutLatest,
        queueItemType.HYPERVISOR_STATUS: queue_items_list_withoutLatest,
        queueItemType.PRICE: queue_items_list_withoutLatest,
        queueItemType.LATEST_MULTIFEEDISTRIBUTION: queue_items_list,
        queueItemType.REWARD_STATUS: queue_items_list,
        queueItemType.HYPERVISOR_STATIC: queue_items_list,
        queueItemType.REWARD_STATIC: queue_items_list,
        queueItemType.REVENUE_OPERATION: queue_items_list,
    }

    # build a result
    result = []
    for queue_item in queue_items_list:
        if queue_item in types_combination:
            tmp_result = types_combination[queue_item]
            # tmp_result.append(queue_item)
            result.append(tmp_result)

    return result


def create_priority_queueItemType_inSequence() -> list[list[queueItemType]]:
    """Will process one type item at a time ( loop)
        This is a FIFO processing but for each type, not for all types at the same time.

    Returns:
        list[list[queueItemType]]:
    """
    # create an ordered list of queue item types
    queue_items_list = CONFIGURATION["_custom_"]["cml_parameters"].queue_types or list(
        queueItemType
    )
    # order by priority
    queue_items_list.sort(key=lambda x: x.order, reverse=False)

    # queue is processed in creation order:
    #   Include for each queue item type the types that need to be processed before it
    types_combination = {
        queueItemType.OPERATION: [],
        queueItemType.BLOCK: [],
        queueItemType.HYPERVISOR_STATUS: [],
        queueItemType.PRICE: [],
        queueItemType.LATEST_MULTIFEEDISTRIBUTION: [],
        queueItemType.REWARD_STATUS: [],
        queueItemType.HYPERVISOR_STATIC: [],
        queueItemType.REWARD_STATIC: [],
        queueItemType.REVENUE_OPERATION: [],
    }

    # build a result
    result = []
    for queue_item in queue_items_list:
        if queue_item in types_combination:
            tmp_result = types_combination[queue_item]
            tmp_result.append(queue_item)
            result.append(tmp_result)

    return result


def create_priority_queueItemType_customOrder() -> list[list[queueItemType]]:
    """Custom


    Returns:
        list[list[queueItemType]]:
    """
    # create an ordered list of queue item types
    queue_items_list = CONFIGURATION["_custom_"]["cml_parameters"].queue_types or list(
        queueItemType
    )
    # order by priority
    queue_items_list.sort(key=lambda x: x.order, reverse=False)

    # queue is processed in creation order:
    #   Include for each queue item type the types that need to be processed before it
    types_combination = {
        queueItemType.OPERATION: [],
        queueItemType.BLOCK: [],
        queueItemType.HYPERVISOR_STATUS: [
            queueItemType.OPERATION,
            queueItemType.BLOCK,
            queueItemType.HYPERVISOR_STATIC,
            queueItemType.PRICE,
        ],
        queueItemType.PRICE: [
            queueItemType.OPERATION,
            queueItemType.BLOCK,
        ],
        queueItemType.LATEST_MULTIFEEDISTRIBUTION: [],
        queueItemType.REWARD_STATUS: [
            queueItemType.BLOCK,
            queueItemType.PRICE,
            queueItemType.HYPERVISOR_STATUS,
            queueItemType.REWARD_STATIC,
        ],
        queueItemType.HYPERVISOR_STATIC: [],
        queueItemType.REWARD_STATIC: [queueItemType.HYPERVISOR_STATIC],
        queueItemType.REVENUE_OPERATION: [
            queueItemType.BLOCK,
            queueItemType.HYPERVISOR_STATIC,
            queueItemType.PRICE,
        ],
    }

    # build a result
    result = []
    for queue_item in queue_items_list:
        if queue_item in types_combination:
            tmp_result = types_combination[queue_item]
            tmp_result.append(queue_item)
            result.append(tmp_result)

    return result


class queue_item_selector:
    def __init__(
        self,
        queue_items_list: list[list[queueItemType]] | None = None,
        find: dict | None = None,
        sort: list[tuple[str, int]] | None = None,
    ):
        # initialize queue items list
        self.init_queue_items_list(queue_items_list)
        # save parameters
        self._find = find
        self._sort = sort

    def init_queue_items_list(
        self, queue_items_list: list[list[queueItemType]] | None = None
    ):
        if not queue_items_list:
            self.queue_items_list = (
                create_priority_queueItemType_latestOut()
            )  # create_priority_queueItemType()
        self._current_queue_item_index = 0

    @property
    def current_queue_item_types(self) -> list[queueItemType]:
        return self.queue_items_list[self._current_queue_item_index]

    @property
    def find(self) -> dict:
        return self._find

    @property
    def sort(self) -> list[tuple[str, int]]:
        return self._sort

    def next(self):
        if self._current_queue_item_index + 1 < len(self.queue_items_list):
            self._current_queue_item_index += 1
        else:
            self._current_queue_item_index = 0


def create_selector_per_network(
    queue_items_list: list[list[queueItemType]] | None = None,
    find: dict | None = None,
    sort: list | None = None,
) -> dict[str, dict[str, queue_item_selector]]:
    result = {}

    for protocol in CONFIGURATION["script"]["protocols"]:
        if not protocol in result:
            result[protocol] = {}

        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        for network in networks:
            # create index
            result[protocol][network] = queue_item_selector(
                queue_items_list=queue_items_list, find=find, sort=sort
            )

    return result
