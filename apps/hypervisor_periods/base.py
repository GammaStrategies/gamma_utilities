from datetime import datetime
import logging
from apps.feeds.operations import feed_operations_hypervisors
from apps.feeds.utils import get_hypervisors_data_for_apr
from bins.database.helpers import get_from_localdb
from bins.errors.general import ProcessingError
from bins.general.enums import Chain, Protocol, error_identity, text_to_protocol
from bins.w3.builders import build_hypervisor


# TODO: handle last on-the-fly hype prices


# Handling hypervisor periods ( op.block -> op.block-1 -> op.block ...)
class hypervisor_periods_base:
    # OVERRIDE FUNCTIONS
    def reset(self):
        # reset all vars
        self.result = []

    def _execute_preLoop(self, hypervisor_data: dict):
        """executed before the loop ( one time )

        Args:
            hypervisor_data (dict): _description_
        """
        pass

    def _execute_inLoop(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        """Right at the start of the loop

        Args:
            chain (Chain):
            hypervisor_address (str):
            data (dict):
            idx (int):
            last_item (dict):
            current_item (dict):
        """
        pass

    def _execute_inLoop_startItem(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        """executed when a new start hypervisor period is found
            Take into considaration rebalace consequences here:  [if this is an actual rebalance ( last item exists and it was an end item and supply changed)].
        Args:
            chain (Chain):
            hypervisor_address (str):
            data (dict):
            idx (int):
            last_item (dict):
            current_item (dict):
        """
        pass

    def _execute_inLoop_endItem(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        """executed when a new end hypervisor period is found

        Args:
            chain (Chain):
            hypervisor_address (str):
            data (dict):
            idx (int):
            last_item (dict):
            current_item (dict):
        """
        pass

    def _execute_postLoop(self, hypervisor_data: dict):
        """executed after the loop ( one time )

        Args:
            hypervisor_data (dict):
        """
        pass

    def _scrape_last_item(
        self,
        chain: Chain,
        hypervisor_address: str,
        block: int,
        protocol: Protocol,
        hypervisor_status: dict | None = None,
    ) -> dict:
        """Only executed when the last available item is an initial value and defined end block is greater than last available block"""
        # should meke sure supply doesn change
        # should have an "operations" key =[]
        # should have any specific field
        # pricing is a challenge when scraping new items
        pass

    # EXECUTE
    def execute_processes_within_hypervisor_periods(
        self,
        chain: Chain,
        hypervisor_address: str,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
        block_ini: int | None = None,
        block_end: int | None = None,
        try_solve_errors: bool = False,
        only_use_last_items: int | None = None,
    ) -> list:
        # check if any of block or timestamp has been supplied
        if (
            not timestamp_end
            and not block_ini
            and not only_use_last_items
            or not timestamp_end
            and not block_end
        ):
            raise ValueError("timestamps or blocks must be provided")

        # reset all variables
        self.reset()

        # get data from database
        if data_list := get_hypervisors_data_for_apr(
            network=chain.database_name,
            hypervisor_addresses=[hypervisor_address],
            timestamp_ini=timestamp_ini or 1400000000 if not block_ini else None,
            timestamp_end=timestamp_end,
            block_ini=block_ini,
            block_end=block_end,
            only_return_last_items=only_use_last_items,
        ):
            # loop for each hypervisor data found { _id: <hypervisor_address> ,  {status: <hypervisor status data + operations + rewards...>}
            for hype_data in data_list:
                # check data consistency
                if not "status" in hype_data:
                    raise ValueError(f"Invalid hypervisor data: {hype_data}")

                # check first item
                self._hypervisor_periods_first_item_checks(
                    chain=chain,
                    first_item=hype_data["status"][0],
                    timestamp_ini=timestamp_ini,
                    block_ini=block_ini,
                    try_solve_errors=try_solve_errors,
                    only_use_last_items=only_use_last_items,
                )

                ###### START #######
                # define loop working vars
                last_item = None
                last_item_type = None

                ##### EXECUTE PRE-LOOP FUNCTION
                self._execute_preLoop(hypervisor_data=hype_data)

                # log from to dates data found for this hypervisor
                try:
                    _i = datetime.fromtimestamp(hype_data["status"][0]["timestamp"])
                    _e = datetime.fromtimestamp(hype_data["status"][-1]["timestamp"])
                    logging.getLogger(__name__).debug(
                        f" total data found for {chain.database_name} {hype_data['status'][0]['address']} from {_i} [{hype_data['status'][0]['timestamp']}] to {_e} [{hype_data['status'][-1]['timestamp']}]  ({(_e - _i).total_seconds()/(60*60*24):,.2f} days)"
                    )
                except Exception:
                    pass
                # loop thu each hype status data ( hypervisor status found for that particular time period )
                for idx, status_data in enumerate(hype_data["status"]):
                    # execute the loop work
                    if returned_type := self._loop_work(
                        chain=chain,
                        idx=idx,
                        hype_data=hype_data,
                        hypervisor_address=hypervisor_address,
                        current_item=status_data,
                        last_item=last_item,
                        last_item_type=last_item_type,
                        try_solve_errors=try_solve_errors,
                    ):
                        last_item_type = returned_type

                    # set last item
                    last_item = status_data

                    # if this is the last idx and last_item == ini, then we can decide wether to scrape a new item using the last block/timestamp defined or not
                    if idx == len(hype_data["status"]) - 1 and last_item_type == "ini":
                        # we rarely need to scrape a new item, because normal operations narrow choices to items at operation blocks or -1 blocks...
                        # but if we scrape at current time periods and last known item is an initial value, then we need to scrape a new item,
                        # and prices ( being current prices )
                        if block_end and last_item["block"] < block_end:
                            # scrape block_end
                            if new_last_item := self._scrape_last_item(
                                chain=chain,
                                hypervisor_address=hypervisor_address,
                                block=block_end,
                                protocol=text_to_protocol(status_data["dex"]),
                                hypervisor_status=status_data,
                            ):
                                logging.getLogger(__name__).debug(
                                    f" {chain.database_name}'s {status_data['dex']} {hypervisor_address} creating the last item on-the-fly at block_end {block_end}"
                                )
                                # last loop work
                                self._loop_work(
                                    chain=chain,
                                    idx=idx + 1,
                                    hype_data=hype_data,
                                    hypervisor_address=hypervisor_address,
                                    current_item=new_last_item,
                                    last_item=last_item,
                                    last_item_type=last_item_type,
                                    try_solve_errors=try_solve_errors,
                                )
                            else:
                                # we should be creating a new item but the _scrape_last_item function did not return anything... probably bc its not implemented
                                # may be a problem when len(hype_data["status"]) == 1
                                if len(hype_data["status"]) == 1:
                                    logging.getLogger(__name__).warning(
                                        f" {chain.database_name}'s {status_data['dex']} {hypervisor_address} has only one item and _scrape_last_item did not return anything, so it has not enough data to calculate returns."
                                    )

                        elif timestamp_end and last_item["timestamp"] < timestamp_end:
                            # convert timestamp_end to block_end and scrape block_end
                            hypervisor = build_hypervisor(
                                network=chain.database_name,
                                protocol=text_to_protocol(status_data["dex"]),
                                block=0,
                                hypervisor_address=hypervisor_address,
                                cached=True,
                            )
                            if block := hypervisor.blockNumberFromTimestamp(
                                timestamp=timestamp_end
                            ):
                                # scrape block_end
                                if new_last_item := self._scrape_last_item(
                                    chain=chain,
                                    hypervisor_address=hypervisor_address,
                                    block=block,
                                    protocol=text_to_protocol(status_data["dex"]),
                                    hypervisor_status=status_data,
                                ):
                                    logging.getLogger(__name__).debug(
                                        f" {chain.database_name}'s {status_data['dex']} {hypervisor_address} creating the last item on-the-fly at block_end {block}"
                                    )
                                    # last loop work
                                    self._loop_work(
                                        chain=chain,
                                        idx=idx + 1,
                                        hype_data=hype_data,
                                        hypervisor_address=hypervisor_address,
                                        current_item=new_last_item,
                                        last_item=last_item,
                                        last_item_type=last_item_type,
                                        try_solve_errors=try_solve_errors,
                                    )
                                else:
                                    #
                                    if len(hype_data["status"]) == 1:
                                        logging.getLogger(__name__).warning(
                                            f" {chain.database_name}'s {status_data['dex']} {hypervisor_address} has only one item and _scrape_last_item did not return anything, so it has not enough data to calculate returns."
                                        )

                        else:
                            #   no need to scrape because last_item is the last item of the defined period, that happen to be a initial value
                            logging.getLogger(__name__).debug(
                                f" {chain.database_name} {status_data['address']} last index {idx} is an initial value, but also the last period value."
                            )

                ##### EXECUTE OUT-LOOP FUNCTION
                self._execute_postLoop(hypervisor_data=hype_data)

        else:
            logging.getLogger(__name__).error(
                f"   {chain.database_name} {hypervisor_address} has no data from {block_ini or (datetime.fromtimestamp(timestamp_ini) if timestamp_ini else None)} to {block_end or (datetime.fromtimestamp(timestamp_end) if timestamp_end else None)}"
            )

        return self.result

    def _loop_work(
        self,
        chain: Chain,
        idx: int,
        hype_data: dict,
        hypervisor_address: str,
        current_item: dict,
        last_item: dict,
        last_item_type: str,
        try_solve_errors: bool,
    ) -> str:
        ##### EXECUTE IN-LOOP FUNCTION
        self._execute_inLoop(
            chain=chain,
            hypervisor_address=hypervisor_address,
            data=current_item,
            idx=idx,
            last_item=last_item,
            current_item=current_item,
        )

        # define local vars for later use
        wanted_topics = ["deposit", "withdraw", "rebalance", "zeroBurn"]
        operation_types = [op["topic"] for op in current_item["operations"]]
        last_operation_types = (
            [op["topic"] for op in last_item["operations"]] if last_item else []
        )

        # hypervisors without operations are end values
        if not current_item["operations"]:
            # <<<<<<<<<<<< this is an end value <<<<<<<<<<<<<<<<<<<<<<<<<<

            # make sure last item was not an end value
            if last_item_type != "ini":
                # check if this is the first idx to discard it if it is
                if idx == 0:
                    logging.getLogger(__name__).debug(
                        f" {chain.database_name} {current_item['address']} index {idx} is an end value but has no operations, because its a -1 block. Discarding it safely."
                    )
                    return None

                raise ValueError(
                    f" {chain.database_name} {current_item['address']} index {idx} is an end value but last item was considered end also. Last item block: {last_item['block']} current block: {current_item['block']} last ops: {last_operation_types} current ops: {operation_types}"
                )

            # define last item type
            last_item_type = "end"

            # check totalSupply has not changed
            self._hypervisors_periods_checkTotalSupplyChanged(
                chain=chain,
                hypervisor_address=hypervisor_address,
                data=hype_data,
                idx=idx,
                last_item=last_item,
                current_item=current_item,
                try_solve_errors=try_solve_errors,
            )

            # check fee Growth
            self._hypervisors_periods_checkfeeGrowths(
                chain=chain,
                hypervisor_address=hypervisor_address,
                data=hype_data,
                idx=idx,
                last_item=last_item,
                current_item=current_item,
                try_solve_errors=try_solve_errors,
            )

            ##### EXECUTE IN-LOOP END ITEM FUNCTION
            self._execute_inLoop_endItem(
                chain=chain,
                hypervisor_address=hypervisor_address,
                data=hype_data,
                idx=idx,
                last_item=last_item,
                current_item=current_item,
            )

        elif (
            not any([x in wanted_topics for x in operation_types])
            and last_item
            and last_item["totalSupply"] == current_item["totalSupply"]
        ):
            # <<<<<<<<<<<< this is an end value <<<<<<<<<<<<<<<<<<<<<<<<<<
            # because the initial query discards topics outside wanted topics, this is a correct end value.

            # make sure last item was not an end value
            if last_item_type != "ini":
                raise ValueError(
                    f"  {chain.database_name} {current_item['address']} index {idx} is an end value but last item was considered end also. Last item block: {last_item['block']} current block: {current_item['block']} last ops: {last_operation_types} current ops: {operation_types}"
                )
            # define last item type
            last_item_type = "end"

            # check totalSupply has not changed
            self._hypervisors_periods_checkTotalSupplyChanged(
                chain=chain,
                hypervisor_address=hypervisor_address,
                data=hype_data,
                idx=idx,
                last_item=last_item,
                current_item=current_item,
                try_solve_errors=try_solve_errors,
            )
            # check fee Growth
            self._hypervisors_periods_checkfeeGrowths(
                chain=chain,
                hypervisor_address=hypervisor_address,
                data=hype_data,
                idx=idx,
                last_item=last_item,
                current_item=current_item,
                try_solve_errors=try_solve_errors,
            )

            # execute process function
            ##### EXECUTE IN-LOOP END ITEM FUNCTION
            self._execute_inLoop_endItem(
                chain=chain,
                hypervisor_address=hypervisor_address,
                data=hype_data,
                idx=idx,
                last_item=last_item,
                current_item=current_item,
            )

        else:
            # <<<<<<<<<<<< this is an initial value <<<<<<<<<<<<<<<<<<<<<<<<<<
            # it has operations
            if last_item_type and last_item_type != "end":
                # check if they are consecutive blocks
                if last_item["block"] + 1 == current_item["block"]:
                    logging.getLogger(__name__).debug(
                        f" {chain.database_name} {current_item['address']} index {idx} is an initial value but last item was considered initial also. Blocks are consecutive (noproblm). Last item block: {last_item['block']} current block: {current_item['block']}  last ops: {last_operation_types} current ops: {operation_types}"
                    )
                else:
                    # raise ProcessingError to rescrape operations between blocks
                    raise ProcessingError(
                        chain=chain,
                        item={
                            "hypervisor_address": hypervisor_address,
                            "dex": current_item["dex"],
                            "ini_block": last_item["block"],
                            "end_block": current_item["block"],
                            "ini_operations": operation_types,
                            "end_operations": last_operation_types,
                        },
                        identity=error_identity.NO_HYPERVISOR_PERIOD_END,
                        action="rescrape",
                        message=f"  {chain.database_name} {current_item['address']} index {idx} is an initial value but last item was considered initial also. Last item block: {last_item['block']} current block: {current_item['block']}  last ops: {last_operation_types} current ops: {operation_types}. Rescrape.",
                    )
            last_item_type = "ini"

            # This should not have any topics outside wanted topics
            if not any([x in wanted_topics for x in operation_types]):
                raise ValueError(
                    f" {chain.database_name} {current_item['address']} index {idx} has not wanted operations {operation_types}"
                )

            # execute process function
            ##### EXECUTE IN-LOOP START ITEM FUNCTION
            self._execute_inLoop_startItem(
                chain=chain,
                hypervisor_address=hypervisor_address,
                data=hype_data,
                idx=idx,
                last_item=last_item,
                current_item=current_item,
            )

        # if no errors
        return last_item_type

    # CHECKS
    def _hypervisor_periods_first_item_checks(
        self,
        chain: Chain,
        first_item: dict,
        timestamp_ini: int | None,
        block_ini: int | None,
        try_solve_errors: bool,
        only_use_last_items: int | None = None,
    ):
        # 1) check first item has uncollected fees
        if (
            float(first_item["fees_uncollected"]["qtty_token0"]) > 0
            or float(first_item["fees_uncollected"]["qtty_token1"]) > 0
        ):
            if (
                timestamp_ini != None
                or block_ini != None
                or only_use_last_items != None
            ):
                # this may happen when timestamp_ini or block_ini are defined and those are not the first temporal points of this hypervisor
                logging.getLogger(__name__).debug(
                    f"         {chain.database_name} {first_item['address']} has uncollected fees on the first item but seems not a problem bc hype has data before {first_item['block']} block ."
                )
            else:
                if try_solve_errors:
                    # uncollected fees on the first item cannot occur if all time hypervisor data is defined.
                    # there are missing operations between blocks
                    try:
                        # scrape operations between blocks for this hypervisor: ini_block = static hype block   end_block = first block
                        ini_block = get_from_localdb(
                            network=chain.database_name,
                            collection="static",
                            find={"address": first_item["address"]},
                            limit=1,
                        )[0]["block"]
                        end_block = first_item["block"]

                        feed_operations_hypervisors(
                            network=chain.database_name,
                            hypervisor_addresses=[first_item["address"]],
                            block_ini=ini_block,
                            block_end=end_block,
                        )
                    except Exception as e:
                        logging.getLogger(__name__).exception(
                            f"  Error while feeding operations of {first_item['address']} ->  {e}"
                        )
                else:
                    raise ValueError(
                        f" {chain.database_name} {first_item['address']} has uncollected fees on the first item. There are missing operations before block. {first_item['block']}"
                    )

    def _hypervisors_periods_checkTotalSupplyChanged(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
        try_solve_errors: bool = False,
    ):
        if last_item["totalSupply"] != current_item["totalSupply"]:
            # blocks should not be consecutive
            if last_item["block"] + 1 != current_item["block"]:
                if not try_solve_errors:
                    # raise ValueError(
                    #     f"     FAIL index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
                    # )
                    supply_diff = (
                        int(current_item["totalSupply"]) - int(last_item["totalSupply"])
                    ) / int(last_item["totalSupply"])
                    raise ProcessingError(
                        chain=chain,
                        item={
                            "hypervisor_address": hypervisor_address,
                            "dex": current_item["dex"],
                            "ini_block": last_item["block"],
                            "end_block": current_item["block"],
                            "supply_difference": supply_diff,
                            "ini_supply": int(last_item["totalSupply"]),
                            "end_supply": int(current_item["totalSupply"]),
                        },
                        identity=error_identity.SUPPLY_DIFFERENCE,
                        action="rescrape",
                        message=f" FAIL index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}. Rescrape.",
                    )

                logging.getLogger(__name__).debug(
                    f" Trying solve: index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
                )

                # # make sure data is exact
                # if diff := test_inequality_hypervisor(
                #     network=chain.database_name, hypervisors=[last_item, current_item]
                # ):
                #     # check if totalSupply is solved
                #     if diff[0]["block"] == last_item["block"]:
                #         if diff[0]["totalSupply"] != last_item["totalSupply"]:
                #             # solved
                #             logging.getLogger(__name__).debug(
                #                 f"  index {idx} changed last_item totalSupply to {diff[0]['totalSupply']}"
                #             )
                #         if diff[1]["block"] == current_item["block"]:
                #             if diff[1]["totalSupply"] != current_item["totalSupply"]:
                #                 # solved
                #                 logging.getLogger(__name__).debug(
                #                     f"  index {idx} changed current_item totalSupply to {diff[1]['totalSupply']}"
                #                 )
                #     else:
                #         if diff[0]["block"] == current_item["block"]:
                #             if diff[0]["totalSupply"] != current_item["totalSupply"]:
                #                 # solved
                #                 logging.getLogger(__name__).debug(
                #                     f"  index {idx} changed current_item totalSupply to {diff[0]['totalSupply']}"
                #                 )
                #         if diff[1]["block"] == last_item["block"]:
                #             if diff[1]["totalSupply"] != last_item["totalSupply"]:
                #                 # solved
                #                 logging.getLogger(__name__).debug(
                #                     f"  index {idx} changed last_item totalSupply to {diff[1]['totalSupply']}"
                #                 )

                #     # save diff to db
                #     if db_return := get_default_localdb(
                #         network=chain.database_name
                #     ).update_items_to_database(data=diff, collection_name="status"):
                #         logging.getLogger(__name__).debug(
                #             f"  db return {db_return.modified_count} items modified"
                #         )
                #     else:
                #         logging.getLogger(__name__).error(f"  db return nothing")

                # scrape operations between blocks for this hypervisor
                feed_operations_hypervisors(
                    network=chain.database_name,
                    hypervisor_addresses=[hypervisor_address],
                    block_ini=last_item["block"],
                    block_end=current_item["block"],
                    max_blocks_step=5000,
                )

                # # raise error
                # supply_diff = (
                #     int(current_item["totalSupply"]) - int(last_item["totalSupply"])
                # ) / int(last_item["totalSupply"])

                # # raise error to rescrape
                # raise ProcessingError(
                #     chain=chain,
                #     item={
                #         "hypervisor_address": hypervisor_address,
                # "dex": current_item["dex"],
                #         "ini_block": last_item["block"],
                #         "end_block": current_item["block"],
                #         "supply_difference": supply_diff,
                #         "ini_supply": int(last_item["totalSupply"]),
                #         "end_supply": int(current_item["totalSupply"]),
                #     },
                #     identity=error_identity.SUPPLY_DIFFERENCE,
                #     action="rescrape",
                #     message=f" Hypervisor supply at START differ {supply_diff:,.5%} from END, meaning there are missing operations in between. Rescrape.",
                # )

            else:
                supply_diff = (
                    int(current_item["totalSupply"]) - int(last_item["totalSupply"])
                ) / int(last_item["totalSupply"])
                raise ProcessingError(
                    chain=chain,
                    item={
                        "hypervisor_address": hypervisor_address,
                        "dex": current_item["dex"],
                        "ini_block": last_item["block"],
                        "end_block": current_item["block"],
                        "supply_difference": supply_diff,
                        "ini_supply": int(last_item["totalSupply"]),
                        "end_supply": int(current_item["totalSupply"]),
                    },
                    identity=error_identity.SUPPLY_DIFFERENCE,
                    action="rescrape",
                    message=f" FAIL index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}. Rescrape.",
                )
                # raise ValueError(
                #     f"      FAIL index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  consecutive blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
                # )

    def _hypervisors_periods_checkfeeGrowths(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
        try_solve_errors: bool = False,
    ):
        """Check if feegrowth always grows"""

        # check if feeGrowthGlobal is always growing
        if int(last_item["pool"]["feeGrowthGlobal0X128"]) > int(
            current_item["pool"]["feeGrowthGlobal0X128"]
        ):
            raise ValueError(
                f"     FAIL index {idx} feeGrowthGlobal0X128 is decreasing from {last_item['feeGrowthGlobal0X128']} to {current_item['feeGrowthGlobal0X128']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
            )

        if int(last_item["pool"]["feeGrowthGlobal1X128"]) > int(
            current_item["pool"]["feeGrowthGlobal1X128"]
        ):
            raise ValueError(
                f"     FAIL index {idx} feeGrowthGlobal1X128 is decreasing from {last_item['feeGrowthGlobal1X128']} to {current_item['feeGrowthGlobal1X128']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
            )

        # check if feeGrowthInside last is always growing
