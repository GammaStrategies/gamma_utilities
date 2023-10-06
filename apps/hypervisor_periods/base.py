import logging
from apps.feeds.operations import feed_operations_hypervisors
from apps.feeds.utils import get_hypervisors_data_for_apr
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain


# def execute_processes_within_hypervisor_periods(
#     chain: Chain,
#     hypervisor_address: str,
#     ini_process_function: callable,
#     end_process_function: callable,
#     timestamp_ini: int | None = None,
#     timestamp_end: int | None = None,
#     block_ini: int | None = None,
#     block_end: int | None = None,
#     try_solve_errors: bool = False,
# ):
#     # check if any of block or timestamp has been supplied
#     if not timestamp_end and not block_ini or not timestamp_end and not block_end:
#         raise ValueError("timestamps or blocks must be provided")

#     # get data from database
#     if data_list := get_hypervisors_data_for_apr(
#         network=chain.database_name,
#         hypervisor_addresses=[hypervisor_address],
#         timestamp_ini=timestamp_ini or 1400000000,
#         timestamp_end=timestamp_end,
#         block_ini=block_ini,
#         block_end=block_end,
#     ):
#         # loop for each hypervisor data found { _id: <hypervisor_address> ,  {status: <hypervisor status data + operations + rewards...>}
#         for hype_data in data_list:
#             # check data consistency
#             if not "status" in hype_data:
#                 raise ValueError(f"Invalid hypervisor data: {hype_data}")

#             # check first item
#             hypervisor_periods_first_item_checks(
#                 chain=chain,
#                 first_item=hype_data["status"][0],
#                 timestamp_ini=timestamp_ini,
#                 block_ini=block_ini,
#                 try_solve_errors=try_solve_errors,
#             )

#             ###### START #######
#             # define loop working vars
#             last_item = None
#             last_item_type = None

#             # loop thu each hype status data ( hypervisor status found for that particular time period )
#             for idx, status_data in enumerate(hype_data["status"]):
#                 # define local vars for later use
#                 wanted_topics = ["deposit", "withdraw", "rebalance", "zeroBurn"]
#                 operation_types = [op["topic"] for op in status_data["operations"]]
#                 last_operation_types = (
#                     [op["topic"] for op in last_item["operations"]] if last_item else []
#                 )

#                 # hypervisors without operations are end values
#                 if not status_data["operations"]:
#                     # <<<<<<<<<<<< this is an end value <<<<<<<<<<<<<<<<<<<<<<<<<<

#                     # make sure last item was not an end value
#                     if last_item_type != "ini":
#                         # check if this is the first idx to discard it if it is
#                         if idx == 0:
#                             logging.getLogger(__name__).warning(
#                                 f" {chain.database_name} {status_data['address']} index {idx} is an end value but has no operations, because its a -1 block. Discarding it safely."
#                             )
#                             continue

#                         raise ValueError(
#                             f" {chain.database_name} {status_data['address']} index {idx} is an end value but last item was considered end also. Last item block: {last_item['block']} current block: {status_data['block']} last ops: {last_operation_types} current ops: {operation_types}"
#                         )

#                     # define last item type
#                     last_item_type = "end"

#                     # check totalSupply has not changed
#                     hypervisors_periods_checkTotalSupplyChanged(
#                         chain=chain,
#                         hypervisor_address=hypervisor_address,
#                         data=hype_data,
#                         idx=idx,
#                         last_item=last_item,
#                         current_item=status_data,
#                         try_solve_errors=try_solve_errors,
#                     )

#                     # check fee Growth
#                     hypervisors_periods_checkfeeGrowths(
#                         chain=chain,
#                         hypervisor_address=hypervisor_address,
#                         data=hype_data,
#                         idx=idx,
#                         last_item=last_item,
#                         current_item=status_data,
#                         try_solve_errors=try_solve_errors,
#                     )

#                     # execute process function
#                     if end_process_function:
#                         end_process_function(
#                             chain=chain,
#                             hypervisor_address=hypervisor_address,
#                             data=hype_data,
#                             idx=idx,
#                             last_item=last_item,
#                             current_item=status_data,
#                         )

#                 elif (
#                     not any([x in wanted_topics for x in operation_types])
#                     and last_item
#                     and last_item["totalSupply"] == status_data["totalSupply"]
#                 ):
#                     # <<<<<<<<<<<< this is an end value <<<<<<<<<<<<<<<<<<<<<<<<<<
#                     # because the initial query discards topics outside wanted topics, this is a correct end value.

#                     # make sure last item was not an end value
#                     if last_item_type != "ini":
#                         raise ValueError(
#                             f"  {chain.database_name} {status_data['address']} index {idx} is an end value but last item was considered end also. Last item block: {last_item['block']} current block: {status_data['block']} last ops: {last_operation_types} current ops: {operation_types}"
#                         )
#                     # define last item type
#                     last_item_type = "end"

#                     # check totalSupply has not changed
#                     hypervisors_periods_checkTotalSupplyChanged(
#                         chain=chain,
#                         hypervisor_address=hypervisor_address,
#                         data=hype_data,
#                         idx=idx,
#                         last_item=last_item,
#                         current_item=status_data,
#                         try_solve_errors=try_solve_errors,
#                     )

#                     # check fee Growth
#                     hypervisors_periods_checkfeeGrowths(
#                         chain=chain,
#                         hypervisor_address=hypervisor_address,
#                         data=hype_data,
#                         idx=idx,
#                         last_item=last_item,
#                         current_item=status_data,
#                         try_solve_errors=try_solve_errors,
#                     )

#                     # execute process function
#                     if end_process_function:
#                         end_process_function(
#                             chain=chain,
#                             hypervisor_address=hypervisor_address,
#                             data=hype_data,
#                             idx=idx,
#                             last_item=last_item,
#                             current_item=status_data,
#                         )

#                 else:
#                     # it has operations:
#                     # this is an initial value
#                     if last_item_type and last_item_type != "end":
#                         # check if they are consecutive blocks
#                         if last_item["block"] + 1 == status_data["block"]:
#                             logging.getLogger(__name__).warning(
#                                 f" {chain.database_name} {status_data['address']} index {idx} is an initial value but last item was considered initial also. Blocks are consecutive (noproblm). Last item block: {last_item['block']} current block: {status_data['block']}  last ops: {last_operation_types} current ops: {operation_types}"
#                             )
#                         else:
#                             raise ValueError(
#                                 f"  {chain.database_name} {status_data['address']} index {idx} is an initial value but last item was considered initial also. Last item block: {last_item['block']} current block: {status_data['block']}  last ops: {last_operation_types} current ops: {operation_types}"
#                             )
#                     last_item_type = "ini"

#                     # This should not have any topics outside wanted topics
#                     if not any([x in wanted_topics for x in operation_types]):
#                         raise ValueError(
#                             f" {chain.database_name} {status_data['address']} index {idx} has not wanted operations {operation_types}"
#                         )

#                     # execute process function
#                     if ini_process_function:
#                         ini_process_function(
#                             chain=chain,
#                             hypervisor_address=hypervisor_address,
#                             data=hype_data,
#                             idx=idx,
#                             last_item=last_item,
#                             current_item=status_data,
#                         )

#                 last_item = status_data

#                 # if this is the last idx and last_item == ini, then we can decide wether to scrape a new item using the last block/timestamp defined or not
#                 if idx == len(hype_data["status"]) - 1 and last_item_type == "ini":
#                     if block_end and last_item["block"] < block_end:
#                         # scrape block_end

#                         pass
#                     elif timestamp_end and last_item["timestamp"] < timestamp_end:
#                         # convert timestamp_end to block_end and scrape block_end

#                         pass
#                     else:
#                         #   no need to scrape because last_item is the last item of the defined period, that happen to be a initial value
#                         logging.getLogger(__name__).debug(
#                             f" {chain.database_name} {status_data['address']} last index {idx} is an initial value, but also the last period value."
#                         )

#     else:
#         logging.getLogger(__name__).error(
#             f"   {chain.database_name} {hypervisor_address} has no data from {block_ini} to {block_end}"
#         )


# Handling hypervisor periods ( op.block -> op.block-1 -> op.block ...)
class hypervisor_periods_base:
    # OVERRIDE FUNCTIONS
    def reset(self):
        # reset all vars
        self.result = []

    def execute_preLoop(self, hypervisor_data: dict):
        """executed before the loop ( one time )

        Args:
            hypervisor_data (dict): _description_
        """
        pass

    def execute_inLoop(
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

    def execute_inLoop_startItem(
        self,
        chain: Chain,
        hypervisor_address: str,
        data: dict,
        idx: int,
        last_item: dict,
        current_item: dict,
    ):
        """executed when a new start hypervisor period is found

        Args:
            chain (Chain):
            hypervisor_address (str):
            data (dict):
            idx (int):
            last_item (dict):
            current_item (dict):
        """
        pass

    def execute_inLoop_endItem(
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

    def execute_postLoop(self, hypervisor_data: dict):
        """executed after the loop ( one time )

        Args:
            hypervisor_data (dict):
        """
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
            timestamp_ini=timestamp_ini or 1400000000,
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
                self.hypervisor_periods_first_item_checks(
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
                self.execute_preLoop(hypervisor_data=hype_data)

                # loop thu each hype status data ( hypervisor status found for that particular time period )
                for idx, status_data in enumerate(hype_data["status"]):
                    ##### EXECUTE IN-LOOP FUNCTION
                    self.execute_inLoop(
                        chain=chain,
                        hypervisor_address=hypervisor_address,
                        data=hype_data,
                        idx=idx,
                        last_item=last_item,
                        current_item=status_data,
                    )

                    # define local vars for later use
                    wanted_topics = ["deposit", "withdraw", "rebalance", "zeroBurn"]
                    operation_types = [op["topic"] for op in status_data["operations"]]
                    last_operation_types = (
                        [op["topic"] for op in last_item["operations"]]
                        if last_item
                        else []
                    )

                    # hypervisors without operations are end values
                    if not status_data["operations"]:
                        # <<<<<<<<<<<< this is an end value <<<<<<<<<<<<<<<<<<<<<<<<<<

                        # make sure last item was not an end value
                        if last_item_type != "ini":
                            # check if this is the first idx to discard it if it is
                            if idx == 0:
                                logging.getLogger(__name__).warning(
                                    f" {chain.database_name} {status_data['address']} index {idx} is an end value but has no operations, because its a -1 block. Discarding it safely."
                                )
                                continue

                            raise ValueError(
                                f" {chain.database_name} {status_data['address']} index {idx} is an end value but last item was considered end also. Last item block: {last_item['block']} current block: {status_data['block']} last ops: {last_operation_types} current ops: {operation_types}"
                            )

                        # define last item type
                        last_item_type = "end"

                        # check totalSupply has not changed
                        self.hypervisors_periods_checkTotalSupplyChanged(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                            try_solve_errors=try_solve_errors,
                        )

                        # check fee Growth
                        self.hypervisors_periods_checkfeeGrowths(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                            try_solve_errors=try_solve_errors,
                        )

                        ##### EXECUTE IN-LOOP END ITEM FUNCTION
                        self.execute_inLoop_endItem(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                        )

                    elif (
                        not any([x in wanted_topics for x in operation_types])
                        and last_item
                        and last_item["totalSupply"] == status_data["totalSupply"]
                    ):
                        # <<<<<<<<<<<< this is an end value <<<<<<<<<<<<<<<<<<<<<<<<<<
                        # because the initial query discards topics outside wanted topics, this is a correct end value.

                        # make sure last item was not an end value
                        if last_item_type != "ini":
                            raise ValueError(
                                f"  {chain.database_name} {status_data['address']} index {idx} is an end value but last item was considered end also. Last item block: {last_item['block']} current block: {status_data['block']} last ops: {last_operation_types} current ops: {operation_types}"
                            )
                        # define last item type
                        last_item_type = "end"

                        # check totalSupply has not changed
                        self.hypervisors_periods_checkTotalSupplyChanged(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                            try_solve_errors=try_solve_errors,
                        )

                        # check fee Growth
                        self.hypervisors_periods_checkfeeGrowths(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                            try_solve_errors=try_solve_errors,
                        )

                        # execute process function
                        ##### EXECUTE IN-LOOP END ITEM FUNCTION
                        self.execute_inLoop_endItem(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                        )

                    else:
                        # it has operations:
                        # this is an initial value
                        if last_item_type and last_item_type != "end":
                            # check if they are consecutive blocks
                            if last_item["block"] + 1 == status_data["block"]:
                                logging.getLogger(__name__).warning(
                                    f" {chain.database_name} {status_data['address']} index {idx} is an initial value but last item was considered initial also. Blocks are consecutive (noproblm). Last item block: {last_item['block']} current block: {status_data['block']}  last ops: {last_operation_types} current ops: {operation_types}"
                                )
                            else:
                                raise ValueError(
                                    f"  {chain.database_name} {status_data['address']} index {idx} is an initial value but last item was considered initial also. Last item block: {last_item['block']} current block: {status_data['block']}  last ops: {last_operation_types} current ops: {operation_types}"
                                )
                        last_item_type = "ini"

                        # This should not have any topics outside wanted topics
                        if not any([x in wanted_topics for x in operation_types]):
                            raise ValueError(
                                f" {chain.database_name} {status_data['address']} index {idx} has not wanted operations {operation_types}"
                            )

                        # execute process function
                        ##### EXECUTE IN-LOOP START ITEM FUNCTION
                        self.execute_inLoop_startItem(
                            chain=chain,
                            hypervisor_address=hypervisor_address,
                            data=hype_data,
                            idx=idx,
                            last_item=last_item,
                            current_item=status_data,
                        )

                    last_item = status_data

                    # if this is the last idx and last_item == ini, then we can decide wether to scrape a new item using the last block/timestamp defined or not
                    if idx == len(hype_data["status"]) - 1 and last_item_type == "ini":
                        if block_end and last_item["block"] < block_end:
                            # scrape block_end

                            pass
                        elif timestamp_end and last_item["timestamp"] < timestamp_end:
                            # convert timestamp_end to block_end and scrape block_end

                            pass
                        else:
                            #   no need to scrape because last_item is the last item of the defined period, that happen to be a initial value
                            logging.getLogger(__name__).debug(
                                f" {chain.database_name} {status_data['address']} last index {idx} is an initial value, but also the last period value."
                            )

                ##### EXECUTE OUT-LOOP FUNCTION
                self.execute_postLoop(hypervisor_data=hype_data)

        else:
            logging.getLogger(__name__).error(
                f"   {chain.database_name} {hypervisor_address} has no data from {block_ini} to {block_end}"
            )

        return self.result

    # CHECKS
    def hypervisor_periods_first_item_checks(
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

    def hypervisors_periods_checkTotalSupplyChanged(
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
                    raise ValueError(
                        f"     FAIL index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
                    )

                logging.getLogger(__name__).warning(
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
                )
            else:
                raise ValueError(
                    f"      FAIL index {idx} changed totalSupply from {last_item['totalSupply']} to {current_item['totalSupply']}  consecutive blocks: {last_item['block']}  {current_item['block']} {chain.fantasy_name}'s {hypervisor_address}"
                )

    def hypervisors_periods_checkfeeGrowths(
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
