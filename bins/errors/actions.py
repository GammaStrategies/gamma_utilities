import logging
from apps.feeds.operations import feed_operations
from bins.errors.general import ProcessingError
from bins.general.enums import error_identity, text_to_protocol
from bins.w3.builders import build_hypervisor


def process_error(error: ProcessingError):
    """Take action based on error identity"""

    if error.identity == error_identity.RETURN_NONE:
        pass
    elif error.identity == error_identity.OVERLAPED_PERIODS:
        pass
    elif error.identity == error_identity.SUPPLY_DIFFERENCE:
        # this can indicate missing operation between two blocks
        if error.action == "rescrape":
            rescrape_block_ini = error.item["ini_block"]
            rescrape_block_end = error.item["end_block"]

            # rescrape operations for this chain between defined blocks
            logging.getLogger(__name__).info(
                f" Rescraping operations for {error.chain.database_name} between blocks {rescrape_block_ini} and {rescrape_block_end}. Reason: supply difference error"
            )
            feed_operations(
                protocol="gamma",
                network=error.chain.database_name,
                block_ini=rescrape_block_ini,
                block_end=rescrape_block_end,
            )

    elif error.identity == error_identity.NEGATIVE_FEES:
        if error.action == "rescrape":
            # check if this is an old hypervisor
            if "visor" in error.item.get(
                "hypervisor_name", ""
            ).lower() or error.item.get("hypervisor_symbol", "").lower().startswith(
                "v"
            ):
                # this is an old hypervisor, Cant do anything
                logging.getLogger(__name__).debug(
                    f" The old hypervisor {error.item.get('hypervisor_name')} has negative uncollected fees between periods. Nothing to do"
                )
            else:
                return

                # this is an old hypervisor, Cant do anything
                logging.getLogger(__name__).debug(
                    f" Checking {error.item.get('hypervisor_name')} has a correct uncollected fees between ini/end period. Rescraping both blocks"
                )
                # check if hype uncollected are correct
                ini_hype = build_hypervisor(
                    network=error.chain.database_name,
                    protocol=text_to_protocol(error.item["dex"]),
                    hypervisor_address=error.item["hypervisor_address"],
                    block=error.item["ini_block"],
                )
                end_hype = build_hypervisor(
                    network=error.chain.database_name,
                    protocol=text_to_protocol(error.item["dex"]),
                    hypervisor_address=error.item["hypervisor_address"],
                    block=error.item["end_block"],
                )
                ini_uncollected = ini_hype.get_fees_uncollected()
                end_uncollected = end_hype.get_fees_uncollected()

                fees0_uncollected = (
                    end_uncollected["qtty_token0"] - ini_uncollected["qtty_token0"]
                )
                fees1_uncollected = (
                    end_uncollected["qtty_token1"] - ini_uncollected["qtty_token1"]
                )

                if (
                    fees0_uncollected != error.item["fees0_uncollected"]
                    or fees1_uncollected != error.item["fees1_uncollected"]
                ):
                    # TODO: save new hypes to db
                    # logging.getLogger(__name__).debug(f" Hypervisor {error.item.get('hypervisor_name')} has incorrect uncollected fees between ini/end period. Saving new data to db")
                    # ini_hype.to_dict()
                    # end_hype.to_dict()
                    pass

                # this is a new hypervisor, try rescrape
                rescrape_block_ini = error.item["ini_block"]
                rescrape_block_end = error.item["end_block"]
                # rescrape operations for this chain between defined blocks
                logging.getLogger(__name__).info(
                    f" Rescraping operations for {error.chain.database_name} between blocks {rescrape_block_ini} and {rescrape_block_end}. Reason: negative fees error"
                )
                feed_operations(
                    protocol="gamma",
                    network=error.chain.database_name,
                    block_ini=rescrape_block_ini,
                    block_end=rescrape_block_end,
                )

    elif error.identity == error_identity.INVALID_MFD:
        if error.action == "remove":
            # remove invalid mfd? TODO
            pass
    else:
        logging.getLogger(__name__).warning(
            f"Unknown error identity {error.identity}. Can't process error"
        )