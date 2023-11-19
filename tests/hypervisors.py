import logging
from bins.configuration import CONFIGURATION
from bins.database.common.objects.hypervisor import (
    hypervisor_status_object,
    transformer_hypervisor_status,
)
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain, Protocol, text_to_chain, text_to_protocol
from bins.w3.builders import build_db_hypervisor, build_db_hypervisor_multicall
from tests.utils import compare_dictionaries, get_status_hypes_of_each_protocol


def test_hypervisors():
    chains = (
        [
            text_to_chain(text=network)
            for network in CONFIGURATION["_custom_"]["cml_parameters"].networks
        ]
        if CONFIGURATION["_custom_"]["cml_parameters"].networks
        else list(Chain)
    )
    protocols = (
        [
            text_to_protocol(text=protocol)
            for protocol in CONFIGURATION["_custom_"]["cml_parameters"].protocols
        ]
        if CONFIGURATION["_custom_"]["cml_parameters"].protocols
        else None
    )

    for chain in chains:
        logging.getLogger(__name__).debug(f" Testing hypervisors on {chain} STARTED")
        # get a representative list of hypervisors ( all protocols n cashuistics )
        for hype_status in get_status_hypes_of_each_protocol(
            chain=chain, qtty=1, cashuistics=True, protocols=protocols
        ):
            logging.getLogger(__name__).info(
                f" Testing {chain} {hype_status['dex']} {hype_status['symbol']} on block {hype_status['block']}"
            )
            # build hypervisor and compare (multicall)
            test_multicall(chain=chain, hypervisor_status=hype_status)

            # build hypervisor and compare (singlecall)
            test_singlecall(chain=chain, hypervisor_status=hype_status)

            # convert to object and test object functions
            test_conversion(chain=chain, hypervisor_status=hype_status)

            # end of hypervisor test
            logging.getLogger(__name__).info(
                f" Ended test for {chain} {hype_status['dex']} {hype_status['symbol']} at block {hype_status['block']}"
            )

        # end of chain test
        logging.getLogger(__name__).debug(f" Testing hypervisors on {chain} ENDED")


#### HELPER FUNCTIONS ####


def test_multicall(chain: Chain, hypervisor_status: dict):
    # build hypervisor and compare (multicall)
    logging.getLogger(__name__).debug(
        f" (multicall) Building {chain.fantasy_name} hypervisor {hypervisor_status['symbol']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
    )
    _result = False
    if _testing_hype_status_multicall := build_db_hypervisor_multicall(
        address=hypervisor_status["address"],
        network=chain.database_name,
        block=hypervisor_status["block"],
        dex=hypervisor_status["dex"],
        pool_address=hypervisor_status["pool"]["address"],
        token0_address=hypervisor_status["pool"]["token0"]["address"],
        token1_address=hypervisor_status["pool"]["token1"]["address"],
    ):
        logging.getLogger(__name__).debug(f" 1- (multicall)  build successfull")
        # compare
        logging.getLogger(__name__).debug(
            f" (multicall) Comparing {chain.fantasy_name} hypervisor {hypervisor_status['symbol']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
        )
        # remove ids from database dict object
        if "_id" in hypervisor_status:
            hypervisor_status.pop("_id")
        if "id" in hypervisor_status:
            hypervisor_status.pop("id")
        # compare
        isEqual, field = compare_dictionaries(
            _testing_hype_status_multicall, hypervisor_status
        )
        if isEqual:
            logging.getLogger(__name__).debug(
                f" 1.1- (multicall)  comparison successfull"
            )
        else:
            logging.getLogger(__name__).info(
                f" 1.1- (multicall)  ERROR -> comparison unsuccessfull: different field value found at: {field}. {chain.fantasy_name} hypervisor {hypervisor_status['address']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
            )
    else:
        logging.getLogger(__name__).info(
            f" 1- (multicall)  ERROR -> build unsuccessfull {chain.fantasy_name} hypervisor {hypervisor_status['address']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
        )


def test_singlecall(chain: Chain, hypervisor_status: dict):
    logging.getLogger(__name__).debug(
        f" (singlecall) Building {chain.fantasy_name} hypervisor {hypervisor_status['symbol']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
    )
    if _testing_hype_status_singlecall := build_db_hypervisor(
        address=hypervisor_status["address"],
        network=chain.database_name,
        block=hypervisor_status["block"],
        dex=hypervisor_status["dex"],
        cached=False,
    ):
        logging.getLogger(__name__).debug(f" 2- (singlecall)  build successfull")
        # compare
        logging.getLogger(__name__).debug(
            f" (singlecall) Comparing hypervisor {hypervisor_status['symbol']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
        )
        # remove ids from database dict object
        if "_id" in hypervisor_status:
            hypervisor_status.pop("_id")
        if "id" in hypervisor_status:
            hypervisor_status.pop("id")
        # compare
        isEqual, field = compare_dictionaries(
            _testing_hype_status_singlecall, hypervisor_status
        )
        if isEqual:
            logging.getLogger(__name__).debug(
                f" 2.1- (singlecall) comparison successfull"
            )
        else:
            logging.getLogger(__name__).info(
                f" 2.1- (singlecall)  ERROR -> comparison unsuccessfull: different field value found at: {field} {chain.fantasy_name} hypervisor {hypervisor_status['address']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
            )
    else:
        logging.getLogger(__name__).info(
            f" 2- (singlecall)  ERROR -> build unsuccessfull {chain.fantasy_name} hypervisor {hypervisor_status['address']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
        )


def test_conversion(chain: Chain, hypervisor_status: dict):
    logging.getLogger(__name__).debug(
        f" (object) Converting to object hypervisor {hypervisor_status['symbol']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
    )
    # remove ids from database dict object
    if "_id" in hypervisor_status:
        hypervisor_status.pop("_id")
    if "id" in hypervisor_status:
        hypervisor_status.pop("id")

    # convert to object
    hype_object = hypervisor_status_object(
        transformer=transformer_hypervisor_status, **hypervisor_status
    )
    try:
        # test object functions
        fees_uncollected_gamma, fees_uncollected_lp = hype_object.get_fees_uncollected()

        fees_collected_gamma, fees_collected_lp = hype_object.get_fees_collected()

        gamma_fee = hype_object.get_gamma_fee()
        dex_protocol_fee = hype_object.get_dex_protocol_fee()
        liquidity_in_range = hype_object.get_inRange_liquidity()
        liquidity = hype_object.get_total_liquidity()
        underlying_value = hype_object.get_underlying_value()

        total_fees_collected_0 = fees_collected_gamma.token0 + fees_collected_lp.token0
        total_fees_collected_1 = fees_collected_gamma.token1 + fees_collected_lp.token1

        grossFees0, grossFees1 = hype_object.calculate_gross_fees(
            collected_fees0=total_fees_collected_0,
            collected_fees1=total_fees_collected_1,
        )
        grossVolume0, grossVolume1 = hype_object.calculate_gross_volume(
            gross_fees0=grossFees0, gross_fees1=grossFees1
        )

        # log
        logging.getLogger(__name__).debug(f" (object) object test successfull")

    except Exception as e:
        logging.getLogger(__name__).info(
            f" (object) ERROR object test unsuccessfull  {chain.fantasy_name} hypervisor {hypervisor_status['address']} {hypervisor_status['dex']} at block {hypervisor_status['block']}"
        )
        logging.getLogger(__name__).exception(f" ERROR object test->  {e}")
