import logging
import concurrent.futures
import tqdm
from apps.feeds.status import create_reward_status_from_hype_status
from bins.configuration import CONFIGURATION
from bins.database.common.db_collections_common import database_local
from bins.general.enums import Chain, Protocol, text_to_chain
from bins.w3.builders import build_db_hypervisor


## main rescraping func
def manual_reScrape(
    chain: Chain,
    loop_work: callable,
    find: dict = {},
    sort: list = [("block", -1)],
    db_collection: str = "status",
    threaded: bool = True,
):
    """Rescrape database items

    Args:
        chain (Chain):
        loop_work:  func to loop thru
        find (dict, optional): . Defaults to all.
        sort (list, optional): . Defaults to descending block
        db_collection (str, optional):  database collection to beguin rescraping with ... defaults to hypervisor "status",
        threaded (bool, optional): Defaults to yes,
    """

    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_collection = db_collection
    db_name = f"{chain.database_name}_gamma"
    batch_size = 100000

    if not sort:
        sort = [("block", -1)]

    logging.getLogger(__name__).info(
        f" Starting a manual {'threaded ' if threaded else ''}rescraping process using {db_collection} for {chain} {f'using filter: {find}' if find else ''} {f'sorted by : {sort}' if sort else ''}"
    )

    if database_items := database_local(
        mongo_url=mongo_url, db_name=db_name
    ).get_items_from_database(
        collection_name=db_collection,
        find=find,
        batch_size=batch_size,
        sort=sort,
    ):
        error = 0
        ok = 0
        with tqdm.tqdm(total=len(database_items)) as progress_bar:
            if threaded:
                # prepare arguments for threaded
                args = (
                    (
                        item,
                        chain,
                    )
                    for item in database_items
                )
                with concurrent.futures.ThreadPoolExecutor() as ex:
                    for result in ex.map(lambda p: loop_work(*p), args):
                        if result:
                            ok += 1
                        else:
                            error += 1

                        progress_bar.set_description(f" ok: {ok} error: {error}")
                        progress_bar.update(1)
            else:
                for item in database_items:
                    if result := loop_work(item, chain):
                        ok += 1
                    else:
                        error += 1

                    progress_bar.set_description(f" ok: {ok} error: {error}")
                    progress_bar.update(1)


## specialized definitions for rescraping
def reScrape_loopWork_hypervisor_status(
    hype_status: dict,
    chain: Chain,
) -> bool:
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{chain.database_name}_gamma"
    try:
        if hypervisor := build_db_hypervisor(
            address=hype_status["address"],
            network=chain.database_name,
            block=hype_status["block"],
            dex=hype_status["dex"],
        ):
            # TODO: compare n log diffs

            # add to database
            database_local(mongo_url=mongo_url, db_name=db_name).set_status(
                data=hypervisor
            )
            return True
    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while processing hypervisor status {hype_status['address']} at block {hype_status['block']} -> error {e}"
        )

    return False


def reScrape_loopWork_rewards_status(rewarder_status: dict, chain: Chain) -> bool:
    mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
    db_name = f"{chain.database_name}_gamma"
    try:
        # get hypervisor static data from database
        if hypervisor_status := database_local(
            mongo_url=mongo_url, db_name=db_name
        ).get_items_from_database(
            collection_name="status",
            find={
                "address": rewarder_status["hypervisor_address"],
                "block": rewarder_status["block"],
            },
            limit=1,
        ):
            # get the only result item
            hypervisor_status = hypervisor_status[0]

            # get rewarder static
            rewarder_static = database_local(
                mongo_url=mongo_url, db_name=db_name
            ).get_items_from_database(
                collection_name="rewards_static",
                find={
                    "hypervisor_address": rewarder_status["hypervisor_address"],
                    "rewarder_registry": rewarder_status["rewarder_registry"],
                },
            )
            if not rewarder_static:
                logging.getLogger(__name__).error(
                    f" No rewarder static found for hypervisor {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} "
                )
                return False
            else:
                rewarder_static = rewarder_static[0]

            # create rewarder status from hype
            for new_rewarder_status in create_reward_status_from_hype_status(
                hypervisor_status=hypervisor_status,
                rewarder_static=rewarder_static,
                network=chain.database_name,
            ):
                # save to database
                if float(new_rewarder_status["rewards_perSecond"]) > 0:
                    err = False
                    # compare main differences
                    if rewarder_status["block"] != new_rewarder_status["block"]:
                        logging.getLogger(__name__).error(
                            f" Blocks differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} original: {rewarder_status['block']} -> new: {new_rewarder_status['block']}"
                        )
                        err = True
                    if (
                        rewarder_status["rewards_perSecond"]
                        != new_rewarder_status["rewards_perSecond"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" rewards_perSecond differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['rewards_perSecond']} -> new: {new_rewarder_status['rewards_perSecond']}"
                        )
                    if (
                        rewarder_status["rewardToken_price_usd"]
                        != new_rewarder_status["rewardToken_price_usd"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" rewardToken_price_usd differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['rewardToken_price_usd']} -> new: {new_rewarder_status['rewardToken_price_usd']}"
                        )
                    if (
                        rewarder_status["token0_price_usd"]
                        != new_rewarder_status["token0_price_usd"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" token0_price_usd differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['token0_price_usd']} -> new: {new_rewarder_status['token0_price_usd']}"
                        )
                    if (
                        rewarder_status["token1_price_usd"]
                        != new_rewarder_status["token1_price_usd"]
                    ):
                        logging.getLogger(__name__).debug(
                            f" token1_price_usd differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['token1_price_usd']} -> new: {new_rewarder_status['token1_price_usd']}"
                        )
                    if rewarder_status["apr"] != new_rewarder_status["apr"]:
                        logging.getLogger(__name__).debug(
                            f" apr differ for hype {rewarder_status['hypervisor_address']} rewarder {rewarder_status['rewarder_address']} block {rewarder_status['block']} original: {rewarder_status['apr']} -> new: {new_rewarder_status['apr']}"
                        )

                    if not err:
                        # add to database
                        database_local(
                            mongo_url=mongo_url, db_name=db_name
                        ).set_rewards_status(data=new_rewarder_status)
                        return True
                else:
                    logging.getLogger(__name__).debug(
                        f" {chain.database_name}'s hypervisor {new_rewarder_status['hypervisor_address']} at block {new_rewarder_status['block']} not saved bc has 0 rewards per second"
                    )
                    return True

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while processing rewarder status {rewarder_status['id']} -> error {e}"
        )

    return False


def main(option=None):
    # TODO: add options logic
    # options are collection, custom find, custom sort, threaded
    for protocol in CONFIGURATION["script"]["protocols"]:
        # override networks if specified in cml
        networks = (
            CONFIGURATION["_custom_"]["cml_parameters"].networks
            or CONFIGURATION["script"]["protocols"][protocol]["networks"]
        )

        for network in networks:
            for dex in CONFIGURATION["script"]["protocols"][protocol]["networks"][
                network
            ]:
                if option == "rewards_status":
                    manual_reScrape(
                        chain=text_to_chain(network),
                        loop_work=reScrape_loopWork_rewards_status,
                        find={"dex": dex},
                        sort=[("block", -1)],
                        db_collection="rewards_status",
                        threaded=True,
                    )
                elif option == "status":
                    manual_reScrape(
                        chain=text_to_chain(network),
                        loop_work=reScrape_loopWork_hypervisor_status,
                        find={"dex": dex},
                        sort=[("block", -1)],
                        db_collection="status",
                        threaded=True,
                    )
