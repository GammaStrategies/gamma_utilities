import logging
import tqdm
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain
from bins.log.telegram_logger import send_to_telegram
from bins.mixed.price_utilities import price_scraper


def telegram_checks_tokens_without_price(chains: list[Chain] | None = None):
    """Get a list of tokens that can't get prices from by using the price_scraper (current configuration).
        Log it to telegram

    Args:
        chains (list[Chain] | None, optional): list of chains to process. Defaults to All.

    """

    chains = chains or list(Chain)

    with tqdm.tqdm(total=len(chains)) as progress_bar:
        for chain in chains:
            # get tokens from static collection
            static_hypes = get_from_localdb(
                network=chain.database_name, collection="static", find={}
            )
            if not static_hypes:
                logging.getLogger(__name__).warning(
                    f" No tokens found in {chain.database_name} static collection"
                )
                continue

            # create a list of unique tokens from the hypervisor['pool'] token0 and token1 fields
            tokens = {
                x["pool"]["token0"]["address"]: x["pool"]["token0"]["symbol"]
                for x in static_hypes
            } | {
                x["pool"]["token1"]["address"]: x["pool"]["token1"]["symbol"]
                for x in static_hypes
            }
            if not tokens:
                logging.getLogger(__name__).error(
                    f" No tokens found in {chain.database_name} static collection"
                )
                continue
            # try get prices for all those at current block
            price_helper = price_scraper(thegraph=False)

            for token_address, token_symbol in tokens.items():
                try:
                    _tmpPrice, _tmpSource = price_helper.get_price(
                        network=chain.database_name,
                        token_id=token_address,
                        block=0,
                    )
                    if not _tmpPrice:
                        send_to_telegram.error(
                            msg=[
                                f"<b>\n {chain.fantasy_name} {token_symbol} is not getting the price correctly </b>",
                                f"<pre>{token_address}</pre>",
                                f" ",
                            ],
                            topic="prices",
                            dtime=True,
                        )

                except Exception as e:
                    logging.getLogger(__name__).exception(
                        f" Error getting price for {chain.database_name} {token_symbol} {token_address} -> {e}"
                    )

                progress_bar.set_description(
                    f" Just checked {chain.fantasy_name} {token_symbol} "
                )
                progress_bar.refresh()
            progress_bar.update(1)
