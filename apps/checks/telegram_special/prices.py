import logging
import tqdm
from apps.checks.prices.general import price_analyzer
from bins.database.helpers import get_from_localdb
from bins.general.enums import Chain
from bins.log.telegram_logger import send_to_telegram


def telegram_checks_tokens_without_price():
    """Get a list of tokens that can't get prices from by using the price_scraper (current configuration).
    Log it to telegram
    """
    helper = None
    helper = price_analyzer()
    for chain in Chain:
        helper.items = []
        helper.check_tokens_without_price(chains=[chain])

        if helper.items:

            # create a list of missing prices
            _grouped_msgs = [
                x.telegram_message for x in helper.items if x.name != "summary"
            ]

            # find summary
            for item in helper.items:
                if item.name == "summary":
                    # send all missing prices
                    send_to_telegram.info(
                        msg=_grouped_msgs,
                        topic="prices",
                        dtime=True,
                    )
                    # send summary
                    send_to_telegram.info(
                        msg=item.telegram_message,
                        topic="prices",
                        dtime=True,
                    )
                    # exit loop
                    break
