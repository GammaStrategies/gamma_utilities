import sys
import datetime as dt
import requests
import logging
import time
import threading

from requests import exceptions as req_exceptions


# TODO: implement httpx + any concurrency manager
# TODO: implement requests-cache


#
def post_request(url: str, query: str, retry=0, max_retry=2, wait_secs=5) -> dict:

    try:
        request = requests.post(url, json={"query": query})
        return request.json()
    except (
        req_exceptions.ConnectionError,
        ConnectionResetError,
        ConnectionError,
    ) as err:
        # blocking us?  wait and try as many times as defined
        logging.getLogger(__name__).warning(
            "Connection to {} has been closed...".format(url)
        )
    except:
        logging.getLogger(__name__).exception(
            "Unexpected error while posting request at {} .error: {}".format(
                url, sys.exc_info()[0]
            )
        )

    # check if retry is needed
    if retry < max_retry:
        logging.getLogger(__name__).warning(
            "    Waiting {} seconds to retry {} query for the {} time.".format(
                wait_secs, url, retry
            )
        )
        time.sleep(wait_secs)
        # retry
        return post_request(
            url=url,
            query=query,
            retry=retry + 1,
            max_retry=max_retry,
            wait_secs=wait_secs,
        )

    # return empty dict
    return dict()


def get_request(url, retry=0, max_retry=2, wait_secs=5) -> dict:
    result = dict()
    # query url
    try:
        result = requests.get(url).json()
        return result

    except (
        req_exceptions.ConnectionError,
        ConnectionResetError,
        ConnectionError,
    ) as err:
        # thegraph blocking us?
        # wait and try one last time
        logging.getLogger(__name__).warning("Connection error to {}...".format(url))
    except:
        logging.getLogger(__name__).exception(
            "Unexpected error while retrieving json from {}     .error: {}".format(
                url, sys.exc_info()[0]
            )
        )

    if retry < max_retry:
        logging.getLogger(__name__).debug(
            "    Waiting {} seconds to retry {} query for the {} time.".format(
                wait_secs, url, retry
            )
        )
        time.sleep(wait_secs)
        return get_request(
            url=url, retry=retry + 1, max_retry=max_retry, wait_secs=wait_secs
        )


class rate_limit:
    def __init__(self, rate_max_sec: float):
        self.rate_max_sec: float = rate_max_sec
        self.rate_sec: int = 0
        self.rate_count_lastupdate: dt.datetime = dt.datetime.now() - dt.timedelta(
            hours=8
        )
        self.lock = threading.Lock()  # threading.RLock

    def hit(self) -> bool:
        """Report a query to rate limit and
           return if I'm safe proceeding

        Returns:
           [bool] -- Im I safe ??
        """
        with self.lock:
            # update qtty rate per second so sum only if 1 sec has not yet passed
            if (dt.datetime.now() - self.rate_count_lastupdate).total_seconds() <= 1:
                # set current rate
                self.rate_sec += 1
            else:
                self.rate_sec = 1
                # update last date
                self.rate_count_lastupdate = dt.datetime.now()

        # return if i'm save to go
        return self.im_safe()

    def im_safe(self) -> bool:
        """Is it safe to continue

        Returns:
           bool -- [description]
        """
        return self.rate_sec <= self.rate_max_sec

    def continue_when_safe(self):
        """Wait here till rate is in bounds"""

        while (self.im_safe) == False:
            with self.lock:
                if (
                    dt.datetime.now() - self.rate_count_lastupdate
                ).total_seconds() <= 1:
                    # set current rate
                    self.rate_sec += 1
                else:
                    self.rate_sec = 1
                    # update last date
                    self.rate_count_lastupdate = dt.datetime.now()

        # keep track
        self.hit()
