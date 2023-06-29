import sys
from datetime import datetime, timezone, timedelta
import requests
import logging
import time
import threading

from requests import exceptions as req_exceptions


# TODO: implement httpx + any concurrency manager
# TODO: implement requests-cache


#
def post_request(
    url: str,
    query: str,
    retry: int = 0,
    max_retry: int = 2,
    wait_secs: int = 5,
    timeout_secs: int = 10,
) -> dict:
    try:
        request = requests.post(url=url, json={"query": query}, timeout=timeout_secs)
        return request.json()
    except (req_exceptions.ConnectionError, ConnectionError) as err:
        # blocking us?  wait and try as many times as defined
        logging.getLogger(__name__).warning(f"Connection to {url} has been closed...")
    except req_exceptions.ReadTimeout as err:
        logging.getLogger(__name__).warning(f"Connection to {url} has timed out...")
    except Exception:
        logging.getLogger(__name__).exception(
            f"Unexpected error while posting request at {url} .error: {sys.exc_info()[0]}"
        )

    # check if retry is needed
    if retry < max_retry:
        logging.getLogger(__name__).warning(
            f"    Waiting {wait_secs} seconds to retry {url} query for the {retry} time."
        )

        time.sleep(wait_secs)
        # retry
        return post_request(
            url=url,
            query=query,
            retry=retry + 1,
            max_retry=max_retry,
            wait_secs=wait_secs,
            timeout_secs=timeout_secs,
        )

    # return empty dict
    return {}


def get_request(
    url,
    retry: int = 0,
    max_retry: int = 2,
    wait_secs: int = 5,
    timeout_secs: int = 10,
) -> dict:
    try:
        return get_response(
            url=url,
            retry=retry,
            max_retry=max_retry,
            wait_secs=wait_secs,
            timeout_secs=timeout_secs,
        ).json()
    except Exception as e:
        pass


def get_response(
    url,
    retry: int = 0,
    max_retry: int = 2,
    wait_secs: int = 5,
    timeout_secs: int = 10,
):
    # query url
    try:
        return requests.get(url=url, timeout=timeout_secs)

    except (req_exceptions.ConnectionError, ConnectionError) as err:
        # wait and try one last time
        logging.getLogger(__name__).warning(f"Connection error to {url}...")
    except req_exceptions.ReadTimeout as err:
        logging.getLogger(__name__).warning(f"Connection to {url} has timed out...")
    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Unexpected error while retrieving json from {url}     .error: {e}"
        )

    if retry < max_retry:
        logging.getLogger(__name__).debug(
            f"    Waiting {wait_secs} seconds to retry {url} query for the {retry} time."
        )

        time.sleep(wait_secs)
        return get_response(
            url=url,
            retry=retry + 1,
            max_retry=max_retry,
            wait_secs=wait_secs,
            timeout_secs=timeout_secs,
        )


class rate_limit:
    def __init__(self, rate_max_sec: float):
        self.rate_max_sec: float = rate_max_sec
        self.rate_sec: int = 0
        self.rate_count_lastupdate: datetime = datetime.now(timezone.utc) - timedelta(
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
            if (
                datetime.now(timezone.utc) - self.rate_count_lastupdate
            ).total_seconds() <= 1:
                # set current rate
                self.rate_sec += 1
            else:
                self.rate_sec = 1
                # update last date
                self.rate_count_lastupdate = datetime.now(timezone.utc)

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
        _safe_break = datetime.now(timezone.utc)
        while (self.im_safe) == False:
            if (datetime.now(timezone.utc) - _safe_break).total_seconds() >= 30:
                logging.getLogger(__name__).error(
                    f"Waited for 30 seconds for rate limit to be safe.  Breaking."
                )
                return True
            with self.lock:
                if (
                    datetime.now(timezone.utc) - self.rate_count_lastupdate
                ).total_seconds() <= 1:
                    # set current rate
                    self.rate_sec += 1
                else:
                    self.rate_sec = 1
                    # update last date
                    self.rate_count_lastupdate = datetime.now(timezone.utc)

        # keep track
        self.hit()
