from dataclasses import dataclass
import logging
import random
import time

from bins.configuration import CONFIGURATION
from bins.general.enums import cuType


class w3Provider:
    def __init__(self, url: str, type: str):
        self._url = url
        self._type = type
        self._is_available = True

        self._attempts = 0
        self._failed_attempts = 0
        self._failed_attempts_aggregated = 0  # forever failed attempts

        self._max_failed_attempts = 3
        self._max_failed_attempts_aggregated = (
            50  # every time it hits, cooldown increases by 120 seconds
        )

        # cooldown
        self._cooldown = 120
        self._cooldown_start = time.time()

        # CU costs tracker
        self.compute_unit_cost = 0

    @property
    def url(self) -> str:
        return self._url

    @property
    def url_short(self) -> str:
        """Anonymize url keys as much possible, returning the url without protocol and only till first /
            ( log purposes only )
        Returns:
            str: short url without protocol and only till first /
        """

        return self._url.replace("https://", "").replace("http://", "").split("/")[0]

    @property
    def is_available(self) -> bool:
        # check if we are in cooldown
        if (
            not self._is_available
            and self._cooldown_start + self._cooldown < time.time()
        ):
            # cooldown finished
            self._is_available = True
            # reset failed attempts
            self._failed_attempts_aggregated += self._failed_attempts
            self._failed_attempts = 0
            self._cooldown_start = time.time()

        return self._is_available

    @property
    def attempts(self) -> int:
        return self._attempts

    @property
    def failed_attempts(self) -> int:
        return self._failed_attempts

    @failed_attempts.setter
    def failed_attempts(self, value: int):
        self._failed_attempts = value
        # modify state
        self._modify_state()

    @property
    def type(self) -> str:
        # public or private
        return self._type

    @property
    def cooldown(self) -> int:
        return self._cooldown

    # modify status
    def add_failed(self, error: Exception | None = None):
        # add one failed attempt
        self._failed_attempts += 1

        if error:
            # get severity
            cooldown = cooldown_severity(error=error, rpc=self)
            self._modify_state(cooldown=cooldown)
        else:
            self._modify_state()

    def add_attempt(self, method: cuType | None = None):
        self._attempts += 1
        self._add_compute_unit(method=method)

    def _set_unavailable(self, cooldown: int = 120):
        self._is_available = False
        self._cooldown = cooldown
        self._cooldown_start = time.time()

    def _modify_state(self, cooldown: int = 120):
        # check if we need to disable this provider

        if (
            self._failed_attempts_aggregated
            and self._failed_attempts_aggregated % self._max_failed_attempts_aggregated
            == 0
        ):
            # disable 24-48 hours on public rpc's. Private rpc's are more important, so we disable them for less time
            cooldown = (
                random.randint(86400, 172800)
                if self.type == "public"
                else random.randint(3600, 7200)
            )

            self._set_unavailable(cooldown=cooldown)
            logging.getLogger(__name__).warning(
                f"  max aggregated failed attempts hit. Cooling {self._url} down to {self._cooldown/60/60} hours"
            )

        elif self._failed_attempts > self._max_failed_attempts:
            # disable
            self._set_unavailable(cooldown=cooldown)
            logging.getLogger(__name__).warning(
                f"  max failed attempts hit. Cooling {self._url} down to {self._cooldown} seconds"
            )

    # stats
    def get_stats(self) -> dict:
        return {
            "url": self._url,
            "type": self._type,
            "attempts": self._attempts,
            "failed_attempts": (
                self._failed_attempts + self._failed_attempts_aggregated
            )
            / self._attempts
            if self._attempts
            else 0,
            "is_available": self._is_available,
            "cooldown": self._cooldown,
            "compute_unit_cost": self.compute_unit_cost,
        }

    def _add_compute_unit(self, method: cuType | None = None):
        self.compute_unit_cost += self._compute_unit_prices(method)

    def _compute_unit_prices(self, method: cuType | None = None) -> int:
        """Compute unit costs for a method

        Args:
            method (cuType): method

        Returns:
            int: cu quantity
        """
        if not method:
            # guess standard
            return 26

        if method == cuType.eth_call:
            return 26
        elif method == cuType.eth_getBlockByNumber:
            return 16
        elif method == cuType.eth_chainId:
            return 0
        elif method == cuType.eth_getFilterLogs:
            return 75
        elif method == cuType.eth_getLogs:
            return 75
        elif method == cuType.eth_getBalance:
            return 19
        elif method == cuType.eth_getTransactionReceipt:
            return 15
        elif method == cuType.eth_getCode:
            return 19
        else:
            return 0


class w3Providers:
    def __init__(self):
        self.providers = {}
        self.setup()

    # setup
    def setup(self):
        for key_name in CONFIGURATION["sources"].get(
            "w3Providers_default_order", ["public", "private"]
        ):
            # add provider type if not exists
            self.providers[key_name] = {}

            for network, rpcUrls in (
                CONFIGURATION["sources"]
                .get("w3Providers", {})
                .get(key_name, {})
                .items()
            ):
                # add network if not exists
                if network not in self.providers[key_name]:
                    self.providers[key_name][network] = []

                if rpcUrls:
                    # convert to w3Providers
                    self.providers[key_name][network].extend(
                        [w3Provider(url=rpcUrl, type=key_name) for rpcUrl in rpcUrls]
                    )

    def get_rpc_list(
        self,
        network: str,
        rpcKey_names: list[str] | None = None,
        shuffle: bool = True,
        availability_filter: bool = True,
    ) -> list[w3Provider]:
        """Get a list of w3Provider from configuration file

        Args:
            network (str): network name
            rpcKey_names (list[str] | None, optional): private or public or whatever is placed in config w3Providers. Defaults to None.
            shuffle (bool, optional): shuffle configured order. Defaults to True.
            availability_filter (bool, optional): return only available rpc's. Defaults to True.

        Returns:
            list[w3Provider]: w3Provider list
        """
        result = []
        # load configured rpc url's
        for key_name in rpcKey_names or CONFIGURATION["sources"].get(
            "w3Providers_default_order", ["public", "private"]
        ):
            if rpcUrls := self.providers.get(key_name, {}).get(network, []):
                # shuffle if needed
                if shuffle:
                    random.shuffle(rpcUrls)

                # add to result
                if availability_filter:
                    result.extend([x for x in rpcUrls if x.is_available])
                else:
                    result.extend(rpcUrls)
        # # if there are no results, and len(rpcKey_names) == 1, try to get from the other type
        # if not result and len(rpcKey_names or []) == 1:
        #     # get the other type
        #     other_type = (
        #         "public"
        #         if rpcKey_names[0] == "private"
        #         else "private"
        #         if rpcKey_names[0] == "public"
        #         else None
        #     )
        #     if other_type:
        #         logging.getLogger(__name__).warning(
        #             f" No {rpcKey_names[0]} rpc's available and no other type configured for this call. Trying with the other type"
        #         )
        #         result = self.providers.get(other_type, {}).get(network, [])

        return result

    def get_rpc(self, network: str, url: str) -> w3Provider:
        for key_name in CONFIGURATION["sources"].get(
            "w3Providers_default_order", ["public", "private"]
        ):
            for provider in self.providers[key_name][network]:
                if provider.url == url:
                    return provider

        raise Exception(f"RPC {url} not found in providers list")

    def get_stats(self) -> dict:
        result = {}
        for key_name in CONFIGURATION["sources"].get(
            "w3Providers_default_order", ["public", "private"]
        ):
            result[key_name] = {}
            for network, rpcUrls in (
                CONFIGURATION["sources"]
                .get("w3Providers", {})
                .get(key_name, {})
                .items()
            ):
                # add network if not exists
                if network not in result[key_name]:
                    result[key_name][network] = []

                if rpcUrls:
                    # convert to w3Providers
                    result[key_name][network].extend(
                        [x.get_stats() for x in self.providers[key_name][network]]
                    )

        return result

    def totals(self, type: str = None, network: str = None) -> tuple[int, int]:
        """Total attempts and total CUs for a type and network

        Args:
            type (str, optional): type. Defaults to None.
            network (str, optional): network. Defaults to None.

        Returns:
            tuple[int, int]: total attempts, total CUs
        """
        total_atempts = 0
        total_cus = 0
        # self.providers[key_name][network]
        for key_name, net_data in self.providers.items():
            # skip if type is not the one we want
            if type and key_name != type:
                continue

            for chain_key, rpc_list in net_data.items():
                # skip if network is not the one we want
                if network and chain_key != network:
                    continue

                for rpc in rpc_list:
                    total_atempts += rpc.attempts
                    total_cus += rpc.compute_unit_cost

        return total_atempts, total_cus


# singleton
RPC_MANAGER = w3Providers()


# TODO: implement at w3Provider.add_failed ?
def cooldown_severity(error: Exception, rpc: w3Provider) -> int:
    """Retrieve a cooldown severity from returned rpc errors
        When zero is returned, no cooldown is needed
    Args:
        error (Exception): error
        rpc (w3Provider): rpc
    Returns:
        int: cooldown severity
    """
    # default cooldown
    cooldown = random.randint(40, 120)

    if rpc.type == "private":
        cooldown = random.randint(10, 40)

    try:
        # Process dict info only
        if error.args and isinstance(error.args[0], dict):
            # check if we need to disable this provider
            if (
                error.args[0].get("code", 0) == -32000
                and error.args[0].get("message", "").startswith("too many requests")
                or error.args[0]
                .get("message", "")
                .startswith("Upgrade to an archive plan add-on for your account")
            ):
                logging.getLogger(__name__).debug(
                    f"  too many requests for {rpc.url_short}"
                )
                # return random cooldown between 2.5 and 5 minutes
                cooldown = random.randint(150, 300)

            elif error.args[0].get("code", 0) == -32000 and error.args[0].get(
                "message", ""
            ).startswith("missing trie node"):
                # disable by a small amount of time
                logging.getLogger(__name__).debug(
                    f"  rpc {rpc.url_short} has no data at the specified block (public node)"
                )
                # return random cooldown
                cooldown = random.randint(40, 90)

        # there are plenty of error types to handle here .. like MaxRetryError

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Error while checking cooldown severity-> {e}"
        )

    # return result
    return cooldown
