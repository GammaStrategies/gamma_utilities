from dataclasses import dataclass
import logging
import random
import time

from bins.configuration import CONFIGURATION


class w3Provider:
    def __init__(self, url: str, type: str):
        self._url = url
        self._type = type
        self._is_available = True

        self._attempts = 0
        self._failed_attempts = 0
        self._failed_attempts_aggregated = 0  # forever failed attempts

        self._max_failed_attempts = 5
        self._max_failed_attempts_aggregated = (
            200  # every time it hits, cooldown increases by 120 seconds
        )

        # cooldown
        self._cooldown = 120
        self._cooldown_start = time.time()

    @property
    def url(self) -> str:
        return self._url

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

        self._modify_state()

    @property
    def type(self) -> str:
        # public or private
        return self._type

    @property
    def cooldown(self) -> int:
        return self._cooldown

    def add_failed(self):
        # add one failed attempt
        self.failed_attempts += 1

    def add_attempt(self):
        self._attempts += 1

    def _modify_state(self):
        # check if we need to disable this provider

        if (
            self._failed_attempts_aggregated
            and self._failed_attempts_aggregated % self._max_failed_attempts_aggregated
            == 0
        ):
            # disable
            self._is_available = False
            # increase cooldown
            self._cooldown *= 2
            # beguin cooldown
            self._cooldown_start = time.time()
            logging.getLogger(__name__).debug(
                f"  max aggregated failed attempts hit. Cooling {self._url} down to {self._cooldown} seconds"
            )

        elif self._failed_attempts > self._max_failed_attempts:
            # disable
            self._is_available = False
            # beguin cooldown
            self._cooldown_start = time.time()
            logging.getLogger(__name__).debug(
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
        }


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
        self, network: str, rpcKey_names: list[str] | None = None, shuffle: bool = True
    ) -> list[w3Provider]:
        """Get a list of w3Provider from configuration file

        Args:
            network (str): network name
            rpcKey_names (list[str] | None, optional): private or public or whatever is placed in config w3Providers. Defaults to None.
            shuffle (bool, optional): shuffle configured order. Defaults to True.

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
                result.extend([x for x in rpcUrls if x.is_available])
        #
        return result

    def get_rpc(self, network: str, url: str) -> w3Provider:
        for key_name in CONFIGURATION["sources"].get(
            "w3Providers_default_order", ["public", "private"]
        ):
            for provider in self.providers[key_name][network]:
                if provider.url == url:
                    return provider

        raise Exception(f"RPC {url} not found in providers list")


# singleton
RPC_MANAGER = w3Providers()
