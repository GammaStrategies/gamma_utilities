import contextlib
from datetime import datetime, timezone
import sys
import os
import logging
import threading

from ..general import file_utilities, net_utilities
from ..database.common.db_collections_common import db_collections_common

CACHE_LOCK = threading.Lock()  ##threading.RLock


class file_backend:
    def __init__(self, filename: str, folder_name: str, reset: bool = False):
        """Cache class properties

        Args:
           filename (str):
           folder_name (str): like "data/cache"
           reset (bool, optional): create a clean cache file ( deleting the present one ) . Defaults to False.
        """
        self.file_name = filename
        self.folder_name = folder_name

        self._cache = {}  # {  network_id: "<contract address>": value, ...}

        # init object
        self._pre_init_cache(reset)
        self._init_cache()

    def _pre_init_cache(self, reset: bool):
        if self.folder_name != "":
            # check if folder exists
            if not os.path.exists(self.folder_name):
                # Create a new directory because it does not exist
                os.makedirs(name=self.folder_name, exist_ok=True)

            if reset:
                # delete file
                try:
                    if os.path.isfile(f"{self.folder_name}/{self.file_name}.json"):
                        os.remove(f"{self.folder_name}/{self.file_name}.json")
                except Exception:
                    # error could not delete file
                    logging.getLogger("special").exception(
                        f" Could not delete cache file:  {self.folder_name}/{self.file_name}.json     .error: {sys.exc_info()[0]}"
                    )

        # init price cache
        self._cache = {}

    def _load_cache_file(self, lock: bool = True) -> dict:
        if lock:
            with CACHE_LOCK:
                temp_loaded_cache = file_utilities.load_json(
                    filename=self.file_name, folder_path=self.folder_name
                )
        else:
            temp_loaded_cache = file_utilities.load_json(
                filename=self.file_name, folder_path=self.folder_name
            )
        return temp_loaded_cache

    def _save_tofile(self, lock: bool = True):
        if lock:
            with CACHE_LOCK:
                # save file
                file_utilities.save_json(
                    filename=self.file_name,
                    data=self._cache,
                    folder_path=self.folder_name,
                )
        else:
            # save file
            file_utilities.save_json(
                filename=self.file_name, data=self._cache, folder_path=self.folder_name
            )

    def _init_cache(self):
        # place some loading logic
        #  _cache = _load_cache_file()
        # ...
        pass

    # PUBLIC
    def add_data(self, data, **kwargs) -> bool:
        pass

    def get_data(self, **kwargs):
        pass


class db_collections_cache(db_collections_common):
    def __init__(self, mongo_url: str, db_name: str, db_collections: dict = None):
        if db_collections is None:
            db_collections = {"static": {"id": True}}
        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )


class database_backend:
    def __init__(self, collection: db_collections_cache, reset: bool = False):
        """Cache class properties

        Args:
           collection (db_collections_common):
           reset (bool, optional): create a clean cache file ( deleting the present one ) . Defaults to False.
        """
        self.db_collection = collection

        self._cache = {}  # {  network_id: "<contract address>": value, ...}

        # init object
        self._pre_init_cache(reset)
        self._init_cache()

    def _pre_init_cache(self, reset: bool):
        if reset:
            # wipe database
            try:
                # TODO: remove collection from database
                pass
            except Exception:
                # error could not delete file
                logging.getLogger(__name__).exception(
                    f" Could not remove collection   .error: {sys.exc_info()[0]}"
                )

        # init price cache
        self._cache = {}

    def _load_cache_file(self) -> dict:
        # TODO: get from database
        # self.db_collection.get_all_items()
        raise NotImplementedError("Not implemented yet")

    def _init_cache(self):
        # place some loading logic
        #  _cache = _load_cache_file()
        # ...
        pass

    # PUBLIC
    def add_data(self, data, **kwargs) -> bool:
        pass

    def get_data(self, **kwargs):
        pass


class standard_property_cache(file_backend):
    def _init_cache(self):
        temp_loaded_cache = self._load_cache_file()
        _loaded = 0
        if temp_loaded_cache != None:
            for chainId, val1 in temp_loaded_cache.items():
                # v = { "<address>:{<block>:{<key>:<val>..."}
                # network id
                if not int(chainId) in self._cache:
                    self._cache[int(chainId)] = dict()

                for address, val2 in val1.items():
                    # contract address
                    if not address in self._cache[int(chainId)]:
                        self._cache[int(chainId)][address] = dict()

                    for block, val3 in val2.items():
                        # block
                        if not int(block) in self._cache[int(chainId)][address]:
                            self._cache[int(chainId)][address][int(block)] = dict()

                        # set props
                        self._cache[int(chainId)][address][int(block)] = val3
                        _loaded += 1

        # log price cache loaded qtty
        # logging.getLogger("special").debug(
        #     "          {:,.0f} loaded from {}  cache file ".format(
        #         _loaded, self.file_name))

    def add_data(
        self, chain_id, address: str, block: int, key: str, data, save2file=False
    ) -> bool:
        """
        Args:
           data (dict): data to cache
           kwargs:  query arguments. Must contain network and block to be cached
        Returns:
           bool: success or fail
        """

        # convert to lower
        address = address.lower()
        key = key.lower()
        chain_id = chain_id
        block = int(block)

        with CACHE_LOCK:
            # NETWORK and BLOCK must be present when caching
            if not chain_id in self._cache:
                self._cache[chain_id] = dict()
            if not address in self._cache[chain_id]:
                self._cache[chain_id][address] = dict()
            if not block in self._cache[chain_id][address]:
                self._cache[chain_id][address][block] = dict()

            # save data to var
            self._cache[chain_id][address][block][key] = data

        if save2file:
            # save file to disk
            self._save_tofile()

        return True

    def get_data(self, chain_id, address: str, block: int, key: str):
        """Retrieves data from cache

        Returns:
           dict: Can return None if not found
        """

        # convert to lower
        address = address.lower()
        key = key.lower()

        # use it for key in cache
        return (
            self._cache.get(chain_id, {}).get(address, {}).get(block, {}).get(key, None)
        )


class mutable_property_cache(standard_property_cache):
    """Only save mutable fields to cache and
    only one value of each fixed defined property"""

    def __init__(
        self, filename: str, folder_name: str, reset: bool = False, fixed_fields=None
    ):
        # init
        super().__init__(filename=filename, folder_name=folder_name, reset=reset)

        if fixed_fields:
            # {<fixed field>:< found in cache?>}  found in cache is always false at the beginning
            self.fixed_fields = fixed_fields
        else:
            # default fixed fields
            self.fixed_fields = {"decimals": False, "symbol": False}

    def add_data(
        self, chain_id, address: str, block: int, key: str, data, save2file=False
    ) -> bool:
        """
            Fixed fields will not be added to cache
        Args:
           data (dict): data to cache
           kwargs:  query arguments. Must contain network and block to be cached
        Returns:
           bool: success or fail
        """
        # avoid saving defined fixed fields to cache
        if key in self.fixed_fields and self.is_fixedfield_inCache(
            chain_id=chain_id, address=address, key=key
        ):
            return True
        else:
            return super().add_data(
                chain_id=chain_id,
                address=address,
                block=block,
                key=key,
                data=data,
                save2file=save2file,
            )

    def get_data(self, chain_id, address: str, block: int, key: str):
        """Retrieves data from cache.


        Returns:
           dict: Can return None if not found
        """

        # convert to lower
        address = address.lower()
        key = key.lower()

        if key in self.fixed_fields:
            if result := self.get_anyblock_value(
                chain_id=chain_id, address=address, key=key
            ):
                return result

        # try return the block asked for
        return (
            self._cache.get(chain_id, {}).get(address, {}).get(block, {}).get(key, None)
        )

    def get_anyblock_value(self, chain_id: str, address: str, key: str):
        """retrieve a value from any block in cache
            If no data found, None will be returned

        Args:
            chain_id (str):
            address (str):
            key (str):

        Returns:
            any type cached: or None
        """
        try:
            # try get the variable using any block
            any_block = next(iter(self._cache[chain_id][address]))
            return self._cache[chain_id][address][any_block][key]
        except Exception:
            self.fixed_fields[key] = False

        return None

    def is_fixedfield_inCache(self, chain_id: str, address: str, key: str) -> bool:
        """is fixed field present in cache ?

        Args:
            chain_id (str):
            address (str):
            key (str):

        Returns:
            bool:
        """
        if not self.fixed_fields[key] and self.get_anyblock_value(
            chain_id=chain_id, address=address, key=key
        ):
            self.fixed_fields[key] = True

        return self.fixed_fields[key]


class standard_thegraph_cache(file_backend):
    """
    Save historic block queries indefinitely to disk
    ( queries without the block argument defined will not be saved )
    """

    # THE GRAPH VARS & HELPERS
    RATE_LIMIT = net_utilities.rate_limit(rate_max_sec=4)  # thegraph rate limiter

    def _init_cache(self):
        # init price cache
        self._cache = self._load_cache_file()

        # set new dict if no file has been loaded
        if self._cache is None:
            self._cache = {}

        _loaded = 0
        with contextlib.suppress(Exception):
            for network in self._cache.keys():
                _loaded += len(self._cache[network].keys())

        # log price cache loaded qtty
        # logging.getLogger("special").debug(
        #     "          {:,.0f} loaded from {}  cache file ".format(
        #         _loaded, self.file_name))

    def add_data(self, data, **kwargs) -> bool:
        """Only historic data (block query) is
           actually cached indefinitely.

        Args:
           data (dict): data to cache
           kwargs:  query arguments. Must contain network and block to be cached
        Returns:
           bool: success or fail
        """

        # NETWORK and BLOCK must be present when caching
        if "network" not in kwargs or "block" not in kwargs:
            # not added
            return False
        network = kwargs["network"]
        block = kwargs["block"].strip()
        # create key
        key = self._build_key(kwargs)

        with CACHE_LOCK:
            # create path
            if kwargs["network"] not in self._cache.keys():
                self._cache[network] = {}
            if block not in self._cache[network].keys():
                self._cache[network][block] = {}
            # set value
            self._cache[network][block][key] = data

            # save cache to file
            self._save_tofile(lock=False)

        return True

    def get_data(self, **kwargs):
        """Retrieves data from cache

        Returns:
           dict: Can return None if not found
        """

        with contextlib.suppress(Exception):
            # extract network and block from kwargs
            network = kwargs["network"].strip()
            block = kwargs["block"].strip()
            # create key
            key = self._build_key(kwargs)
            if key != "":
                # use it for key in cache
                return self._cache[network][block][key]
        # not in cache
        return None

    def _build_key(self, args: dict) -> str:
        result = ""
        try:
            # create a sorted list of keys without network nor block
            tmp_keys = sorted([k for k in args if k not in ["network", "block"]])
            # unify query string
            for k in tmp_keys:
                if result != "":
                    result += "__"
                result += f"{k.strip()}={args[k].strip()}"

        except Exception:
            # not in cache
            logging.getLogger(__name__).exception(
                f" Unexpected error while building cache key  .error: {sys.exc_info()[0]}"
            )

        return result


class price_cache(standard_property_cache):
    def _init_cache(self):
        # init price cache
        temp_loaded_cache = self._load_cache_file()
        _loaded = 0
        if temp_loaded_cache != None:
            for chainId, val1 in temp_loaded_cache.items():
                # v = { "<address>:{<block>:{<key>:<val>..."}

                for address, val2 in val1.items():
                    # contract address ( token id)

                    for block, val3 in val2.items():
                        # block

                        # non zero blocks and zero values are discarded
                        if int(block) > 0:
                            for token, value in val3.items():
                                # only values > 0
                                if value > 0:
                                    # init cache network id
                                    if not chainId in self._cache:
                                        self._cache[chainId] = dict()
                                    # init cache address
                                    if not address in self._cache[chainId]:
                                        self._cache[chainId][address] = dict()
                                    # init block
                                    if not int(block) in self._cache[chainId][address]:
                                        self._cache[chainId][address][
                                            int(block)
                                        ] = dict()
                                    # set token value
                                    self._cache[chainId][address][int(block)][
                                        token
                                    ] = value
                                    _loaded += 1

        # log price cache loaded qtty
        # logging.getLogger("special").debug(
        #     "          {:,.0f} loaded from {}  cache file ".format(
        #         _loaded, self.file_name))


class pool_token_cache:
    # cache specific to handle geckoterminal pool tokens

    def __init__(self, time_limit: int = 3600):
        """init cache

        Args:
            time_limit (int, optional): maximum seconds to keep cached value. Defaults to 600.
        """
        self._cache = dict()
        self._time_limit = time_limit

    @property
    def cache(self):
        return self._cache

    def set_data(self, key: str, subkey: str, data: dict) -> bool:
        """Add data to the "key" dict

        Args:
            key (str):
            subkey (str):
            data (dict):

        Returns:
            bool:
        """
        try:
            _now = datetime.now(timezone.utc).timestamp()
            data["cache_timestamp"] = _now
            # create key if not present
            if key not in self._cache:
                self._cache[key] = dict()

            # update cache
            self._cache[key][subkey] = data

            return True

        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Unexpected error while setting pool_token_cache data. error: {e}"
            )

        return False

    def get_data(self, key: str, subkey: str) -> dict:
        """Get data from the "key" dict

        Args:
            key (str): _description_
            subkey (str): _description_

        Returns:
            dict: _description_
        """
        _now = datetime.now(timezone.utc).timestamp()
        # check if key is present
        if key in self._cache:
            # check if value_key is present
            if subkey in self._cache[key]:
                # check if cache_timestamp is present and add it if not: (saved cache may not have it)
                if "cache_timestamp" not in self._cache[key][subkey]:
                    self._cache[key][subkey]["cache_timestamp"] = _now

                # check if value is still valid
                if (
                    _now - self._cache[key][subkey].pop("cache_timestamp")
                    < self._time_limit
                ):
                    return self._cache[key][subkey]

        return None
