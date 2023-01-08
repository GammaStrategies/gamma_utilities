import sys
import os
import logging
import threading

from bins.general import file_utilities, net_utilities


CACHE_LOCK = threading.Lock()


class standard_property_cache():
    
    _cache = dict()  # {  network_id: "<contract address>": value, ...}

    def __init__(self, filename: str, folder_name: str, reset:bool=False):
        """  Cache class properties
             
         Args:
            filename (str): 
            folder_name (str): like "data/cache"
            reset (bool, optional): create a clean cache file ( deleting the present one ) . Defaults to False.
         """
        self.file_name = filename
        self.folder_name = folder_name

        # init object
        self.init_cache(reset)

    def init_cache(self, reset:bool):

        # check if folder exists
        if not os.path.exists(self.folder_name):
            # Create a new directory because it does not exist
            os.makedirs(name=self.folder_name, exist_ok=True)

        if reset:
            # delete file 
            try:
                if os.path.isfile("{}/{}.json".format(self.folder_name,self.file_name)):
                    os.remove("{}/{}.json".format(self.folder_name,self.file_name))
            except:
                # error could not delete file
                logging.getLogger("special").exception(" Could not delete cache file:  {}     .error: {}".format("{}/{}.json".format(self.folder_name,self.file_name), sys.exc_info()[0]))


        # init price cache
        self._cache = dict()
        temp_loaded_cache = file_utilities.load_json(filename=self.file_name, folder_path=self.folder_name)
        _loaded = 0
        if temp_loaded_cache != None:
            for chainId, val1 in temp_loaded_cache.items():
                # v = { "<address>:{<block>:{<key>:<val>..."}
                # network id
                if not int(chainId) in self._cache:
                    self._cache[int(chainId)] = dict()

                for address,val2 in val1.items():
                    # contract address
                    if not address in self._cache[int(chainId)]:
                        self._cache[int(chainId)][address] = dict()

                    for block,val3 in val2.items():
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

    def add_data(self, chain_id, address:str, block:int, key:str, data, save2file=False)->bool:
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
            self.save_tofile()
            

        return True
   
    def get_data(self, chain_id, address:str, block:int, key:str):
        """ Retrieves data from cache

         Returns:
            dict: Can return None if not found
         """
        
        # convert to lower 
        address = address.lower()
        key = key.lower()

        try:
            # use it for key in cache
            return self._cache[chain_id][address][block][key]
        except:
            pass

        # not in cache
        return None

    def save_tofile(self):
        with CACHE_LOCK:
            # save file
            file_utilities.save_json(filename=self.file_name, data=self._cache, folder_path=self.folder_name)


class standard_thegraph_cache:
    """
        Save historic block queries indefinitely to disk
        ( queries without the block argument defined will not be saved )
    """
    
    _cache = dict()
        ## {  network: 
        #        block:
        #           key__is__long__query
        #           }

    def __init__(self, filename: str, folder_name: str, reset:bool=False):
        """  Save historic block queries indefinitely to disk
            ( queries without the block argument defined will not be saved )
         Args:
            filename (str): like "price_cache_uniswap"
            folder_name (str): like "data/cache"
            reset (bool, optional): create a clean cache file ( deleting the present one ) . Defaults to False.
         """

        self.file_name = filename + "_cache"
        self.folder_name = folder_name
        
        # THE GRAPH VARS & HELPERS
        self.RATE_LIMIT = net_utilities.rate_limit(rate_max_sec=4) # thegraph rate limiter

        # init object
        self.init_cache(reset)

    def init_cache(self,reset:bool):

        # check if folder exists
        if not os.path.exists(self.folder_name):
            # Create a new directory because it does not exist
            os.makedirs(name=self.folder_name, exist_ok=True)

        if reset:
            # delete file 
            try:
                if os.path.isfile("{}/{}.json".format(self.folder_name,self.file_name)):
                    os.remove("{}/{}.json".format(self.folder_name,self.file_name))
            except:
                # error could not delete file
                logging.getLogger("special").exception(" Could not delete cache file:  {}     .error: {}".format("{}/{}.json".format(self.folder_name,self.file_name), sys.exc_info()[0]))

        # init price cache
        self._cache = file_utilities.load_json(
            filename=self.file_name, folder_path=self.folder_name
        )
        # set new dict if no file has been loaded
        if self._cache == None:
            self._cache = dict()
        
        _loaded = 0
        try:
            for network in self._cache.keys():
                _loaded += len(self._cache[network].keys())
        except:
            pass

        # log price cache loaded qtty
        # logging.getLogger("special").debug(
        #     "          {:,.0f} loaded from {}  cache file ".format(
        #         _loaded, self.file_name))


    def add_data(self, data, **kwargs)->bool:
        """ Only historic data (block query) is
            actually cached indefinitely.
            
         Args:
            data (dict): data to cache
            kwargs:  query arguments. Must contain network and block to be cached
         Returns:
            bool: success or fail
         """

        # NETWORK and BLOCK must be present when caching
        if "network" in kwargs and "block" in kwargs:
            network = kwargs["network"]
            block = kwargs["block"].strip()
            # create key
            key = self._build_key(kwargs)

            with CACHE_LOCK:
                # create path
                if not kwargs["network"] in self._cache.keys():
                    self._cache[network] = dict()
                if not block in self._cache[network].keys():
                    self._cache[network][block] = dict()
                # set value
                self._cache[network][block][key] = data

                # save cache to file
                file_utilities.save_json(
                    filename=self.file_name, data=self._cache, folder_path=self.folder_name)
            
            return True
        # not added
        return False

    def get_data(self, **kwargs):
        """ Retrieves data from cache

         Returns:
            dict: Can return None if not found
         """
        
        try:
            # extract network and block from kwargs 
            network = kwargs["network"].strip()
            block = kwargs["block"].strip()
            # create key
            key = self._build_key(kwargs)
            if key != "":
                # use it for key in cache
                return self._cache[network][block][key]
        except:
            pass

        # not in cache
        return None

    def _build_key(self, args:dict)->str:

        result = ""
        try:
            # create a sorted list of keys without network nor block
            tmp_keys =  sorted([k for k in args.keys() if not k in ["network","block"]])
            # unify query string 
            for k in tmp_keys:
                if result != "":
                    result += "__"
                result += "{}={}".format(k.strip(),args[k].strip())
                
        except:
            # not in cache
            logging.getLogger(__name__).exception(" Unexpected error while building cache key  .error: {}".format(sys.exc_info()[0]))

        return result


class price_cache(standard_property_cache):

    def init_cache(self, reset:bool):

        # check if folder exists
        if not os.path.exists(self.folder_name):
            # Create a new directory because it does not exist
            os.makedirs(name=self.folder_name, exist_ok=True)

        if reset:
            # delete file 
            try:
                if os.path.isfile("{}/{}.json".format(self.folder_name,self.file_name)):
                    os.remove("{}/{}.json".format(self.folder_name,self.file_name))
            except:
                # error could not delete file
                logging.getLogger("special").exception(" Could not delete cache file:  {}     .error: {}".format("{}/{}.json".format(self.folder_name,self.file_name), sys.exc_info()[0]))

        # init price cache
        self._cache = dict()
        temp_loaded_cache = file_utilities.load_json(filename=self.file_name, folder_path=self.folder_name)
        _loaded = 0
        if temp_loaded_cache != None:
            for chainId, val1 in temp_loaded_cache.items():
                # v = { "<address>:{<block>:{<key>:<val>..."}
                
                for address,val2 in val1.items():
                    # contract address ( token id)

                    for block,val3 in val2.items():
                        # block
                        
                        # non zero blocks and zero values are discarded
                        if int(block) > 0:
                            
                            for token,value in val3.items():
                                # only values > 0
                                if value >0:
                                    
                                    # init cache network id
                                    if not chainId in self._cache:
                                        self._cache[chainId] = dict()
                                    # init cache address
                                    if not address in self._cache[chainId]:
                                        self._cache[chainId][address] = dict()
                                    # init block 
                                    if not int(block) in self._cache[chainId][address]:
                                        self._cache[chainId][address][int(block)] = dict()
                                    # set token value
                                    self._cache[chainId][address][int(block)][token] = value
                                    _loaded += 1


        # log price cache loaded qtty
        # logging.getLogger("special").debug(
        #     "          {:,.0f} loaded from {}  cache file ".format(
        #         _loaded, self.file_name))


