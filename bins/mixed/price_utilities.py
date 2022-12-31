







# save to cache = queue item -> exit
# load from cache = query global cache

#   cache loop saves content to file


class uniswapv3_price_cache:

    _COMMON_ADDRESSES={
        "ethereum":
            {"0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": {"symbol":"WETH",},
            "0x6B175474E89094C44Da98b954EedeAC495271d0F": {"symbol":"DAI",},
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": {"symbol":"USDC",},
            "0xdAC17F958D2ee523a2206206994597C13D831ec7": {"symbol":"USDT",},

            },
        "optimism":
            {"0x4200000000000000000000000000000000000006": {"symbol":"WETH",},
            "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": {"symbol":"DAI",},
            "0x7F5c764cBc14f9669B88837ca1490cCa17c31607": {"symbol":"USDC",},
            "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58": {"symbol":"USDT",},
            }
    }

    def __init__(self, filename: str, folder_name: str, reset:bool=False):
        """ 
         Args:
            filename (str): like "price_cache_uniswap"
            folder_name (str): like "data/cache"
            reset (bool, optional): create a clean cache file ( deleting the present one ) . Defaults to False.
         """

        self.file_name = filename
        self.file_name_queue = "{}_queue".format(self.file_name)
        self.folder_name = folder_name
        
        # inits 
        self.init_threading()
        self.init_cache(reset)
        self.init_queue()
        self.init_savecontroler()

   ## CONFIG ##
    def init_threading(self):

        self.stop = False
        self.isAlive = False
        self.thread = None

    def init_cache(self,reset:bool):

        self.price_cache = dict()
    
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
        loaded_dta = file_utilities.load_json(filename=self.file_name, folder_path=self.folder_name)
        _price_loaded_count = 0
        _price_processed_count = 0
        try:
            for network, data in loaded_dta.items():

                for token_id in data.keys():
                    for block in data[token_id].keys():
                        # only load non zero blocks ( zero blocks represent current prices)
                        if data[token_id][block] != 0:
                            # checks
                            if not network in self.price_cache.keys():
                                self.price_cache[network] = dict()
                            if not token_id.lower() in self.price_cache[network].keys():
                                self.price_cache[network][token_id.lower()] = dict()
                            if not int(block) in self.price_cache[network][token_id.lower()].keys():
                                self.price_cache[network][token_id.lower()][int(block)] = 0
                            # add item to cache
                            self.price_cache[network][token_id.lower()][int(block)] = data[token_id][block]
                            # increase counter
                            _price_loaded_count += 1 

                        _price_processed_count +=1
        except:
            # todo: identify no file vs error 
            pass


        # log price cache loaded qtty
        logging.getLogger("special").debug(
            "          {:,.0f} uniswapV3 prices loaded in cache ( of {:,.0f} processed)".format(
                _price_loaded_count, _price_processed_count)
            )
    
    def init_queue(self):
        # Prices already processed by the threaded loop
        self.threaded_price_results = Queue()
        # price to be processed by the threaded loop
        self.threaded_price_queue = Queue()

    def init_savecontroler(self):
        # define a time control var to be used to save cache file at interval
        self._save_controller = general_utils.time_controller(seconds_frame=30)

   ## PUBLIC ##
    def get_usd_price(self, network: str, token_id: str, block: int= 0, dequeue:bool=True)-> tuple:
        """ 
            return: price_usd_token
         """   
        
        if not self.threaded_price_results.empty() and dequeue:
            # try dequeue threaded work 
            self._dequeue_threaded_price_results()
            dequeue = False

        # try return price from cached values
        try:
            return self.price_cache[network][token_id][block]
        except:
            # price+block does not exist in cache
            pass

        # log query
        logging.getLogger("special").debug(" Trying to get UniswapV3 price for {}'s token {} at block {}".format(network, token_id, block))

        # query
        try:
            # build url and query
            _url = self.__url_constructor(network=network)
            _query = self.__build_query(token_id=token_id, block=block)

            # request data to thegraph api
            _data = net_utilities.post_request(url=_url, query=_query, retry=0, max_retry=2, wait_secs=5)

            token_symbol = _data["data"]["token"]["symbol"]
            # decide what to use to get to price ( value or volume )
            if float(_data["data"]["token"]["totalValueLockedUSD"]) > 0 and float(_data["data"]["token"]["totalValueLocked"]) > 0:
                # get unit usd price from value locked
                price = float(_data["data"]["token"]["totalValueLockedUSD"])/float(_data["data"]["token"]["totalValueLocked"])
            elif "volume" in _data["data"]["token"] and float(_data["data"]["token"]["volume"]) > 0 and "volumeUSD" in _data["data"]["token"] and float(_data["data"]["token"]["volumeUSD"]) > 0:
                # get unit usd price from volume
                price = float(_data["data"]["token"]["volumeUSD"])/float(_data["data"]["token"]["volume"])
            else:
                # no way 
                price = 0 
            
            if not network in self.price_cache.keys():
                self.price_cache[network] = dict()     # init pool address
            if not token_id in self.price_cache[network].keys():
                self.price_cache[network][token_id] = dict()     # init pool address
            if not block in self.price_cache[network][token_id].keys():
                self.price_cache[network][token_id][int(block)] = 0  # init prices

            # decide mannualy on certain circumstances (DAI USDC...) 
            if price == 0:
                price = self._price_special_case(address=token_id, network=network)

            # save to cache
            self.price_cache[network][token_id][int(block)] = price

            if dequeue:
                # dequeue is false when called from threaded

                # try dequeue threaded work 
                self._dequeue_threaded_price_results()


                # save cache to json file
                if self._save_controller.has_time_passed():
                    file_utilities.save_json(filename=self.file_name, data=self.price_cache, folder_path=self.folder_name)
                    self._save_controller.hit()

        except ZeroDivisionError:
            # one or all prices are zero. Cant continue
            # return zeros
            logging.getLogger("special").warning(
                "one or all price variables of {}'s token {} (address {}) at block {} from uniswap subgraph are ZERO. Can't get price.  --> data: {}".format(
                    network, token_symbol, token_id, block, _data["data"]))
            return 0
        except(KeyError, TypeError) as err:
            # errors': [{'message': 'indexing_error'}]
            if "errors" in _data:
                for error in _data["errors"]:
                    logging.getLogger("special").error(
                        "Unexpected error while getting price of {}'s token address {} at block {} from uniswap subgraph   data:{}      .error: {}".format(
                        network, token_id, block, _data, error))
            else:
                logging.getLogger("special").exception(
                "Unexpected error while getting price of {}'s token address {} at block {} from uniswap subgraph   data:{}      .error: {}".format(
                    network, token_id, block, _data, sys.exc_info()[0]))
            return 0
        except:
            logging.getLogger("special").exception(
                "Unexpected error while getting price of {}'s token address {} at block {} from uniswap subgraph   data:{}      .error: {}".format(
                    network, token_id, block, _data, sys.exc_info()[0]))
            # return zeros
            return 0
        
        # return result
        return self.price_cache[network][token_id][int(block)]

   ## QUEUE 
    def save_to_queue(self, network:str, token_ids_blocks:list):
        """ Save the list to queue file so it can be processed to cache while doing other things
            
         Args:
            network (str): ethereum or polygon or ...
            token_ids_blocks (list): [ [token_id, block ],[token_id, block ], ... ]

         """
        for i in [{"network":network, "token_id":i[0].lower(), "block":i[1]} for i in token_ids_blocks]:
            self.threaded_price_queue.put(i)

    def process_queue(self):
        try:
            tmp = self.threaded_price_queue.get() # {"network": , "token_id": , "block": }
            if not tmp == None and tmp["block"] != 0:
                # log query
                #logging.getLogger("special").debug(
                #    "      univ3 price call from queue of {}'s {} token for block {}".format(tmp["network"], tmp["token_id"], tmp["block"]))
                tmp["price"] = self.get_usd_price(network=tmp["network"], token_id=tmp["token_id"].lower(), block=tmp["block"], dequeue=False)
                if tmp["price"] != 0:
                    # add result to queue
                    self.threaded_price_results.put(tmp)
                    #logging.getLogger("special").debug(
                    #            "      threaded univ3 price added price {} to queue of {}'s {} token for block {}".format(tmp["price"], tmp["network"], tmp["token_id"], tmp["block"]))
           
        except:
            logging.getLogger(__name__).exception("Unexpected error while process_queue uniswap v3 price   .error: {}".format(sys.exc_info()[0]))

   ## THREAD  ##
    def start(self): 

        # define loop
        def _processQueueloop():
            
            # initialize 
            self.stop = False
            self.isAlive = True

            while not self.stop:
                try:
                    #_startime = dt.datetime.utcnow()
                    self.process_queue()
                    # calc time passed
                    #_timelapse = dt.datetime.utcnow() - _startime
                except:
                    logging.getLogger(__name__).exception("Unexpected error while threading uniswap v3 price cache queue process  .error: {}".format(sys.exc_info()[0]))

            logging.getLogger(__name__).debug(" uniswap v3 price cache queue thread exited ")

        # start loop
        self.on_open() # announce
        self.thread = threading.Thread(target=_processQueueloop, name="univ3_price_queue")
        self.thread.start()

    def on_open(self):
        logging.getLogger(__name__).debug("Starting uniswap v3 price cache queue thread")

    def close(self):
        """ close thread process
        """
        logging.getLogger(__name__).debug("Stopping uniswap v3 price cache queue thread")
        # i'm telling myself...
        self.stop = True
        # closing thread, if not already closed
        if not self.thread == None and not self.thread.ident == None:
            self.thread.join()
            self.thread = None
    
        # tell anyone that i'm dead (if they care)
        self.isAlive = False # brainer is finished

   ## HELPERS ##
    def __build_query(self, token_id:str, block=0):
        if block != 0:
            return """{{
                    token(
                        id: "{}"
                        block: {{number: {} }}
                    ) {{
                        id
                        derivedETH
                        totalValueLocked
                        totalValueLockedUSD
                        symbol
                    }}
                    _meta {{
                        block {{
                        number
                        timestamp
                        }}
                    }}
                    }}
            """.format(token_id, block)
        else:
            return """{{
                    token(
                        id: "{}"
                    ) {{
                        id
                        derivedETH
                        totalValueLocked
                        totalValueLockedUSD
                        symbol
                    }}
                    _meta {{
                        block {{
                        number
                        timestamp
                        }}
                    }}
                    }}
            """.format(token_id)
        
    def __url_constructor(self, network:str)->str:
        """ Build theGraph url

         Args:
            network (str): 

         Returns:
            str: url 
         """
        _URLS = {"ethereum":"https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
                "polygon":"https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-polygon",
                "optimism":"https://api.thegraph.com/subgraphs/name/ianlapham/optimism-post-regenesis",
                "arbitrum":"https://api.thegraph.com/subgraphs/name/ianlapham/arbitrum-minimal",
                "celo":"https://api.thegraph.com/subgraphs/name/jesse-sawa/uniswap-celo",
        }
        try:
            return _URLS[network]
        except:
            return ""

    def _dequeue_threaded_price_results(self):
        try:
            _startime = dt.datetime.utcnow()
            while(self.threaded_price_results.empty() == False):
  
                item = self.threaded_price_results.get()
                #  item = {"network": , "token_id": , "block":  ,"price":}
                if item["price"] > 0:
                    if not item["network"] in self.price_cache:
                        self.price_cache[item["network"]] = dict()
                    if not item["token_id"].lower() in self.price_cache[item["network"]]:
                        self.price_cache[item["network"]][item["token_id"].lower()] = dict()
                    # only save if u need to ... 
                    if not item["block"] in self.price_cache[item["network"]][item["token_id"].lower()] or self.price_cache[item["network"]][item["token_id"].lower()][item["block"]] == 0:
                        # save to cache
                        self.price_cache[item["network"]][item["token_id"].lower()][item["block"]] = item["price"]
       

                # exit loop after a minute processing...
                if (dt.datetime.utcnow()-_startime).total_seconds()>(60*1):
                    logging.getLogger(__name__).error(" univ3 price time to dequeue expired. Somethng is wrong here... ".format())
                    break
        except:
            pass

    def _price_special_case(self, address:str, network:str)->float:
        """ When returned price is zero, decide if can be manually set.
            on certain circumstances USDC or DAI can be 1 usd easy... 

         Args:
            address (str): contract address ( token)

         Returns:
            float: price

         """
        try:
            if address.lower() == "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1".lower() and network=="optimism": # DAI in optimism
                return 1
        except:
            pass

        return 0


    def get_common_prices(self):
        pass
        # TODO: retrieve USDC DAI ETH prices at all possible blocks


