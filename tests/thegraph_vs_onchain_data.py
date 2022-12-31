import sys
import os
import datetime as dt
import logging
from web3 import Web3
from pathlib import Path
import tqdm
import concurrent.futures


# append parent directory pth
CURRENT_FOLDER = os.path.dirname(os.path.realpath(__file__))
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
sys.path.append(PARENT_FOLDER)


from bins.apis import etherscan_utilities, thegraph_utilities
from bins.cache import cache_utilities
from bins.general import net_utilities, file_utilities,general_utilities
from bins.w3 import  onchain_utilities
from bins.log import log_helper




# Compare GAMMA subgraph data vs GAMMA direct chain queries  (using same block)
    # - liquidity is all GOOD(equal) with one exception (WBTC	renBTC 500)
    # - feeGrowthGlobalX128 is all GOOD(equal)
    # - upper and lower ticks are all GOOD(equal)
    
    # - tokensOwed are predominantly BAD(different) but seem to have repeated values spread thru all hyervisors.  in web3 are taken from univ3 pool contract positions field, so should be straight forward.
    # - feeGrowthInsideLastX128 have two error differences:  
    #               WBTC  renBTC 500
    #               agEUR USDC   100
    # - feeGrowthOutsideX128 is predominantly BAD(different): sourced thru a direct univ3 contract call ( no formulas involved)
def test_thegraph_vs_onchain_data_fees(configuration:dict, block:int=0, threaded:bool=False):
    """ Compares subgraph data vs direct chain queries
        Using a web3 provider and a subgraph endpoint, compare both at fees level:  
            all fields involved in uncollected fees, including owed fees

        precision: A fixed block (latest - 50) is used.

     Args:
        configuration (dict): config.yaml
        block (int): force block number. when 0, latest block - 50 is chosen
        threaded (bool): Try execute as threaded to diminish processing time

     """
    
    # Setup vars
    protocol = "gamma"
    network = "ethereum"
    web3Provider_url = configuration["sources"]["web3Providers"][network]
    csv_filename = "{}_test_thegraph_vs_onchain_data_fees{}.csv".format(network, "_threaded" if threaded else "")
    csv_filename = os.path.join(CURRENT_FOLDER,csv_filename)

    # setup thegraph helpers
    uniswapv3_helper = thegraph_utilities.uniswapv3_scraper(cache=True, cache_savePath="data/cache", convert=True)
    gamma_helper = thegraph_utilities.gamma_scraper(cache=True, cache_savePath="data/cache", convert=True)

    # setup onchain dta provider
    web3Provider = Web3(Web3.HTTPProvider(web3Provider_url, request_kwargs={'timeout': 120})) 
    # add middleware as needed
    if network != "ethereum":
        web3Provider.middleware_onion.inject(geth_poa_middleware, layer=0)

    # get a block to pivot from: DUE TO THEGRAPH BEING X blocks behind, lets substract 50 just ic
    block = (web3Provider.eth.get_block('latest').number - 50) if block == 0 else block


    # setup result data var
    result = list()

    # get a list of hypervisors from thegraph
    hypervisors = gamma_helper.get_all_results(network=network, query_name="uniswapV3Hypervisors", block=""" number:{} """.format(block))

    
    # keep a progress bar ()
    with tqdm.tqdm(total=len(hypervisors)) as progress_bar:

        if not threaded:
            # loop thru
            for hypervisor in hypervisors:
                # create status name
                hypervisor_name = "{} {}'s {}-{} ({:,.2%}) ".format(
                            protocol, network,
                            hypervisor["pool"]["token0"]["symbol"],hypervisor["pool"]["token1"]["symbol"],
                            int(hypervisor["pool"]["fee"])/100000
                    )

                # progress
                progress_bar.set_description("processing {}".format(hypervisor_name))

                # add to result
                result.append(do_loop_work(hypervisor, web3Provider, uniswapv3_helper, block, protocol, network))

                # update progress
                progress_bar.update(1)
        else:
            # threaded
            args = ((hypervisor, web3Provider, uniswapv3_helper, block, protocol, network) for hypervisor in hypervisors)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                for result_item in ex.map(lambda p: do_loop_work(*p), args):
                    # progress
                    progress_bar.set_description("processed {}-{} {}".format(result_item["token0"],result_item["token1"],result_item["fee"]))
                    # append to resutl
                    result.append(result_item)
                    # update progress
                    progress_bar.update(1)


    # sort keys 
    sorted_keys = list()
    item_keys = list(result[0].keys())
    for k in item_keys:


        k_items = k.split("_")

        if not len(k_items) >= 3:
            # block , hypervisor, etc..
            if not k in sorted_keys:
                sorted_keys.append(k)
        else:
            # k[0] # base or limit
            # k[1] # graph or w3 or comparison
            var3 = "_".join(k_items[2:])
            # add all possibilities
            for var1 in ["base", "limit"]:
                for var2 in ["graph","w3","comparison"]:
                    nobj = "{}_{}_{}".format(var1,var2,var3)
                    if not nobj in sorted_keys:
                        sorted_keys.append(nobj)
    # remove file
    try:
        os.remove(csv_filename)
    except:
        pass
    # save result to csv file
    file_utilities.SaveCSV(filename=csv_filename, columns=sorted_keys, rows=result)


def do_loop_work(hypervisor, web3Provider, uniswapv3_helper, block, protocol, network):

    # create status name
    hypervisor_name = "{} {}'s {}-{} ({:,.2%}) ".format(
                protocol, network,
                hypervisor["pool"]["token0"]["symbol"],hypervisor["pool"]["token1"]["symbol"],
                int(hypervisor["pool"]["fee"])/100000
        )


    ## WEB3 INFO ## 
    # get onchain data
    gamma_web3Helper = onchain_utilities.gamma_hypervisor(address=hypervisor["id"], web3Provider=web3Provider)
    # fix the block num. on all queries from now on
    gamma_web3Helper.block = block

    ## THEGRAPH INFO ###
    # general vars
    decimals_token0 = int(hypervisor["pool"]["token0"]["decimals"])
    decimals_token1 = int(hypervisor["pool"]["token1"]["decimals"])

    # get uniswap pool and tick info  [ feeGrowthGlobal0X128 and feeGrowthGlobal1X128]
    univ3_pool = uniswapv3_helper.get_all_results(network=network, query_name="pools", where=""" id: "{}" """.format(hypervisor["pool"]["id"]), block=""" number:{} """.format(block))[0]
    # create an array of ticks 
    ticks = [
            int(hypervisor["baseUpper"]), 
            int(hypervisor["baseLower"]),
            int(hypervisor["limitUpper"]),
            int(hypervisor["limitLower"]),
        ]
    # get all ticks [feeGrowthOutside1X128 and feeGrowthOutside0X128]
    univ3_ticks = uniswapv3_helper.get_all_results(network=network, query_name="ticks", where=""" tickIdx_in: {}, poolAddress: "{}" """.format(ticks, hypervisor["pool"]["id"]), block=""" number:{} """.format(block))
    # classify univ3 ticks in a dict
    feeGrowthOutside = dict()
    for tick in univ3_ticks:
        if not int(tick["tickIdx"]) in feeGrowthOutside:
            feeGrowthOutside[int(tick["tickIdx"])] = {"feeGrowthOutside0X128":tick["feeGrowthOutside0X128"], "feeGrowthOutside1X128":tick["feeGrowthOutside1X128"]}
    

    # create a res item to fill with comparison data
    result_item = dict()

    # loop thu both positions to create a result item ( liquidity, ticks, feeGrowths and tokens owed fields)
    for pos in ["base","limit"]:

        # set the graph data fields
        typname = "graph"

        result_item["{}_{}_liquidity".format(pos,typname)] = hypervisor["{}Liquidity".format(pos)]
        result_item["{}_{}_lowerTick".format(pos,typname)] = int(hypervisor["{}Lower".format(pos)])
        result_item["{}_{}_upperTick".format(pos,typname)] = int(hypervisor["{}Upper".format(pos)])

        result_item["{}_{}_feeGrowthGlobal0X128".format(pos,typname)] = univ3_pool["feeGrowthGlobal0X128"]
        result_item["{}_{}_feeGrowthGlobal1X128".format(pos,typname)] = univ3_pool["feeGrowthGlobal1X128"]

        for y in ["lower","upper"]:
            for i in [0,1]:
                try:
                    result_item["{}_{}_feeGrowthOutside{}X128_{}".format(pos,typname,i,y)] = feeGrowthOutside[result_item["{}_{}_{}Tick".format(pos,typname,y)]]["feeGrowthOutside{}X128".format(i)] if len(feeGrowthOutside)>0 else 0
                except KeyError:
                    # : one of hypervisor's tick is not present at uniswapv3 subgraph when querying for it, but it is showing up in gamma's subgraph...
                    logging.getLogger(__name__).error("{} tick {} is not present in uniswap's subgraph using <tickIdx_in: {}, poolAddress:{} > in the query.".format(hypervisor_name, result_item["{}_{}_{}Tick".format(pos,typname,y)], ticks, hypervisor["pool"]["id"]))
                    result_item["{}_{}_feeGrowthOutside{}X128_{}".format(pos,typname,i,y)] = 0

        result_item["{}_{}_feeGrowthInsideLast0X128".format(pos,typname)] = hypervisor["{}FeeGrowthInside0LastX128".format(pos)]
        result_item["{}_{}_feeGrowthInsideLast1X128".format(pos,typname)] = hypervisor["{}FeeGrowthInside1LastX128".format(pos)]

        result_item["{}_{}_tokensOwed0".format(pos,typname)] = hypervisor["{}TokensOwed0".format(pos)]
        result_item["{}_{}_tokensOwed1".format(pos,typname)] = hypervisor["{}TokensOwed1".format(pos)]


        # set onchain data fields
        #       preparation
        w3position = gamma_web3Helper.getBasePosition if pos == "base" else gamma_web3Helper.getLimitPosition
        w3tickUpper = gamma_web3Helper.baseUpper if pos == "base" else gamma_web3Helper.limitUpper
        w3tickLower = gamma_web3Helper.baseLower if pos == "base" else gamma_web3Helper.limitLower
        w3pool_position = gamma_web3Helper.pool.position(ownerAddress=Web3.toChecksumAddress(gamma_web3Helper.address.lower()),
                                                            tickLower=w3tickLower,
                                                            tickUpper=w3tickUpper)
        w3Ticks_lower = gamma_web3Helper.pool.ticks(w3tickLower)
        w3Ticks_upper = gamma_web3Helper.pool.ticks(w3tickUpper)

        #       data
        typname = "w3"

        result_item["{}_{}_liquidity".format(pos,typname)] = w3position["liquidity"]
        result_item["{}_{}_lowerTick".format(pos,typname)] = w3tickLower
        result_item["{}_{}_upperTick".format(pos,typname)] = w3tickUpper

        result_item["{}_{}_feeGrowthGlobal0X128".format(pos,typname)] = gamma_web3Helper.pool.feeGrowthGlobal0X128
        result_item["{}_{}_feeGrowthGlobal1X128".format(pos,typname)] = gamma_web3Helper.pool.feeGrowthGlobal1X128

        result_item["{}_{}_feeGrowthOutside0X128_lower".format(pos,typname)] = w3Ticks_lower["feeGrowthOutside0X128"]
        result_item["{}_{}_feeGrowthOutside1X128_lower".format(pos,typname)] = w3Ticks_lower["feeGrowthOutside1X128"]
        result_item["{}_{}_feeGrowthOutside0X128_upper".format(pos,typname)] = w3Ticks_upper["feeGrowthOutside0X128"]
        result_item["{}_{}_feeGrowthOutside1X128_upper".format(pos,typname)] = w3Ticks_upper["feeGrowthOutside1X128"]

        result_item["{}_{}_feeGrowthInsideLast0X128".format(pos,typname)] = w3pool_position["feeGrowthInside0LastX128"]
        result_item["{}_{}_feeGrowthInsideLast1X128".format(pos,typname)] = w3pool_position["feeGrowthInside1LastX128"]

        result_item["{}_{}_tokensOwed0".format(pos,typname)] = w3pool_position["tokensOwed0"]
        result_item["{}_{}_tokensOwed1".format(pos,typname)] = w3pool_position["tokensOwed1"]


    # compare all fields (% change -->   [theGraph - onChain / theGraph ]  )
    keys_loop = list(set([x.replace("base_","").replace("limit_","").replace("w3_","").replace("graph_","") for x in result_item.keys()]))
    for k in keys_loop:
        for pos in ["base","limit"]:
            result_item["{}_comparison_{}".format(pos,k)] =  ((result_item["{}_graph_{}".format(pos,k)]-result_item["{}_w3_{}".format(pos,k)])/result_item["{}_graph_{}".format(pos,k)]) if result_item["{}_graph_{}".format(pos,k)] != 0 else 0


    # add general hypervisor info to result item
    appendableItem = {
        "block":block,
        "hypervisor_id": hypervisor["id"],
        "pool_id":hypervisor["pool"]["id"],
        "token0":hypervisor["pool"]["token0"]["symbol"],
        "token1":hypervisor["pool"]["token1"]["symbol"],
        "fee":hypervisor["pool"]["fee"]          
        }
    appendableItem.update(result_item) # merge

    return appendableItem







# START ####################################################################################################################
if __name__ == "__main__":
    os.chdir(PARENT_FOLDER)
    # convert command line arguments to dict variables
    cml_parameters = general_utilities.convert_commandline_arguments(sys.argv[1:])
    # load configuration
    configuration = general_utilities.load_configuration(cfg_name=cml_parameters["config_file"]) if "config_file" in cml_parameters else general_utilities.load_configuration()
    # check configuration
    general_utilities.check_configuration_file(configuration)
    # setup logging
    log_helper.setup_logging(customconf=configuration)
    # add cml_parameters into loaded config ( this is used later on to load again the config file to be able to update on-the-fly vars)
    if not "_custom_" in configuration.keys():
        configuration["_custom_"] = dict()
    configuration["_custom_"]["cml_parameters"] = cml_parameters
    ##### main ######
    __module_name = Path(os.path.abspath(__file__)).stem
    logging.getLogger(__name__).info(" Start {}   ----------------------> ".format(__module_name))
    # start time log
    _startime = dt.datetime.utcnow()


    test_thegraph_vs_onchain_data_fees(configuration=configuration,block=16285148,threaded=True)


    # end time log
    _timelapse = dt.datetime.utcnow() - _startime
    logging.getLogger(__name__).info(" took {:,.2f} minutes to complete".format(_timelapse.total_seconds()/60))
    logging.getLogger(__name__).info(" Exit {}    <----------------------".format(__module_name))




