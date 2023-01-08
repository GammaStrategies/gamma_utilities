
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


from bins.w3 import protocol_comparator
from bins.general import general_utilities, file_utilities
from bins.log import log_helper



def test_comparator_phase1(configuration):

    protocols = list(configuration["script"]["protocols"].keys())

    for protocol in protocols:

        helper = protocol_comparator.comparator_v1(configuration=configuration, protocol=protocol)
        networks = configuration["script"]["protocols"][protocol]["networks"]
        filters = configuration["script"]["protocols"][protocol]["filters"]
        output = configuration["script"]["protocols"][protocol]["output"]

        for network in list(networks.keys()):

            # apply filters
            if "hypervisors_included" in filters.keys() and network in filters["hypervisors_included"].keys():
                # set script to only scrape those addresses 
                addresses = filters["hypervisors_included"][network]
            else:
                # scrape for hypervisor addresses
                # TODO: scrape either onchain data or thegraph 
                raise NotImplemented(" ERR ... many pending TODOs ")
           
            # set current working folder
            current_folder = os.path.join(configuration["script"]["protocols"][protocol]["output"]["files"]["save_path"], protocol, network)
            
            with tqdm.tqdm(total=100) as progress_bar:
                # create callback progress funtion
                def _update_progress(text, remaining=None, total=None):
                            progress_bar.set_description(text)
                            # set total
                            if total:
                                progress_bar.total = total
                            # update current
                            if remaining:                   
                                progress_bar.update(((total-remaining)-progress_bar.n))
                            else:
                                progress_bar.update(1)
                            # refresh 
                            progress_bar.refresh()                    

                # try load files if exist
                main_frame = None
                try:
                    main_frame = load_files(folder_path=current_folder, addresses=addresses, network=network, progress_callback=_update_progress)
                    
                    # TODO: check if loaded files datetimes are inside the configured force_timeframe field, if exists
                    # if "force_timeframe" in filters.keys():
                    #     # 
                    #     start_timestamp = dt.datetime.timestamp(dt.datetime.strptime(filters["force_timeframe"]["start_time"], "%Y-%m-%dT%H:%M:%S"))
                    #     end_timestamp = dt.datetime.timestamp(dt.datetime.strptime(filters["force_timeframe"]["end_time"], "%Y-%m-%dT%H:%M:%S"))
                    #     _mfkeys = list(main_frame.keys())
                    #     for hypervisor_id in _mfkeys:
                    #         timestamp_list = [x["timestamp"] for x in main_frame[hypervisor_id][operation] for operation in ["deposits","withdraws","rebalances","fees"]]


                    # make sure all addresses have been loaded
                    if len(main_frame.keys()) != len(addresses):
                        # not all addresses are loaded. Find those not loaded and scrape m
                        addresses_left = general_utilities.differences(list(main_frame.keys()), addresses)
                        # scrape addresses left
                        partial_frame = helper.create_collection(addresses=addresses_left, network=network, progress_callback=_update_progress)
                        # add em to main_frame
                        main_frame.update(partial_frame)
                except FileNotFoundError:
                    # no file to be loaded
                    pass
                except:
                    logging.getLogger(__name__).exception(" Unexpected error loading json information for {}'s {}      .error: {}".format( protocol, network, sys.exc_info()[0]))
                    
                
                # scrape all 
                if main_frame == None:
                    main_frame = helper.create_collection(addresses=addresses, network=network, progress_callback=_update_progress)

                # 
                rem_progress= len(main_frame.keys())
                tot_progress = len(main_frame.keys())
                for hypervisor_id,hypervisor in main_frame.items(): 
                    # static info
                    try:
                        # progress
                        _update_progress(text=" getting {} {}'s {} hypervisor static info".format(protocol, network, hypervisor_id), remaining=rem_progress, total=tot_progress)

                        hypervisor["static"] = helper.create_static(address=hypervisor_id, block=0, network=network)
                    except:
                        logging.getLogger(__name__).exception(" Unexpected error creating static information for {}'s {} hypervisor {}      .error: {}".format( protocol, network, hypervisor_id, sys.exc_info()[0]))
                    
                    # status historic [ block ini --> end]
                    try:
                        # progress
                        _update_progress(text=" getting {} {}'s {} hypervisor status info".format(protocol, network, hypervisor_id), remaining=rem_progress, total=tot_progress)

                        #hypervisor["status"] = helper.create_status_autoblocks(address=hypervisor_id, network=network, progress_callback=_update_progress)
                        hypervisor["status"] = helper.create_status_fromOPS(hypervisor=hypervisor, progress_callback=_update_progress)
                    except:
                        logging.getLogger(__name__).exception(" Unexpected error creating historic status information for {}'s {} hypervisor {}      .error: {}".format( protocol, network, hypervisor_id, sys.exc_info()[0]))
                    
                    #populate all status
                    try:
                        # progress
                        _update_progress(text=" getting {} {}'s {} hypervisor status extra info".format(protocol, network, hypervisor_id), remaining=rem_progress, total=tot_progress)

                        helper.populate_all_status(hypervisor=hypervisor,progress_callback=_update_progress)
                    except:
                        logging.getLogger(__name__).exception(" Unexpected error populating all status complementary information for {}'s {} hypervisor {}      .error: {}".format( protocol, network, hypervisor_id, sys.exc_info()[0]))
                    
                     #build chart
                    try:
                        # progress
                        _update_progress(text=" creating {} {}'s {} hypervisor chart".format(protocol, network, hypervisor_id), remaining=rem_progress, total=tot_progress)

                        hypervisor["chart"] = helper.create_chart(hypervisor=hypervisor, progress_callback=_update_progress)
                    except:
                        logging.getLogger(__name__).exception(" Unexpected error building chart data for {}'s {} hypervisor {}      .error: {}".format( protocol, network, hypervisor_id, sys.exc_info()[0]))
                    

                    # save hypervisor data to file
                    file_utilities.save_json(filename=hypervisor_id.lower(), data=hypervisor, folder_path=current_folder)

                    # progress update 
                    rem_progress -=1


        helper = networks = filters = None
        
        
def load_files(folder_path:str, addresses:list, network:str, progress_callback=None)->dict:
    """   Load files specified to dict

     Args:
        folder (str): full path to json files
        addresses (str): address list to load. place an empty list for any
        network (str): network name ( mainnet=ethereum)
        progress_callback (func, optional): _description_. Defaults to None.

     Returns:
        dict: hypervisor list as { <hipervisor id>: <template oftype hypervisor>}
     """
    result = dict()

    _addresses = [x.lower() for x in addresses]
    # list of files sorted by date ( oldest first )
    file_list = sorted([f for f in os.scandir(folder_path) if (f.is_file() and f.name.startswith("0x"))], key=os.path.getmtime, reverse=False)
    for file in file_list:
        fname = file.name.split(".")[0]
        # only load addresses specified
        if len(_addresses) == 0 or fname.lower() in _addresses:
            
            # load file ( template oftype root)
            tmp_data = file_utilities.load_json(filename=file.name.split(".")[0], folder_path=folder_path)
            
            # add loaded data to result
            if not tmp_data["static"]["id"] in result:
                result[tmp_data["static"]["id"]] = tmp_data

    
    return result




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


    test_comparator_phase1(configuration=configuration)
    


    # end time log
    _timelapse = dt.datetime.utcnow() - _startime
    logging.getLogger(__name__).info(" took {:,.2f} seconds to complete".format(_timelapse.total_seconds()))
    logging.getLogger(__name__).info(" Exit {}    <----------------------".format(__module_name))


