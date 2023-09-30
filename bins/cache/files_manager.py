import logging
from bins.configuration import CONFIGURATION
from bins.general.file_utilities import get_files, load_json, save_json


def reset_cache_files():
    """Reset the cache files leaving the item with more keys ( first one )
    so that inmutable fields are not deleted
    """
    # load all cache files, one by one, and search for value
    folder_path = (
        CONFIGURATION.get("cache", {}).get("save_path", "data/cache") + "/onchain"
    )
    logging.getLogger(__name__).info(f" Resetting cache files from {folder_path}")
    for file in get_files(path=folder_path):
        try:
            # load file
            file_json = load_json(filename=file.split(".")[0], folder_path=folder_path)
            new_file_json = {}

            for chain_id, hypervisors in file_json.items():
                new_file_json[chain_id] = {}
                for hypervisor_address, blocks in hypervisors.items():
                    new_file_json[chain_id][hypervisor_address] = {}
                    _temp_block = 0
                    _temp_hype_data = {}
                    for block, hypervisor in blocks.items():
                        # loop thru blocks finding the one with more keys
                        if len(hypervisor.keys()) > len(_temp_hype_data.keys()):
                            _temp_hype_data = hypervisor
                            _temp_block = block

                    # set the one with more keys to the result
                    new_file_json[chain_id][hypervisor_address] = {
                        _temp_block: _temp_hype_data
                    }

            # save file
            logging.getLogger(__name__).debug(
                f" Resetting cache file {file} from {folder_path}"
            )
            save_json(
                filename=file.split(".")[0], folder_path=folder_path, data=new_file_json
            )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Can't reset cache file {file} from {folder_path}"
            )
