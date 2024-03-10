from apps.repair.prices.database import repair_prices_from_database
from apps.repair.prices.logs import repair_prices_from_logs
from apps.repair.prices.status import repair_prices_from_status
from bins.configuration import CONFIGURATION


#####
def repair_prices(min_count: int = 1):
    repair_prices_from_logs(min_count=min_count, add_to_queue=True)

    repair_prices_from_status(
        max_repair_per_network=CONFIGURATION["_custom_"]["cml_parameters"].maximum
        or 500
    )

    repair_prices_from_database(
        max_repair_per_network=CONFIGURATION["_custom_"]["cml_parameters"].maximum or 50
    )


####
