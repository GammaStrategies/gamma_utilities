from decimal import Decimal
import logging
from bins.general.enums import Chain
from bins.general.net_utilities import get_response


def get_csv_analytics_data_from_endpoint(
    chain: Chain,
    hypervisor_address: str,
    period: str | int,
    domain: str = "wire2.gamma.xyz",
) -> list[dict]:
    """Get analytics data from gamma endpoint"""

    _url = f"https://{domain}/frontend/analytics/returns/csv?hypervisor_address={hypervisor_address}&chain={chain.database_name}&period={period}"

    try:
        result = get_response(
            url=_url,
        ).text.split("\n")
        # filter
        if result[-1] == "":
            result = result[:-1]
        # convert csv string list to list of dictionaries
        result = [dict(zip(result[0].split(","), x.split(","))) for x in result[1:]]

        # convert string types to int, float or keep as string
        for row in result:
            for key, value in row.items():
                if key in ["timestamp", "timestamp_from", "block", "period_seconds"]:
                    row[key] = int(value)
                elif key in [
                    "chain",
                    "address",
                    "name",
                    "protocol",
                    "dex",
                    "symbol",
                    "datetime_from",
                    "datetime_to",
                ]:
                    row[key] = value
                else:
                    try:
                        row[key] = Decimal(value)
                    except:
                        pass

    except Exception as e:
        logging.getLogger(__name__).exception(
            f" Unexpected error while getting analytics csv data"
        )
    return result
