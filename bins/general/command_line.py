import sys
import argparse


def parse_commandLine_args():

    par_main = argparse.ArgumentParser(
        prog="tool_me.py", description=" Gamma tools ", epilog=""
    )

    par_main.add_argument(
        "-c", "--config", type=str, help="load custom configuration .yaml file"
    )

    par_main.add_argument(
        "--log_subfolder", type=str, help="specify a subfolder log name to save logs to"
    )

    # exclusive group
    exGroup = par_main.add_mutually_exclusive_group()

    # manual database feed
    par_db_feed = exGroup.add_argument(
        "--db_feed",
        choices=["operations", "status", "static", "prices", "user_status"],
        help="feed database",
    )

    # auto database feed service
    par_service = exGroup.add_argument(
        "--service",
        choices=["local", "global"],
        help=" execute an infinite loop service",
    )
    par_network_service = exGroup.add_argument(
        "--service_network",
        choices=["ethereum", "polygon", "optimism", "arbitrum", "celo"],
        help=" infinite loop service for the specified network using the Gamma protocol",
    )

    # checks
    par_check = exGroup.add_argument(
        "--check",
        choices=["prices"],
        help=" execute checks ",
    )

    # analysis
    par_analysis = exGroup.add_argument(
        "--analysis",
        choices=["ethereum", "optimism", "polygon", "arbitrum"],
        help=" execute analysis ",
    )

    # debug
    par_main.add_argument(
        "--debug",
        help=" debug mode",
    )

    # datetimes
    par_main.add_argument(
        "--ini_datetime",
        type=str,
        help="specify an initial datetime: format Y-m-dTH:M:S ",
    )
    par_main.add_argument(
        "--end_datetime",
        type=str,
        help="specify an ending datetime: format Y-m-dTH:M:S ",
    )

    # print helpwhen no command is passed
    return par_main.parse_args(args=None if sys.argv[1:] else ["--help"])
