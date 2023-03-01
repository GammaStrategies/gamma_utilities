import sys
import argparse


def parse_commandLine_args():

    par_main = argparse.ArgumentParser(
        prog="tool_me.py", description=" Gamma tools ", epilog=""
    )
    # parser.add_argument("square", type=int,
    #                     help="display a square of a given number")
    par_main.add_argument(
        "-c", "--config", type=str, help="load custom configuration .yaml file"
    )

    # exclusive group
    exGroup = par_main.add_mutually_exclusive_group()

    # manual database feed
    par_db_feed = exGroup.add_argument(
        "--db_feed",
        choices=["operations", "status", "static", "prices"],
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

    # print helpwhen no command is passed
    return par_main.parse_args(args=None if sys.argv[1:] else ["--help"])
