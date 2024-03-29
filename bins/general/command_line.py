import sys
import argparse

from ..general.enums import Chain, Protocol, queueItemType


# validations
class ValidateNetworks(argparse.Action):
    def __call__(self, parser, args, networks, option_string=None):
        valid_subjects = [x.database_name for x in Chain]

        # modify only if not empty
        if result := [
            network
            for item in networks
            for network in item.split(" ")
            if network in valid_subjects
        ]:
            setattr(args, self.dest, result)


class ValidateProtocols(argparse.Action):
    def __call__(self, parser, args, protocols, option_string=None):
        valid_subjects = [x.database_name for x in Protocol]

        # modify only if not empty
        if result := [
            protocol
            for item in protocols
            for protocol in item.split(" ")
            if protocol in valid_subjects
        ]:
            setattr(args, self.dest, result)


class ValidateQueueTypes(argparse.Action):
    def __call__(self, parser, args, queue_types, option_string=None):
        valid_subjects = [x.value for x in queueItemType]

        # modify only if not empty
        if result := [
            queueItemType[queue_type.upper()]
            for item in queue_types
            for queue_type in item.split(" ")
            if queue_type.lower() in valid_subjects
        ]:
            setattr(args, self.dest, result)


class ValidateAddresses(argparse.Action):
    def __call__(self, parser, args, addresses, option_string=None):
        # modify only if not empty
        if result := [
            address.lower()
            for item in addresses
            for address in item.split(" ")
            if address[:2] == "0x" and len(address) == 42
        ]:
            setattr(args, self.dest, result)


def parse_commandLine_args():
    # main parsers
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
        choices=[
            "operations",
            "status",
            "static",
            "static_feeRecipients",
            "prices",
            "user_status",
            "queue",
            "static_hypervisors",
            "static_rewards",
            "frontend_revenue_stats",
            "returns",
            "global_reports",
            # latest
            "latest_returns",
            "latest_prices",
            "latest_multifeedistributor",
            "create_json_prices",
            "create_price_paths_json",
            ##
        ],
        help="feed database",
    )

    # auto database feed service
    par_service = exGroup.add_argument(
        "--service",
        choices=[
            "local",
            "global",
            "queue",
            "operations",
            "current_prices",
            "latest",
            "revenue_operations",
        ],
        help=" execute an infinite loop service",
    )
    par_network_service = exGroup.add_argument(
        "--service_network",
        choices=[Chain.database_name for Chain in Chain],
        help=" infinite loop service for the specified network using the Gamma protocol",
    )

    # checks
    par_check = exGroup.add_argument(
        "--check",
        choices=[
            "prices",
            "analytics",
            "all",
            "hypervisor_status",
            "queue",
            "reward_status",
            "operations",
            "analytics_telegram",
            "revenue_telegram",
            "prices_telegram",
        ],
        help=" execute checks",
    )
    # repairs
    par_repair = exGroup.add_argument(
        "--repair",
        choices=[
            "prices",
            "database",
            "all",
            "hypervisor_status",
            "queue",
            "reward_status",
            "operations",
            "returns",
            "special",
        ],
        help=" execute repairs",
    )

    # analysis
    par_analysis = exGroup.add_argument(
        "--analysis",
        choices=["user", "network", "queue", "benchmark_logs", "user_deposits"],
        help=" execute analysis",
    )

    par_rescrape = exGroup.add_argument(
        "--rescrape",
        choices=[
            "all",
            "status",
            "rewards_status",
            "status_fees_collected",
            "revenue_operations",
            "queue",
        ],
        help=" execute a rescraping of the current database items",
    )

    # tests
    par_test = exGroup.add_argument(
        "--test",
        choices=["protocols", "hypervisors"],
        help=" execute tests ",
    )

    # debug
    par_main.add_argument(
        "--debug",
        action="store_true",
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
    par_main.add_argument(
        "--ini_block",
        type=int,
        help="specify an initial block",
    )
    par_main.add_argument(
        "--end_block",
        type=int,
        help="specify an ending block ",
    )
    par_main.add_argument(
        "--user_address",
        type=str,
        help="specify a user address to be analyzed",
    )
    par_main.add_argument(
        "--hypervisor_address",
        type=str,
        help="specify a hypervisor address to be analyzed",
    )

    # TODO: replace hypervisor_address and user_address with XXXX_addresses
    par_main.add_argument(
        "--hypervisor_addresses",
        action=ValidateAddresses,
        nargs="+",
        help="specify a list of addresses to be processed. Enclose within ' ' separator being an empty space ",
    )

    par_main.add_argument(
        "--do_prices",
        action="store_true",
        help=" execute prices analysis on service network",
    )
    par_main.add_argument(
        "--maximum",
        type=int,
        help=" Set a maximum number of x to be executed. When more than <maximum> items are found to process, only <maximum> will be processed",
    )
    par_main.add_argument(
        "--do_userStatus",
        action="store_true",
        help=" execute the user status feed inside the network feed loop",
    )
    par_main.add_argument(
        "--do_repairs",
        action="store_true",
        help=" execute auto error repair inside the network feed loop",
    )
    par_main.add_argument(
        "--networks",
        action=ValidateNetworks,
        nargs="+",
        # type=str,
        help=" specify a list of networks to be processed. Enclose within ' ' separator being an empty space ",
    )
    par_main.add_argument(
        "--protocols",
        action=ValidateProtocols,
        nargs="+",
        # type=str,
        help=" specify a list of protocols to be processed. Enclose within ' ' separator being an empty space ",
    )
    par_main.add_argument(
        "--min_loop_time",
        type=int,
        help=" specify the minimum number of minutes the loop should cost in order to start again",
    )

    par_main.add_argument(
        "--check_logs",
        type=str,
        nargs="+",
        help=" specify the files or folders where log files to be included in check and repair process are stored",
    )
    par_main.add_argument(
        "--rewrite",
        action="store_true",
        help=" rewrite information in database",
    )
    # queue type
    par_main.add_argument(
        "--queue_types",
        action=ValidateQueueTypes,
        nargs="+",
        help=" Types of queue to process ",
    )
    par_main.add_argument(
        "--queue_count",
        type=int,
        help=" queue count gte to process ",
    )
    par_main.add_argument(
        "--max_queue_count",
        type=int,
        help=" queue count lte to process ",
    )

    par_main.add_argument(
        "--queue_level",
        type=int,
        help=" queue item's count variable number to process first",
    )

    par_main.add_argument(
        "--save_config",
        type=str,
        help=" Specify a yaml configuration file to be saved to database",
    )

    par_main.add_argument(
        "--donot_enforce_contract_creation",
        action="store_true",
        help=" Do not enforce static hypervisor info to have contract creation block and timestamp. When true, if not found, current block and timestamp will be used",
    )

    # telegram config #####
    par_main.add_argument(
        "--telegram_token",
        type=str,
        help=" Telegram bot token to send messages",
    )
    par_main.add_argument(
        "--telegram_chat_id",
        type=str,
        help=" Telegram chat id to send messages",
    )

    par_main.add_argument(
        "--send_to_telegram",
        action="store_true",
        help=" send defined output to telegram (analysis)",
    )

    # print helpwhen no command is passed
    return par_main.parse_args(args=None if sys.argv[1:] else ["--help"])


def parse_commandLine_args_2work():
    # main parsers
    par_main = argparse.ArgumentParser(
        prog="tool_me.py", description=" Gamma tools ", epilog=""
    )

    par_main.add_argument(
        "-c", "--config", type=str, help="load custom configuration .yaml file"
    )

    par_main.add_argument(
        "--log_subfolder", type=str, help="specify a subfolder log name to save logs to"
    )

    # subparsers
    subparsers = par_main.add_subparsers(help="Sub-commands help")

    # subparser: manual database feed
    par_db_feed = subparsers.add_parser(
        "db_feed", help=" one time feed database with specific data"
    )
    par_db_feed.add_argument(
        "--type",
        "-t",
        choices=[
            "operations",
            "status",
            "static",
            "prices",
            "user_status",
            "rewards",
            "impermanent_v1",
        ],
        help="feed database",
    )

    # subparser: auto database feed service
    par_service = subparsers.add_parser(
        "service", help=" execute an infinite loop global service"
    )
    par_service_exclusive = par_service.add_mutually_exclusive_group()
    par_service_exclusive.add_argument(
        "--type",
        "-t",
        choices=["local", "global"],
        help=" execute an infinite loop service",
    )
    par_service_exclusive.add_argument(
        "--network",
        "-n",
        choices=["ethereum", "polygon", "optimism", "arbitrum", "binance"],
        help=" execute an infinite loop service for the specified network",
    )

    # subparser: checks
    par_check = subparsers.add_parser(
        "check", help=" database problems identifier and solver"
    )
    par_check.add_argument(
        "--type", "-t", choices=["prices", "database"], help=" execute checks "
    )

    # subparser: analysis
    par_analysis = subparsers.add_parser("analysis", help=" execute analysis")
    # par_analysis_exclusive = par_analysis.add_mutually_exclusive_group()
    par_analysis.add_argument(
        "--type",
        "-t",
        choices=["user", "network"],
        required=True,
        help=" analysis scope ",
    )

    main_args, _ = par_main.parse_known_args()
    parser_step2 = argparse.ArgumentParser(parents=[par_main])
    if main_args.user:
        analysis_user(parser_step2)
    elif main_args.network:
        analysis_network(parser_step2)
    analysis_common(parser_step2)

    # print helpwhen no command is passed
    return par_main.parse_args(args=None if sys.argv[1:] else ["--help"])


def analysis_user(argument_parser: argparse.ArgumentParser):
    argument_parser.add_argument(
        "--user_address",
        "-a",
        required=True,
        help=" user address to execute the analysis ",
    )
    # argument_parser.add_argument(
    #     "--hypervisor_address",
    #     "-h",
    #     required=False,
    #     help=" hypervisor address to execute the analysis ",
    # )


def analysis_network(argument_parser: argparse.ArgumentParser):
    argument_parser.add_argument(
        "--network",
        "-n",
        choices=["ethereum", "polygon", "optimism", "arbitrum", "binance"],
        required=True,
        help=" network to execute the analysis ",
    )

    argument_parser.add_argument(
        "--hypervisor_address",
        "-a",
        required=False,
        help=" hypervisor address to execute the analysis ",
    )


def analysis_common(argument_parser: argparse.ArgumentParser):
    argument_parser.add_argument(
        "--ini_datetime",
        "-i",
        required=False,
        help=" initial datetime to execute the analysis. Format: Y-m-dTH:M:S",
    )
    argument_parser.add_argument(
        "--end_datetime",
        "-e",
        required=False,
        help=" ending datetime to execute the analysis. Format: Y-m-dTH:M:S",
    )
