import logging
import tqdm
import concurrent.futures

from decimal import Decimal, getcontext
from datetime import datetime, timedelta

from ..configuration import CONFIGURATION
from ..general.general_utilities import log_execution_time
from ..database.common.db_collections_common import database_local, database_global
from ..database.helpers import get_price_from_db

from ..converters.onchain import convert_hypervisor_fromDict
from datetime import timezone


class user_operation:
    user_address: str
    hypervisor_address: str
    block: int
    logIndex: int
    timestamp: int
    operation_id: str
    topic: str

    token0_in = Decimal("0")
    token1_in = Decimal("0")
    token0_out = Decimal("0")
    token1_out = Decimal("0")

    shares_in = Decimal("0")
    shares_out = Decimal("0")

    fees_token0_in = Decimal("0")
    fees_token1_in = Decimal("0")

    # TODO: how to handle rewards in token and its price?
    # rewards = [
    #    {
    #       "token_symbol": str,
    #       "qtty": str,
    #       "price_usd": str,
    # },
    # ]
    # rewards_usd_in = Decimal("0")

    price_usd_token0 = Decimal("0")
    price_usd_token1 = Decimal("0")
    price_usd_share = Decimal("0")

    underlying_token0_per_share = Decimal("0")
    underlying_token1_per_share = Decimal("0")


class user_operations_hypervisor_builder:
    def __init__(self, hypervisor_address: str, network: str, protocol: str):
        """Fast forward emulate gamma hypervisor contract using database data

        Args:
            hypervisor_address (str):
            network (str):
            protocol (str):
            t_ini (int): initial timestamp
            t_end (int): end timestamp
        """

        # set global vars
        self._hypervisor_address = hypervisor_address.lower()
        self._network = network
        self._protocol = protocol

        self.__blacklist_addresses = ["0x0000000000000000000000000000000000000000"]

        # load static
        self._static = self._get_static_data()
        # load prices for all status blocks ( speedup process)
        self._prices = self._get_prices()

        # init rewarders masterchefs
        self._rewarders_list = []
        self._rewarders_lastTime_update = None

        # manual user shares
        self._manual_user_shares = {}

        # control var (itemsprocessed): list of operation ids processed
        self.ids_processed = []
        # control var time order :  last block always >= current
        self.last_block_processed: int = 0

    # setup
    def _get_static_data(self):
        """_load hypervisor's static data from database"""
        # static
        try:
            return self.local_db_manager.get_items_from_database(
                collection_name="static", find={"id": self.address}
            )[0]
        except IndexError:
            raise ValueError(f"Static data not found for {self.address}")

    def _get_prices(self) -> dict:
        """_load prices from database"""
        # database link
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        global_db_manager = database_global(mongo_url=mongo_url)

        # define query
        or_query = [
            {"address": self._static["pool"]["token0"]["address"]},
            {"address": self._static["pool"]["token1"]["address"]},
        ]
        find = {"$or": or_query, "network": self.network}
        sort = [("block", 1)]

        result = {}
        for x in global_db_manager.get_items_from_database(
            collection_name="usd_prices", find=find, sort=sort
        ):
            if x["block"] not in result:
                result[x["block"]] = {}
            result[x["block"]][x["address"]] = x["price"]

        return result

    @property
    def rewarders_list(self) -> list:
        """Masterchef addresses that reward this hypervisor

        Returns:
            list: rewarders addresses
        """
        # get rewarders addresses from database every 20 minutes
        if self._rewarders_lastTime_update is None or (
            datetime.utcnow() - self._rewarders_lastTime_update
        ).total_seconds() >= (60 * 10):
            # update rewarders
            self._rewarders_list = (
                self.local_db_manager.get_distinct_items_from_database(
                    collection_name="rewards_static",
                    field="rewarder_address",
                    condition={"hypervisor_address": self._hypervisor_address},
                )
            )

            # add custom rewarders
            if self.network == "polygon":
                self._rewarders_list += [
                    "0xf099FA1Bd92f8AAF4886e8927D7bd3c15bA0BbFd".lower(),  # Masterchef poly Quickswap
                    "0x158B99aE660D4511e4c52799e1c47613cA47a78a".lower(),  # dQuick poly Quickswap
                    "0xFa3deAFecd0Fad0b083AB050cF30E1d541720680".lower(),  # wmatic poly Quickswap
                    "0xc35f556C8Ac05FB484A703eE96A2f997F8CAC957".lower(),  # dQuick poly Quickswap
                    "0x9bA5cE366f99f2C10A38ED35159c60CC558ca626".lower(),  # ankr poly Quickswap
                    "0x5554EdCaf47189894315d845D5B19eeB14D79048".lower(),  # ghst poly Quickswap
                    "0x780e2496141f97dd48ed8cb296f5c0828f1cb317".lower(),  # ldo poly Quickswap
                    "0xae361ab6c12e9c8aff711d3ddc178be6da2a7472".lower(),  # gddy poly Quickswap
                    "0xB6985ce301E6C6c4766b4479dDdc152d4eD0f2d3".lower(),  # davos poly Quickswap
                    "0xC84Ec966b4E6473249d64763366212D175b5c2bd".lower(),  # wmatic2 poly Quickswap
                    "0x7636e51D352cf89a7A05aE7b66f8841c368080Ff".lower(),  # lcd poly Quickswap
                    "0x88F54579fbB2a33c33342709A4B2a9A07dA94EE2".lower(),  # Axl poly Quickswap
                    "0xF5052aD621D4fb273b752536A2625Ae0Fea3eb0E".lower(),  # Wombat poly Quickswap
                    "0x570d60a60baa356d47fda3017a190a48537fcd7d".lower(),  # Masterchef poly uniswapv3
                    "0xdb909073b3815a297024db0d72fafdfc6db5a7b7".lower(),  # Ankr poly Uniswapv3
                    "0x4d7a374fce77eec67b3a002549a3a49deec9307c".lower(),  # Davos poly Uniswapv3
                    "0x68678Cf174695fc2D27bd312DF67A3984364FFDd".lower(),  # Masterchef Quickswap
                    "0x43e867915E4fBf7e3648800bF9bB5A4Bc7A49F37".lower(),  # Giddy poly Quickswap
                ]
            elif self.network == "optimism":
                self._rewarders_list += [
                    "0xc7846d1bc4d8bcf7c45a7c998b77ce9b3c904365".lower(),  # Masterchef Optimism uniswapv3
                    "0xf099FA1Bd92f8AAF4886e8927D7bd3c15bA0BbFd".lower(),  # uniswapv3
                    "0xAfBB6c1a235e105e568CCD4FD915dfFF76C415E1".lower(),  # op labs uniswapv3
                ]
            elif self.network == "arbitrum":
                self._rewarders_list += [
                    "0x72E4CcEe48fB8FEf18D99aF2965Ce6d06D55C8ba".lower(),  # Masterchef Arbitrum zyberswap
                    "0x9BA666165867E916Ee7Ed3a3aE6C19415C2fBDDD".lower(),  # Masterchef Arbitrum zyberswap
                ]
            elif self.network == "binance":
                self._rewarders_list += [
                    "0x374cc2276b842fecd65af36d7c60a5b78373ede1".lower(),  # Voter Binance thena
                    "0x3a1d0952809f4948d15ebce8d345962a282c4fcb".lower(),  #  thena_voter_v3 Binance thena
                ]

            # update time
            self._rewarders_lastTime_update = datetime.utcnow()
        # return rewarders
        return self._rewarders_list

    @property
    def local_db_manager(self) -> database_local:
        mongo_url = CONFIGURATION["sources"]["database"]["mongo_server_url"]
        db_name = f"{self.network}_{self.protocol}"
        return database_local(mongo_url=mongo_url, db_name=db_name)

    # public
    @property
    def address(self) -> str:
        """hypervisor address

        Returns:
            str:
        """
        return self._hypervisor_address

    @property
    def network(self) -> str:
        return self._network

    @property
    def protocol(self) -> str:
        return self._protocol

    @property
    def dex(self) -> str:
        return self._static.get("dex", "")

    @property
    def symbol(self) -> str:
        return self._static.get("symbol", "")

    # action loop

    def _process_operations(
        self, rewrite: bool | None = None, initial_block: int | None = None
    ):
        """process all operations"""

        initial_block = None if rewrite else initial_block or self.get_starting_block()
        # get operations:
        # {  }
        operations = self.get_hypervisor_operations(initial_block=initial_block)

        _errors = 0
        with tqdm.tqdm(total=len(operations), leave=False) as progress_bar:
            # transform operations decimals128 fields to decimal
            for operation in [
                self.local_db_manager.convert_d128_to_decimal(item=op)
                for op in operations
            ]:
                # progress show
                progress_bar.set_description(
                    f' processing 0x..{operation["address"][-4:]}  {operation["blockNumber"]}  {operation["topic"]}'
                )
                progress_bar.refresh()

                if operation["id"] not in self.ids_processed:
                    # linear processing check
                    if operation["blockNumber"] < self.last_block_processed:
                        logging.getLogger(__name__).error(
                            f""" Not processing operation with a lower block than last processed: {operation["blockNumber"]}  CHECK operation id: {operation["id"]}"""
                        )
                        continue

                    # process operation
                    self._process_operation(operation)

                    # add operation as proceesed
                    self.ids_processed.append(operation["id"])

                    # set last block number processed
                    self.last_block_processed = operation["blockNumber"]
                else:
                    logging.getLogger(__name__).debug(
                        f""" Operation already processed {operation["id"]}. Not processing"""
                    )

                # update progress
                progress_bar.update(1)

    @log_execution_time
    def _process_operation(self, operation: dict):
        # set current block
        self.current_block = operation["blockNumber"]
        # set current logIndex
        self.current_logIndex = operation["logIndex"]

        if operation["topic"] == "deposit":
            # check if it is a Gamma NFT hypervisor
            if operation["sender"] != operation["to"]:
                raise ValueError(
                    f" {self.network}'s {self.address} is an old Gamma NFT hypervisor ... remove from processing"
                )

            self._add_user_status(status=self._process_deposit(operation=operation))

        elif operation["topic"] == "withdraw":
            # check if it is a Gamma NFT hypervisor
            if operation["sender"] != operation["to"]:
                raise ValueError(
                    f" {self.network}'s {self.address} is an old Gamma NFT hypervisor ... remove from processing"
                )

            self._add_user_status(status=self._process_withdraw(operation=operation))

        elif operation["topic"] == "transfer":
            # retrieve new status
            op_source, op_destination = self._process_transfer(operation=operation)
            # add to collection
            if op_source:
                self._add_user_status(status=op_source)
            if op_destination:
                self._add_user_status(status=op_destination)

        elif operation["topic"] == "rebalance":
            self._process_rebalance(operation=operation)

        elif operation["topic"] == "approval":
            # TODO: approval topic
            # self._add_user_status(self._process_approval(operation=operation))
            pass

        elif operation["topic"] == "zeroBurn":
            self._process_zeroBurn(operation=operation)

        elif operation["topic"] == "setFee":
            # TODO: setFee topic
            pass

        elif operation["topic"] == "report":
            # global status for all addresses
            raise NotImplementedError(
                f""" {operation["topic"]} topic not implemented yet"""
            )

        else:
            raise NotImplementedError(
                f""" {operation["topic"]} topic not implemented yet"""
            )

    # Topic transformers

    def _process_deposit(self, operation: dict) -> user_operation:
        #
        new_user_operation = user_operation()
        new_user_operation.operation_id = operation["id"]
        new_user_operation.topic = operation["topic"]
        new_user_operation.user_address = operation["to"].lower()
        new_user_operation.hypervisor_address = operation["address"]
        new_user_operation.block = operation["blockNumber"]
        new_user_operation.logIndex = operation["logIndex"]
        new_user_operation.timestamp = operation["timestamp"]

        # prices
        price_usd_t0 = self.get_price(
            block=new_user_operation.block,
            address=self._static["pool"]["token0"]["address"],
        )
        price_usd_t1 = self.get_price(
            block=new_user_operation.block,
            address=self._static["pool"]["token1"]["address"],
        )
        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, new_user_operation.block)

        # set prices
        new_user_operation.price_usd_token0 = price_usd_t0
        new_user_operation.price_usd_token1 = price_usd_t1

        # add manual shares
        self.modify_manual_user_shares(
            user_address=new_user_operation.user_address,
            amount=Decimal(operation["shares"]),
        )

        new_user_operation.shares_in = Decimal(operation["shares"]) / (
            10 ** Decimal(operation["decimals_contract"])
        )
        new_user_operation.token0_in = Decimal(operation["qtty_token0"]) / (
            10 ** Decimal(operation["decimals_token0"])
        )
        new_user_operation.token1_in = Decimal(operation["qtty_token1"]) / (
            10 ** Decimal(operation["decimals_token1"])
        )

        # add undelying tokenX per share at this block
        new_user_operation.underlying_token0_per_share = operation[
            "underlying_token0_perShare"
        ]
        new_user_operation.underlying_token1_per_share = operation[
            "underlying_token1_perShare"
        ]

        # result
        return new_user_operation

    def _process_withdraw(self, operation: dict) -> user_operation:
        #
        new_user_operation = user_operation()
        new_user_operation.operation_id = operation["id"]
        new_user_operation.topic = operation["topic"]
        new_user_operation.user_address = operation["sender"].lower()
        new_user_operation.hypervisor_address = operation["address"]
        new_user_operation.block = operation["blockNumber"]
        new_user_operation.logIndex = operation["logIndex"]
        new_user_operation.timestamp = operation["timestamp"]

        price_usd_t0 = self.get_price(
            block=new_user_operation.block,
            address=self._static["pool"]["token0"]["address"],
        )
        price_usd_t1 = self.get_price(
            block=new_user_operation.block,
            address=self._static["pool"]["token1"]["address"],
        )
        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, new_user_operation.block)

        # set prices
        new_user_operation.price_usd_token0 = price_usd_t0
        new_user_operation.price_usd_token1 = price_usd_t1

        # subtract manual shares
        self.modify_manual_user_shares(
            user_address=new_user_operation.user_address,
            amount=-Decimal(operation["shares"]),
        )

        new_user_operation.shares_out = Decimal(operation["shares"]) / (
            10 ** Decimal(operation["decimals_contract"])
        )
        new_user_operation.token0_out = Decimal(operation["qtty_token0"]) / (
            10 ** Decimal(operation["decimals_token0"])
        )
        new_user_operation.token1_out = Decimal(operation["qtty_token1"]) / (
            10 ** Decimal(operation["decimals_token1"])
        )

        # add undelying tokenX per share at this block
        new_user_operation.underlying_token0_per_share = operation[
            "underlying_token0_perShare"
        ]
        new_user_operation.underlying_token1_per_share = operation[
            "underlying_token1_perShare"
        ]

        # result
        return new_user_operation

    def _process_transfer(
        self, operation: dict
    ) -> tuple[user_operation, user_operation]:
        if operation["dst"] == "0x0000000000000000000000000000000000000000":
            # expect a withdraw topic on next operation ( same block))
            # do nothing
            pass
        elif operation["src"] == "0x0000000000000000000000000000000000000000":
            # expect a deposit topic on next operation ( same block)
            # do nothing
            pass
        else:
            # check if transfer is to/from a rewarder
            if operation["dst"].lower() in self.rewarders_list:
                # TODO: staking into masterchef implementation
                logging.getLogger(__name__).debug(
                    f"{operation['src']} user staking into {operation['dst']} not processed yet. TODO: implement"
                )
            elif operation["src"].lower() in self.rewarders_list:
                # TODO: unstaking out of masterchef implementation
                logging.getLogger(__name__).debug(
                    f"{operation['dst']} user unstaking from {operation['src']} not processed yet. TODO: implement"
                )
            else:
                # transfer all values to other user address
                return self._transfer_to_user(operation=operation)

        # result
        return None, None

    def _process_rebalance(self, operation: dict):
        """Rebalance affects all users positions

        Args:
            operation (dict):

        Returns:
            user_status: _description_
        """

        # share fees with all accounts with shares
        self._share_fees_with_acounts(operation)

    def _process_approval(self, operation: dict):
        # TODO: approval
        pass

    def _process_zeroBurn(self, operation: dict):
        # share fees with all acoounts proportionally
        self._share_fees_with_acounts(operation)

    def _transfer_to_user(
        self, operation: dict
    ) -> tuple[user_operation, user_operation]:
        # block
        block = operation["blockNumber"]
        # contract address
        hypervisor_address = operation["address"].lower()
        # set user address
        address_source = operation["src"].lower()
        address_destination = operation["dst"].lower()

        # USD prices
        price_usd_t0 = self.get_price(
            block=block, address=self._static["pool"]["token0"]["address"]
        )
        price_usd_t1 = self.get_price(
            block=block, address=self._static["pool"]["token1"]["address"]
        )
        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, block)

        #  Source
        source_user_operation = user_operation()
        source_user_operation.operation_id = operation["id"]
        source_user_operation.topic = operation["topic"]
        source_user_operation.user_address = address_source
        source_user_operation.hypervisor_address = hypervisor_address
        source_user_operation.block = operation["blockNumber"]
        source_user_operation.logIndex = operation["logIndex"]
        source_user_operation.timestamp = operation["timestamp"]

        source_user_operation.price_usd_token0 = price_usd_t0
        source_user_operation.price_usd_token1 = price_usd_t1

        # subtract manual shares
        self.modify_manual_user_shares(
            user_address=source_user_operation.user_address,
            amount=-Decimal(operation["qtty"]),
        )

        source_user_operation.shares_out = Decimal(operation["qtty"]) / (
            10 ** Decimal(operation["decimals_contract"])
        )

        # subtract underlying tokens
        source_user_operation.token0_out = (
            operation["underlying_token0_perShare"] * source_user_operation.shares_out
        )
        source_user_operation.token1_out = (
            operation["underlying_token1_perShare"] * source_user_operation.shares_out
        )
        # add undelying tokenX per share at this block
        source_user_operation.underlying_token0_per_share = operation[
            "underlying_token0_perShare"
        ]
        source_user_operation.underlying_token1_per_share = operation[
            "underlying_token1_perShare"
        ]

        # Destination
        destination_user_operation = user_operation()
        destination_user_operation.operation_id = operation["id"]
        destination_user_operation.topic = operation["topic"]
        destination_user_operation.user_address = address_destination
        destination_user_operation.hypervisor_address = hypervisor_address
        destination_user_operation.block = operation["blockNumber"]
        destination_user_operation.logIndex = operation["logIndex"]
        destination_user_operation.timestamp = operation["timestamp"]

        destination_user_operation.price_usd_token0 = price_usd_t0
        destination_user_operation.price_usd_token1 = price_usd_t1

        # add manual shares
        self.modify_manual_user_shares(
            user_address=destination_user_operation.user_address,
            amount=Decimal(operation["qtty"]),
        )
        destination_user_operation.shares_in = source_user_operation.shares_out

        # add underlying tokens
        destination_user_operation.token0_in = source_user_operation.token0_out
        destination_user_operation.token1_in = source_user_operation.token1_out
        # add undelying tokenX per share at this block
        destination_user_operation.underlying_token0_per_share = operation[
            "underlying_token0_perShare"
        ]
        destination_user_operation.underlying_token1_per_share = operation[
            "underlying_token1_perShare"
        ]
        # result
        return source_user_operation, destination_user_operation

    def _stake_to_rewarder(self, operation: dict):
        # get rewarder from database
        if rewards := self.get_rewards_status(
            rewarder_address=operation["dst"].lower(), block=operation["blockNumber"]
        ):
            for reward in rewards:
                pass
        else:
            logging.getLogger(__name__).debug(
                f" No rewards found in database using rewarder {operation['dst'].lower()} for hypervisor {operation['address']} at block {operation['blockNumber']}"
            )

    def _unstake_to_rewarder(self, operation: dict):
        if rewards := self.get_rewards_status(
            rewarder_address=operation["src"].lower(), block=operation["blockNumber"]
        ):
            for reward in rewards:
                pass
        else:
            logging.getLogger(__name__).debug(
                f" No rewards found in database using rewarder {operation['src'].lower()} for hypervisor {operation['address']} at block {operation['blockNumber']}"
            )

    @log_execution_time
    def _share_fees_with_acounts(self, operation: dict):
        # block
        block = operation["blockNumber"]

        # get current total contract_address shares qtty
        total_shares = self.get_hypervisor_supply(
            block=operation["blockNumber"], logIndex=operation["logIndex"]
        )

        # total fees collected ( gross fees )
        total_fees_collected_token0 = Decimal(operation["qtty_token0"]) / (
            Decimal(10) ** Decimal(operation["decimals_token0"])
        )
        total_fees_collected_token1 = Decimal(operation["qtty_token1"]) / (
            Decimal(10) ** Decimal(operation["decimals_token1"])
        )
        # Gamma as protocol fees takes the hypervisor fee from each collection and the rest is distributed to the LPs
        protocol_revenue_fee = Decimal(
            1 / self._static["fee"] if self._static["fee"] < 100 else 1 / 10
        )
        gamma_fees_collected_token0 = total_fees_collected_token0 * protocol_revenue_fee
        gamma_fees_collected_token1 = total_fees_collected_token1 * protocol_revenue_fee

        # User fees are the rest of the fees collected
        user_fees_collected_token0 = (
            total_fees_collected_token0 - gamma_fees_collected_token0
        )
        user_fees_collected_token1 = (
            total_fees_collected_token1 - gamma_fees_collected_token1
        )

        # check if any fees have actually been collected to proceed ...
        if total_shares == Decimal("0"):
            # there is no deposits yet... hypervisor is in testing or seting up mode
            if user_fees_collected_token0 == user_fees_collected_token1 == Decimal("0"):
                logging.getLogger(__name__).debug(
                    f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet and collected fees are zero"
                )
            else:
                logging.getLogger(__name__).warning(
                    f" Not processing 0x..{self.address[-4:]} fee collection as it has no deposits yet but fees collected fees are NON zero --> token0: {user_fees_collected_token0}  token1: {user_fees_collected_token1}"
                )
            # exit
            return
        if user_fees_collected_token0 == user_fees_collected_token1 == Decimal("0"):
            # there is no collection made ... but hypervisor changed tick boundaries
            logging.getLogger(__name__).debug(
                f" Not processing 0x..{self.address[-4:]} fee collection as it has not collected any fees."
            )
            # exit
            return

        # USD prices
        price_usd_t0 = self.get_price(
            block=block, address=self._static["pool"]["token0"]["address"]
        )
        price_usd_t1 = self.get_price(
            block=block, address=self._static["pool"]["token1"]["address"]
        )
        # check prices
        self._check_prices(price_usd_t0, price_usd_t1, block)

        # underlying token qtty ( uncollected fees are zero at this point)
        (
            total_underlying_token0,
            total_underlying_token1,
        ) = self.get_hypervisor_underlying(operation=operation)

        # usd price per share
        price_usd_share = (
            total_underlying_token0 * price_usd_t0
            + total_underlying_token1 * price_usd_t1
        ) / total_shares

        # create a gamma as user operation adding up fees collected
        gamma_db_operation = self.create_gamma_protocol_fees_db_operation(
            operation=operation,
            price_usd_token0=price_usd_t0,
            price_usd_token1=price_usd_t1,
            price_usd_share=price_usd_share,
            fees_collected_token0=gamma_fees_collected_token0,
            fees_collected_token1=gamma_fees_collected_token1,
        )

        # get all users in current block
        users_addresses = self.get_hypervisor_users(
            block=operation["blockNumber"], logIndex=operation["logIndex"]
        )

        # control var to keep track of total percentage applied
        ctrl_total_percentage_applied = Decimal("0")
        ctrl_total_shares_applied = Decimal("0")

        # create fee sharing loop for threaded processing
        def loop_share_fees(
            user_address,
        ) -> tuple[user_operation, Decimal]:
            # get user shares
            if user_shares := self.get_manual_user_shares(
                user_address=user_address, block=block, logIndex=operation["logIndex"]
            ):
                new_user_operation = user_operation()
                new_user_operation.operation_id = operation["id"]
                new_user_operation.topic = operation["topic"]
                new_user_operation.user_address = user_address
                new_user_operation.hypervisor_address = self.address
                new_user_operation.block = operation["blockNumber"]
                new_user_operation.logIndex = operation["logIndex"]
                new_user_operation.timestamp = operation["timestamp"]

                new_user_operation.price_usd_token0 = price_usd_t0
                new_user_operation.price_usd_token1 = price_usd_t1
                new_user_operation.price_usd_share = price_usd_share
                percentage = user_shares / total_shares

                new_user_operation.fees_token0_in = (
                    percentage * user_fees_collected_token0
                )
                new_user_operation.fees_token1_in = (
                    percentage * user_fees_collected_token1
                )

                # add undelying tokenX per share at this block
                new_user_operation.underlying_token0_per_share = operation[
                    "underlying_token0_perShare"
                ]
                new_user_operation.underlying_token1_per_share = operation[
                    "underlying_token1_perShare"
                ]

                # return
                return (
                    user_shares,
                    percentage,
                    self.convert_user_operation_toDb(new_user_operation),
                )
            else:
                return 0, 0, None

        # create list to store results
        user_status_list = [gamma_db_operation]

        # go thread all: Get all user status with shares to share fees with
        with concurrent.futures.ThreadPoolExecutor() as ex:
            for user_shares, percentage, result_converted in ex.map(
                loop_share_fees,
                users_addresses,
            ):
                if result_converted:
                    # apply result
                    ctrl_total_shares_applied += user_shares
                    # add user share to total processed control var
                    ctrl_total_percentage_applied += percentage
                    # add to list
                    user_status_list.append(result_converted)

        # add resulting user status to mongo database in bulk
        if user_status_list:
            self._add_user_status_bulk(user_status_list)

        # control remainders
        if ctrl_total_percentage_applied != Decimal("1"):
            fee0_remainder = (
                Decimal("1") - ctrl_total_percentage_applied
            ) * user_fees_collected_token0
            fee1_remainder = (
                Decimal("1") - ctrl_total_percentage_applied
            ) * user_fees_collected_token1
            feeUsd_remainder = (fee0_remainder * price_usd_t0) + (
                fee1_remainder * price_usd_t1
            )

            if ctrl_total_percentage_applied < Decimal("1"):
                logging.getLogger(__name__).warning(
                    " The total percentage applied while calc. user fees fall behind 100% at {}'s {} hype {} block {} -> {}".format(
                        self.network,
                        self.protocol,
                        self.address,
                        block,
                        (ctrl_total_percentage_applied),
                    )
                )
            else:
                logging.getLogger(__name__).warning(
                    " The total percentage applied while calc. user fees fall exceeds 100% at {}'s {} hype {} block {} -> {}".format(
                        self.network,
                        self.protocol,
                        self.address,
                        block,
                        (ctrl_total_percentage_applied),
                    )
                )

            # add remainders to global vars
            # self._modify_global_data(
            #     block=block,
            #     add=True,
            #     fee0_remainder=fee0_remainder,
            #     fee1_remainder=fee1_remainder,
            # )

            # log error if value is significant
            if (Decimal("1") - ctrl_total_percentage_applied) > Decimal("0.0001"):
                logging.getLogger(__name__).error(
                    " Only {:,.2f} of the fees value has been distributed to current accounts. remainder: {}   tot.shares: {}  remainder usd: {}  block:{}".format(
                        ctrl_total_percentage_applied,
                        (Decimal("1") - ctrl_total_percentage_applied),
                        ctrl_total_shares_applied,
                        feeUsd_remainder,
                        block,
                    )
                )

    # Collection

    def _add_user_status(self, status: user_operation):
        """add user status to database

        Args:
            status :
        """
        if status.user_address not in self.__blacklist_addresses:
            # add status to database
            self.local_db_manager.set_user_operation(
                self.convert_user_operation_toDb(status=status)
            )

        elif status.user_address != "0x0000000000000000000000000000000000000000":
            logging.getLogger(__name__).debug(
                f"Not adding blacklisted account {status.user_address} user status"
            )

    @log_execution_time
    def _add_user_status_bulk(self, operations: list[user_operation]):
        """add user status to database all at once

        Args:
            status (user_status):
        """
        # add status to database
        self.local_db_manager.set_user_operations_bulk(operations)

    # Special
    def create_gamma_protocol_fees_db_operation(
        self,
        operation: dict,
        price_usd_token0: float,
        price_usd_token1: float,
        price_usd_share: float,
        fees_collected_token0: float,
        fees_collected_token1: float,
    ) -> dict:
        """add gamma protocol fees to database"""

        # define an address for Gamma
        user_address = "gamma"

        new_user_operation = user_operation()
        new_user_operation.operation_id = operation["id"]
        new_user_operation.topic = operation["topic"]
        new_user_operation.user_address = user_address
        new_user_operation.hypervisor_address = self.address
        new_user_operation.block = operation["blockNumber"]
        new_user_operation.logIndex = operation["logIndex"]
        new_user_operation.timestamp = operation["timestamp"]

        new_user_operation.price_usd_token0 = price_usd_token0
        new_user_operation.price_usd_token1 = price_usd_token1
        new_user_operation.price_usd_share = price_usd_share

        new_user_operation.fees_token0_in = fees_collected_token0
        new_user_operation.fees_token1_in = fees_collected_token1

        # add undelying tokenX per share at this block
        new_user_operation.underlying_token0_per_share = operation[
            "underlying_token0_perShare"
        ]
        new_user_operation.underlying_token1_per_share = operation[
            "underlying_token1_perShare"
        ]

        return self.convert_user_operation_toDb(new_user_operation)

    # General helpers

    @log_execution_time
    def get_hypervisor_users(
        self, block: int | None = None, logIndex: int | None = None
    ):
        addresses_not_in = self.rewarders_list + self.__blacklist_addresses
        match = {
            "address": self.address,
            "src": {"$nin": addresses_not_in},
            "dst": {"$nin": addresses_not_in},
            "topic": {
                "$in": [
                    "transfer",
                    "deposit",
                ]
            },
        }

        if block and logIndex:
            match["$or"] = [
                {"blockNumber": {"$lt": block}},
                {
                    "$and": [
                        {"blockNumber": {"$lte": block}},
                        {"logIndex": {"$lte": logIndex}},
                    ]
                },
            ]
        elif block:
            match["blockNumber"] = {"$lte": block}

        query = [
            {"$match": match},
            {
                "$group": {
                    "_id": {
                        "$cond": [{"$eq": ["$topic", "transfer"]}, "$dst", "$sender"]
                    }
                }
            },
        ]
        # return list of users
        return [
            x["_id"]
            for x in self.local_db_manager.get_items_from_database(
                collection_name="operations", aggregate=query
            )
        ]

    def get_hypervisor_operations(
        self,
        initial_block: int | None = None,
        topics=["transfer", "deposit", "withdraw", "rebalance", "zeroBurn"],
    ) -> list[dict]:
        """Get all hypervisor operations from block to latest

        Args:
            initial_block (int, optional): The block from where to start. Defaults to 0.

        Returns:
            list: [
                    {
                        <operation fields>
                        status: {
                            totalSupply: ( in decimal ) ,
                            fees_uncollected: { qtty_token0: , qtty_token1:} ,
                            totalAmounts:  {total0, total1},
                            undelying_token0:
                            undelying_token1:
                            },
                        underlying_token0_perShare: (  deployed + uncollected fees )
                        underlying_token1_perShare:
                    }
                ]

        """

        # build find and sort
        match = {
            "address": self.address.lower(),
            "qtty_token0": {"$ne": "0"},
            "qtty_token1": {"$ne": "0"},
            "src": {"$ne": "0x0000000000000000000000000000000000000000"},
            "dst": {"$ne": "0x0000000000000000000000000000000000000000"},
            "topic": {"$in": topics},
        }
        if initial_block:
            match["blockNumber"] = {"$gte": initial_block}

        query = [
            {"$match": match},
            {
                "$lookup": {
                    "from": "status",
                    "let": {
                        "operation_hype_address": "$address",
                        "operation_block": "$blockNumber",
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {
                                            "$eq": [
                                                "$address",
                                                "$$operation_hype_address",
                                            ]
                                        },
                                        {"$eq": ["$block", "$$operation_block"]},
                                    ],
                                }
                            }
                        },
                        {
                            "$project": {
                                "totalSupply": {
                                    "$divide": [
                                        {"$toDecimal": "$totalSupply"},
                                        {"$pow": [10, "$decimals"]},
                                    ]
                                },
                                "fees_uncollected": "$fees_uncollected",
                                "totalAmounts": "$totalAmounts",
                                "underlying_token0": {
                                    "$divide": [
                                        {
                                            "$sum": [
                                                {"$toDecimal": "$totalAmounts.total0"},
                                                {
                                                    "$toDecimal": "$fees_uncollected.qtty_token0"
                                                },
                                            ]
                                        },
                                        {"$pow": [10, "$pool.token0.decimals"]},
                                    ]
                                },
                                "underlying_token1": {
                                    "$divide": [
                                        {
                                            "$sum": [
                                                {"$toDecimal": "$totalAmounts.total1"},
                                                {
                                                    "$toDecimal": "$fees_uncollected.qtty_token1"
                                                },
                                            ]
                                        },
                                        {"$pow": [10, "$pool.token1.decimals"]},
                                    ]
                                },
                            }
                        },
                        {"$unset": ["_id"]},
                    ],
                    "as": "status",
                }
            },
            {"$unset": ["_id"]},
            {"$unwind": "$status"},
            {
                "$addFields": {
                    "underlying_token0_perShare": {
                        "$ifNull": [
                            {
                                "$cond": [
                                    {"$eq": ["$status.totalSupply", 0]},
                                    0,
                                    {
                                        "$divide": [
                                            {"$toDecimal": "$status.underlying_token0"},
                                            {"$toDecimal": "$status.totalSupply"},
                                        ]
                                    },
                                ]
                            },
                            0,
                        ]
                    },
                    "underlying_token1_perShare": {
                        "$ifNull": [
                            {
                                "$cond": [
                                    {"$eq": ["$status.totalSupply", 0]},
                                    0,
                                    {
                                        "$divide": [
                                            {"$toDecimal": "$status.underlying_token1"},
                                            {"$toDecimal": "$status.totalSupply"},
                                        ]
                                    },
                                ]
                            },
                            0,
                        ]
                    },
                }
            },
            {"$unset": ["_id"]},
            {"$sort": {"blockNumber": 1, "logIndex": 1}},
        ]

        return self.local_db_manager.get_items_from_database(
            collection_name="operations", aggregate=query
        )

    def get_starting_block(self) -> int | None:
        """Get the block to beguin with --> last block found in database -1

        Returns:
            int: initial block
        """
        # get already procesed operations from database
        try:
            last_operation = sorted(
                self.local_db_manager.get_distinct_items_from_database(
                    collection_name="user_operations",
                    field="block",
                    condition={"hypervisor_address": self.address},
                )
            )
            # return the last block -1
            if len(last_operation) > 1:
                return last_operation[-2]
            else:
                return last_operation[-1]
        except Exception as e:
            logging.getLogger(__name__).error(
                f" No user operations found for {self.network}'s {self.address} from db:  {e}. Starting sync from the beguining."
            )
            return None

    def get_user_shares(
        self, user_address: str, block: int | None = None, logIndex: int | None = None
    ) -> Decimal:
        """

        Args:
            user_address (str): _description_
            block (int | None, optional): _description_. Defaults to None.
            logIndex (int | None, optional): _description_. Defaults to None.

        Returns:
            Decimal: _description_
        """
        match = {
            "address": self.address,
            "$and": [
                {"src": {"$nin": self.__blacklist_addresses + self.rewarders_list}},
                {"dst": {"$nin": self.__blacklist_addresses + self.rewarders_list}},
                {
                    "$or": [
                        {"sender": user_address},
                        {"to": user_address},
                        {"src": user_address},
                        {"dst": user_address},
                    ]
                },
            ],
            "topic": {"$in": ["deposit", "withdraw", "transfer"]},
        }

        hypervisor_block_condition = "$eq"  # get the precise hypervisor status block
        if block and logIndex:
            match["$and"].append(
                {
                    "$or": [
                        {"blockNumber": {"$lt": block}},
                        {
                            "$and": [
                                {"blockNumber": {"$lte": block}},
                                {"logIndex": {"$lte": logIndex}},
                            ]
                        },
                    ]
                }
            )
        else:
            # get last hypervisor status block
            hypervisor_block_condition = "$gte"

        query = [
            {"$match": match},
            {
                "$addFields": {
                    "shares_in": {
                        "$cond": [
                            {"$eq": ["$topic", "deposit"]},
                            {"$toDecimal": "$shares"},
                            0,
                        ]
                    },
                    "shares_out": {
                        "$cond": [
                            {"$eq": ["$topic", "withdraw"]},
                            {"$toDecimal": "$shares"},
                            0,
                        ]
                    },
                    "shares_transfer_in": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$eq": ["$topic", "transfer"]},
                                    {
                                        "$eq": [
                                            "$dst",
                                            user_address,
                                        ]
                                    },
                                ]
                            },
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "shares_transfer_out": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$eq": ["$topic", "transfer"]},
                                    {
                                        "$eq": [
                                            "$src",
                                            user_address,
                                        ]
                                    },
                                ]
                            },
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                }
            },
            {
                "$group": {
                    "_id": "$address",
                    "user_shares": {
                        "$sum": {
                            "$subtract": [
                                {"$sum": ["$shares_in", "$shares_transfer_in"]},
                                {"$sum": ["$shares_out", "$shares_transfer_out"]},
                            ]
                        }
                    },
                }
            },
        ]
        try:
            return self.local_db_manager.convert_d128_to_decimal(
                self.local_db_manager.get_items_from_database(
                    collection_name="operations", aggregate=query
                )[0]
            )["user_shares"]
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Can't find {self.network}'s {self.address} shares for {user_address}. Return Zero"
            )
        return Decimal("0")

    @log_execution_time
    def get_manual_user_shares(
        self, user_address: str, block: int | None = None, logIndex: int | None = None
    ):
        # TODO: remove this after debugging
        # tmp_shares = self.get_user_shares(user_address, block, logIndex)
        if not user_address in self._manual_user_shares:
            # self._manual_user_shares[user_address] = tmp_shares
            self._manual_user_shares[user_address] = self.get_user_shares(
                user_address, block, logIndex
            )
        # else:
        # TODO: remove this after debugging
        # if tmp_shares != self._manual_user_shares[user_address]:
        #     logging.getLogger(__name__).warning(
        #         f"Manual shares for {user_address} are not equal to the ones in the db. Manual: {self._manual_user_shares[user_address]}, db: {tmp_shares}"
        #     )

        return self._manual_user_shares[user_address]

    def modify_manual_user_shares(self, user_address: str, amount: Decimal):
        # only modify is address is already there
        if user_address in self._manual_user_shares:
            self._manual_user_shares[user_address] += amount

    @log_execution_time
    def get_price(self, block: int, address: str) -> Decimal:
        # TODO: remove this when global db is ready
        try:
            return Decimal(self._prices[block][address])
        except Exception:
            # price is not found in cached data.
            pass

        # Try to get it from global db
        try:
            return Decimal(
                get_price_from_db(
                    network=self.network, token_address=address, block=block
                )
            )
        except Exception:
            logging.getLogger(__name__).error(
                f" Can't find {self.network}'s {self.address} usd price for {address} at block {block}. Return Zero"
            )
            Decimal("0")

    @log_execution_time
    def get_hypervisor_supply(
        self, block: int | None = None, logIndex: int | None = None
    ) -> Decimal:
        """get hypervisor total supply at a specific block

        Args:
            block (int, optional): block number. Defaults to None.

        Returns:
            Decimal: total supply
        """
        match = {"address": self.address, "topic": "transfer"}
        if logIndex and block:
            match["$or"] = [
                {"blockNumber": {"$lt": block}},
                {
                    "$and": [
                        {"blockNumber": {"$lte": block}},
                        {"logIndex": {"$lte": logIndex}},
                    ]
                },
            ]
        elif block:
            match["blockNumber"] = {"$lte": block}

        query = [
            {"$match": match},
            {
                "$addFields": {
                    "shares_in": {
                        "$cond": [
                            {
                                "$eq": [
                                    "$src",
                                    "0x0000000000000000000000000000000000000000",
                                ]
                            },
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "shares_out": {
                        "$cond": [
                            {
                                "$eq": [
                                    "$dst",
                                    "0x0000000000000000000000000000000000000000",
                                ]
                            },
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                }
            },
            {
                "$group": {
                    "_id": "$address",
                    "total_shares": {
                        "$sum": {"$subtract": ["$shares_in", "$shares_out"]}
                    },
                }
            },
        ]
        return self.local_db_manager.convert_d128_to_decimal(
            self.local_db_manager.get_items_from_database(
                collection_name="operations", aggregate=query
            )[0]
        )["total_shares"]

    @log_execution_time
    def get_hypervisor_underlying(self, operation: dict) -> tuple[Decimal, Decimal]:
        underlying_token0 = underlying_token1 = Decimal(0)
        if operation["topic"] == "rebalance":
            # underlying token qtty ( uncollected fees are zero at this point)
            underlying_token0 = Decimal(operation["totalAmount0"]) / (
                Decimal(10) ** Decimal(operation["decimals_token0"])
            )
            underlying_token1 = Decimal(operation["totalAmount1"]) / (
                Decimal(10) ** Decimal(operation["decimals_token1"])
            )
        else:
            # get from hypervisor status
            hype_status = self.local_db_manager.get_items_from_database(
                collection_name="status",
                find={"address": self.address, "block": operation["blockNumber"]},
            )[0]
            underlying_token0 = Decimal(hype_status["totalAmounts"]["total0"]) / (
                10 ** Decimal(operation["decimals_token0"])
            )
            underlying_token1 = Decimal(hype_status["totalAmounts"]["total1"]) / (
                10 ** Decimal(operation["decimals_token1"])
            )

        return underlying_token0, underlying_token1

    def get_hypervisor_pricePerShare(
        self, block: int, price_usd_t0: Decimal, price_usd_t1: Decimal
    ) -> Decimal:
        # get from hypervisor status
        hype_status = self.local_db_manager.get_items_from_database(
            collection_name="status",
            find={"address": self.address, "block": block},
        )[0]
        underlying_token0 = Decimal(hype_status["totalAmounts"]["total0"]) / (
            10 ** Decimal(self._static["pool"]["token0"]["decimals"])
        )
        underlying_token1 = Decimal(hype_status["totalAmounts"]["total1"]) / (
            10 ** Decimal(self._static["pool"]["token1"]["decimals"])
        )
        return (
            underlying_token0 * price_usd_t0 + underlying_token1 * price_usd_t1
        ) / Decimal(hype_status["totalSupply"])

    @log_execution_time
    def get_rewards_status(self, rewarder_address: str, block: int) -> list | None:
        """get rewards status

        Args:
            block (int): block number

        Returns:
            list: rewards status given by the rewarder at the specific block
        """
        # get from hypervisor status
        if rewards_status := self.local_db_manager.get_items_from_database(
            collection_name="rewards_status",
            find={
                "hypervisor_address": self.address,
                "rewarder_address": rewarder_address,
                "block": block,
            },
        ):
            return rewards_status
        else:
            return None

    # Transformers
    def convert_user_operation_toDb(self, status: user_operation) -> dict:
        """convert  type to a suitable format to be uploaded to database

        Args:
            status (user_status):

        Returns:
            dict:
        """
        # convert to dictionary
        result = self.convert_user_operation_to_dict(status=status)
        # convert decimal to decimal128
        result = self.local_db_manager.convert_decimal_to_d128(item=result)
        # return
        return result

    def convert_user_operation_fromDb(self, status: dict) -> user_operation:
        """convert database dict type to user_status

        Args:
            status (user_status):

        Returns:
            dict:
        """
        # convert to decimal
        result = self.local_db_manager.convert_d128_to_decimal(item=status)
        # convert to dictionary
        result = self.convert_user_operation_from_dict(status=result)
        # return
        return result

    def convert_user_operation_to_dict(self, status: user_operation) -> dict:
        fields_excluded = []

        return {
            p: getattr(status, p)
            for p in [
                a
                for a in dir(status)
                if not a.startswith("__")
                and not callable(getattr(status, a))
                and a not in fields_excluded
            ]
        }

    def convert_user_operation_from_dict(self, status: dict) -> user_operation:
        fields_excluded = []
        result = user_operation()
        for p in [
            a
            for a in dir(result)
            if not a.startswith("__")
            and not callable(getattr(result, a))
            and a not in fields_excluded
        ]:
            setattr(result, p, status[p])

        return result

    # Checks and Error reporting
    def _check_prices(self, price0, price1, block: int):
        """Raise Value error on price0 or price1 not available

        Args:
            price0 (_type_):
            price1 (_type_):
            block (int):

        Raises:
            ValueError:
        """

        message = None
        # check if prices are available
        if not price0 and not price1:
            message = f" Prices not available for {self.network}'s {self._static['pool']['token0']['address']} and {self._static['pool']['token1']['address']} token addresses at {block} block. Solve to continue."
        elif not price0:
            message = f" Price not available for {self.network}'s {self._static['pool']['token0']['address']} token address at {block} block. Solve to continue."
        elif not price1:
            message = f" Price not available for {self.network}'s {self._static['pool']['token1']['address']} token address at {block} block. Solve to continue."

        if message:
            raise ValueError(message)
