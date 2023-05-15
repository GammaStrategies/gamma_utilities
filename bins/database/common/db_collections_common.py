import logging

from bson.decimal128 import Decimal128, create_decimal128_context
from decimal import Decimal, localcontext
from datetime import datetime
from pymongo.errors import ConnectionFailure, BulkWriteError
from pymongo import DESCENDING, ASCENDING
from bins.database.common.db_managers import MongoDbManager


class db_collections_common:
    def __init__(self, mongo_url: str, db_name: str, db_collections: dict = None):
        if db_collections is None:
            db_collections = {"static": {"id": True}}
        self._db_mongo_url = mongo_url
        self._db_name = db_name
        self._db_collections = db_collections

    # actual db saving
    def save_items_to_database(
        self,
        data: list[dict],
        collection_name: str,
    ):
        """Save multiple items in a collection at once ( in bulk)

        Args:
            data (list[dict]): _description_
            collection_name (str): _description_
        """
        try:
            # create bulk data object
            bulk_data = [{"filter": {"id": item["id"]}, "data": item} for item in data]

            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                _db_manager.add_items_bulk(
                    coll_name=collection_name, data=bulk_data, upsert=True
                )
        except BulkWriteError as bwe:
            logging.getLogger(__name__).error(
                f"  Error while saving multiple items to {collection_name} collection database. Items qtty: {len(data)}  error-> {bwe.details}"
            )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to save multiple items to mongo's {collection_name} collection. Items qtty: {len(data)}  error-> {e}"
            )

    def save_item_to_database(
        self,
        data: dict,
        collection_name: str,
    ):
        try:
            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                _db_manager.add_item(
                    coll_name=collection_name, dbFilter={"id": data["id"]}, data=data
                )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to save data to mongo's {collection_name} collection.  Item: {data}    error-> {e}"
            )

    def replace_item_to_database(
        self,
        data: dict,
        collection_name: str,
    ):
        try:
            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                _db_manager.replace_item(
                    coll_name=collection_name, dbFilter={"id": data["id"]}, data=data
                )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to replace data in mongo's {collection_name} collection.  Item: {data}    error-> {e}"
            )

    def replace_items_to_database(
        self,
        data: list[dict],
        collection_name: str,
    ):
        """Replace multiple items in a collection at once ( in bulk)

        Args:
            data (list[dict]): _description_
            collection_name (str): _description_
        """
        try:
            # create bulk data object
            bulk_data = [{"filter": {"id": item["id"]}, "data": item} for item in data]

            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                _db_manager.replace_items_bulk(
                    coll_name=collection_name, data=bulk_data, upsert=True
                )
        except BulkWriteError as bwe:
            logging.getLogger(__name__).error(
                f"  Error while replacing multiple items in {collection_name} collection database. Items qtty: {len(data)}  error-> {bwe.details}"
            )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to replace multiple items in mongo's {collection_name} collection.  Items qtty: {len(data)}    error-> {e}"
            )

    def query_items_from_database(
        self,
        query: list[dict],
        collection_name: str,
    ) -> list:
        with MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        ) as _db_manager:
            result = list(
                _db_manager.get_items(coll_name=collection_name, aggregate=query)
            )
        return result

    def get_items_from_database(self, collection_name: str, **kwargs) -> list:
        with MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        ) as _db_manager:
            return [
                x
                for x in self.get_cursor(
                    db_manager=_db_manager, collection_name=collection_name, **kwargs
                )
            ]

    def get_distinct_items_from_database(
        self, collection_name: str, field: str, condition: dict = None
    ):
        if condition is None:
            condition = {}
        with MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        ) as _db_manager:
            result = list(
                _db_manager.get_distinct(
                    coll_name=collection_name, field=field, condition=condition
                )
            )
        return result

    def get_cursor(self, db_manager: MongoDbManager, collection_name: str, **kwargs):
        return db_manager.get_items(coll_name=collection_name, **kwargs)

    @staticmethod
    def convert_decimal_to_d128(item: dict) -> dict:
        """Converts a dictionary decimal values to BSON.decimal128, recursivelly.
            The function iterates a dict looking for types of Decimal and converts them to Decimal128.
            Embedded dictionaries and lists are called recursively.

        Args:
            item (dict):

        Returns:
            dict: converted values dict
        """
        if item is None:
            return None

        for k, v in list(item.items()):
            if isinstance(v, dict):
                db_collections_common.convert_decimal_to_d128(v)
            elif isinstance(v, list):
                for l in v:
                    db_collections_common.convert_decimal_to_d128(l)
            elif isinstance(v, Decimal):
                decimal128_ctx = create_decimal128_context()
                with localcontext(decimal128_ctx) as ctx:
                    item[k] = Decimal128(ctx.create_decimal(str(v)))

        return item

    @staticmethod
    def convert_d128_to_decimal(item: dict) -> dict:
        """Converts a dictionary decimal128 values to decimal, recursivelly.
            The function iterates a dict looking for types of Decimal128 and converts them to Decimal.
            Embedded dictionaries and lists are called recursively.

        Args:
            item (dict):

        Returns:
            dict: converted values dict
        """
        if item is None:
            return None

        for k, v in list(item.items()):
            if isinstance(v, dict):
                db_collections_common.convert_d128_to_decimal(v)
            elif isinstance(v, list):
                for l in v:
                    db_collections_common.convert_d128_to_decimal(l)
            elif isinstance(v, Decimal128):
                item[k] = v.to_decimal()

        return item

    @staticmethod
    def convert_decimal_to_float(item: dict) -> dict:
        """Converts a dictionary decimal values to float, recursivelly.
            The function iterates a dict looking for types of Decimal and converts them to float.
            Embedded dictionaries and lists are called recursively.

        Args:
            item (dict):

        Returns:
            dict: converted values dict
        """
        if item is None:
            return None

        for k, v in list(item.items()):
            if isinstance(v, dict):
                db_collections_common.convert_decimal_to_float(v)
            elif isinstance(v, list):
                for l in v:
                    db_collections_common.convert_decimal_to_float(l)
            elif isinstance(v, Decimal):
                item[k] = float(v)

        return item


class database_global(db_collections_common):
    """global database helper
    "blocks":
        item-> {id: <network>_<block_number>
                network:
                block:
                timestamp:
                }
    "usd_prices":
        item-> {id: <network>_<block_number>_<address>
                network:
                block:
                address:
                price:
                }
    """

    def __init__(
        self, mongo_url: str, db_name: str = "global", db_collections: dict = None
    ):
        if db_collections is None:
            db_collections = {
                "blocks": {
                    "mono_indexes": {"id": True, "network": False, "block": False},
                    "multi_indexes": [],
                },
                "usd_prices": {
                    "mono_indexes": {"id": True, "address": False},
                    "multi_indexes": [],
                },
            }
        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    def set_price_usd(
        self, network: str, block: int, token_address: str, price_usd: float
    ):
        data = {
            "id": f"{network}_{block}_{token_address}",
            "network": network,
            "block": int(block),
            "address": token_address,
            "price": float(price_usd),
        }

        self.save_item_to_database(data=data, collection_name="usd_prices")

    def set_block(self, network: str, block: int, timestamp: datetime.timestamp):
        data = {
            "id": f"{network}_{block}",
            "network": network,
            "block": block,
            "timestamp": timestamp,
        }
        self.save_item_to_database(data=data, collection_name="blocks")

    def get_unique_prices_addressBlock(self, network: str) -> list:
        """get addresses and blocks already present in database
            with price greater than zero.

        Args:
            network (str):

        Returns:
            list:
        """
        return self.get_items_from_database(
            collection_name="usd_prices", find={"network": network, "price": {"$gt": 0}}
        )

    def get_price_usd(
        self,
        network: str,
        block: int,
        address: str,
    ) -> list[dict]:
        """get usd price from block

        Args:
            network (str): ethereum, optimism, polygon....
            block (int): number
            address (str): token address

        Returns:
            list[dict]: list of price dict obj
        """
        return self.get_items_from_database(
            collection_name="usd_prices",
            find={"id": f"{network}_{block}_{address}"},
        )

    def get_price_usd_closestBlock(
        self,
        network: str,
        block: int,
        address: str,
    ) -> dict:
        """get usd price from closest block to <block>

        Args:
            network (str):
            block (int): number
            address (str): token address

        Returns:
            dict:
        """
        return self.query_items_from_database(
            query=self.query_blocks_closest(network=network, block=block),
            collection_name="usd_prices",
        )

    def get_timestamp(
        self,
        network: str,
        block: int,
    ) -> dict:
        return self.get_items_from_database(
            collection_name="blocks",
            find={"network": network, "block": block},
        )

    def get_closest_timestamp(self, network: str, block: int) -> dict:
        return self.query_items_from_database(
            query=self.query_blocks_closest(network=network, block=block),
            collection_name="blocks",
        )

    def get_block(
        self,
        network: str,
        timestamp: int,
    ) -> dict:
        return self.get_items_from_database(
            collection_name="blocks", find={"network": network, "timestamp": timestamp}
        )

    def get_closest_block(self, network: str, timestamp: int) -> dict:
        return self.query_items_from_database(
            query=self.query_blocks_closest(network=network, timestamp=timestamp),
            collection_name="blocks",
        )

    def get_all_block_timestamp(self, network: str) -> list:
        """get all blocks and timestamps from database
            sorted by block
        Args:
            network (str):

        Returns:
            list: of sorted blocks timestamps
        """
        return self.get_items_from_database(
            collection_name="blocks", find={"network": network}, sort=[("block", 1)]
        )

    @staticmethod
    def query_prices_addressBlocks(network: str) -> list[dict]:
        """get addresses and blocks of usd prices present at database and greater than zero

        Args:
            network (str):

        Returns:
            list[dict]:
        """
        return [
            {"$match": {"network": network, "price": {"$gt": 0}}},
        ]

    @staticmethod
    def query_blocks_closest(
        network: str, block: int = 0, timestamp: int = 0
    ) -> list[dict]:
        """find closest block/timestamp item in database

        Args:
            network (str):
            block (int, optional): . Defaults to 0.
            timestamp (int, optional): . Defaults to 0.

        Raises:
            NotImplementedError: when no block or timestamp are provided

        Returns:
            list[dict]:
        """
        if block != 0:
            _search = [block, "$block"]
        elif timestamp != 0:
            _search = [timestamp, "$timestamp"]
        else:
            raise NotImplementedError(
                " provide either block or timestamp. If both are provided, block will be used "
            )
        return [
            {"$match": {"network": network}},
            # Project a diff field that's the absolute difference along with the original doc.
            {
                "$project": {
                    "diff": {"$abs": {"$subtract": _search}},
                    "doc": "$$ROOT",
                }
            },
            # Order the docs by diff
            {"$sort": {"diff": 1}},
            # Take the first one
            {"$limit": 1},
        ]


class database_local(db_collections_common):
    """local database helper
    "static":
        item-> {id: <hypervisor_address>_
                "address": "",  # hypervisor id
                "created": None,  # datetime
                "fee": 0,  # 500
                "network": "",  # polygon
                "name": "",  # xWMATIC-USDC05
                "pool_id": "",  # pool id
                "tokens": [  db_objec_model.token... ],

    "operations":
        item-> {id: <logIndex>_<transactionHash>
                {
                    "_id" : ObjectId("63e0f19e2309ec2395434e4b"),
                    "transactionHash" : "0x8bf414df76a612ce2110cabec4fcaefd9cfc6aaeddd29d7850ac6fa2786adbb4",
                    "blockHash" : "0x286390969e2ddfa3aed6ed885c793bc78bb1974ec7f019116bed6b3edd5fa294",
                    "blockNumber" : 12590365,
                    "address" : "0x9a98bffabc0abf291d6811c034e239e916bbcec0",
                    "timestamp" : 1623108400,
                    "decimals_token0" : 18,
                    "decimals_token1" : 6,
                    "decimals_contract" : 18,
                    "tick" : -197716,
                    "totalAmount0" : "3246736264521404428",
                    "totalAmount1" : "6762363410",
                    "qtty_token0" : "3741331192922089",
                    "qtty_token1" : "0",
                    "topic" : "rebalance",
                    "logIndex" : 118,
                    "id" : "118_0x8bf414df76a612ce2110cabec4fcaefd9cfc6aaeddd29d7850ac6fa2786adbb4"
                }
                ...
                }

    "status":
        item-> {id: <hypervisor address>_<block_number>
                network:
                block:
                address:
                qtty_token0: 0,  # token qtty   (this is tvl = deployed_qtty + owed fees + parked_qtty )
                qtty_token1: 0,  #
                deployed_token0: 0,  # tokens deployed into pool
                deployed_token1: 0,  #
                parked_token0: 0,  # tokens sitting in hype contract ( sleeping )
                parked_token1: 0,  #
                supply: 0,  # total Suply

                }

    "user_status":
        item-> {id: <wallet_address>_<block_number>
                network:
                block:
                address:  <wallet_address>
                ...

                }
    """

    def __init__(self, mongo_url: str, db_name: str, db_collections: dict = None):
        if db_collections is None:
            db_collections = {
                "static": {
                    "mono_indexes": {"id": True, "address": False},
                    "multi_indexes": [],
                },
                "operations": {
                    "mono_indexes": {
                        "id": True,
                        "blockNumber": False,
                        "address": False,
                        "timestamp": False,
                    },
                    "multi_indexes": [
                        [("blockNumber", ASCENDING), ("logIndex", ASCENDING)],
                    ],
                },
                "status": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "address": False,
                        "timestamp": False,
                    },
                    "multi_indexes": [],
                },
                "user_status": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "address": False,
                        "hypervisor_address": False,
                        "timestamp": False,
                        "logIndex": False,
                    },
                    "multi_indexes": [
                        [("block", DESCENDING), ("logIndex", DESCENDING)],
                    ],
                },
                "user_operations": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "user_address": False,
                        "hypervisor_address": False,
                        "timestamp": False,
                        "logIndex": False,
                        "topic": False,
                    },
                    "multi_indexes": [
                        [("block", DESCENDING), ("logIndex", DESCENDING)],
                    ],
                },
                "rewards_static": {
                    "mono_indexes": {
                        "id": True,
                        "hypervisor_address": False,
                        "rewarder_address": False,
                    },
                    "multi_indexes": [],
                },
                "rewards_status": {
                    "mono_indexes": {
                        "id": True,
                        "hypervisor_address": False,
                        "rewarder_address": False,
                        "block": False,
                        "timestamp": False,
                    },
                    "multi_indexes": [],
                },
            }

        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    # static

    def set_static(self, data: dict):
        data["id"] = data["address"]
        self.save_item_to_database(data=data, collection_name="static")

    def get_gamma_service_fees(self) -> dict:
        """Get the agreed service fee (%) to be collected by the gamma protocol for each hypervisor

        Returns:
            dict: {"hypervisor address":  {"symbol":<symbol>, "dex":<dex> ,"fee":<fee>} }
        """
        # some hypes have the pool fee at gamma fees field: those are 1/10 hardcoded
        return {
            x["address"]: {
                x["symbol"],
                x["dex"],
                (1 / x["fee"]) if x["fee"] < 100 else 1 / 10,
            }
            for x in self.get_items_from_database(
                collection_name="static", projection={"address", "symbol", "dex", "fee"}
            )
        }

    def get_unique_tokens(self) -> list:
        """Get a unique token list from static database

        Returns:
            list:
        """
        return self.get_items_from_database(
            collection_name="static", aggregate=self.query_unique_token_addresses()
        )

    def get_mostUsed_tokens1(self, limit: int = 10) -> list:
        """Return the addresses of the top used tokens1, present in static database

        Args:
            limit (int, optional): . Defaults to 5.

        Returns:
            list: of {"token":<address>}
        """
        return self.get_items_from_database(
            collection_name="static",
            aggregate=self.query_status_mostUsed_token1(limit=limit),
        )

    # operation

    def set_operation(self, data: dict):
        self.replace_item_to_database(data=data, collection_name="operations")

    def get_all_operations(self, hypervisor_address: str) -> list:
        """find all hypervisor operations from db
            sort by lowest block and lowest logIndex first

        Args:
            hypervisor_address (str): address

        Returns:
            list: hypervisor status list
        """
        find = {"address": hypervisor_address}
        sort = [("blockNumber", 1), ("logIndex", 1)]
        return self.get_items_from_database(
            collection_name="operations", find=find, sort=sort
        )

    def get_hype_operations_btwn_timestamps(
        self,
        hypervisor_address: str,
        timestamp_ini: int,
        timestamp_end: int,
    ) -> list:
        return self.query_items_from_database(
            collection_name="operations",
            query=self.query_operations_btwn_timestamps(
                hypervisor_address=hypervisor_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
            ),
        )

    def get_unique_operations_addressBlock(self, topics: list = None) -> list:
        """Retrieve a list of unique blocks + hypervisor addresses present in operations collection

        Returns:
            list: of  {
                    "address" : "0x407e99b20d61f245426031df872966953909e9d3",
                    "block" : 12736656
                    }
        """
        query = []
        if topics:
            query.append({"$match": {"topics": {"$in": topics}}})

        query.extend(
            (
                {
                    "$group": {
                        "_id": {"address": "$address", "block": "$blockNumber"},
                    }
                },
                {
                    "$project": {
                        "address": "$_id.address",
                        "block": "$_id.block",
                    }
                },
                {"$unset": ["_id"]},
            )
        )

        debug_query = f"{query}"

        return self.get_items_from_database(
            collection_name="operations", aggregate=query
        )

    def get_user_operations(
        self, user_address: str, timestamp_ini: int | None, timestamp_end: int | None
    ) -> list[dict]:
        """Get all operations for a user

        Args:
            user_address (str):
            timestamp_ini (int | None):
            timestamp_end (int | None):

        Returns:
            list[dict]:  {
                "hypervisor_address: str,
                "operations": list[ operation dict]
            }


        """
        find = {
            "$or": [
                {"src": user_address},
                {"dst": user_address},
                {"from": user_address},
                {"to": user_address},
            ]
        }

        if timestamp_ini and timestamp_end:
            find["$and"] = [
                {"timestamp": {"$lte": timestamp_end}},
                {"timestamp": {"$gte": timestamp_ini}},
            ]
        elif timestamp_ini:
            find["timestamp"] = {"$gte": timestamp_ini}
        elif timestamp_end:
            find["timestamp"] = {"$lte": timestamp_end}

        sort = [("block", 1)]
        return self.get_items_from_database(
            collection_name="status", find=find, sort=sort
        )

    # status

    def set_status(self, data: dict):
        # define database id
        data["id"] = f"{data['address']}_{data['block']}"
        self.save_item_to_database(data=data, collection_name="status")

    def get_all_status(self, hypervisor_address: str) -> list:
        """find all hypervisor status from db
            sort by lowest block first

        Args:
            hypervisor_address (str): address

        Returns:
            list: hypervisor status list
        """
        find = {"address": hypervisor_address}
        sort = [("block", 1)]
        return self.get_items_from_database(
            collection_name="status", find=find, sort=sort
        )

    def get_hype_status_btwn_blocks(
        self,
        hypervisor_address: str,
        block_ini: int,
        block_end: int,
    ) -> list:
        return self.query_items_from_database(
            collection_name="status",
            query=self.query_status_btwn_blocks(
                hypervisor_address=hypervisor_address,
                block_ini=block_ini,
                block_end=block_end,
            ),
        )

    def get_hype_status_blocks(self, hypervisor_address: str, blocks: list) -> list:
        find = {"address": hypervisor_address, "block": {"$in": blocks}}
        sort = [("block", 1)]
        return self.get_items_from_database(
            collection_name="status", find=find, sort=sort
        )

    def get_unique_status_addressBlock(self) -> list:
        """Retrieve a list of unique blocks + hypervisor addresses present in status collection

        Returns:
            list: of {
                    "address" : "0x407e99b20d61f245426031df872966953909e9d3",
                    "block" : 12736656
                    }
        """
        query = [
            {
                "$group": {
                    "_id": {"address": "$address", "block": "$block"},
                }
            },
            {
                "$project": {
                    "address": "$_id.address",
                    "block": "$_id.block",
                }
            },
            {"$unset": ["_id"]},
        ]
        return self.get_items_from_database(collection_name="status", aggregate=query)

    def get_status_feeReturn_data(
        self,
        hypervisor_address: str,
        timestamp_ini: int,
        timestamp_end: int,
    ) -> list:
        return self.query_items_from_database(
            collection_name="status",
            query=self.query_status_feeReturn_data(
                hypervisor_address=hypervisor_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
            ),
        )

    def get_status_feeReturn_data_alternative(
        self,
        hypervisor_address: str,
        timestamp_ini: int,
        timestamp_end: int,
    ) -> list:
        return self.query_items_from_database(
            collection_name="status",
            query=self.query_status_feeReturn_data_alternative(
                hypervisor_address=hypervisor_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
            ),
        )

    # user status

    def set_user_status(self, data: dict):
        """

        Args:
            data (dict):
        """
        # define database id
        data[
            "id"
        ] = f"{data['address']}_{data['block']}_{data['logIndex']}_{data['hypervisor_address']}"

        # convert decimal to bson compatible and save
        self.replace_item_to_database(data=data, collection_name="user_status")

    def set_user_status_bulk(self, data: list[dict]):
        """Bulk insert user status

        Args:
            data (list[dict]):
        """
        # define database ids
        for item in data:
            item[
                "id"
            ] = f"{item['address']}_{item['block']}_{item['logIndex']}_{item['hypervisor_address']}"

        # convert decimal to bson compatible and save
        self.replace_items_to_database(data=data, collection_name="user_status")

    def get_user_status(
        self, address: str, block_ini: int = 0, block_end: int = 0
    ) -> list:
        # convert bson to Decimal
        find = {"address": address}
        sort = [("block", 1)]
        return [
            self.convert_d128_to_decimal(item=item)
            for item in self.get_items_from_database(
                collection_name="user_status", find=find, sort=sort
            )
        ]

    # user operations
    def set_user_operation(self, data: dict):
        """

        Args:
            data (dict):
        """
        # define database id
        data[
            "id"
        ] = f"{data['user_address']}_{data['block']}_{data['logIndex']}_{data['hypervisor_address']}"

        # convert decimal to bson compatible and save
        self.replace_item_to_database(data=data, collection_name="user_operations")

    def set_user_operations_bulk(self, data: list[dict]):
        """Bulk insert user operations

        Args:
            data (list[dict]):
        """
        # define database ids
        for item in data:
            item[
                "id"
            ] = f"{item['user_address']}_{item['block']}_{item['logIndex']}_{item['hypervisor_address']}"

        # convert decimal to bson compatible and save
        self.replace_items_to_database(data=data, collection_name="user_operations")

    # rewards static

    def set_rewards_static(self, data: dict):
        """Save rewarder static data to db

        Args:
            data (dict):        "network":
                                "block":
                                "timestamp":
                                "hypervisor_address":
                                "rewarder_address":
                                "rewarder_type":        class type of rewarder, may be a masterchef or a thena rewarder or zyberswap masterchef ...
                                "rewarder_refIds": [],  list of pids or any other info to identify the hypervisor inside the rewarder
                                "rewardToken":
                                "rewardToken_symbol":
                                "rewardToken_decimals":
                                "rewards_perSecond":   ( not converted to decimal)
                                "total_hypervisorToken_qtty":  Total amount of hypervisor tokens inside the rewards contract
        """
        # define database id using the first refid if present ( being pid or any other info to identify the hypervisor inside the rewarder)
        # dbid = data["rewarder_refIds"][0] if data["rewarder_refIds"] else 0
        # define database id-->  hypervisorAddress_rewarderAddress
        data["id"] = f"{data['hypervisor_address']}_{data['rewarder_address']}"
        self.save_item_to_database(data=data, collection_name="rewards_static")

    def get_rewards_static(
        self, rewarder_address: str | None = None, hypervisor_address: str | None = None
    ) -> list:
        """Get rewarders static data from db.
            Specify either address or hypervisor_address

        Args:
            rewarder_address (str | None): rewarder address
            hypervisor_address (str | None, optional): hype address. Defaults to None.

        Returns:
            list: of rewarders found
        """
        find = {}
        if rewarder_address:
            find["rewarder_address"] = rewarder_address
        if hypervisor_address:
            find["hypervisor_address"] = hypervisor_address

        return self.get_items_from_database(collection_name="rewards_static", find=find)

    # rewards status
    def set_rewards_status(self, data: dict):
        """Save rewarder status data to db

        Args:
            data (dict):        "network":
                                "block":
                                "timestamp":
                                "hypervisor_address":
                                "rewarder_address":
                                "rewarder_type":        class type of rewarder, may be a masterchef or a thena rewarder or zyberswap masterchef ...
                                "rewarder_refIds": [],  list of pids or any other info to identify the hypervisor inside the rewarder
                                "rewardToken":
                                "rewardToken_symbol":
                                "rewardToken_decimals":
                                "rewards_perSecond":   ( not converted to decimal)
                                "total_hypervisorToken_qtty":  Total amount of hypervisor tokens inside the rewards contract
                                "apr"
                                "hypervisor_symbol"
                                "hyperivsor_share price"
                                "token 0 price"
                                "token 1 price"
                                "rewardToken_price"

        """
        # define database id-->  hypervisorAddress_rewarderAddress_block
        data[
            "id"
        ] = f"{data['hypervisor_address']}_{data['rewarder_address']}_{data['block']}"
        self.save_item_to_database(data=data, collection_name="rewards_status")

    # all

    def get_items(self, collection_name: str, **kwargs) -> list:
        """Any

        Returns:
            list: of results
        """
        return self.get_items_from_database(collection_name=collection_name, **kwargs)

    def get_max_field(self, collection: str, field: str) -> list:
        """get the maximum field present in db
        Args:
            collection (str): _description_
            field (str): _description_

        Returns:
            list: of { "max": <value>}
        """
        return self.get_items_from_database(
            collection_name=collection,
            aggregate=self.query_max(field=field),
        )

    # queries

    @staticmethod
    def query_unique_addressBlocks() -> list[dict]:
        """retriev

        Args:
            field (str): ca

        Returns:
            list[dict]: _description_
        """
        # return query
        return [
            {
                "$group": {
                    "_id": {"address": "$address", "block": "$blockNumber"},
                }
            },
            {
                "$project": {
                    "address": "$_id.address",
                    "block": "$_id.block",
                }
            },
            {"$unset": ["_id"]},
        ]

    @staticmethod
    def query_unique_token_addresses() -> list[dict]:
        """Unique token list using status database

        Returns:
            list[dict]:
        """
        return [
            {
                "$group": {
                    "_id": "$pool.address",
                    "items": {"$push": "$$ROOT"},
                }
            },
            {"$project": {"_id": "$_id", "last": {"$last": "$items"}}},
            {
                "$project": {
                    "_id": "$_id",
                    "token": ["$last.pool.token0.address", "$last.pool.token1.address"],
                }
            },
            {"$unwind": "$token"},
            {"$group": {"_id": "$token"}},
        ]

    @staticmethod
    def query_operations_btwn_timestamps(
        hypervisor_address: str,
        timestamp_ini: int,
        timestamp_end: int,
    ) -> list[dict]:
        """get operations between timestamps

        Args:
            timestamp_ini (datetime.timestamp): initial timestamp
            timestamp_end (datetime.timestamp): end timestamp

        Returns:
            list[dict]:
        """
        return [
            {
                "$match": {
                    "address": hypervisor_address,
                    "timestamp": {"$gte": timestamp_ini, "$lte": timestamp_end},
                }
            },
            {"$sort": {"blockNumber": -1, "logIndex": 1}},
        ]

    @staticmethod
    def query_status_btwn_blocks(
        hypervisor_address: str,
        block_ini: datetime.timestamp,
        block_end: datetime.timestamp,
    ) -> list[dict]:
        """get status between blocks"""
        return [
            {
                "$match": {
                    "address": hypervisor_address,
                    "block": {"$gte": block_ini, "$lte": block_end},
                }
            },
            {"$sort": {"block": -1}},
        ]

    @staticmethod
    def query_status_mostUsed_token1(limit: int = 5) -> list[dict]:
        """return the top most used token1 address of static database
            ( may be used in status too)

        Returns:
            list[dict]: _description_
        """
        return [
            {
                "$group": {
                    "_id": {"token1": "$pool.token1.address"},
                    "symbol": {"$last": "$pool.token1.symbol"},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": limit},
            {
                "$project": {
                    "token": "$_id.token1",
                    "symbol": "$symbol",
                }
            },
            {"$unset": ["_id"]},
        ]

    @staticmethod
    def query_max(field: str) -> list[dict]:
        return [
            {
                "$group": {
                    "_id": "id",
                    "max": {"$max": f"${field}"},
                }
            },
            {"$unset": ["_id"]},
        ]

    # @staticmethod
    # def query_status_feeAPY(
    #     hypervisor_address: str,
    #     timestamp_ini: int,
    #     timestamp_end: int,
    # ) -> list[dict]:
    #     """get all status and their block operations between timestamps

    #         a new field called "operations" with a list of operations will be available at status object dict

    #     Args:
    #         hypervisor_address (str): hypervsor address
    #         timestamp_ini (int): initial timestamp greater or equal to
    #         timestamp_end (int): ending timestamp less or equal to

    #     Returns:
    #         list[dict]: query
    #     """
    #     return [
    #         {
    #             "$match": {
    #                 "address": hypervisor_address,
    #                 "$and": [
    #                     {"timestamp": {"$gte": timestamp_ini}},
    #                     {"timestamp": {"$lte": timestamp_end}},
    #                 ],
    #             }
    #         },
    #         {
    #             "$lookup": {
    #                 "from": "operations",
    #                 "let": {
    #                     "status_address": "$address",
    #                     "status_block": "$block",
    #                     "topics": ["deposit", "withdraw", "rebalance", "zeroBurn"],
    #                 },
    #                 "pipeline": [
    #                     {
    #                         "$match": {
    #                             "$expr": {
    #                                 "$and": [
    #                                     {"$eq": ["$address", "$$status_address"]},
    #                                     {"$eq": ["$blockNumber", "$$status_block"]},
    #                                     {"$in": ["$topic", "$$topics"]},
    #                                 ],
    #                             }
    #                         }
    #                     }
    #                 ],
    #                 "as": "operations",
    #             }
    #         },
    #         {"$sort": {"block": 1}},
    #     ]

    @staticmethod
    def query_status_feeReturn_data(
        hypervisor_address: str,
        timestamp_ini: int,
        timestamp_end: int,
    ) -> list[dict]:
        """Get data to construct feeAPY APR using equal totalSupply values between blocks to identify APY period.
            This is a prone to error method as totalSupply may sporadically coincide in diff periods ...

        Args:
            hypervisor_address (str):
            timestamp_ini (int):
            timestamp_end (int):

        Returns:
            list[dict]:
                            "period_days" : 0.01653935185185185,
                            "ini_block" : NumberInt(14937408),
                            "end_block" : NumberInt(14937491),
                            "ini_timestamp" : NumberInt(1654849152),
                            "end_timestamp" : NumberInt(1654850581),
                            "ini_supply" : NumberDecimal("91.89181431665138243"),
                            "end_supply" : NumberDecimal("91.89181431665138243"),
                            "ini_tvl0" : NumberDecimal("47083.654951511146650678"),
                            "ini_tvl1" : NumberDecimal("69.395815272326034611"),
                            "ini_fees_uncollected0" : NumberDecimal("6.523979236566497"),
                            "ini_fees_uncollected1" : NumberDecimal("0.01195312935918579"),
                            "ini_fees_owed0" : NumberDecimal("0.0000"),
                            "ini_fees_owed1" : NumberDecimal("0.0000"),
                            "end_tvl0" : NumberDecimal("47083.654951511146650678"),
                            "end_tvl1" : NumberDecimal("69.395815272326034611"),
                            "end_fees_uncollected0" : NumberDecimal("6.523979236566497"),
                            "end_fees_uncollected1" : NumberDecimal("0.01195312935918579"),
                            "end_fees_owed0" : NumberDecimal("0.0000"),
                            "end_fees_owed1" : NumberDecimal("0.0000"),
                            "error_ini" : NumberInt(0),
                            "error_end" : NumberInt(0)
        """
        return [
            {
                "$match": {
                    "address": hypervisor_address,
                    "$and": [
                        {"timestamp": {"$lte": timestamp_end}},
                        {"timestamp": {"$gte": timestamp_ini}},
                    ],
                }
            },
            {"$sort": {"block": 1}},
            {
                "$group": {
                    "_id": "$totalSupply",
                    "items": {"$push": "$$ROOT"},
                    "max_block": {"$max": "$block"},
                    "min_block": {"$min": "$block"},
                    "max_timestamp": {"$max": "$timestamp"},
                    "min_timestamp": {"$min": "$timestamp"},
                }
            },
            {
                "$addFields": {
                    "period_days": {
                        "$divide": [
                            {"$subtract": ["$max_timestamp", "$min_timestamp"]},
                            60 * 60 * 24,
                        ]
                    }
                }
            },
            {"$sort": {"min_block": 1}},
            {
                "$project": {
                    "max_block": "$max_block",
                    "min_block": "$min_block",
                    "max_timestamp": "$max_timestamp",
                    "min_timestamp": "$min_timestamp",
                    "period_days": "$period_days",
                    "ini_snapshot": {"$arrayElemAt": ["$items", 0]},
                    "end_snapshot": {"$arrayElemAt": ["$items", -1]},
                }
            },
            {
                "$project": {
                    "max_block": "$max_block",
                    "min_block": "$min_block",
                    "max_timestamp": "$max_timestamp",
                    "min_timestamp": "$min_timestamp",
                    "period_days": "$period_days",
                    "ini_snapshot": "$ini_snapshot",
                    "end_snapshot": "$end_snapshot",
                    "error_ini": {"$subtract": ["$ini_snapshot.block", "$min_block"]},
                    "error_end": {"$subtract": ["$end_snapshot.block", "$max_block"]},
                }
            },
            {
                "$project": {
                    "period_days": "$period_days",
                    "ini_block": "$ini_snapshot.block",
                    "end_block": "$end_snapshot.block",
                    "ini_timestamp": "$ini_snapshot.timestamp",
                    "end_timestamp": "$end_snapshot.timestamp",
                    "ini_supply": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.totalSupply"},
                            {"$pow": [10, "$ini_snapshot.decimals"]},
                        ]
                    },
                    "end_supply": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.totalSupply"},
                            {"$pow": [10, "$end_snapshot.decimals"]},
                        ]
                    },
                    "ini_tvl0": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.totalAmounts.total0"},
                            {"$pow": [10, "$ini_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "ini_tvl1": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.totalAmounts.total1"},
                            {"$pow": [10, "$ini_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "ini_fees_uncollected0": {
                        "$divide": [
                            {
                                "$toDecimal": "$ini_snapshot.fees_uncollected.qtty_token0"
                            },
                            {"$pow": [10, "$ini_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "ini_fees_uncollected1": {
                        "$divide": [
                            {
                                "$toDecimal": "$ini_snapshot.fees_uncollected.qtty_token1"
                            },
                            {"$pow": [10, "$ini_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "ini_fees_owed0": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.tvl.fees_owed_token0"},
                            {"$pow": [10, "$ini_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "ini_fees_owed1": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.tvl.fees_owed_token1"},
                            {"$pow": [10, "$ini_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "end_tvl0": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.totalAmounts.total0"},
                            {"$pow": [10, "$end_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "end_tvl1": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.totalAmounts.total1"},
                            {"$pow": [10, "$end_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "end_fees_uncollected0": {
                        "$divide": [
                            {
                                "$toDecimal": "$end_snapshot.fees_uncollected.qtty_token0"
                            },
                            {"$pow": [10, "$end_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "end_fees_uncollected1": {
                        "$divide": [
                            {
                                "$toDecimal": "$end_snapshot.fees_uncollected.qtty_token1"
                            },
                            {"$pow": [10, "$end_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "end_fees_owed0": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.tvl.fees_owed_token0"},
                            {"$pow": [10, "$end_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "end_fees_owed1": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.tvl.fees_owed_token1"},
                            {"$pow": [10, "$end_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "error_ini": "$error_ini",
                    "error_end": "$error_end",
                }
            },
            {"$unset": ["_id"]},
        ]

    @staticmethod
    def query_status_feeReturn_data_alternative(
        hypervisor_address: str, timestamp_ini: int, timestamp_end: int
    ) -> list[dict]:
        """

            old descript: return a list of status ordered by block matching deposit,withdraw,rebalance and zeroBurn operation blocks and those same blocks -1
            Each status has a order field indicating if this is the initial period status with a "first" value
            or this is the end of the perios status with the "last" value

        Args:
            hypervisor_address (str):
            timestamp_ini (int):
            timestamp_end (int):

        Returns:
            list[dict]:   Each status has an <order> field indicating if this is the initial period status with a "first" value
            or this is the end of the perios status with the "last" value
        """

        # return [
        #     {
        #         "$match": {
        #             "address": hypervisor_address,
        #             "topic": {"$in": ["deposit", "withdraw", "rebalance", "zeroBurn"]},
        #             "$and": [
        #                 {"timestamp": {"$lte": timestamp_end}},
        #                 {"timestamp": {"$gte": timestamp_ini}},
        #             ],
        #         }
        #     },
        #     {"$group": {"_id": "$blockNumber", "address": {"$first": "$address"}}},
        #     {"$sort": {"_id": 1}},
        #     {
        #         "$lookup": {
        #             "from": "status",
        #             "let": {"op_block": {"$toInt": "$_id"}, "op_address": "$address"},
        #             "pipeline": [
        #                 {
        #                     "$match": {
        #                         "$expr": {
        #                             "$and": [
        #                                 {"$eq": ["$address", "$$op_address"]},
        #                                 {
        #                                     "$or": [
        #                                         {"$eq": ["$block", "$$op_block"]},
        #                                         {
        #                                             "$eq": [
        #                                                 "$block",
        #                                                 {
        #                                                     "$subtract": [
        #                                                         "$$op_block",
        #                                                         1,
        #                                                     ]
        #                                                 },
        #                                             ]
        #                                         },
        #                                     ]
        #                                 },
        #                             ],
        #                         }
        #                     }
        #                 },
        #                 {
        #                     "$addFields": {
        #                         "order": {
        #                             "$cond": [
        #                                 {"$eq": ["$block", "$$op_block"]},
        #                                 "first",
        #                                 "last",
        #                             ]
        #                         },
        #                     }
        #                 },
        #                 {"$sort": {"block": 1}},
        #             ],
        #             "as": "status",
        #         }
        #     },
        #     {"$project": {"_id": "$$ROOT.status"}},
        #     {"$unwind": "$_id"},
        #     {"$replaceRoot": {"newRoot": "$_id"}},
        #     {"$sort": {"block": 1}},
        # ]

        return [
            {
                "$match": {
                    "address": hypervisor_address,
                    "$and": [
                        {"timestamp": {"$lte": timestamp_end}},
                        {"timestamp": {"$gte": timestamp_ini}},
                    ],
                }
            },
            {"$sort": {"block": 1}},
            {
                "$group": {
                    "_id": "$totalSupply",
                    "items": {"$push": "$$ROOT"},
                    "max_block": {"$max": "$block"},
                    "min_block": {"$min": "$block"},
                    "max_timestamp": {"$max": "$timestamp"},
                    "min_timestamp": {"$min": "$timestamp"},
                }
            },
            {
                "$addFields": {
                    "period_days": {
                        "$divide": [
                            {"$subtract": ["$max_timestamp", "$min_timestamp"]},
                            60 * 60 * 24,
                        ]
                    }
                }
            },
            {"$sort": {"min_block": 1}},
            {"$unwind": "$items"},
            {"$replaceRoot": {"newRoot": "$items"}},
            {"$unset": ["_id", "id"]},
        ]

    @staticmethod
    def query_all_users(
        user_address: str, timestamp_ini: int = None, timestamp_end: int = None
    ) -> list[dict]:
        _match = {
            "$or": [
                {"src": user_address},
                {"dst": user_address},
                {"from": user_address},
                {"to": user_address},
            ]
        }

        if timestamp_ini and timestamp_end:
            _match["$and"] = [
                {"timestamp": {"$lte": timestamp_end}},
                {"timestamp": {"$gte": timestamp_ini}},
            ]
        elif timestamp_ini:
            _match["timestamp"] = {"$gte": timestamp_ini}
        elif timestamp_end:
            _match["timestamp"] = {"$lte": timestamp_end}

        return [{"$match": _match}, {"$sort": {"timestamp": 1}}]
