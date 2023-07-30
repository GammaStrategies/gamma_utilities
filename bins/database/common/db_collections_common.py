import logging
import time

from bson.decimal128 import Decimal128, create_decimal128_context
from decimal import Decimal, localcontext
from datetime import datetime
from pymongo.errors import ConnectionFailure, BulkWriteError
from pymongo import DESCENDING, ASCENDING
from ...database.common.database_ids import (
    create_id_block,
    create_id_current_price,
    create_id_hypervisor_returns,
    create_id_hypervisor_status,
    create_id_price,
    create_id_rewards_static,
    create_id_rewards_status,
    create_id_user_operation,
    create_id_user_status,
)
from ...database.common.db_managers import MongoDbManager
from ...general.enums import queueItemType
from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)


class db_collections_common:
    def __init__(self, mongo_url: str, db_name: str, db_collections: dict = None):
        if db_collections is None:
            db_collections = {"static": {"id": True}}
        self._db_mongo_url = mongo_url
        self._db_name = db_name
        self._db_collections = db_collections

    def delete_item(self, collection_name: str, item_id: str) -> DeleteResult:
        """Delete an item from a collection

        Args:
            collection_name (str): _description_
            item_id (str): _description_
        """
        db_result = None
        with MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        ) as _db_manager:
            db_result = _db_manager.del_item(
                coll_name=collection_name, dbFilter={"id": item_id}
            )

        return db_result

    def delete_items(
        self,
        data: list[dict],
        collection_name: str,
    ) -> BulkWriteResult:
        """Delete multiple items at once ( in bulk)

        Args:
            data (list[dict]):
            collection_name (str):
        """
        result = None
        try:
            # create bulk data object
            bulk_data = [{"filter": {"id": item["id"]}, "data": item} for item in data]

            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                result = _db_manager.del_items_in_bulk(
                    coll_name=collection_name, data=bulk_data
                )
        except BulkWriteError as bwe:
            logging.getLogger(__name__).error(
                f"  Error while replacing multiple items in {collection_name} collection database. Items qtty: {len(data)}  error-> {bwe.details}"
            )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to replace multiple items in mongo's {collection_name} collection.  Items qtty: {len(data)}    error-> {e}"
            )

        return result

    # actual db saving
    def save_items_to_database(
        self,
        data: list[dict],
        collection_name: str,
    ) -> BulkWriteResult:
        """Save multiple items in a collection at once ( in bulk)

        Args:
            data (list[dict]): _description_
            collection_name (str): _description_
        """
        # TODO: solve error not saving  :

        try:
            # create bulk data object
            bulk_data = [{"filter": {"id": item["id"]}, "data": item} for item in data]

            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                return _db_manager.add_items_bulk(
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
    ) -> UpdateResult:
        try:
            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                return _db_manager.add_item(
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
    ) -> UpdateResult:
        try:
            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                return _db_manager.replace_item(
                    coll_name=collection_name, dbFilter={"id": data["id"]}, data=data
                )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unable to replace data in mongo's {collection_name} collection.  Item: {data}    error-> {e}"
            )

    def replace_items_to_database(
        self,
        data: list[dict],
        collection_name: str,
    ) -> BulkWriteResult:
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
                return _db_manager.replace_items_bulk(
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

    def insert_if_not_exists(
        self,
        data: dict,
        collection_name: str,
    ) -> UpdateResult:
        try:
            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                return _db_manager.insert_if_not_exists(
                    coll_name=collection_name, dbFilter={"id": data["id"]}, data=data
                )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to insert if not exists data in mongo's {collection_name} collection.  Item: {data}    error-> {e}"
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
    ) -> list:
        if condition is None:
            condition = {}
        with MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        ) as _db_manager:
            result = _db_manager.get_distinct(
                coll_name=collection_name, field=field, condition=condition
            )

        return result

    def get_cursor(self, db_manager: MongoDbManager, collection_name: str, **kwargs):
        return db_manager.get_items(coll_name=collection_name, **kwargs)

    def find_one_and_update(self, collection_name: str, find: dict, update: dict):
        with MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        ) as _db_manager:
            return _db_manager.find_one_and_update(
                coll_name=collection_name, dbFilter=find, update=update
            )

    @property
    def db_manager(self) -> MongoDbManager:
        return MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        )

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
                    if isinstance(l, dict):
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
                    if isinstance(l, dict):
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
                    if isinstance(l, dict):
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
                    "mono_indexes": {
                        "id": True,
                        "address": False,
                        "block": False,
                        "network": False,
                    },
                    "multi_indexes": [
                        [
                            ("address", ASCENDING),
                            ("block", ASCENDING),
                            ("network", ASCENDING),
                        ],
                    ],
                },
                "current_usd_prices": {
                    "mono_indexes": {"id": True, "address": False},
                    "multi_indexes": [
                        [
                            ("address", ASCENDING),
                            ("network", ASCENDING),
                        ],
                    ],
                },
            }
        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    def set_price_usd(
        self,
        network: str,
        block: int,
        token_address: str,
        price_usd: float,
        source: str,
    ) -> UpdateResult:
        data = {
            "id": create_id_price(
                network=network, block=block, token_address=token_address
            ),
            "network": network,
            "block": int(block),
            "address": token_address,
            "price": float(price_usd),
            "source": source,
        }

        return self.save_item_to_database(data=data, collection_name="usd_prices")

    def set_current_price_usd(
        self,
        network: str,
        token_address: str,
        price_usd: float,
        source: str,
    ):
        data = {
            "id": create_id_current_price(network=network, token_address=token_address),
            "network": network,
            "timestamp": int(datetime.now().timestamp()),
            "address": token_address,
            "price": float(price_usd),
            "source": source,
        }

        self.save_item_to_database(data=data, collection_name="current_usd_prices")

    def set_block(
        self, network: str, block: int, timestamp: datetime.timestamp
    ) -> UpdateResult:
        data = {
            "id": create_id_block(network=network, block=block),
            "network": network,
            "block": block,
            "timestamp": timestamp,
        }
        return self.save_item_to_database(data=data, collection_name="blocks")

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
            find={
                "id": create_id_price(
                    network=network, block=block, token_address=address
                )
            },
        )

    def get_prices_usd_last(self, network: str) -> list[dict]:
        """get last block known prices of all tokens present in the database"""
        return self.get_items_from_database(
            collection_name="usd_prices",
            aggregate=self.query_last_prices(network=network),
        )

    def get_price_usd_closestBlock(
        self,
        network: str,
        block: int,
        address: str,
        limit: int = 2,
    ) -> list[dict]:
        """get usd price from closest block to <block>

        Args:
            network (str):
            block (int): number
            address (str): token address

        Returns:
            dict:
        """
        return self.query_items_from_database(
            query=self.query_blocks_closest(network=network, block=block, limit=limit),
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
        network: str, block: int = 0, timestamp: int = 0, limit: int = 10
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
            {"$limit": limit},
        ]

    @staticmethod
    def query_last_prices(network: str, limit: int | None = None) -> list[dict]:
        """get last prices from database

        Args:
            network (str):

        Returns:
            list[dict]:
        """
        query = [
            {"$match": {"network": network}},
            {"$sort": {"block": -1}},
        ]
        if limit:
            query.append({"$limit": limit})

        query.append({"$group": {"_id": "$address", "doc": {"$first": "$$ROOT"}}})
        query.append({"$replaceRoot": {"newRoot": "$doc"}})
        return query


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
                "queue": {
                    "mono_indexes": {
                        "id": True,
                        "type": False,
                    },
                    "multi_indexes": [],
                },
                "hypervisor_returns": {
                    "mono_indexes": {
                        "id": True,
                        "address": False,
                        "ini_block": False,
                        "end_block": False,
                        "ini_timestamp": False,
                        "end_timestamp": False,
                    },
                    "multi_indexes": [],
                },
            }

        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    # queue
    def set_queue_item(self, data: dict) -> UpdateResult:
        # data should already have a unique id ( is an operation )
        # save to db
        return self.replace_item_to_database(data=data, collection_name="queue")

    def get_queue_item(
        self, types: list[queueItemType] | None = None, find: dict | None = None
    ) -> dict | None:
        if not find:
            find = {"processing": 0}
        if types:
            find["type"] = {"$in": types}

        # get one item from queue
        return self.find_one_and_update(
            collection_name="queue",
            find=find,
            update={"$set": {"processing": time.time()}},
        )

    def del_queue_item(self, id: str) -> DeleteResult:
        return self.delete_item(collection_name="queue", item_id=id)

    def free_queue_item(self, db_queue_item: dict) -> UpdateResult:
        """set queue object free to be processed again

        Args:
            db_queue_item (dict):
        """
        logging.getLogger(__name__).debug(
            f" freeing {db_queue_item['type']}:  {db_queue_item['id']} from queue"
        )
        db_queue_item["processing"] = 0
        return self.replace_item_to_database(
            data=db_queue_item, collection_name="queue"
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

    def set_operation(self, data: dict) -> UpdateResult:
        return self.replace_item_to_database(data=data, collection_name="operations")

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

    def get_hypervisor_operations(
        self,
        hypervisor_address: str,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
        block_ini: int | None = None,
        block_end: int | None = None,
    ) -> list:
        return self.query_items_from_database(
            collection_name="operations",
            query=self.query_operations(
                hypervisor_address=hypervisor_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
                block_ini=block_ini,
                block_end=block_end,
            ),
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
            query.append({"$match": {"topic": {"$in": topics}}})

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
        data["id"] = create_id_hypervisor_status(
            hypervisor_address=data["address"], block=data["block"]
        )
        return self.save_item_to_database(data=data, collection_name="status")

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
        data["id"] = create_id_user_status(
            user_address=data["address"],
            block=data["block"],
            logIndex=data["logIndex"],
            hypervisor_address=data["hypervisor_address"],
        )

        # convert decimal to bson compatible and save
        self.replace_item_to_database(data=data, collection_name="user_status")

    def set_user_status_bulk(self, data: list[dict]):
        """Bulk insert user status

        Args:
            data (list[dict]):
        """
        # define database ids
        for item in data:
            item["id"] = create_id_user_status(
                user_address=item["address"],
                block=item["block"],
                logIndex=item["logIndex"],
                hypervisor_address=item["hypervisor_address"],
            )

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
    def set_user_operation(self, data: dict) -> UpdateResult:
        """

        Args:
            data (dict):
        """
        # define database id
        data["id"] = create_id_user_operation(
            user_address=data["user_address"],
            block=data["block"],
            logIndex=data["logIndex"],
            hypervisor_address=data["hypervisor_address"],
        )

        # convert decimal to bson compatible and save
        return self.replace_item_to_database(
            data=data, collection_name="user_operations"
        )

    def set_user_operations_bulk(self, data: list[dict]):
        """Bulk insert user operations

        Args:
            data (list[dict]):
        """
        # define database ids
        for item in data:
            item["id"] = create_id_user_operation(
                user_address=data["user_address"],
                block=data["block"],
                logIndex=data["logIndex"],
                hypervisor_address=data["hypervisor_address"],
            )

        # convert decimal to bson compatible and save
        self.replace_items_to_database(data=data, collection_name="user_operations")

    # user rewards
    def get_user_rewards_operations(
        self, user_address: str, rewarders_addresses: list[str]
    ) -> list[dict]:
        """Get all rewarder addresses and its operations linked to an user address
            "_id": rewarder_address
            "staked" current qtty stakiet in the rewarder
            "operations" user operations linked to the rewarder
        Args:
            user_address (str):

        Returns:
            list[dict]:
        """
        return [
            self.convert_d128_to_decimal(item=item)
            for item in self.get_items_from_database(
                collection_name="operations",
                aggregate=self.query_user_allRewarder_transactions(
                    user_address=user_address, rewarders_addresses=rewarders_addresses
                ),
            )
        ]

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
        data["id"] = create_id_rewards_static(
            hypervisor_address=data["hypervisor_address"],
            rewarder_address=data["rewarder_address"],
            rewardToken_address=data["rewardToken"],
        )
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
    def set_rewards_status(self, data: dict) -> UpdateResult:
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
        # define database id-->
        #           OLD= hypervisorAddress_rewarderAddress_block
        #           NEW = hypervisorAddress_rewarderAddress_tokenAddress_block
        data["id"] = create_id_rewards_status(
            hypervisor_address=data["hypervisor_address"],
            rewarder_address=data["rewarder_address"],
            rewardToken_address=data["rewardToken"],
            block=data["block"],
        )
        return self.save_item_to_database(data=data, collection_name="rewards_status")

    # hypervisor returns
    def set_hypervisor_returns(self, data: dict) -> UpdateResult:
        # create id
        data["id"] = create_id_hypervisor_returns(
            hypervisor_address=data["address"],
            ini_block=data["ini_block"],
            end_block=data["end_block"],
        )

        # save
        return self.replace_item_to_database(
            data=data, collection_name="hypervisor_returns"
        )

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

    @staticmethod
    def query_uncollected_fees(
        hypervisor_address: str | None = None,
        timestamp: int | None = None,
        block: int | None = None,
    ) -> list[dict]:
        """If no hypervisor_address is provided, it will return all uncollected fees

        Args:
            hypervisor_address (str | None, optional): . Defaults to None.
            timestamp (int):  lower than or equeal to this timestamp. Defaults to None.
            block (int):  lower than or equal to this block. Defaults to None.

        Returns:
            list[dict]: {
             "address":
            "symbol":
            "block":
            "timestamp":
            "uncollectedFees0":
            "uncollectedFees1":
            "owedFees0":
            "owedFees1"
            "totalFees0"
            "totalFees1"
            }
        """
        query = [
            {"$sort": {"block": -1}},
            {
                "$group": {
                    "_id": "$address",
                    "item": {"$first": "$$ROOT"},
                }
            },
            {
                "$project": {
                    "address": "$_id",
                    "symbol": "$item.symbol",
                    "block": "$item.block",
                    "timestamp": "$item.timestamp",
                    "uncollectedFees0": {
                        "$divide": [
                            {"$toDecimal": "$item.fees_uncollected.qtty_token0"},
                            {"$pow": [10, "$item.pool.token0.decimals"]},
                        ]
                    },
                    "uncollectedFees1": {
                        "$divide": [
                            {"$toDecimal": "$item.fees_uncollected.qtty_token1"},
                            {"$pow": [10, "$item.pool.token1.decimals"]},
                        ]
                    },
                    "owedFees0": {
                        "$divide": [
                            {"$toDecimal": "$item.tvl.fees_owed_token0"},
                            {"$pow": [10, "$item.pool.token0.decimals"]},
                        ]
                    },
                    "owedFees1": {
                        "$divide": [
                            {"$toDecimal": "$item.tvl.fees_owed_token1"},
                            {"$pow": [10, "$item.pool.token1.decimals"]},
                        ]
                    },
                }
            },
            {
                "$addFields": {
                    "totalFees0": {"$sum": ["uncollectedFees0", "$owedFees0"]},
                    "totalFees1": {"$sum": ["uncollectedFees1", "$owedFees1"]},
                }
            },
            {"$unset": ["_id"]},
        ]

        if hypervisor_address:
            if block:
                query.insert(
                    0,
                    {
                        "$match": {
                            "address": hypervisor_address,
                            "block": {"$lte": block},
                        }
                    },
                )
            elif timestamp:
                query.insert(
                    0,
                    {
                        "$match": {
                            "address": hypervisor_address,
                            "block": {"$lte": block},
                        }
                    },
                )
            else:
                query.insert(0, {"$match": {"address": hypervisor_address}})

        # debug_query = f"{query}"
        return query

    @staticmethod
    def query_operations(
        hypervisor_address: str,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
        block_ini: int | None = None,
        block_end: int | None = None,
    ) -> list[dict]:
        """get operations

            Supply end timestamp or block to get operations until that point in time
            Supply both timestamp and block to get operations between those points in time

        Args:
            timestamp_ini (int): initial timestamp
            timestamp_end (int): end timestamp
            block_ini (int): initial block
            block_end (int): end block

        Returns:
            list[dict]:
        """
        _match = {
            "address": hypervisor_address,
            "timestamp": {"$gte": timestamp_ini, "$lte": timestamp_end},
        }
        if block_ini and block_end:
            _match["blockNumber"] = {"$gte": block_ini, "$lte": block_end}
        elif block_ini:
            _match["blockNumber"] = {"$gte": block_ini}
        elif block_end:
            _match["blockNumber"] = {"$lte": block_end}
        elif timestamp_ini and timestamp_end:
            _match["timestamp"] = {"$gte": timestamp_ini, "$lte": timestamp_end}
        elif timestamp_ini:
            _match["timestamp"] = {"$gte": timestamp_ini}
        elif timestamp_end:
            _match["timestamp"] = {"$lte": timestamp_end}

        return [
            {"$match": _match},
            {"$sort": {"blockNumber": -1, "logIndex": 1}},
        ]

    @staticmethod
    def query_operations_summary(
        hypervisor_address: str | None = None,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
        block_ini: int | None = None,
        block_end: int | None = None,
    ) -> list[dict]:
        """_summary_

        Args:
            hypervisor_address (str): _description_
            timestamp_ini (int | None, optional): _description_. Defaults to None.
            timestamp_end (int | None, optional): _description_. Defaults to None.
            block_ini (int | None, optional): _description_. Defaults to None.
            block_end (int | None, optional): _description_. Defaults to None.

        Returns:
            list[dict]: {
                            "_id" : "0x7f92463e24b2ea1f7267aceed3ad68f7a956d2d8",
                            "address" : "0x7f92463e24b2ea1f7267aceed3ad68f7a956d2d8",
                            "block_ini" : NumberInt(13591510),
                            "block_end" : NumberInt(15785095),
                            "timestamp_ini" : NumberInt(1636587581),
                            "timestamp_end" : NumberInt(1666218047),
                            "deposits_token0" : NumberDecimal("95.257532479790920"),
                            "deposits_token1" : NumberDecimal("8.1970"),
                            "withdraws_token0" : NumberDecimal("84.270732146125590648"),
                            "withdraws_token1" : NumberDecimal("1.619807984061774091"),
                            "collectedFees_token0" : NumberDecimal("27.464269473110871371"),
                            "collectedFees_token1" : NumberDecimal("1.388745951660998281"),
                            "zeroBurnFees_token0" : NumberDecimal("0.0000"),
                            "zeroBurnFees_token1" : NumberDecimal("0.0000")
                        }

        """
        query = [
            {
                "$project": {
                    "address": "$address",
                    "block": "$blockNumber",
                    "timestamp": "$timestamp",
                    "deposits_token0": {
                        "$cond": [
                            {"$eq": ["$topic", "deposit"]},
                            {"$toDecimal": "$qtty_token0"},
                            0,
                        ]
                    },
                    "deposits_token1": {
                        "$cond": [
                            {"$eq": ["$topic", "deposit"]},
                            {"$toDecimal": "$qtty_token1"},
                            0,
                        ]
                    },
                    "withdraws_token0": {
                        "$cond": [
                            {"$eq": ["$topic", "withdraw"]},
                            {"$toDecimal": "$qtty_token0"},
                            0,
                        ]
                    },
                    "withdraws_token1": {
                        "$cond": [
                            {"$eq": ["$topic", "withdraw"]},
                            {"$toDecimal": "$qtty_token1"},
                            0,
                        ]
                    },
                    "collectedFees_token0": {
                        "$cond": [
                            {"$eq": ["$topic", "rebalance"]},
                            {"$toDecimal": "$qtty_token0"},
                            0,
                        ]
                    },
                    "collectedFees_token1": {
                        "$cond": [
                            {"$eq": ["$topic", "rebalance"]},
                            {"$toDecimal": "$qtty_token1"},
                            0,
                        ]
                    },
                    "zeroBurnFees_token0": {
                        "$cond": [
                            {"$eq": ["$topic", "zeroBurn"]},
                            {"$toDecimal": "$qtty_token0"},
                            0,
                        ]
                    },
                    "zeroBurnFees_token1": {
                        "$cond": [
                            {"$eq": ["$topic", "zeroBurn"]},
                            {"$toDecimal": "$qtty_token1"},
                            0,
                        ]
                    },
                    "decimals_token0": "$decimals_token0",
                    "decimals_token1": "$decimals_token1",
                }
            },
            {
                "$group": {
                    "_id": "$address",
                    "address": {"$first": "$address"},
                    "block_ini": {"$first": "$block"},
                    "block_end": {"$last": "$block"},
                    "timestamp_ini": {"$first": "$timestamp"},
                    "timestamp_end": {"$last": "$timestamp"},
                    "deposits_token0": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$deposits_token0"},
                                {"$pow": [10, "$decimals_token0"]},
                            ]
                        }
                    },
                    "deposits_token1": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$deposits_token1"},
                                {"$pow": [10, "$decimals_token1"]},
                            ]
                        }
                    },
                    "withdraws_token0": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$withdraws_token0"},
                                {"$pow": [10, "$decimals_token0"]},
                            ]
                        }
                    },
                    "withdraws_token1": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$withdraws_token1"},
                                {"$pow": [10, "$decimals_token1"]},
                            ]
                        }
                    },
                    "collectedFees_token0": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$collectedFees_token0"},
                                {"$pow": [10, "$decimals_token0"]},
                            ]
                        }
                    },
                    "collectedFees_token1": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$collectedFees_token1"},
                                {"$pow": [10, "$decimals_token1"]},
                            ]
                        }
                    },
                    "zeroBurnFees_token0": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$zeroBurnFees_token0"},
                                {"$pow": [10, "$decimals_token0"]},
                            ]
                        }
                    },
                    "zeroBurnFees_token1": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$zeroBurnFees_token1"},
                                {"$pow": [10, "$decimals_token1"]},
                            ]
                        }
                    },
                }
            },
            {"$unset": ["_id"]},
        ]

        # build match
        _and = []
        _match = {}

        # add block or timestamp in query
        if block_ini:
            _and.append({"blockNumber": {"$gte": block_ini}})
        elif timestamp_ini:
            _and.append({"timestamp": {"$gte": timestamp_ini}})
        if block_end:
            _and.append({"blockNumber": {"$lte": block_end}})
        elif timestamp_end:
            _and.append({"timestamp": {"$lte": timestamp_end}})

        # add hype address
        if hypervisor_address:
            _and.append({"address": {"$lte": hypervisor_address}})

        # add to query
        if _and:
            _match["$and"] = _and
            query.insert(0, {"$match": _match})
        # debug_query = f"{query}"
        return query

    @staticmethod
    def query_rewarders_by_rewardRegistry() -> list[dict]:
        """return the query to build
               list of {
                    "_id": <rewarder_registry>,
                    "rewarders": [<rewarder_address>,...]
               }

        Returns:
            query:
        """
        return [
            {
                "$group": {
                    "_id": "$rewarder_registry",
                    "rewarders": {"$push": "$rewarder_address"},
                }
            }
        ]

    @staticmethod
    def query_all_user_Rewarder_transactions(
        user_address: str, rewarder_address: str
    ) -> list[dict]:
        """It will return a list of operations with the following Extra fields:
                qtty_in: qtty staked in the rewarder
                qtty_out: qtty unstaked from the rewarder
                staked_in_rewarder: qtty_in - qtty_out

                u can sum all operations <staked_in_rewarder> field to get the total staked in the rewarder

        Args:
            user_address (str):
            rewarder_address (str):

        Returns:
            list[dict]:
        """
        return [
            {
                "$match": {
                    "topic": "transfer",
                    "$or": [
                        {"dst": user_address},
                        {"src": user_address},
                    ],
                }
            },
            {
                "$addFields": {
                    "qtty_in": {
                        "$cond": [
                            {"$eq": ["$dst", rewarder_address]},
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "qtty_out": {
                        "$cond": [
                            {"$eq": ["$src", rewarder_address]},
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "user_address": {
                        "$cond": [{"$eq": ["$dst", rewarder_address]}, "$src", "$dst"]
                    },
                }
            },
            {
                "$addFields": {
                    "staked_in_rewarder": {"$subtract": ["$qtty_in", "$qtty_out"]},
                }
            },
        ]

    @staticmethod
    def query_user_allRewarder_transactions(
        user_address: str, rewarders_addresses: list[str]
    ) -> list[dict]:
        """Get all user transactions and summary status for all rewarders in the network
                returns a list of operations with the following fields:
                    "_id": <rewarder_address>,
                    "staked: <total_staked_in_rewarder>,
                    "operations": [<operation>,...]
        Args:
            user_address (str):
            rewarders_addresses (list[str]):

        Returns:
            list[dict]: _description_
        """
        return [
            {
                "$match": {
                    "topic": "transfer",
                    "src": {"$ne": "0x0000000000000000000000000000000000000000"},
                    "dst": {"$ne": "0x0000000000000000000000000000000000000000"},
                    "$or": [
                        {
                            "$and": [
                                {"dst": user_address},
                                {"src": {"$in": rewarders_addresses}},
                            ]
                        },
                        {
                            "$and": [
                                {"src": user_address},
                                {"dst": {"$in": rewarders_addresses}},
                            ]
                        },
                    ],
                }
            },
            {
                "$addFields": {
                    "qtty_in": {
                        "$cond": [
                            {"$in": ["$dst", rewarders_addresses]},
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "qtty_out": {
                        "$cond": [
                            {"$in": ["$src", rewarders_addresses]},
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "user_address": {
                        "$cond": [{"$eq": ["$dst", user_address]}, "$dst", "$src"]
                    },
                    "rewarder_address": {
                        "$cond": [{"$eq": ["$src", user_address]}, "$dst", "$src"]
                    },
                }
            },
            {
                "$addFields": {
                    "staked_in_rewarder": {"$subtract": ["$qtty_in", "$qtty_out"]},
                }
            },
            {
                "$group": {
                    "_id": "$rewarder_address",
                    "staked": {"$sum": "$staked_in_rewarder"},
                    "operations": {"$push": "$$ROOT"},
                }
            },
        ]

    @staticmethod
    def query_locs_apr_hypervisor_data_calculation(
        hypervisor_address: str | None = None,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
        block_ini: int | None = None,
        block_end: int | None = None,
    ) -> list[dict]:
        """Returns a list of hypervisor status following LOC APR method calculation-> (operation) to (operation+1)(block-1)

        Args:
            hypervisor_address (str | None, optional): . Defaults to None.
            timestamp_ini (int | None, optional): greater or equal to . Defaults to None.
            timestamp_end (int | None, optional): lower or equal to. Defaults to None.
            block_ini (int | None, optional): greater or equal to  . Defaults to None.
            block_end (int | None, optional): lower or equal to  . Defaults to None.

        Returns:
            list[dict]: _description_
        """
        # build match
        _and = []
        _match = {
            "qtty_token0": {"$ne": "0"},
            "qtty_token1": {"$ne": "0"},
            "src": {"$ne": "0x0000000000000000000000000000000000000000"},
            "dst": {"$ne": "0x0000000000000000000000000000000000000000"},
            "topic": {
                "$in": [
                    "transfer",
                    "deposit",
                    "withdraw",
                    "rebalance",
                    "zeroBurn",
                ]
            },
        }
        query = [
            {"$sort": {"blockNumber": 1}},
            {
                "$group": {
                    "_id": "$address",
                    "block_ini": {"$push": "$blockNumber"},
                    "block_end": {
                        "$push": {"$toInt": {"$subtract": ["$blockNumber", 1]}}
                    },
                }
            },
            {
                "$project": {
                    "hypervisor_address": "$_id",
                    "blocks": {"$setUnion": ["$block_ini", "$block_end"]},
                    "_id": 0,
                }
            },
            {"$addFields": {"block_size": {"$size": "$blocks"}}},
            # discard less than 3 ops
            {"$match": {"block_size": {"$gte": 3}}},
            # discard first and last blocks
            {
                "$project": {
                    "hypervisor_address": "$hypervisor_address",
                    "block": {
                        "$slice": [
                            "$blocks",
                            1,
                            {"$toInt": {"$subtract": ["$block_size", 2]}},
                        ]
                    },
                }
            },
            {"$unwind": "$block"},
            # find hype status
            {
                "$lookup": {
                    "from": "status",
                    "let": {"op_block": "$block", "op_address": "$hypervisor_address"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$address", "$$op_address"]},
                                        {
                                            "$or": [
                                                {"$eq": ["$block", "$$op_block"]},
                                            ]
                                        },
                                    ],
                                }
                            }
                        },
                        {"$limit": 1},
                    ],
                    "as": "status",
                }
            },
            {"$unwind": "$status"},
            {
                "$group": {
                    "_id": "$hypervisor_address",
                    "status": {"$push": "$status"},
                }
            },
        ]

        # add block or timestamp in query
        if block_ini:
            _and.append({"blockNumber": {"$gte": block_ini}})
        elif timestamp_ini:
            _and.append({"timestamp": {"$gte": timestamp_ini}})
        if block_end:
            _and.append({"blockNumber": {"$lte": block_end}})
        elif timestamp_end:
            _and.append({"timestamp": {"$lte": timestamp_end}})

        # add hype address
        if hypervisor_address:
            _and.append({"address": {"$lte": hypervisor_address}})

        # add to query
        if _and:
            _match["$and"] = _and

        # add match to query
        query.insert(0, {"$match": _match})

        # debug_query = f"{query}"
        return query
