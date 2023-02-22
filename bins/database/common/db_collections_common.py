import logging
from dataclasses import dataclass, field, asdict, InitVar

from bson.decimal128 import Decimal128
from decimal import Decimal
from datetime import datetime

from bins.database.common.db_managers import MongoDbManager
from bins.database.common.db_general_models import (
    tool_mongodb_general,
    tool_database_id,
)
from bins.database.common.db_object_models import usd_price


class db_collections_common:
    def __init__(
        self,
        mongo_url: str,
        db_name: str,
        db_collections: dict = {"static": {"id": True}},
    ):

        self._db_mongo_url = mongo_url
        self._db_name = db_name
        self._db_collections = db_collections

    # actual db saving
    def save_items_to_database(
        self,
        data: list[dict],
        collection_name: str,
    ):
        """Save dictionary values to the database collection replacing any equal id defined

        Args:
            data (list): data list following tool_mongodb_general class to be saved to database in a dict format
            collection_name (str): collection name to save data to
        """
        # add item by item to database
        for key, item in data.items():
            # add to mongodb
            self.save_item_to_database(data=item, collection_name=collection_name)

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
                " Unable to save data to mongo's {} collection.  Item: {}    error-> {}".format(
                    collection_name, data, e
                )
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
                " Unable to replace data in mongo's {} collection.  Item: {}    error-> {}".format(
                    collection_name, data, e
                )
            )

    def query_items_from_database(
        self,
        query: list[dict],
        collection_name: str,
    ) -> list:
        # db_manager = self.create_db_manager()
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
            result = _db_manager.get_items(coll_name=collection_name, **kwargs)
            result = list(result)
        return result

    def get_distinct_items_from_database(
        self, collection_name: str, field: str, condition: dict = {}
    ):
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

    @staticmethod
    def convert_decimal_to_d128(item: dict) -> dict:
        """Converts a dictionary decimal values to BSON.decimal128, recursivelly.
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
                MongoDbManager.convert_decimal_to_d128(v)
            elif isinstance(v, list):
                for l in v:
                    MongoDbManager.convert_decimal_to_d128(l)
            elif isinstance(v, Decimal):
                item[k] = Decimal128(str(v))

        return item

    @staticmethod
    def convert_d128_to_decimal(item: dict) -> dict:
        """Converts a dictionary decimal128 values to decimal, recursivelly.
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
                MongoDbManager.convert_d128_to_decimal(v)
            elif isinstance(v, list):
                for l in v:
                    MongoDbManager.convert_d128_to_decimal(l)
            elif isinstance(v, Decimal128):
                item[k] = v.to_decimal()

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
        self,
        mongo_url: str,
        db_name: str = "global",
        db_collections: dict = {"blocks": {"id": True}, "usd_prices": {"id": True}},
    ):
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
        # result = self.query_items_from_database(
        #     query=self.query_prices_addressBlocks(network=network),
        #     collection_name="usd_prices",
        # )
        result = self.get_items_from_database(
            collection_name="usd_prices", find={"network": network, "price": {"$gt": 0}}
        )
        return result

    def get_price_usd(
        self,
        network: str,
        block: int,
        address: str,
    ) -> float:
        """get usd price from block

        Args:
            network (str): ethereum, optimism, polygon....
            block (int): number
            address (str): token address

        Returns:
            float: price in usd
        """
        return self.get_items_from_database(
            collection_name="usd_prices",
            find={"network": network, "block": block, "address": address},
        )

    def get_timestamp(
        self,
        network: str,
        block: int,
    ) -> datetime.timestamp:
        result = self.query_items_from_database(
            query=self.query_timestamp(network=network, block=block),
            collection_name="blocks",
        )
        return result

    def get_block(
        self,
        network: str,
        timestamp: datetime.timestamp,
    ) -> int:
        result = self.query_items_from_database(
            query=self.query_block(network=network, timestamp=timestamp),
            collection_name="blocks",
        )
        return result

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
    def query_block(network: str, timestamp: datetime.timestamp) -> list[dict]:

        # return query
        return []

    @staticmethod
    def query_timestamp(network: str, block: int) -> list[dict]:

        # return query
        return []

    @staticmethod
    def query_usd_price(network: str, block: int, address: str) -> list[dict]:

        # return query
        return []

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
    """

    def __init__(
        self,
        mongo_url: str,
        db_name: str,
        db_collections: dict = {
            "static": {"id": True},
            "operations": {"id": True},
            "status": {"id": True},
        },
    ):
        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    def get_items(self, collection_name: str, **kwargs) -> list:
        """Any

        Returns:
            list: of results
        """
        return self.get_items_from_database(collection_name=collection_name, **kwargs)

    # specific collection related
    def set_static(self, data: dict):
        data["id"] = data["address"]
        self.save_item_to_database(data=data, collection_name="static")

    def set_operation(self, data: dict):
        self.replace_item_to_database(data=data, collection_name="operations")

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
        timestamp_ini: datetime.timestamp,
        timestamp_end: datetime.timestamp,
    ) -> list:
        return self.query_items_from_database(
            collection_name="operations",
            query=self.query_operations_btwn_timestamps(
                hypervisor_address=hypervisor_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
            ),
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

    def get_unique_operations_addressBlock(self) -> list:
        """Retrieve a list of unique blocks + hypervisor addresses present in operations collection

        Returns:
            list: of  {
                    "address" : "0x407e99b20d61f245426031df872966953909e9d3",
                    "block" : 12736656
                    }
        """
        query = [
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
        return self.get_items_from_database(
            collection_name="operations", aggregate=query
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

    def get_unique_tokens(self) -> list:
        """Get a unique token list from static database

        Returns:
            list:
        """
        return self.get_items_from_database(
            collection_name="static", aggregate=self.query_unique_token_addresses()
        )

    def get_mostUsed_tokens1(self, limit: int = 5) -> list:
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
        timestamp_ini: datetime.timestamp,
        timestamp_end: datetime.timestamp,
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
