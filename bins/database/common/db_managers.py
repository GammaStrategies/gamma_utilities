from pymongo import DeleteOne, MongoClient
from pymongo.errors import ConnectionFailure, BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from pymongo.cursor import Cursor
from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)


class MongoDbManager:
    def __init__(self, url: str, db_name: str, collections: dict):
        """Mongo database helper

        Args:
           url (str): full mongodb url
           db_name (str): database name
           collections (dict): {
                             <collection name>: {
                                "mono_indexes": { <field>:<uniqueness>, ...} ...}
                                "multi_indexes": [  [<field>, ORDER, ...]  ]
                           example: {
                               "static":
                                       {"id":True,
                                       },
                               "returns":
                                       {"id":True
                                       },
                               }
        """

        # connect to mongo database
        try:
            self.mongo_client = MongoClient(url)
        except ConnectionFailure as e:
            raise ValueError(f"Failed not connect to {url}") from e
        self.database = self.mongo_client[db_name]

        # Retrieve database collection names
        self.database_collections = self.database.list_collection_names()

        # define collection configurations
        self.collections_config = collections

        # Setup collections and their indexes
        self.configure_collections()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        # xception handling here
        self.mongo_client.close()

    def configure_collections(self):
        """define collection names and create indexes"""
        for coll_name, fields in self.collections_config.items():
            # mono indexes
            for field, unique in fields.get("mono_indexes", {}).items():
                self.database[coll_name].create_index(field, unique=unique)
            # multi indexes
            for field in fields.get("multi_indexes", []):
                self.database[coll_name].create_index(field)

    def create_collection(self, coll_name: str, **indexes):
        """Creates a collection if it does not exist.
        Arguments:
           indexes = [ <collection field name>:str = <unique>:bool  ]
        """

        if coll_name not in self.database_collections:
            # mono indexes
            for field, unique in indexes.get("mono_indexes", {}).items():
                self.database[coll_name].create_index(field, unique=unique)
            # multi indexes
            for fields in indexes.get("multi_indexes", []):
                self.database[coll_name].create_index(fields)

            # refresh database collection names
            self.database_collections = self.database.list_collection_names()

    def del_item(self, coll_name: str, dbFilter: dict) -> DeleteResult:
        # check collection configuration exists
        if coll_name not in self.collections_config.keys():
            raise ValueError(
                f" No configuration found for {coll_name} database collection."
            )
        # add/ update to database (add or replace)
        return self.database[coll_name].delete_one(filter=dbFilter)

    def del_items_in_bulk(self, coll_name: str, data: list) -> BulkWriteResult:
        # check collection configuration exists
        if coll_name not in self.collections_config.keys():
            raise ValueError(
                f" No configuration found for {coll_name} database collection."
            )

        return self.database[coll_name].bulk_write(
            [DeleteOne(filter=item["filter"]) for item in data]
        )

    def add_item(
        self, coll_name: str, dbFilter: dict, data: dict, upsert=True
    ) -> UpdateResult:
        """Add or Update item

        Args:
           coll_name (str): collection name
           dbFilter (dict): filter to use as to replacement filter, like { address:<>, chain:<>}
           data (dict): data to save
           upsert (bool, optional): replace or add item. Defaults to True.

        Raises:
           ValueError: if coll_name is not defined at the class init <collections> field
        """

        # check collection configuration exists
        if coll_name not in self.collections_config.keys():
            raise ValueError(
                f" No configuration found for {coll_name} database collection."
            )
        # create collection if it does not exist yet
        self.create_collection(
            coll_name=coll_name, **self.collections_config[coll_name]
        )

        # add/ update to database (add or replace)
        return self.database[coll_name].update_one(
            filter=dbFilter, update={"$set": data}, upsert=True
        )

    def add_items_bulk(
        self, coll_name: str, data: list, upsert=True
    ) -> BulkWriteResult:
        """Add or Update item

        Args:
           coll_name (str): collection name
           dbFilter (dict): filter to use as to replacement filter, like { address:<>, chain:<>}
           data (dict): data to save
           upsert (bool, optional): replace or add item. Defaults to True.

        Raises:
           ValueError: if coll_name is not defined at the class init <collections> field
        """

        # check collection configuration exists
        if coll_name not in self.collections_config.keys():
            raise ValueError(
                f" No configuration found for {coll_name} database collection."
            )
        # create collection if it does not exist yet
        self.create_collection(
            coll_name=coll_name, **self.collections_config[coll_name]
        )

        # add/ update to database (add or replace)
        return self.database[coll_name].bulk_write(
            [
                UpdateOne(filter=item["filter"], update=item["data"], upsert=upsert)
                for item in data
            ]
        )

    def replace_item(
        self, coll_name: str, dbFilter: dict, data: dict, upsert=True
    ) -> UpdateResult:
        """Add or Update item

        Args:
           coll_name (str): collection name
           dbFilter (dict): filter to use as to replacement filter, like { address:<>, chain:<>}
           data (dict): data to save
           upsert (bool, optional): replace or add item. Defaults to True.

        Raises:
           ValueError: if coll_name is not defined at the class init <collections> field
        """

        # check collection configuration exists
        if coll_name not in self.collections_config.keys():
            raise ValueError(
                f" No configuration found for {coll_name} database collection."
            )
        # create collection if it does not exist yet
        self.create_collection(
            coll_name=coll_name, **self.collections_config[coll_name]
        )

        # add/ update to database (add or replace)
        return self.database[coll_name].replace_one(
            filter=dbFilter, replacement=data, upsert=True
        )

    def replace_items_bulk(
        self, coll_name: str, data: list, upsert=True
    ) -> BulkWriteResult:
        """Add or Update items

        Args:
           coll_name (str): collection name
           data (list): list of data to save
           upsert (bool, optional): replace or add item. Defaults to True.

        Raises:
           ValueError: if coll_name is not defined at the class init <collections> field
        """

        # check collection configuration exists
        if coll_name not in self.collections_config.keys():
            raise ValueError(
                f" No configuration found for {coll_name} database collection."
            )
        # create collection if it does not exist yet
        self.create_collection(
            coll_name=coll_name, **self.collections_config[coll_name]
        )

        # add/ update to database (add or replace)
        return self.database[coll_name].bulk_write(
            [
                ReplaceOne(filter=item["filter"], replacement=item["data"], upsert=True)
                for item in data
            ]
        )

    def get_items(self, coll_name: str, **kwargs) -> Cursor:
        """get items cursor from database

        Args:
           coll_name (str): _description_
           **kwargs:  examples->   --FIND-----------------------
                                   find={  "product_id":<product id>,
                                                 "time": {
                                                   "$lte": <date>,
                                                   "$gte": <date>
                                                     }
                                       }
                                   projection={'_id': False}
                                   batch_size=100
                                   sort=[(<field_01>,1), (<field_02>,-1) ]

                                   --AGGREGATE-------------------
                                   aggregate=[{  "$match": {
                                                           "time": {"$gte" : date_from, "$lte" : date_stop }
                                                           }
                                               },
                                               { "$group": {
                                                           "_id": "stuff",
                                                           "high": {"$max" : "$price"},
                                                           "low": {"$min": "$price"}
                                                       }
                                               }]
                                   allowDiskUse=<bool>
        """

        # when no argunments, return all
        if len(kwargs.keys()) == 0:
            kwargs["find"] = {}

        # build FIND result
        if "find" in kwargs:
            if "batch_size" in kwargs:
                if "sort" in kwargs:
                    if "projection" in kwargs:
                        return (
                            (
                                self.database[coll_name]
                                .find(
                                    kwargs["find"],
                                    projection=kwargs["projection"],
                                    batch_size=kwargs["batch_size"],
                                )
                                .sort(kwargs["sort"])
                                .limit(kwargs["limit"])
                            )
                            if "limit" in kwargs and kwargs["limit"]
                            else (
                                self.database[coll_name]
                                .find(
                                    kwargs["find"],
                                    projection=kwargs["projection"],
                                    batch_size=kwargs["batch_size"],
                                )
                                .sort(kwargs["sort"])
                            )
                        )
                    else:
                        return (
                            (
                                self.database[coll_name]
                                .find(kwargs["find"], batch_size=kwargs["batch_size"])
                                .sort(kwargs["sort"])
                                .limit(kwargs["limit"])
                            )
                            if "limit" in kwargs and kwargs["limit"]
                            else (
                                self.database[coll_name]
                                .find(kwargs["find"], batch_size=kwargs["batch_size"])
                                .sort(kwargs["sort"])
                            )
                        )

                else:
                    return (
                        self.database[coll_name].find(
                            kwargs["find"],
                            projection=kwargs["projection"],
                            batch_size=kwargs["batch_size"],
                        )
                        if "projection" in kwargs
                        else self.database[coll_name].find(
                            kwargs["find"], batch_size=kwargs["batch_size"]
                        )
                    )
            elif "sort" in kwargs:
                if "limit" in kwargs and kwargs["limit"]:
                    return (
                        (
                            self.database[coll_name]
                            .find(kwargs["find"], projection=kwargs["projection"])
                            .sort(kwargs["sort"])
                            .limit(kwargs["limit"])
                        )
                        if "projection" in kwargs
                        else (
                            self.database[coll_name]
                            .find(kwargs["find"])
                            .sort(kwargs["sort"])
                            .limit(kwargs["limit"])
                        )
                    )
                else:
                    return (
                        (
                            self.database[coll_name]
                            .find(kwargs["find"], projection=kwargs["projection"])
                            .sort(kwargs["sort"])
                        )
                        if "projection" in kwargs
                        else (
                            self.database[coll_name]
                            .find(kwargs["find"])
                            .sort(kwargs["sort"])
                        )
                    )
            elif "limit" in kwargs and kwargs["limit"]:
                return (
                    self.database[coll_name]
                    .find(kwargs["find"], projection=kwargs["projection"])
                    .limit(kwargs["limit"])
                    if "projection" in kwargs
                    else self.database[coll_name]
                    .find(kwargs["find"])
                    .limit(kwargs["limit"])
                )
            elif "projection" in kwargs:
                return self.database[coll_name].find(
                    kwargs["find"], projection=kwargs["projection"]
                )
            else:
                return self.database[coll_name].find(kwargs["find"])

        elif "aggregate" in kwargs:
            if "allowDiskUse" in kwargs:
                return self.database[coll_name].aggregate(
                    kwargs["aggregate"], allowDiskUse=kwargs["allowDiskUse"]
                )
            else:
                return self.database[coll_name].aggregate(kwargs["aggregate"])

    def get_distinct(self, coll_name: str, field: str, condition: dict = None) -> list:
        """get distinct items of a database field

        Args:
            coll_name (str): collection name
            field (str): field to get distinct values from
            condition (dict): like {"dept" : "B"}
        """
        if condition is None:
            condition = {}
        if len(condition.keys()) == 0:
            return self.database[coll_name].distinct(field)
        else:
            return self.database[coll_name].distinct(field, condition)

    def find_one_and_update(
        self, coll_name: str, dbFilter: dict, update: dict
    ) -> dict | None:
        """
        Returns the updated document or None if not found.

        Args:
            coll_name (str):
            dbFilter (dict):  like  {"_id": "counter-id"}
            update (dict):  like  {"$inc":{"sequence_value":1}}
        """
        return self.database[coll_name].find_one_and_update(
            filter=dbFilter, update=update, return_document=True
        )

    def insert_if_not_exists(
        self, coll_name: str, dbFilter: dict, data: dict
    ) -> UpdateResult:
        """Add a document to a collection only if it does not already exist.
        Args:
            coll_name (str):
            dbFilter (dict):  like  {"_id": "counter-id"}
            data (dict):
        """
        update = {"$setOnInsert": data}
        return self.database[coll_name].update_one(
            filter=dbFilter, update=update, upsert=True
        )

    @staticmethod
    def create_database_name(network: str, protocol: str) -> str:
        return f"{protocol}_{network}"
