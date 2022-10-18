from pprint import pprint
import pandas as pd
from DbConnector import DbConnector

class DbController:
    
    def __init__(self, user="geoadmin", pwd="admin", host="localhost", port=27017, database="geolife") -> None:
        """Constructor for the database controller

        Args:
            user (str, optional): Name of mysql user. Defaults to "geoadmin".
            pwd (str, optional): Password of mysql user. Defaults to "admin".
            host (str, optional): Host of the mysql server. Defaults to "localhost".
            port (int, optional): Port the mysql server is listening to. Defaults to 27017.
            database (str, optional): The database to connect to. Defaults to "geolife".
        """
        self.connection = DbConnector(database, host, user, pwd, port)
        self.client = self.connection.client
        self.db = self.connection.db
        
    
    def create_collections(self, collections):
        """Creates all non-existing collections specified in argument.

        Args:
            collections (list or str): The collection / collections to create.
        """
        existing = self.db.list_collection_names()
        for collection in collections if type(collections) is not str else [collections]:
            if collection in existing:
                continue
            print(f"Created collection: {self.db.create_collection(collection)}")
    
    def drop_collections(self, collections):
        """Drops all collections specified in arguement.

        Args:
            collections (list or "all"): List of collection to drop or "all" which drops all collections.
        """
        existing = self.db.list_collection_names()
        for collection in collections if collections != "all" else existing:
            if collection in existing:
                self.db[collection].drop()
                print(f"Dropped collection: {collection}")
    
           
    def insert_dataframe(self, dataframe: pd.DataFrame, collection_name: str, index_as_id=False):
        """Inserts a dataframe into a collection.

        Args:
            dataframe (pd.DataFrame): The dataframe to insert.
            collection_name (str): The collection to insert into.
            index_as_id (bool, optional): Whether you want to use the index as the _id field. Defaults to False.
        """
        if index_as_id:
            dataframe["_id"] = dataframe.index.to_list()
        collection = self.db[collection_name]
        collection.insert_many(dataframe.to_dict("records"))
    
    def select_dataframe(self, collection_name: str, query):
        """Gets a dataframe from the specified colletion.

        Args:
            collection_name (str): The name of the collection to query.
            query (MongoDB query): The query to perform while fetching documents from the specified collection. 
                                   Must be unwound and projected to a pandas friendly format.

        Returns:
            pd.DataFrame: A dataframe containing the fetched doucments.
        """
        collection = self.db[collection_name]
        cursor = collection.aggregate(query)
        df = pd.DataFrame.from_records(list(cursor))
        return df
    
    def insert_many(self, collection_name, data):
        """Inserts data into the selected collection. Can insert several documents at once.

        Args:
            collection_name (str): The collection to insert into.
            data (list(record)): A list of records to insert into the collection.
        """
        collection = self.db[collection_name]
        collection.insert_many(data)
    
    def get_distinct(self, collection_name, feature_name):
        """Gets the distinct occurrences of the feature in the selected collection.

        Args:
            collection_name (str): The collection to query.
            feature_name (str): The field of which you want the distinct values.

        Returns:
            cursor: Contains the distinct values of selected feature.
        """
        collection = self.db[collection_name]
        return collection.distinct(feature_name)
    
    def close_connection(self):
        """Closes the connection.
        """
        self.connection.close_connection()
        
    def aggregate(self, collection_name: str, query):
        """Performs an aggregation on the selected collection.

        Args:
            collection_name (str): The collection to query.
            query (MongoDB query): The query to perform on the collection.

        Returns:
            cursor: Result from query.
        """
        collection = self.db[collection_name]
        return collection.aggregate(query)
    
    def find(self, collection_name, query):
        """Selects some data from the selected collection.

        Args:
            collection_name (str): The collection to query.
            query (MongoDB query): The query to perform.

        Returns:
            cursor: The result of the query.
        """
        collection = self.db[collection_name]
        return collection.find(query)

            