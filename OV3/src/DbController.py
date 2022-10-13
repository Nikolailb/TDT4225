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
        existing = self.db.list_collection_names()
        for collection in collections if type(collections) is not str else [collections]:
            if collection in existing:
                continue
            print(f"Created collection: {self.db.create_collection(collection)}")
    
    def drop_collections(self, collections):
        existing = self.db.list_collection_names()
        for collection in collections if collections != "all" else existing:
            if collection in existing:
                self.db[collection].drop()
                print(f"Dropped collection: {collection}")
    
           
    def insert_dataframe(self, dataframe: pd.DataFrame, collection_name: str, index_as_id=False):
        if index_as_id:
            dataframe["_id"] = dataframe.index.to_list()
        collection = self.db[collection_name]
        collection.insert_many(dataframe.to_dict("records"))
    
    def select_dataframe(self, collection_name: str, query, keep_id=False, parse_dates=None):
        collection = self.db[collection_name]
        cursor = collection.aggregate(query)
        df = pd.DataFrame.from_records(list(cursor))
        if not keep_id and "_id" in df.columns.to_list():
            df.drop(columns=["_id"], inplace=True)
        return df
    
    def get_distinct(self, collection_name, feature_name):
        collection = self.db[collection_name]
        return collection.distinct(feature_name)
    
    def close_connection(self):
        self.connection.close_connection()
        
    def aggregate(self, collection_name: str, query):
        collection = self.db[collection_name]
        return collection.aggregate(query)
    
    def find(self, collection_name, query):
        collection = self.db[collection_name]
        return collection.find(query)

            
def main():
    controller = DbController()
    controller.drop_collections("all")


if __name__ == "__main__":
    main()