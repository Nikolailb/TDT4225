from sqlalchemy import create_engine
import pandas as pd

class DbController:
    
    def __init__(self, user="geoadmin", pwd="admin", host="localhost", port=3306, database="geolife") -> None:
        """Constructor for the database controller

        Args:
            user (str, optional): Name of mysql user. Defaults to "geoadmin".
            pwd (str, optional): Password of mysql user. Defaults to "admin".
            host (str, optional): Host of the mysql server. Defaults to "localhost".
            port (int, optional): Port the mysql server is listening to. Defaults to 3306.
            database (str, optional): The database to connect to. Defaults to "geolife".
        """
        self.engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{database}")
        self._greeting()
        self.tables = []
        
    def create_tables(self):
        """Creates the tables of the database.

        Returns:
            DbController: Returns self to allow for chaining, e.g. x = DbHandler().create_tables().
        """
        with self.engine.begin() as connection:
            self._create_user(connection)
            self._create_activity(connection)
            self._create_track_point(connection)
        return self
     
    def _greeting(self):
        with self.engine.begin() as connection:
            query = 'SHOW VARIABLES LIKE "version"'
            print(f"Currently connected to: {connection.execute(query).first()}")
            print(f"You are connected to the database: {connection.execute('SELECT DATABASE()').first()}")
        print("-" * 75 + "\n")
                      
    def _create_user(self, connection):
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS user (
                id VARCHAR(10) PRIMARY KEY,
                has_labels BOOL NOT NULL
            )
            """
        )
        self.tables.append("user")
        
    def _create_activity(self, connection):
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS activity (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(10) NOT NULL,
            transportation_mode VARCHAR(30),
            start_date_time DATETIME,
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES user(id)
            )
            """
        )
        self.tables.append("activity")
        
    def _create_track_point(self, connection):
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS track_point (
                id INT AUTO_INCREMENT PRIMARY KEY,
                activity_id INT NOT NULL,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME,
                FOREIGN KEY (activity_id) REFERENCES activity(id)
            )
            """
        )
        self.tables.append("track_point")
        
    def insert_dataframe(self, dataframe: pd.DataFrame, table: str, index_as_id=False):
        """Inserts a pandas dataframe into the database.

        Args:
            dataframe (pd.DataFrame): The dataframe containing the data to insert.
            table (str): Which table the dataframe should be inserted into.
            index_as_id (bool, optional): If the index of the dataframe be used as id, if false you have to have a column with the id. Defaults to False.
        """
        with self.engine.begin() as conn:
            dataframe.to_sql(table, con=conn, if_exists="append", index=index_as_id, index_label="id")
    
    def select_dataframe(self, query, use_id_as_index=False, parse_dates=None):
        """Creates a dataframe based on information from query

        Args:
            query (str): Either a sql string or the name of a table
            use_id_as_index (bool, optional): Whether the id should be used as the index of the dataframe. Defaults to False.

        Returns:
            pd.Dataframe: The resulting dataframe
        """
        with self.engine.begin() as conn:
            return pd.read_sql(query, con=conn, index_col="id" if use_id_as_index else None, parse_dates=parse_dates)
        
    def execute_query(self, query):
        with self.engine.begin() as connection:
            return connection.execute(query)
    
    def drop_table(self, table):
        with self.engine.begin() as connection:
            return connection.execute(f"DROP TABLE IF EXISTS {table}")
    
    def drop_all_tables(self):
        for table in self.tables:
            self.drop_table(table)