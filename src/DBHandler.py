from DbConnector import DbConnector
from tabulate import tabulate


class DBHandler:
    
    def __init__(self) -> None:
        self.connector = DbConnector()
        self.db_connection = self.connector.db_connection
        self.cursor = self.connector.cursor
        self.tables = []
    
    def create_tables(self):
        # User table:
        query = \
        """
        CREATE TABLE IF NOT EXISTS user (
            id VARCHAR(10) PRIMARY KEY,
            has_labels BOOL NOT NULL
        )
        """
        self.cursor.execute(query)
        self.tables.append("user")
        # Activity table:
        query = \
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
        self.cursor.execute(query)
        self.tables.append("activity")
        
        # TrackPoint table
        query = \
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
        self.cursor.execute(query)
        self.tables.append("track_point")
        self.db_connection.commit()
        
    def execute_insert(self, query, data):
        self.cursor.execute(query, data)
        self.db_connection.commit()
        return self.cursor._last_insert_id
    
    def execute_bulk_insert(self, query, data):
        self.cursor.executemany(query, data)
        self.db_connection.commit()
        
    def execute_select(self, query, data=()):
        self.cursor.execute(query, data)
        self.db_connection.commit()
        return self.cursor.fetchall()
        
    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)
        self.db_connection.commit()
        
    def drop_all_tables(self):
        for i in reversed(range(len(self.tables))):
            self.drop_table(self.tables[i])
        self.tables = []
            
    def get_table_length(self, table_name):
        if table_name not in self.tables:
            return
        query = "SELECT COUNT(1) FROM %s"
        self.cursor.execute(query % table_name)
        return self.cursor.fetchall()[0][0]
        
        
def main():
    program = None
    try:
        program = DBHandler()
        program.create_tables()
        program.drop_all_tables()
        program.create_tables()
        program.show_tables()
        t = program.execute_query("INSERT INTO user (id, has_labels) VALUES (%s, %s)", ('Todd', 0))
        # t = program.execute_query("SELECT * FROM user")
        program.show_tables()
        program.drop_all_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connector.close_connection()


if __name__ == '__main__':
    main()
