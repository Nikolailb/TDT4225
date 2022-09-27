
from DataHandler import DataLoader
from DbController import DbController

class Task1:
    
    def __init__(self) -> None:
        self.data = None
        self.db_controller = DbController().create_tables()
    
    def load_data(self, read_from_file=False, save_to_file=False):
        """Loads the data from the dataset into pandas dataframes

        Args:
            read_from_file (bool, optional): Read from previously saved files. Defaults to False.
            save_to_file (bool, optional): Write resulting dataframes to file. Defaults to False.
        """
        self.data = DataLoader()
        self.data.load_data(read_from_file=read_from_file, write_to_file=save_to_file)
    
    def insert_data(self):
        """Inserts the data from the dataset into the database.
        
        If there is an exception the tables of the database is dropped, and you are prompted to try again.
        The reason for this is if the tables are not empty, there will be key collisions.
        """
        try:
            self._insert_users()
            self._insert_activities()
            self._insert_track_points()
            print("All insertions complete!")
        except Exception as e:
            print("-" * 75)
            print(repr(e))
            print("Something went wrong, please try again!")
            self.db_handler.drop_all_tables()       
        
    def _insert_users(self):
        print(f"Inserting users...", end="\r")
        self.db_controller.insert_dataframe(self.data.users, "user")
        print(f"Inserting users... Done!")
        
    def _insert_activities(self):
        print(f"Inserting activities, this might take a while...", end="\r")
        self.db_controller.insert_dataframe(self.data.activities, "activity", index_as_id=True)
        print(f"Inserting activities, this might take a while... Done!")
        
    def _insert_track_points(self):
        print(f"Inserting track points, this WILL take some time...", end="\r")
        self.db_controller.insert_dataframe(self.data.track_points, "track_point")
        print(f"Inserting track points, this WILL take some time... Done!")
        
def main():
    task1 = Task1()
    task1.load_data()
    task1.insert_data()

if __name__ == "__main__":
    main()