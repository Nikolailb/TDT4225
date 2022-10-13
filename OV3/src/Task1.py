from DataHandler import DataLoader
from DbController import DbController

class Task1:
    
    def __init__(self) -> None:
        self.data = DataLoader()
        self.db_controller = DbController()
        self.db_controller.create_collections(["user", "activity", "track_point"])
        
    def load_data(self, read_from_file=False, save_to_file=False):
        """Loads the data from the dataset into pandas dataframes

        Args:
            read_from_file (bool, optional): Read from previously saved files. Defaults to False.
            save_to_file (bool, optional): Write resulting dataframes to file. Defaults to False.
        """
        self.data = DataLoader()
        self.data.load_data(read_from_file=read_from_file, write_to_file=save_to_file)
    
    def insert_data(self):
        print()
        print("Inserting users...", end="\r")
        self._insert_users()
        print("Inserting users... Done!")
        print("Inserting activities...", end="\r")
        self._insert_activities()
        print("Inserting activities... Done!")
        print("Inserting track points...", end="\r")
        self._insert_track_points()
        print("Inserting track points... Done\n")    
        
    def _insert_users(self):
        self.db_controller.insert_dataframe(self.data.users.rename(columns={"id": "_id"}), "user")
        
    def _insert_activities(self):
        self.db_controller.insert_dataframe(self.data.activities, "activity", index_as_id=True)
    
    def _insert_track_points(self):
        self.db_controller.insert_dataframe(self.data.track_points, "track_point")
        
def main():
    try:
        task = Task1()
        task.load_data(read_from_file=True)
        task.insert_data()
    except Exception as e:
        print("Something went wrong!")
        print(e)
    finally:
        if task:
            task.db_controller.close_connection()
    
if __name__ == "__main__":
    main()