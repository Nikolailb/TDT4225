from time import time
import traceback
from DataHandler import DataLoader
from DbController import DbController

class Task1:
    
    def __init__(self) -> None:
        self.data = DataLoader()
        self.db_controller = DbController()
        self.db_controller.create_collections(["user", "activity"])
        
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
        start = time()
        self._insert_users()
        print("Inserting users... Done!")
        print(f"Took approximately {time() - start:.3f} seconds.")
        print("Inserting activities...", end="\r")
        start = time()
        self._insert_activities()
        print("Inserting activities... Done!")
        print(f"Took approximately {time() - start:.3f} seconds.")  
        
    def _insert_users(self):
        self.db_controller.insert_many(
            "user", 
            list(self.data.users.apply(lambda row: 
                {
                    "_id": row["id"], 
                    "has_labels": row["has_labels"], 
                    "activities": self.data.activities[self.data.activities["user_id"] == row["id"]].index.to_list()
                }, axis=1).to_numpy()))
        
    def _insert_activities(self):
        self.db_controller.insert_many(
            "activity", 
            list(self.data.activities.apply(lambda row: 
                {
                    "_id": row.name, 
                    "transportation_mode": row["transportation_mode"],
                    "start_date_time": row["start_date_time"], 
                    "end_date_time": row["end_date_time"], 
                    "track_points": self.data.track_points[self.data.track_points["activity_id"] == row.name].drop(columns=["activity_id"]).to_dict("records")
                }, axis=1).to_numpy()))
            
        
def main():
    try:
        task = Task1()
        task.load_data()
        task.insert_data()
    except:
        print("Something went wrong!")
        traceback.print_exc()
    finally:
        if task:
            task.db_controller.close_connection()
    
if __name__ == "__main__":
    main()