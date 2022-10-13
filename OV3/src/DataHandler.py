from datetime import datetime
import math
import os
from pathlib import Path
import pandas as pd
import numpy as np

class DataLoader:
    
    def __init__(self) -> None:
        self.users = []
        self.activities = []
        self.track_points = []
        self.date_format = "%Y-%m-%d %H:%M:%S"
        
        self._user_path = Path("resources/users.csv")
        self._activity_path = Path("resources/activities.csv")
        self._track_point_path = Path("resources/track_points.csv")
        
    def load_data(self, write_to_file=False, read_from_file=False):
        """Load the data from the dataset into pandas dataframes.

        Args:
            write_to_file (bool, optional): Whether you should write the results to file. Defaults to False.
            read_from_file (bool, optional): Whether you should read previously built dataframes from file. Defaults to False.
        """
        if not read_from_file:
            self._load_users()
            self._load_activities()
            self._load_track_points()
        else:
            if self._user_path.exists() and self._activity_path.exists() and self._track_point_path.exists():
                self.users = pd.read_csv(self._user_path, dtype={"id": str})
                self.activities = pd.read_csv(self._activity_path, dtype={"user_id": str})
                self.activities = self.activities.where(self.activities.notna(), None)
                self.activities["start_date_time"] = pd.to_datetime(self.activities["start_date_time"], format=self.date_format)
                self.activities["end_date_time"] = pd.to_datetime(self.activities["end_date_time"], format=self.date_format)
                self.activities.index += 1
                self.track_points = pd.read_csv(self._track_point_path)
                self.track_points["date_time"] = pd.to_datetime(self.track_points["date_time"], format=self.date_format)
                
            else:
                print("Missing files! Run with write_to_file=True before trying again.")
            return
            
        if write_to_file:
            print("Writing result to file...")
            self._user_path.parent.mkdir(parents=True, exist_ok=True)
            self.users.to_csv(self._user_path, index=False)
            self._activity_path.parent.mkdir(parents=True, exist_ok=True)
            self.activities.to_csv(self._activity_path, index=False)
            self._track_point_path.parent.mkdir(parents=True, exist_ok=True)
            self.track_points.to_csv(self._track_point_path, index=False)
    
    def _load_users(self):
        print("Loading users...", end="\r")
        labeled_ids = []
        with open("resources/dataset/labeled_ids.txt") as file:
            labeled_ids = iter([line.strip() for line in file])
        next_labeled_id = next(labeled_ids)
        for obj in os.scandir("resources/dataset/Data"):
            if obj.is_dir():
                has_labels = int(next_labeled_id == obj.name)
                if has_labels:
                    next_labeled_id = next(labeled_ids, -1)
                self.users.append([obj.name, has_labels])
        self.users = pd.DataFrame(self.users, columns=["id", "has_labels"])
        print(f"Loaded {self.users.shape[0]} users!")
    
    def _load_activities(self):
        print("Loading activities...", end="\r")
        def f(row):
            """Helper function for pandas apply. Reads the activities per user.

            Args:
                row (pd.Series): A row from the user dataset
            """
            # Ignore the following 4 lines, just for status indicator in console
            i = int(row.name)
            frq = 5
            if i % frq == 0:
                print(f"Completed {(i / self.users.shape[0]) * 100:.2f}% of activities...", end="\r")
            
            labels = []
            act = []
            if row["has_labels"]:
                with open(os.path.join("resources/dataset/Data", row["id"], "labels.txt")) as file:
                    file.readline()
                    for line in file:
                        st, en, transport = [i.strip().replace("/", "-") for i in line.split("\t")]
                        labels.append([datetime.strptime(st, self.date_format), datetime.strptime(en, self.date_format), transport])
            for obj in os.scandir(os.path.join("resources/dataset/Data", row["id"], "Trajectory")):
                if obj.is_file():
                    with open(obj.path) as file:
                        lines = file.readlines()
                        if len(lines) < 7 or len(lines) > 2506: # No track points or more than 2500 track points. 6 first lines are not counted
                            continue
                        start_date = datetime.strptime(" ".join([i.strip() for i in lines[6].split(",")][5:]), self.date_format)
                        end_date = datetime.strptime(" ".join([i.strip() for i in lines[-1].split(",")][5:]), self.date_format)
                        data = [row["id"], None, start_date, end_date]
                        for start, end, label in labels:
                            if start == start_date and end == end_date and (end - start).total_seconds() > 0:
                                data[1] = label
                                break
                        self.activities.append(data)
                        for line in lines[6:]: # Save data for track_points at the same time for efficiency.
                            lat, lon, _, altitude, data_days, data_date, data_time = [l.strip() for l in line.split(",")]
                            self.track_points.append([len(self.activities), float(lat), float(lon), int(math.floor(float(altitude))), float(data_days), datetime.strptime(f"{data_date} {data_time}", self.date_format)])

        self.users.apply(lambda row: f(row), axis=1)
        self.activities = pd.DataFrame(self.activities, columns=["user_id", "transportation_mode", "start_date_time", "end_date_time"])
        self.activities.index += 1 # Start at 1 instead of 0 to make mysql happy.
        print(f"Loaded {self.activities.shape[0]} activities!               ")
        
    def _load_track_points(self):
        print(f"Loading track points...", end="\r")
        self.track_points = pd.DataFrame(self.track_points, columns=["activity_id", "lat", "lon", "altitude", "date_days", "date_time"])
        print(f"Loaded {self.track_points.shape[0]} track points!") 

def main():
    a = DataLoader()
    a.load_data()
    print(a.activities.dtypes)
    print(a.track_points.dtypes)
    
if __name__ == "__main__":
    main()