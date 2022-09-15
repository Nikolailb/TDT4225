import math
import os

class DataHandler:
    def __init__(self) -> None:
        self.users = {}
        self.activities = {}
        self.track_points = {}
        
    def read_data(self):
        print("Starting to read data, this might take a while...")
        labeled_ids = []
        with open("resources/dataset/labeled_ids.txt") as file:
            labeled_ids = [line.strip() for line in file]
        next_labeled_id = labeled_ids.pop(0)
        for obj in os.scandir("resources/dataset/Data"):
            if obj.is_dir():
                labeled = int(next_labeled_id == obj.name)
                if labeled:
                    next_labeled_id = labeled_ids.pop(0) if len(labeled_ids) > 0 else -1
                self.users[obj.name] = [labeled]
        print(f"Total users: {len(self.users)}")
        
        act_temp_id = 20000
        for user, labeled in self.users.items():
            labels = []
            if labeled[0]:
                with open(os.path.join("resources/dataset/Data", user, "labels.txt")) as file:
                    file.readline()
                    labels = [[i.strip().replace("/", "-") for i in line.split("\t")] for line in file]
            for obj in os.scandir(os.path.join("resources/dataset/Data", user, "Trajectory")):
                if obj.is_file():
                    with open(obj.path) as file:
                        lines = file.readlines()
                        if len(lines) < 7 or len(lines) > 2506:
                            continue
                        start_date = " ".join([i.strip() for i in lines[6].split(",")][5:])
                        end_date = " ".join([i.strip() for i in lines[-1].split(",")][5:])
                        data = None
                        for start, end, label in labels:
                            if start == start_date and end == end_date:
                                data = [user, label, start_date, end_date]
                                break
                        if data is None:
                            data = [user, None, start_date, end_date]
                        self.activities[act_temp_id] = data
                        act_temp_id += 1
                        for i in range(6, len(lines)):
                            lat, lon, _, altitude, data_days, data_date, data_time = [l.strip() for l in lines[i].split(",")]
                            if act_temp_id not in self.track_points:
                                self.track_points[act_temp_id] = []
                            self.track_points[act_temp_id].append([float(lat), float(lon), int(math.floor(float(altitude))), float(data_days), f"{data_date} {data_time}"])
        print(f"Total activities: {len(self.activities)}")
        print(f"Total trackpoints: {sum([len(val) for val in self.track_points.values()])}")
      
    def update_activity_id(self, old_id, new_id):
        if new_id == old_id:
            return
        if new_id in self.activities:
            print("Key crash!")
            self.update_activity_id(new_id, max(self.activities.keys()) + 1)
        self.activities[new_id] = self.activities.pop(old_id)
        self.track_points[new_id] = [track_point for track_point in self.track_points.pop(old_id)]
        
    def get_user_data(self):
        return [[user] + data for user, data in self.users.items()]
    
    def get_activity_data(self):
        return [activity for activity in self.activities.values()]
    
    def get_track_point_data(self):
        return [(activity_id, data) for activity_id, data in self.track_points.items()]
                

def main():
    print(tuple(["sss"] + ["tw" , "ssad"]))
   
def main2():
    obj = DataHandler()
    obj.read_data()
    
if __name__ == "__main__":
    main2()