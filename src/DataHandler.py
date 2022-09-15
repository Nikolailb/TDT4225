import math
import os

class DataHandler:
    def __init__(self) -> None:
        self.users = []
        self.activities = []
        self.track_points = []
        
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
                self.users.append({"id": obj.name, "data": (labeled)})
        print(f"Total users: {len(self.users)}")
        
        for user in self.users:
            labels = []
            if user["data"]:
                with open(os.path.join("resources/dataset/Data", user["id"], "labels.txt")) as file:
                    file.readline()
                    labels = [[i.strip().replace("/", "-") for i in line.split("\t")] for line in file]
            for obj in os.scandir(os.path.join("resources/dataset/Data", user["id"], "Trajectory")):
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
                                data = (user["id"], label, start_date, end_date)
                                break
                        if data is None:
                            data = (user["id"], None, start_date, end_date)
                        self.activities.append({"id": obj.name, "data": data})
                        for i in range(6, len(lines)):
                            lat, lon, _, altitude, data_days, data_date, data_time = [l.strip() for l in lines[i].split(",")]
                            self.track_points.append({"id": obj.name, "data": (float(lat), float(lon), int(math.floor(float(altitude))), float(data_days), f"{data_date} {data_time}")})
        print(f"Total activities: {len(self.activities)}")
        print(f"Total trackpoints: {len(self.track_points)}")
        
        def get_user_data(self):
            user_data = [(user["id"])]
            for user in self.users:
                pass
                

def main():
    print(tuple(["sss"] + ["tw" , "ssad"]))
   
def main2():
    obj = DataHandler()
    obj.read_data()
    
if __name__ == "__main__":
    main()