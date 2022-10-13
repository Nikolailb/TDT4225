from pprint import pprint
from site import USER_SITE
from time import time
import traceback
from tabulate import tabulate
from haversine import haversine
from DbController import DbController
import pandas as pd
import numpy as np

class Task2:
    def __init__(self) -> None:
        self.db_controller = DbController()
        self.questions = [
            self.question_1, self.question_2, self.question_3,
            self.question_4, self.question_5, self.question_6,
            self.question_7, self.question_8, self.question_9,
            self.question_10, self.question_11,
        ]
        
    def question_1(self):
        start = time()
        user_count = next(self.db_controller.aggregate("user", 
        [{
            '$group': {
                '_id': 'user_count', 
                'users': {
                    '$sum': 1
                }
            }
        }]))
        activity_and_tp_count = next(self.db_controller.aggregate("activity", 
        [{
            '$project': {
                '_id': '$_id', 
                'track_points': {
                    '$size': '$track_points'
                }
            }
        }, {
            '$group': {
                '_id': 'counts', 
                'activities': {
                    '$sum': 1
                }, 
                'track_points': {
                    '$sum': '$track_points'
                }
            }
        }]))
        print(f"Number of users: {user_count['users']}")
        print(f"Number of activities: {activity_and_tp_count['activities']}")
        print(f"Number of track points: {activity_and_tp_count['track_points']}")
        print(f"Took approximately {time() - start:.3f} seconds")
    
    def question_2(self):
        start = time()
        stats = self.db_controller.aggregate("user",
        [{
            '$group': {
                '_id': 'average', 
                'average': {
                    '$avg': {
                        '$size': '$activities'
                    }
                }
            }
        }])
        pprint(list(stats))
        print(f"Took approximately {time() - start:.3f} seconds")
        

    def question_3(self):
        start = time()
        top_activities = pd.DataFrame.from_records(list(self.db_controller.aggregate("user", 
        [{
            '$project': {
                '_id': '$_id', 
                'activities': {
                    '$size': '$activities'
                }
            }
        }, {
            '$sort': {
                'activities': -1
            }
        }, {
            '$limit': 20
        }])))
        top_activities.index += 1
        print(tabulate(top_activities, headers="keys", tablefmt="psql"))
        print(f"Took approximately {time() - start:.3f} seconds")
        

    def question_4(self):
        start = time()
        user_taken_taxi = self.db_controller.aggregate("activity", 
        [{
            '$lookup': {
                'from': 'activity', 
                'localField': 'activities', 
                'foreignField': '_id', 
                'as': 'activities'
            }
        }, {
            '$unwind': '$activities'
        }, {
            '$match': {
                'activities.transportation_mode': 'taxi'
            }
        }, {
            '$group': {
                '_id': '$_id'
            }
        }])
        print(f"Users who have used a taxi: {[i['_id'] for i in list(user_taken_taxi)]}")
        print(f"Took approximately {time() - start:.3f} seconds")
        

    def question_5(self):
        start = time()
        mode_count = self.db_controller.aggregate("activity", 
        [{
            '$match': {
                'transportation_mode': {
                    '$ne': None
                }
            }
        }, {
            '$group': {
                '_id': '$transportation_mode', 
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }])
        pprint(list(mode_count))
        print(f"Took approximately {time() - start:.3f} seconds")
        

    def question_6(self):
        start = time()
        per_year = list(self.db_controller.aggregate("activity",
        [{
            '$project': {
                'year': {
                    '$year': '$start_date_time'
                }, 
                'start_date': '$start_date_time', 
                'end_date': '$end_date_time'
            }
        }, {
            '$group': {
                '_id': '$year', 
                'activity_count': {
                    '$sum': 1
                }, 
                'hour_count': {
                    '$sum': {
                        '$dateDiff': {
                            'startDate': '$start_date', 
                            'endDate': '$end_date', 
                            'unit': 'hour'
                        }
                    }
                }
            }
        }, {
            '$sort': {
                'activity_count': -1
            }
        }]))
        
        print(f"The year with the most activities was {per_year[0]['_id']} with {per_year[0]['activity_count']} activities!")
        hour = max(per_year, key=lambda e: e["hour_count"])
        print(f"The year with the most hours was {hour['_id']} with {hour['hour_count']} hours!")
        print(f"Took approximately {time() - start:.3f} seconds")
        

    def question_7(self):
        start = time()
        activity_track = self.db_controller.select_dataframe("user", 
        [{
            '$match': {
                '_id': '112'
            }
        }, {
            '$lookup': {
                'from': 'activity', 
                'localField': 'activities', 
                'foreignField': '_id', 
                'as': 'activities'
            }
        }, {
            '$unwind': '$activities'
        }, {
            '$unwind': '$activities.track_points'
        }, {
            '$project': {
                '_id': 0, 
                'activity_id': '$activities._id', 
                'transportation_mode': '$activities.transportation_mode', 
                'date_time': '$activities.track_points.date_time', 
                'year': {
                    '$year': '$activities.track_points.date_time'
                }, 
                'lat': '$activities.track_points.lat', 
                'lon': '$activities.track_points.lon'
            }
        }, {
            '$match': {
                'year': 2008, 
                'transportation_mode': 'walk'
            }
        }])
        res = 0
        for activity in activity_track["activity_id"].unique():
            t = activity_track[activity_track["activity_id"] == activity][["lat", "lon"]].to_numpy()
            for i in range(t.shape[0] - 1):
                res += haversine(t[i], t[i + 1])
        print(f"User \"112\" walked a total of {res:.3f}km in 2008")
        print(f"Took approximately {time() - start:.3f} seconds")

    def question_8(self):
        start = time()
        feet_to_meter = 0.3048
        
        print("Fetching activities...", end="\r")
        activities = self.db_controller.select_dataframe("user", 
        [{
            '$lookup': {
                'from': 'activity', 
                'localField': 'activities', 
                'foreignField': '_id', 
                'as': 'activities'
            }
        }, {
            '$unwind': '$activities'
        }, {
            '$unwind': '$activities.track_points'
        }, {
            '$match': {
                'activities.track_points.altitude': {
                    '$ne': -777
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'user_id': '$_id', 
                'activity_id': '$activities._id', 
                'altitude': '$activities.track_points.altitude', 
                'date_time': '$activities.track_points.date_time'
            }
        }])
        print("Fetching track points... Done!")
        
        act_track = activities.sort_values(by=["activity_id", "date_time"])
        res = []
        print("Calculating altitude gained per user:")
        users = act_track["user_id"].unique()
        for j, user in enumerate(users):
            print(f"Currently on user {j + 1}/{users.shape[0]}", end="\r")
            altitude_gained = 0
            user_tracks = act_track[act_track['user_id'] == user]["altitude"].to_numpy()
            for i in range(user_tracks.shape[0] - 1):
                altitude_gained += max(user_tracks[i + 1] - user_tracks[i], 0)
            res.append(pd.Series([user, altitude_gained], index=["user_id", "altitude_gained"]))
        res = pd.DataFrame(res).sort_values(by=["altitude_gained"], ascending=False)
        res["altitude_gained"] = res['altitude_gained'] * feet_to_meter
        print()
        print(tabulate(res[:20], headers="keys", tablefmt="psql", showindex=False))
        print(f"Took approximately {time() - start:.3f} seconds")        
            

    def question_9(self):
        start = time()
        res = []
        users = list(self.db_controller.get_distinct("user", "_id"))
        for j, user in enumerate(users):
            print(f"Currently on user {j + 1}/{users.shape[0]}", end="\r")
            activities = list(self.db_controller.aggregate("user", 
            [{
                '$match': {
                    '_id': user
                }
            }, 
            {
                '$lookup': {
                    'from': 'activity', 
                    'localField': 'activities', 
                    'foreignField': '_id', 
                    'as': 'activities'
                }
            }, {
                '$unwind': '$activities'
            }, {
                '$project': {
                    '_id': 0, 
                    'track_points': '$activities.track_points'
                }
            }]))
            invalid_activities = 0
            for activity in activities:
                track_points = activity["track_points"]
                for i in range(len(track_points) - 1):
                    if (track_points[i + 1]["date_time"] - track_points[i]["date_time"]).total_seconds() >= 300:
                        invalid_activities += 1
                        break
            res.append(pd.Series([user, invalid_activities], index=["user_id", "invalid_activities"]))
        
        res = pd.DataFrame(res).sort_values(by=["invalid_activities"], ascending=False)
        print()
        print(tabulate(res, headers="keys", tablefmt="psql", showindex=False))
        print(f"Took approximately {time() - start:.3f} seconds")

    def question_10(self):
        start = time()
        res = self.db_controller.aggregate("user", 
        [{
            '$lookup': {
                'from': 'activity', 
                'localField': 'activities', 
                'foreignField': '_id', 
                'as': 'activities'
            }
        }, {
            '$unwind': '$activities'
        }, {
            '$unwind': '$activities.track_points'
        }, {
            '$project': {
                'lat': {
                    '$round': [
                        '$activities.track_points.lat', 3
                    ]
                }, 
                'lon': {
                    '$round': [
                        '$activities.track_points.lon', 3
                    ]
                }
            }
        }, {
            '$match': {
                'lat': {
                    '$gte': 39.915, 
                    '$lte': 39.917
                }, 
                'lon': {
                    '$gte': 116.396, 
                    '$lte': 116.398
                }
            }
        }, {
            '$group': {
                '_id': '$_id'
            }
        }])
        print("These users have been in the forbidden city:")
        pprint(list(res))
        print(f"Took approximately {time() - start:.3f} seconds")

    def question_11(self):
        start = time()
        pass
        print(f"Took approximately {time() - start:.3f} seconds")
    
    
    
def main():
    try:
        task = Task2()
        task.question_10()
        # for index, question in enumerate(task.questions):
        #     print(f"\nQuestion {index + 1}")
        #     question()
        #     if index < len(task.questions) - 1: input("\nPress enter to recieve the next question:")
    except:
        print("Something went wrong!")
        traceback.print_exc()
    finally:
        if task:
            task.db_controller.close_connection()
    
    
if __name__ == "__main__":
    main()