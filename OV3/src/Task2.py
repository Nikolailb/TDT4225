from pprint import pprint
from site import USER_SITE
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
        user_count = self.db_controller.aggregate("user", 
        [{
            '$group': {
                '_id': 'user_count', 
                'count': {
                    '$sum': 1
                }
            }
        }])
        activity_count = self.db_controller.aggregate("activity", 
        [{
            '$group': {
                '_id': 'user_count', 
                'count': {
                    '$sum': 1
                }
            }
        }])
        track_point_count = self.db_controller.aggregate("track_point", 
        [{
            '$group': {
                '_id': 'user_count', 
                'count': {
                    '$sum': 1
                }
            }
        }])
        print(f"Number of users: {next(user_count)['count']}")
        print(f"Number of activities: {next(activity_count)['count']}")
        print(f"Number of track points: {next(track_point_count)['count']}")
    
    def question_2(self):
        stats = self.db_controller.aggregate("activity",
        [{
        '$group': {
            '_id': '$user_id', 
            'count': {
                '$sum': 1
            }
        }}, 
        {
        '$group': {
            '_id': 'activity_user', 
            'Average': {
                '$avg': '$count'
            }, 
        }}])
        pprint(list(stats))

    def question_3(self):
        top_activities = pd.DataFrame.from_records(list(self.db_controller.aggregate("activity", 
        [{
            '$group': {
                '_id': '$user_id', 
                'count': {
                    '$sum': 1
                }
            }
        }, 
        {
            '$sort': {
                'count': -1
            }
        }, 
        {
            '$limit': 20
        }])))
        top_activities.index += 1
        print(tabulate(top_activities, headers="keys", tablefmt="psql"))

    def question_4(self):
        user_taken_taxi = self.db_controller.aggregate("activity", 
        [{
            '$match': {
                'transportation_mode': {
                    '$eq': 'taxi'
                }
            }
        }, 
        {
            '$group': {
                '_id': '$user_id',
            }
        }])
        print(f"Users who have used a taxi: {[i['_id'] for i in list(user_taken_taxi)]}")

    def question_5(self):
        mode_count = self.db_controller.aggregate("activity", 
        [{
            '$match': {
                'transportation_mode': {
                    '$ne': None
                }
            }
        }, 
        {
            '$group': {
                '_id': '$transportation_mode', 
                'count': {
                    '$sum': 1
                }
            }
        },
        {
            '$sort': {
                'count': -1
            }
        }])
        pprint(list(mode_count))

    def question_6(self):
        acti_per_year = next(self.db_controller.aggregate("activity",
        [{
            '$group': {
                '_id': {
                    '$year': '$start_date_time'
                }, 
                'count': {
                    '$sum': 1
                }
            }
        },
        {
            '$sort': {
                'count': -1
            }
        },
        {
            '$limit': 1
        }]))
        hours_per_year = next(self.db_controller.aggregate("activity",
        [{
            '$group': {
                '_id': {
                    '$year': '$start_date_time'
                }, 
                'total_hours': {
                    '$sum': {
                        '$dateDiff': {
                            'startDate': '$start_date_time', 
                            'endDate': '$end_date_time', 
                            'unit': 'hour'
                        }
                    }
                }
            }
        },
        {
            '$sort': {
                'total_hours': -1
            }
        },
        {
            '$limit': 1
        }]))
        print(f"The year with the most activities was {acti_per_year['_id']} with {acti_per_year['count']} activities!")
        print(f"The year with the most hours was {hours_per_year['_id']} with {hours_per_year['total_hours']} activities!")

    def question_7(self):
        activity_track = self.db_controller.select_dataframe("activity", 
        [{
            '$match': {
                'user_id': {
                    '$eq': '112'
                }, 
                'transportation_mode': {
                    '$eq': 'walk'
                }
            }
        }, {
            '$lookup': {
                'from': 'track_point', 
                'localField': '_id', 
                'foreignField': 'activity_id', 
                'as': 'act_track'
            }
        }, {
            '$unwind': '$act_track'
        }, {
            '$project': {
                'activity_id': '$act_track.activity_id', 
                'date_time': '$act_track.date_time', 
                'year': {
                    '$year': '$act_track.date_time'
                }, 
                'lat': '$act_track.lat', 
                'lon': '$act_track.lon'
            }
        }, {
            '$match': {
                'year': {
                    '$eq': 2008
                }
            }
        }])
        res = 0
        for activity in activity_track["activity_id"].unique():
            t = activity_track[activity_track["activity_id"] == activity][["lat", "lon"]].to_numpy()
            for i in range(t.shape[0] - 1):
                res += haversine(t[i], t[i + 1])
        print(f"User \"112\" walked a total of {res:.3f}km in 2008")

    def question_8(self):
        
        feet_to_meter = 0.3048
        
        print("Fetching activities...", end="\r")
        activities = self.db_controller.select_dataframe("activity", 
        [{
            '$project': {
                '_id': '$_id',
                'user_id': '$user_id'
            }
        }], keep_id=True)
        print("Fetching activities... Done!")
        print("Fetching track points...", end="\r")
        track_points = self.db_controller.select_dataframe("track_point", 
        [{
            '$match': {
                'altitude': {
                    '$gte': 0
                }
            }
        }, {
            '$project': {
                'activity_id': '$activity_id',
                'altitude': '$altitude',
                'date_time': '$date_time'
            }
        }])
        print("Fetching track points... Done!")
        
        act_track = track_points.merge(activities, how="left", left_on="activity_id", right_on="_id").sort_values(by=["activity_id", "date_time"])
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
        
            

    def question_9(self):
        print("Fetching activities...", end="\r")
        activities = self.db_controller.select_dataframe("activity", 
        [{
            '$project': {
                '_id': '$_id',
                'user_id': '$user_id'
            }
        }], keep_id=True)
        print("Fetching activities... Done!")
        print("Fetching track points...", end="\r")
        track_points = self.db_controller.select_dataframe("track_point", 
        [{
            '$project': {
                'activity_id': '$activity_id',
                'date_time': '$date_time'
            }
        }])
        print("Fetching track points... Done!")
        act_track = track_points.merge(activities, how="left", left_on="activity_id", right_on="_id").sort_values(by=["activity_id", "date_time"])
        users = act_track["user_id"].unique()
        res = []
        for j, user in enumerate(users):
            print(f"Currently on user {j + 1}/{users.shape[0]}", end="\r")
            invalid_activities = 0
            user_tracks = act_track[act_track['user_id'] == user]["date_time"].to_numpy()
            for i in range(user_tracks.shape[0] - 1):
                invalid_activities += int(((user_tracks[i + 1] - user_tracks[i]) / np.timedelta64(1, "s")) >= 300)
            res.append(pd.Series([user, invalid_activities], index=["user_id", "invalid_activities"]))
        res = pd.DataFrame(res).sort_values(by=["invalid_activities"], ascending=False)
        print()
        print(tabulate(res, headers="keys", tablefmt="psql", showindex=False))

    def question_10(self):
        pass

    def question_11(self):
        pass
    
    
    
def main():
    try:
        task = Task2()
        task.question_9()
        # for index, question in enumerate(task.questions):
        #     print(f"\nQuestion {index + 1}")
        #     question()
        #     if index < len(task.questions) - 1: input("\nPress enter to recieve the next question:")
    except Exception as e:
        print("Something went wrong!")
        print(e)
    finally:
        if task:
            task.db_controller.close_connection()
    
    
if __name__ == "__main__":
    main()