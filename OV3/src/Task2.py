from pprint import pprint
from tabulate import tabulate
from haversine import haversine
from DbController import DbController
import pandas as pd

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
        # act_track = self.db_controller.select_dataframe("activity", 
        # [{
        #     '$lookup': {
        #         'from': 'track_point', 
        #         'localField': '_id', 
        #         'foreignField': 'activity_id', 
        #         'as': 'act_track'
        #     }
        # }, {
        #     '$unwind': '$act_track'
        # }, {
        #     '$project': {
        #         'user_id': '$user_id', 
        #         'activity_id': '$_id', 
        #         'date_time': '$act_track.date_time', 
        #         'altitude': '$act_track.altitude'
        #     }
        # }])
        
        
        # print(act_track.dtypes)
        # act_track["date_time"] = pd.to_datetime(act_track["date_time"], format="%Y-%m-%d %H:%M:%S")
        # res = []
        # for user in act_track["user_id"].unique():
        #     altitude_gained = 0
        #     tracks = act_track[(act_track["user_id"] == user) & (act_track["altitude"] >= 0)]["altitude"].sort_values(by=["activity_id", "date_time"]).to_numpy()
        #     for i in range(tracks.shape[0] - 1):
        #         if tracks[i] < tracks[i + 1]:
        #             altitude_gained += tracks[i + 1] - tracks[i]
        #     res.append(pd.Series([user, altitude_gained], index=["user_id", "altitude_gained"]))
        # res = pd.DataFrame(res).sort_values(by=["altitude_gained"], ascending=False)
        # print(tabulate(res, headers="keys", tablefmt="psql", showindex=False))
        
        users = list(self.db_controller.get_distinct("user", "_id"))
        res = []
        for j, user in enumerate(users):
            print(f"Currently doing user: {user}. {j+1}/{len(users)}                                                      ", end="\r")
            altitude_gained = 0
            user_activity = self.db_controller.select_dataframe("activity",
            [{
                '$match': {
                    'user_id': {
                        '$eq': user
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
                    'activity_id': '$_id', 
                    'date_time': '$act_track.date_time', 
                    'altitude': '$act_track.altitude'
                }
            }, {
                '$match': {
                    'altitude': {
                        '$gte': 0
                    }
                }    
            }]).sort_values(by=["activity_id", "date_time"])["altitude"].to_numpy()
            print(f"Currently doing user: {user}. {j+1}/{len(users)}. Finished fetching data, starting calculation...", end="\r")
            for i in range(user_activity.shape[0] - 1):
                altitude_gained += max(user_activity[i + 1] - user_activity[i], 0)
            res.append(pd.Series([user, altitude_gained], index=["user_id", "altitude_gained"]))
        res = pd.DataFrame(res).sort_values(by=["altitude_gained"])
        print(tabulate(res, headers="keys", tablefmt="psql", showindex=False))
        
            

    def question_9(self):
        pass

    def question_10(self):
        pass

    def question_11(self):
        pass
    
    
    
def main():
    try:
        task = Task2()
        # task.question_8()
        
        t1 = task.db_controller.select_dataframe()
        
        # for index, question in enumerate(task.questions):
        #     print(f"\nQuestion {index + 1}")
        #     question()
        #     if index < len(task.questions) - 1: input("\nPress enter to recieve the next question:")
    except Exception() as e:
        print("Something went wrong!")
        print(e)
    finally:
        if task:
            task.db_controller.close_connection()
    
    
if __name__ == "__main__":
    main()