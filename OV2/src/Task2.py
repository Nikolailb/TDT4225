
from datetime import datetime
from DbController import DbController
import pandas as pd
import numpy as np
from haversine import haversine
from tabulate import tabulate

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
        users = self.db_controller.execute_query("SELECT COUNT(*) FROM user").first()
        activities = self.db_controller.execute_query("SELECT COUNT(*) FROM activity").first()
        track_points = self.db_controller.execute_query("SELECT COUNT(*) FROM track_point").first()
        print(f"There are {users} users, {activities} activities and {track_points} track points!")
        
    def question_2(self):
        average = self.db_controller.execute_query(
            """
            SELECT AVG(counts.per_user_count) 
            FROM (
                SELECT COUNT(*) as per_user_count
                FROM activity
                GROUP BY activity.user_id
            ) AS counts
            """).first()
        print(f"The average user has {next(iter(average), None)} activities")
        
    def question_3(self):
        users = self.db_controller.select_dataframe(
            """
            SELECT user_id, COUNT(*) AS num_activities
            FROM activity
            GROUP BY user_id
            ORDER BY num_activities DESC
            LIMIT 20
            """
        )
        users.index += 1
        print("Top 20 user by number of activities:")
        print(tabulate(users, headers="keys", tablefmt="psql"))
        
    def question_4(self):
        users = self.db_controller.execute_query(
            """
            SELECT DISTINCT user_id 
            FROM activity
            WHERE transportation_mode = 'taxi'
            """
        ).all()
        print(f"Users who have used a taxi: {np.array(users).flatten()}")
        
    def question_5(self):
        transportation_modes = self.db_controller.select_dataframe(
        """
            SELECT transportation_mode, COUNT(transportation_mode) AS activity_count
            FROM activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY transportation_mode
            ORDER BY activity_count DESC;
        """
        )
        
        print("Instaces of each type of transportation:")
        print(tabulate(transportation_modes, headers="keys", tablefmt="psql", showindex=False))
    
    def question_6(self):
        query = \
        """
            SELECT YEAR(start_date_time), COUNT(*) AS c
            FROM activity
            GROUP BY YEAR(start_date_time), YEAR(end_date_time)
            ORDER BY c DESC
            LIMIT 1
        """
        activities_a_year = iter(list(self.db_controller.execute_query(query).first()))
        print(f"The year with the most activities is: {next(activities_a_year, None)} with a total of {next(activities_a_year, None)} activities!")

        query = \
        """
            SELECT YEAR(start_date_time), SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS c
            FROM activity
            GROUP BY YEAR(start_date_time), YEAR(end_date_time)
            ORDER BY c DESC
            LIMIT 1
        """        
        hours_a_year = iter(list(self.db_controller.execute_query(query).first()))
        print(f"The year with the most hours: {next(hours_a_year, None)} with a total of {next(hours_a_year, None)} hours!")

    def question_7(self):
        act_with_track = self.db_controller.select_dataframe(
            """
                SELECT activity.start_date_time, activity.end_date_time, track_point.activity_id, track_point.lat, track_point.lon
                FROM activity INNER JOIN track_point ON activity.id=track_point.activity_id
                WHERE activity.user_id = 112 AND activity.transportation_mode = 'walk' AND YEAR(activity.start_date_time) = 2008
            """)
        res = 0
        for activity in act_with_track["activity_id"].unique():
            t = act_with_track[act_with_track["activity_id"] == activity][["lat", "lon"]].to_numpy()
            for i in range(t.shape[0] - 1):
                res += haversine(t[i], t[i + 1])

        print(f"User '112' walked {res:.3f}km in 2008")
        
    def question_8(self):
        print("This might take a while...")
        feet_to_meter = 0.3048
        t = self.db_controller.select_dataframe(
            f"""
            SELECT activity.user_id, SUM(_altitude_gained) * {feet_to_meter} AS altitude_gained
            FROM (
                SELECT track_point.activity_id AS activity_id, GREATEST(track_point.altitude - LAG(track_point.altitude) OVER (ORDER BY track_point.id), 0) AS _altitude_gained
                FROM track_point
                WHERE track_point.altitude > 0
            ) AS t LEFT JOIN activity on (t.activity_id = activity.id)
            GROUP BY activity.user_id
            ORDER BY altitude_gained DESC
            LIMIT 20
            """)
        t.index += 1
        print("Altitude gained per user, top 20:")
        print(tabulate(t, headers="keys", tablefmt="psql", disable_numparse=True))
        
    
    def question_9(self):
        print("This might take a while...")
        invalid = self.db_controller.select_dataframe(
            """
            SELECT p.user_id, COUNT(DISTINCT p.activity_id) AS invalid_activities
            FROM (
                SELECT a.user_id, t.activity_id, TIMESTAMPDIFF(MINUTE, LAG(t.date_time) OVER (PARTITION BY t.activity_id ORDER BY t.id), t.date_time) AS time_diff
                FROM activity AS a INNER JOIN track_point AS t ON a.id = t.activity_id
            ) AS p
            WHERE p.time_diff >= 5
            GROUP BY p.user_id
            ORDER BY invalid_activities DESC
            """
        )
        print("Nr of invalid activities per user:")
        print(tabulate(invalid, headers="keys", tablefmt="psql", showindex=False))
    
    def question_10(self):
        users = np.array(self.db_controller.execute_query(
        """
            SELECT DISTINCT act.user_id
            FROM activity AS act INNER JOIN track_point AS tp ON act.id = tp.activity_id
            WHERE (ROUND(tp.lat, 3) BETWEEN 39.915 AND 39.917) AND (ROUND(tp.lon, 3) BETWEEN 116.396 AND 116.398)
        """).all()).flatten()
        print(f"These users have tracked an activity in the forbidden city: {users}")
        
    def question_11(self):
        activities = self.db_controller.select_dataframe("activity")
        
        value_counts = activities[["user_id", "transportation_mode"]].groupby(["user_id", "transportation_mode"]).value_counts()
        res = []
        for index in value_counts.index.unique(level=0):
            res.append((index, value_counts.loc[index].idxmax()))
            
        print("Most used transportation mode per user:")
        print(tabulate(pd.DataFrame(res, columns=["user_id", "transportation_mode"]).sort_values(by=["user_id"]), headers="keys", tablefmt="psql", showindex=False))
        
def main():
    task2 = Task2()
    for index, question in enumerate(task2.questions):
        print(f"\nQuestion {index + 1}")
        question()
        if index < len(task2.questions) - 1: input("\nPress enter to recieve the next question:")
    
if __name__ == "__main__":
    main()
    