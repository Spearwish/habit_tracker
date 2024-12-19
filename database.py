import sqlite3
from datetime import datetime, timedelta


# only init """ """ comments needed

def db_search(target: str, table: str, addon: str, params, cursor):
    """
    Executes a SQL SELECT query on a specified table with params and returns the results.
    :param target: The column to be selected from the table.
    :param table: The name of the SQL table.
    :param addon: Additional SQL clauses, such as WHERE, ...
    :param params: Parameters to be passed to the SQL query for addon specification.
    :param cursor: The database cursor object used to execute the query.
    :return:
    """
    query = f"""SELECT {target} FROM {table} {addon}"""
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    return cursor.fetchall()


class Database:
    """
    This is a Database class serving as a data getter, setter, manipulator and analyzer.
    """

    def __init__(self, db_name: str = "habits.db"):
        """
        Initializes the Database class and sets up a SQLite3 database connector.
        :param: db_name: The name of the SQLite3 database.
        :return: None
        """
        self.connection = sqlite3.connect(db_name)
        self.create_tables()

    def create_tables(self):
        """
        Creates two database tables (if they do not already exists). Habits with columns for id, task, period, habit start_date, duration
        and deadlines with columns id, task, from_date, to_date, checked_off, completetion_date.
        :return: None
        """
        habit_table_query = """
        CREATE TABLE IF NOT EXISTS habits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task TEXT NOT NULL,
            period INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            duration INTEGER NOT NULL
        )
        """

        deadline_table_query = """
        CREATE TABLE IF NOT EXISTS deadlines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task INTEGER NOT NULL,
            from_date TEXT NOT NULL,
            to_date TEXT NOT NULL,
            checked_off INTEGER NOT NULL,
            completion_date TEXT,
            FOREIGN KEY (task) REFERENCES habits (id)
        )
        """

        cursor = self.connection.cursor()
        cursor.execute(habit_table_query)
        cursor.execute(deadline_table_query)
        self.connection.commit()
        cursor.close()

    def insert_habit(self, task: str, period: int, duration: int = 365, date: str = None):
        """
        Inserts a habit data task, period and optional data duration, date into the habits table.
        :param task: The description of a habit.
        :param period: The periodicity (in days).
        :param duration: For how long is the habit defined, defaults to 365 days.
        :param date: Custom start_date, defaults to today's date
        :return: None
        """
        if date:
            start_date = date
        else:
            start_date = datetime.now().strftime("%Y-%m-%d")

        # start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d") testing purposes

        habit_table_query = """
        INSERT INTO habits (task, period, start_date, duration) VALUES (?, ?, ?, ?)
        """

        deadline_table_query = """
        INSERT INTO deadlines (task, from_date, to_date, checked_off) VALUES (?, ?, ?, 0)
        """

        cursor = self.connection.cursor()
        cursor.execute(
            habit_table_query,
            (
                task,
                period,
                start_date,
                duration,
            )
        )

        habit_intervlas = duration // period + 1
        for interval in range(habit_intervlas):
            cursor.execute(
                deadline_table_query,
                (task,
                 datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=interval * period),
                 datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=(interval + 1) * period)
                 # - 1), testing purposes
                 )
            )

        self.connection.commit()
        cursor.close()

    def list_habits(self, habit_period: int = None):
        """
        Lists all available habits and return the result based on habit_period, defaults to listing everything.
        :param habit_period: The period used for filtering the habits.
        :return: List of database entries.
        """
        cursor = self.connection.cursor()

        if habit_period:
            db_data = db_search("*", "habits", "WHERE period = ?", (habit_period,), cursor)
        else:
            db_data = db_search("*", "habits", "", None, cursor)

        cursor.close()
        return db_data

    def delete_habit(self, task: str = None):
        """
        Used for deleting task specific entries in database, if no task is specified it delets all available database entries.
        :param task: The name of the habit to be deleted.
        :return: None
        """
        cursor = self.connection.cursor()

        if task:
            habit_table_query = """DELETE FROM habits WHERE task = ?"""
            deadline_table_query = """DELETE FROM deadlines WHERE task = ?"""
            cursor.execute(habit_table_query, (task,))
            cursor.execute(deadline_table_query, (task,))
        else:
            habit_table_query = """DELETE FROM habits"""
            deadline_table_query = """DELETE FROM deadlines"""
            cursor.execute(habit_table_query)
            cursor.execute(deadline_table_query)

        self.connection.commit()
        cursor.close()

    def check_off_habit(self, task: str, date: str = None):
        """
        Method for marking specified task/habit complete 0 -> 1 in a certain period. If no date is specifed it defaults to todays date.
        :param task: The name of the habit to be checked off.
        :param date: (optional) The completion date specification.
        :return: None
        """
        cursor = self.connection.cursor()

        db_data = db_search("*", "deadlines", "WHERE task = ?", (task,), cursor)

        if date:
            current_time = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        else:
            current_time = datetime.now()  # - timedelta(days=20) testing purposes
        for data in db_data:
            from_date = datetime.strptime(data[2], "%Y-%m-%d %H:%M:%S")
            to_date = datetime.strptime(data[3], "%Y-%m-%d %H:%M:%S")

            if from_date <= current_time <= to_date:
                id = data[0]
                break

        check_off_query = """
        UPDATE deadlines SET checked_off = 1, completion_date = ? WHERE id = ?
        """
        cursor.execute(check_off_query, (current_time, id))
        self.connection.commit()

        cursor.close()

    def get_streak(self, task: str):
        """
        Method for retrieving tasks biggest streak
        :param task: The name of the habit for streak calculation.
        :return: int, maximum streak (in days).
        """
        max_streak = 0
        running_streak = 0
        cursor = self.connection.cursor()

        db_data = db_search("*", "deadlines", "WHERE task = ?", (task,), cursor)

        for data in db_data:
            if data[4]:
                running_streak += 1
                max_streak = running_streak if max_streak < running_streak else max_streak
            else:
                running_streak = 0

        cursor.close()

        return max_streak

    def get_success_rate(self, task: str, duration: int):
        """
        Method for getting success / completion rate of a concrete habit for the last "duration" days.
        :param task: The name of the habit for success rate calculation.
        :param duration: The time span in days for last X days of calculation.
        :return: float ranging from 0.0 to 1.0 , success/ completion rate.
        """
        cursor = self.connection.cursor()
        db_data = db_search("*", "deadlines", "WHERE task = ?", (task,), cursor)

        duration = int(duration) if duration else 30

        current_time = datetime.now()
        from_date = current_time - timedelta(days=duration)

        # including only concerte habits deadlines which are partially or completely streched over a certain duration
        filtered_data = [
            data for data in db_data
            if (
                    (from_date <= datetime.strptime(data[3], "%Y-%m-%d %H:%M:%S") <= current_time)
                    or
                    (from_date <= datetime.strptime(data[2], "%Y-%m-%d %H:%M:%S") <= current_time)
            )
        ]

        check_off_sum = sum(data[4] for data in filtered_data)

        cursor.close()
        try:
            success_rate = check_off_sum / len(filtered_data)
        except ZeroDivisionError:
            success_rate = 0.0

        return success_rate
