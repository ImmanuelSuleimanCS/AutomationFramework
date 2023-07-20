from multiprocessing.pool import ThreadPool as Pool
import datetime
import time
from email import encoders
from email.utils import COMMASPACE, formatdate
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import smtplib

from dbfread import DBF
import pyodbc
import pandas as pd
import os
import json

from cryptography.fernet import Fernet
from typing import Callable, Any, List, Dict


class ConfigProperties:
    def __init__(self, config_structure=None, file_type=None, enviroment_key=None):
        self.config_structure = config_structure
        self.file_type = file_type
        self.enviroment_key = enviroment_key

    def encrypt_config(self, data, key_path, encrypted_data_path):
        # Generate a Fernet key
        key = Fernet.generate_key()
        # Write the key to the specified file path
        with open(f"{key_path}.txt", 'wb') as f:
            f.write(key)
        # Initialize the encryption object with the key
        cipher = Fernet(key)
        # Encrypt each value in the dictionary
        encrypted_data = {}
        if self.file_type == 'json':
            if self.config_structure == 'flat':
                for key, value in data.items():
                    encrypted_data[key] = cipher.encrypt(
                        value.encode()).decode()
            elif self.config_structure == 'nested':
                for key, value in data.items():
                    encrypted_data[key] = {}
                    for sub_key, sub_value in value.items():
                        encrypted_data[key][sub_key] = cipher.encrypt(
                            sub_value.encode()).decode()
            # Write the encrypted dictionary to the specified JSON file path
            with open(f"{encrypted_data_path}.json", 'w') as f:
                json.dump(encrypted_data, f)
        else:
            raise ValueError(
                "Invalid config file format. In the future, YAML, TOML, and INI files will be supported")

    def decrypt_config(self, key_path, data_path):
        # Read the encryption key from the file
        with open(key_path, 'rb') as f:
            key = f.read()
        # Initialize the decryption object with the key
        cipher = Fernet(key)
        # Read the encrypted data from the file
        if self.file_type == 'json':
            with open(data_path, 'r') as f:
                encrypted_data = json.load(f)
            # Decrypt each value in the dictionary
            decrypted_data = {}
            if isinstance(encrypted_data.get(self.enviroment_key), dict) and self.enviroment_key != None:
                for key, value in encrypted_data.get(self.enviroment_key).items():
                    decrypted_data[key] = cipher.decrypt(
                        value.encode()).decode()
            else:
                for key, value in encrypted_data.items():
                    decrypted_data[key] = cipher.decrypt(
                        value.encode()).decode()
        else:
            raise ValueError(
                "Invalid config file format. In the future, YAML, TOML, and INI files will be supported")
        # Return the decrypted data
        return decrypted_data

class SqlOperations:
    def __init__(self, DRIVER, SERVER_NAME, DATABASE_NAME, USERNAME, PASSWORD):
        # Create a connection string
        self.connection_string = f"""DRIVER={{{DRIVER}}};
                            SERVER={SERVER_NAME};
                            DATABASE={DATABASE_NAME};
                            UID={USERNAME};
                            PWD={PASSWORD}"""

    def create_connection(self):
        # Connect to the database
        self.connection = pyodbc.connect(self.connection_string)

    def close_connection(self):
        # Close the database connection
        self.connection.close()

    def DB_Connection(self, connection_string):
        connection = None
        try:
            connection = pyodbc.connect(connection_string)
        except pyodbc.DatabaseError as e:
            print('Database Error:', e)
        except pyodbc.Error as e:
            print('Connection Error:', e)
        finally:
            if connection:
                return connection
            else:
                print("UNABLE TO ESTABLISH CONNECTION")

    def Execute_SQL(self, connection_string=None, sql_statement=None, commit=False):
        try:
            conn = self.DB_Connection(connection_string)
            cursor = conn.cursor()
            cursor.execute(sql_statement)
            if commit:
                conn.commit()
            else:
                conn.rollback()
            print("statement executed without error")
        except Exception as e:
            print(f"SCRIPT FAILURE HAS OCCURED ->\n{e}\n")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def read_data(self, sql_statement, commit=False):
        # Create a connection and cursor
        self.create_connection()
        cursor = self.connection.cursor()
        try:
            # Start a transaction
            cursor.execute("BEGIN TRANSACTION;")
            # Select data using the provided SQL statement
            cursor.execute(sql_statement)
            data = cursor.fetchall()
            # Get the column names from the cursor description
            column_names = [column[0] for column in cursor.description]
            # Commit or rollback the transaction based on the commit parameter
            if commit:
                cursor.execute("COMMIT TRANSACTION;")
            else:
                cursor.execute("ROLLBACK TRANSACTION;")
            return pd.DataFrame([list(record) for record in data], columns=column_names)
        except Exception as e:
            # Rollback the transaction if an error occurs
            cursor.execute("ROLLBACK TRANSACTION;")
            raise e
        finally:
            # Close the cursor and database connection
            cursor.close()
            self.close_connection()

    def insert_data(self, table_name, data, commit=False):
        self.create_connection()
        cursor = self.connection.cursor()
        # Start a transaction
        cursor.execute("BEGIN TRANSACTION;")
        try:
            # Get the column names for the table
            cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
            columns = [f"[{column[0]}]" for column in cursor.description]
            column_list = ', '.join(columns)
            outerstring = []
            for record in data.values.tolist():
                innerstring = ""
                for item in record:
                    if isinstance(item, str):
                        innerstring += f"'{item}',"
                    else:
                        innerstring += f"{item},"
                outerstring.append(f"({innerstring[:-1]})")
            value_list = ",".join(outerstring)
            query = f"INSERT INTO {table_name} ({column_list}) VALUES {value_list}"
            print(query)
            self.Execute_SQL(
                connection_string=self.connection_string, sql_statement=query, commit=commit)
        except Exception as e:
            print(e)
            # Rollback the transaction if an error occurs
            cursor.execute("ROLLBACK TRANSACTION;")
            raise
        finally:
            # Close the database connection
            self.close_connection()

    def update_data(self, table_name, data, unique_code_field, commit=False):
        if type(data) != list:
            data = data.values.tolist()
        self.create_connection()
        cursor = self.connection.cursor()
        # Start a transaction
        cursor.execute("BEGIN TRANSACTION;")
        try:
            # Get the column names for the table
            cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
            columns = [f"{column[0]}" for column in cursor.description]
            column_list = ', '.join(columns)
            value_str = ', '.join([str(tuple(val)) for val in data])
            cols = [item for item in columns if item.lower() !=
                    unique_code_field.lower()]
            set_clause = ",".join([f"[{x}] = temp.[{x}]" for x in cols])
            back_up_tbl = table_name.lower().replace("bak.", "").replace("dbo.", "")
            # Build the SQL statement using the input arguments
            update_sql_statement = f"""
        if object_id('tempdb..#{back_up_tbl}') is not null
        begin
            drop table [#{back_up_tbl}];
        end;
        select top 0
            {column_list}
        into #{back_up_tbl}
        from {table_name};
        insert into #{back_up_tbl}
        values
        {value_str};
        update {table_name}
        set {set_clause}
        from #{back_up_tbl} temp
            left join {table_name} prod
                on temp.{unique_code_field} = prod.{unique_code_field};"""
            print(update_sql_statement)
            self.Execute_SQL(connection_string=self.connection_string,
                             sql_statement=update_sql_statement,
                             commit=commit)
        except Exception as e:
            print(e)
            # Rollback the transaction if an error occurs
            cursor.execute("ROLLBACK TRANSACTION;")
            raise
        finally:
            # Close the database connection
            self.close_connection()

    def execute_sql_from_file(self, sql_file_path, params=None, parameterized=False, commit=False):
        with open(sql_file_path, 'r') as f:
            sql_query = f.read()
        if parameterized and params is not None:
            sql_query = self._replace_params(sql_query, params)
        self.Execute_SQL(sql_statement=sql_query,
                         connection_string=self.connection_string,
                         commit=commit)

    def _replace_params(self, sql_query, params):
        for key, value in params.items():
            sql_query = sql_query.replace(f':{key}', str(value))
        return sql_query

class FileReader:
    @staticmethod
    def READ(file_path, excel_tab="Sheet1", sep="\t"):
        file_extension = file_path.split('.')[-1].lower()
        if file_extension == 'csv':
            df = pd.read_csv(file_path)
        elif file_extension == 'txt':
            df = pd.read_csv(file_path, sep=sep)
        elif file_extension in ['xls', 'xlsx']:
            if excel_tab is None:
                df = pd.read_excel(file_path)
            else:
                df = pd.read_excel(file_path, sheet_name=excel_tab)
        elif file_extension == 'json':
            df = pd.read_json(file_path)
        elif file_extension == 'xml':
            df = pd.read_xml(file_path)
        elif file_extension == 'dbf':
            df = pd.DataFrame([_ for _ in DBF(file_path)])
        else:
            raise ValueError(
                f'Unsupported file type: {file_extension}. Ask Immanuel to add this if you really want it.')
        return df

class EmailSender:
    @staticmethod
    def send_mail(send_from='Landadmin@pakenergy.com', send_to=[],
                  subject="Subject Place Holder", message="",
                  files=[], server="smtp.office365.com",
                  port=587, username=None,
                  password=None, use_tls=True,
                  Bcc=None, Cc=None):
        if not send_to or type(send_to) not in (str,list):
            raise ValueError("Need valid recipients")
        if not username or type(username) != str:
            raise ValueError("Need valid username")
        if not password or type(password) != str:
            raise ValueError("Need valid password")
        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = COMMASPACE.join(send_to) if type(
            send_to) == list else send_to
        msg['Cc'] = COMMASPACE.join(Cc) if type(Cc) == list else Cc
        msg['Bcc'] = COMMASPACE.join(Bcc) if type(Bcc) == list else Bcc
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject
        msg.attach(MIMEText(message))
        for path in files:
            part = MIMEBase('application', "octet-stream")
            with open(path, 'rb') as file:
                part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename={}'.format(Path(path).name))
            msg.attach(part)
        smtp = smtplib.SMTP(server, port)
        if use_tls:
            smtp.starttls()
        smtp.login(username, password)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.quit()

class Scheduler:
    def __init__(self, function_to_execute: Callable[..., Any]):
        self.function_to_execute = function_to_execute
    def schedule_weekly(self, hour: int, day: int, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        if now.weekday() == day and now.hour == hour:
            self.function_to_execute(*args, **kwargs)
            return "Function scheduled and triggered successfully."
        else:
            return "Function not triggered based on the schedule and schedule_params."
        
    def schedule_daily(self, hour: int, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        if now.hour == hour:
            self.function_to_execute(*args, **kwargs)
            return "Function scheduled and triggered successfully."
        else:
            return "Function not triggered based on the schedule and schedule_params."
        
    def schedule_weekdays(self, hour: int, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        if now.weekday() < 5 and now.hour == hour:
            self.function_to_execute(*args, **kwargs)
            return "Function scheduled and triggered successfully."
        else:
            return "Function not triggered based on the schedule and schedule_params."
        
    def schedule_biweekly_even(self, hour: int, days: List[int], *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        if now.weekday() in days and now.hour == hour and (now.isocalendar()[1] % 2) == 0:
            self.function_to_execute(*args, **kwargs)
            return "Function scheduled and triggered successfully."
        else:
            return "Function not triggered based on the schedule and schedule_params."
        
    def schedule_biweekly_odd(self, hour: int, days: List[int], *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        if now.weekday() in days and now.hour == hour and (now.isocalendar()[1] % 2) != 0:
            self.function_to_execute(*args, **kwargs)
            return "Function scheduled and triggered successfully."
        else:
            return "Function not triggered based on the schedule and schedule_params."
        
    def schedule_first_and_third_week(self, hour: int, days: List[int], *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        week_number = (now.day - 1) // 7 + 1
        if now.weekday() in days and now.hour == hour and week_number in [1, 3]:
            self.function_to_execute(*args, **kwargs)
            return "Function scheduled and triggered successfully."
        else:
            return "Function not triggered based on the schedule and schedule_params."
        
    def schedule_monthly(self, hour: int, day: int, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        if now.day == day and now.hour == hour:
            self.function_to_execute(*args, **kwargs)
            return "Function scheduled and triggered successfully."
        else:
            return "Function not triggered based on the schedule and schedule_params."
        
    def schedule_first_business_day(self, hour: int, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        if now.weekday() < 5:
            if now.day == 1:
                self.function_to_execute(*args, **kwargs)
                next_day = now + datetime.timedelta(days=1)
                while next_day.weekday() >= 5:
                    next_day += datetime.timedelta(days=1)
                return "Function scheduled and triggered successfully."
        else:
            next_day = now + datetime.timedelta(days=1)
            while next_day.weekday() >= 5:
                next_day += datetime.timedelta(days=1)
            return "Function not triggered based on the schedule and schedule_params."
    def schedule_second_business_day(self, hour: int, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        if now.weekday() < 5:
            if now.day == 2:
                self.function_to_execute(*args, **kwargs)
                next_day = now + datetime.timedelta(days=1)
                while next_day.weekday() >= 5:
                    next_day += datetime.timedelta(days=1)
                return "Function scheduled and triggered successfully."
        else:
            next_day = now + datetime.timedelta(days=1)
            while next_day.weekday() >= 5:
                next_day += datetime.timedelta(days=1)
            return "Function not triggered based on the schedule and schedule_params."
    def schedule_first_and_fifteenth(self, hour: int, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        now = datetime.datetime.now()
        if now.hour == hour and (now.day ==  1 or now.day == 15):
            self.function_to_execute(*args, **kwargs)
            return "Function scheduled and triggered successfully."
        else:
            return "Function not triggered based on the schedule and schedule_params."

class ParallelRunner:
    @staticmethod
    def run_parallel(func_list, pool_size=10):
        def worker(func_args):
            func, args = func_args
            func(*args)
        pool = Pool(pool_size)
        for func, args in func_list:
            pool.apply_async(worker, ((func, args),))
        pool.close()
        pool.join()

class ScheduledFunctionExecutor:
    def __init__(self, function, schedule_method, schedule_params, arguments, source):
        self.scheduler = Scheduler(function)
        self.function_name = function.__name__
        self.schedule_method = schedule_method
        self.schedule_params = schedule_params
        self.arguments = arguments
        self.source = source

