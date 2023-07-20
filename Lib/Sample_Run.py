import datetime
import os
import sys
import logging
from io import StringIO
from getpass import getpass
from Framework import AutoApi
from Framework.auth_manager import AuthenticationManager

# import functions
from module_1 import __main__ as Func1
from module_2 import __main__ as Func2
from module_3 import Func3

def setup_logger(log_output):
    # Create a logger
    logger = logging.getLogger("ExecutionLog")
    logger.setLevel(logging.INFO)
    # Create a stream handler to write the log messages to a StringIO object
    stream_handler = logging.StreamHandler(log_output)
    stream_handler.setLevel(logging.INFO)
    # Create a file handler to save the log messages to a file
    file_handler = logging.FileHandler("execution.log")
    file_handler.setLevel(logging.INFO)
    # Create a formatter for the log messages
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    # Add the stream and file handlers to the logger
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger

log_output = StringIO()
logger = setup_logger(log_output)
auth_manager = AuthenticationManager(r"super_secret.properties")
username = input("Enter your username: ")
password = getpass("Enter your password: ")

if auth_manager.authenticate(username, password):
    go = False
    RUN_NUM = datetime.datetime.now().hour if go else 8
    scheduled_functions = [AutoApi.ScheduledFunctionExecutor(function=Func1.main,
                                  source="Func1",
                                  schedule_method='schedule_daily',
                                  schedule_params={'hour': RUN_NUM},
                                  arguments={'test_run': False}),
        AutoApi.ScheduledFunctionExecutor(function=Func3.main,
                                  source="Func3",
                                  schedule_method='schedule_daily',
                                  schedule_params={'hour': RUN_NUM},
                                  arguments={'test_run': False}),
        AutoApi.ScheduledFunctionExecutor(function=Func2.main,
                                  source="Func2",
                                  schedule_method='schedule_daily',
                                  schedule_params={'hour': RUN_NUM},
                                  arguments={'test_run': False})]
    error_occurred = False
    for scheduled_function in scheduled_functions:
        try:
            logger.info(
                f"Executing package/function:\t{scheduled_function.source}; {scheduled_function.function_name}; {scheduled_function.schedule_method}; {scheduled_function.schedule_params}")
            schedule_method = getattr(scheduled_function.scheduler, scheduled_function.schedule_method)
            schedule_method(**scheduled_function.schedule_params, **scheduled_function.arguments)
        except Exception as e:
            logger.error(
                f"Error executing function: {scheduled_function.source} -> {scheduled_function.function_name}\t {e}")
            error_occurred = True
    if not error_occurred:
        logger.info("No errors occurred during execution.")
else:
    print("Authentication failed. Access denied.")
    sys.exit(1)

log_text = log_output.getvalue()
for handler in logger.handlers:
    handler.close()
    logger.removeHandler(handler)
log_output.close()
print(log_text)

__start__ = 8
__stop__ = 23
# Check if the current datetime is between 8 am and 23 pm
current_hour = datetime.datetime.now().hour
if current_hour >= __start__ and current_hour < __stop__:
    AutoApi.EmailSender.send_mail(send_from='guy@company.com',
                                     send_to=['someguy@somecompany.com'],
                                     subject="Scheduled Automation Process Log",
                                     message=log_text,
                                     files=[], server="smtp.office365.com",
                                     port=587, username='admin@company.com',
                                     password='secretpassword', use_tls=True,
                                     Bcc=None, Cc=None)
else:
    logger.info("Current time is not within the specified range for sending the email.")