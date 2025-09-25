import os
import logging
from datetime import datetime
from tqdm import tqdm
import pandas as pd

""" 
______________________________________________________________________________________________________________
                               Use this section to house the decorator functions
______________________________________________________________________________________________________________
"""


class TqdmLoggingHandler(logging.Handler):

    def emit(self, record):
        msg = self.format(record)
        tqdm.write(msg)


def logger_decorator(original_function):
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(original_function.__name__)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        if not logger.handlers:
            # Create the FileHandler() and StreamHandler() loggers
            filepath = 'F:\\Python 2.0\\Projects\\Real Life Projects\\Real Estate Analysis\\Logs'
            log_filepath = os.path.join(filepath, original_function.__name__ + ' ' + str(datetime.today().date()) + '.log')
            f_handler = logging.FileHandler(log_filepath)
            f_handler.setLevel(logging.DEBUG)
            c_handler = TqdmLoggingHandler()
            c_handler.setLevel(logging.INFO)
            # Create formatting for the loggers
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                          datefmt='%d-%b-%y %H:%M:%S')
            # Set the formatter for each handler
            f_handler.setFormatter(formatter)
            c_handler.setFormatter(formatter)
            logger.addHandler(f_handler)
            logger.addHandler(c_handler)

            kwargs['logger'] = logger
            kwargs['f_handler'] = f_handler
            kwargs['c_handler'] = c_handler

        result = original_function(*args, **kwargs)

        if result is None:
            pass
        else:
            return result

    return wrapper


def get_us_pw(website):
    """

    :param website:
    :return:
    """
    # Saves the current directory in a variable in order to switch back to it once the program ends
    previous_wd = os.getcwd()
    os.chdir('F:\\Add\\Folder\\Path')

    db = pd.read_excel('document_name.xlsx', index_col=0)
    username = db.loc[website, 'Username']
    pw = db.loc[website, 'Password']
    base_url = db.loc[website, 'Base URL']

    os.chdir(previous_wd)

    return username, base_url, pw

