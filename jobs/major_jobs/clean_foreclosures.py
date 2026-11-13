from gsmls.Salesweb import Salesweb
from gsmls.utility_func import attach_pycharm_debugger

if __name__ == '__main__':

    attach_pycharm_debugger()
    my_session = Salesweb()
    my_session.main_clean()
