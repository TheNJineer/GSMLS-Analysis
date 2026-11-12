from gsmls.Salesweb import Salesweb
from gsmls.utility_func import attach_pycharm_debugger


if __name__ == '__main__':

    attach_pycharm_debugger()
    my_session = Salesweb()
    final_results = False
    
    while final_results is False:
        final_results = my_session.main()

        if final_results is False:
            print(" ==== RETRYING ==== ")
        else:
            print(" ==== FORECLOSURE SCRAPING WAS A SUCCESS ==== ")
            break

