from gsmls.Salesweb import Salesweb


if __name__ == '__main__':

    my_session = Salesweb()
    final_results = False
    
    while final_results is False:
        final_results = my_session.main()

        if final_results is False:
            print(" ==== RETRYING ==== ")
        else:
            print(" ==== FORECLOSURE SCRAPING WAS A SUCCESS ==== ")
            break

