import correlation_tables as ct
import write_to_database as wd
import settings as st


def dict_of_stocks(list_stocks):
    dict_dfs = {}
    for stock in list_stocks:
    dict_dfs[stock]=ct.get_data_pers(stock)
    return dict_dfs
    

def main_method(list_stocks, list_weeks, list_windows, date_end, date_default, url, properties, mode, table):
    dict_dfs = dict_of_stocks(list_stocks)
    table_return = ct.table_of_all_corrs(dict_dfs, list_stocks, list_weeks, list_windows, date_end, date_default)
    wd.write_to_database(table_return, url, properties, mode, table)


if "__name__" == "__main__":
    params = st.parameters_correlation
    info = st.info_database
    main_method(params[0],params[1],params[2],params[3],params[4],info[0], info[1], info[2], info[3])



    
    
    
    
    

    
