from datetime import timedelta
from datetime import datetime as dt
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *

import settings as st
import stats_functions as sf


sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("data_getter").getOrCreate()

# Field structure on the stock price data
field_data = [StructField("Date", DateType(), True), \
              StructField("Open", DoubleType(), True), \
              StructField("High", DoubleType(), True), \
              StructField("Low", DoubleType(), True), \
              StructField("Close", DoubleType(), True), \
              StructField("Volume", DoubleType(), True), \
              StructField("OpenInt", DoubleType(), True)]

schema_data= StructType(field_data)

path_start = st.path_start
final_date = st.final_date

# Methods to retrieve and write data

def get_data(stock_name, path_start):
    """
    Retrieves data from path to S3 bucket and returns columns "Date" and "Close" of stock data
    
    :type stock_name : str                 stock ticker name 
    :type path_start : str                 partial path to S3 bucket
    :rtype           : Spark DataFrame     DataFrame from stock price data
    .
    """
    path_name = path_start+stock_name+".us.txt"
    df_data = spark.read.option("header", "false").option("inferSchema", "false").schema(schema_data).csv(path_name)
    df_data = df_data.na.drop()
    df_final = df_data.select("Date", "Close")
    return df_final



def get_data_pers(stock_name, final_date):
    """
    Returns a persistent DataFrame truncated truncated from final_date
    
    :type stock_name: str                  stock ticker name
    :type final_date: datetime             truncating from final_date in format yyyy-MM-dd
    :rtype          : Spark DataFrame      DataFrame truncated by time
    .
    """
    
    df = get_data(stock_name)
    df= df.filter(df.Date > final_date).persist()
    return df


def table_to_df(table):
    """
    Returns a DataFrame from list of rows
    
    :type table: list[list]               list of rows 
    :rtype     : Spark DataFrame          DataFrame from list of rows
    .
    """
    
    rdd_table = sc.parallelize(table)
    struct_table = rdd_table.map(lambda x: Row(company_1=str(x[0]), company_2=str(x[1]), \
                                               window_days=int(x[2]), num_weeks=int(x[3]), max_corrs=float(x[4][0]), date_max=x[4][1]))
    df_from_table = sqlContext.createDataFrame(struct_table)
    return df_from_table


def column_as_list(df, column_name):
    """
    Returns a list from a DataFrame column
    
    :type  df         : Spark DataFrame   DataFrame of stock prices
    :type  column_name: str               name of column attribute
    :rtype            : list[float]      column as list
    .
    """
    
    l=df.select(column_name).rdd.flatMap(lambda x: x).collect()
    return l


def data_between_dates(df, start_date, end_date):
    """
    Returns a truncated DataFrame by time
    
    :type df        : Spark DataFrame   DataFrame of stock prices
    :type start_date: datetime          from start_date in format yyyy-MM-dd
    :type end_date  : datetime          to end_date in format yyyy-MM-dd
    :rtype          : Spark DataFrame   DataFrame truncated by time
    .
    """
    
    df1 = df.filter(df.Date>start_date).filter(df.Date<=end_date)
    return df1

 
def correlation_columns(df1, df2, start_date, end_date, num_weeks):
    """
    Returns the correlation of the two columns between the two time periods going back as num_weeks
    
    :type df1       : Spark DataFrame   DataFrame of stock prices
    :type df1       : Spark DataFrame   DataFrame of stock prices
    :type start_date: datetime          from start_date in format yyyy-MM-dd
    :type end_date  : datetime          to end_date in in format yyyy-MM-dd
    :type num_weeks : int               number of weeks into the past
                                        assumed larger that the size of the window from start_date to end_date
    :rtype          : float             correlation number
    .
    """
    
    list1 = column_as_list(data_between_dates(df1, start_date, end_date), "Close")
    list2 = column_as_list(data_between_dates(df2, start_date, end_date), "Close")
    return correlation_lists(list1, list2, num_weeks)


def correlation_lists(list1, list2, num_weeks):
    """
    Returns correlation of two lists of numbers

    :type list1    : list[float]      slice of list of stock prices
    :type list2    : list[float]      list of stock prices 
    :type num_weeks: int              number of times the slice moves from end of list2 and back
    :rtype         : list[float]      list of correlations of list1 with slices of list2 
    .
    """
    
    correlation_list=[]
    l=len(list1)
    m=len(list2)
    for i in range(0, min(100, (m-l)//5, num)):
        list2_i = list2[m-5*i-l:m-5*i]
        correlation_list.append(sf.correl(list1,list2_i))
    return correlation_list


def correlation_date_value(df1, df2, start_date, end_date, num_of_weeks):
    """
    Returns a list with a the maximum correlation in the period start_date to end_date, going back
    
    :type df1         : Spark DataFrame         DataFrame of stock prices
    :type df2         : Spark DataFrame         DataFrame of stock prices
    :type start_date  : datetime                from start_date
    :type end_date    : datetime                to end_date
    :type num_of_weeks: int                     number of weeks into the past
    :rtype            : list[float, datetime]   pair with maximum correlation and date of ocurrence
    .
    """
    list_corr = correlation_columns(df1, df2, start_date, end_date, num_of_weeks)    
    m = max(lista)
    i = lista.index(m)
    #if it's zero, then the max ocurrs when end_date, i weeks before,
    date_when = start_date-timedelta(days=7*i)
    return [m, date_when]


def correlation_date_default(date_default):
    return [1, date_default]


def table_of_corrs(table, stock1, stock2, df1, df2, list_weeks, list_windows, date_end, date_default):
    """
    Returns a list of lists (a table) consisting of the stock correlations running over all the parameters
    
    :type table       : list              List of correlations 
    :type stock1      : str               stock ticker name
    :type stock2      : str               stock ticker name
    :type df1         : Spark DataFrame   DataFrame of stock prices
    :type df2         : Spark DataFrame   DataFrame of stock prices
    :type list_weeks  : list[int]         List of weeks               
    :type list_windows: list[int]         List of window sizes in days 
    :type date_end    : datetime          to end_date
    :type date_default: datetime          from a fixed default date 
    :rtype:           : list[list]        list of lists of correlations 
    .
    """
    for window in list_windows:
        date_start = date_end-timedelta(days=window)
        for week in list_weeks:
            lista_both=[]
            if stock1!=stock2:
                lista_both = [stock1,stock2,window,week,correlation_date_value(df1,df2,date_start,date_end,week)]
            else:
                lista_both = [stock1,stock2,window,week,correlation_date_default(date_default)]
            table.append(lista_both)



def table_of_all_corrs(dict_dfs, list_stocks, list_weeks, list_windows, date_end, date_default):
     
    """Returns the union of all the tables from table_of_corrs for all stocks in list_stocks."""

    table_all = []
    for stock1 in list_stocks:
        df1=dict_dfs[stock1]
        for stock2 in list_stocks:
            df2=dict_dfs[stock2]
            table_of_corrs(table_all, stock1, stock2, df1, df2, list_weeks, list_windows, date_end, date_default)
    df_table = table_to_df(table_all)
    return df_table




