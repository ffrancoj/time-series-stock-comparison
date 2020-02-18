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

#Field structure 
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

######Methods to retrieve and write data

#retrieves date and close columns of stock data
def get_data(stock_name, path_start):
    path_name = path_start+stock_name+".us.txt"
    df_data = spark.read.option("header", "false").option("inferSchema", "false").schema(schema_data).csv(path_name)
    df_data = df_data.na.drop()
    df_final = df_data.select("Date", "Close")
    return df_final

#returns dataframe with stock data from final_date on only
def get_data_pers(stock_name, final_date):
    df = get_data(stock_name)
    df= df.filter(df.Date > final_date).persist()
    return df

#writes a list of lists as a dataframe
def table_to_df(table):
    rdd_table = sc.parallelize(table)
    struct_table = rdd_table.map(lambda x: Row(company_1=str(x[0]), company_2=str(x[1]), \
                                               window_days=int(x[2]), num_weeks=int(x[3]), max_corrs=float(x[4][0]), date_max=x[4][1]))
    df_from_table = sqlContext.createDataFrame(struct_table)
    return df_from_table

#writes a dataframe column as a list
def column_as_list(df, column_name):
    l=df.select(column_name).rdd.flatMap(lambda x: x).collect()
    return l

#filter dataframe
def data_between_dates(df, start_date, end_date):
    df1 = df.filter(df.Date>start_date).filter(df.Date<=end_date)
    return df1

#calculates correlation of two columns, shifted by time 
def correlation_columns(df1, df2, start_date, end_date, num_weeks):
    list1 = column_as_list(data_between_dates(df1, start_date, end_date), "Close")
    list2 = column_as_list(data_between_dates(df2, start_date, end_date), "Close")
    return correlation_lists(list1, list2, num_weeks)

#calculates correlation of two vectors
def correlation_lists(list1, list2, num_weeks):
    correlation_list=[]
    l=len(list1)
    m=len(list2)
    for i in range(0, min(100, (m-l)//5, num)):
        list2_i = list2[m-5*i-l:m-5*i]
        correlation_list.append(sf.correl(list1,list2_i))
    return correlation_list

#calculates list of correlations moving window of size end_date - start_date
#moves num_of_weeks back in the past
def correlation_date_value(df1, df2, start_date, end_date, num_of_weeks):
    list_corr = correlation_columns(df1, df2, start_date, end_date, num_of_weeks)    
    m = max(lista)
    i = lista.index(m)
    #if it's zero, then the max ocurrs when end_date, i weeks before,
    date_when = start_date-timedelta(days=7*i)
    return [m, date_when]

#returns the maximum correlation and the week in which this is achieved
def correlation_date_default(date_default):
    return [1, date_default]

#writes a table (list of lists) consisting of stock correlations over all two given stocks
def table_of_corrs(table, stock1, stock2, df1, df2, list_weeks, list_windows, date_end, date_default):
    for window in list_windows:
        date_start = date_end-timedelta(days=window)
        for week in list_weeks:
            lista_both=[]
            if stock1!=stock2:
                lista_both = [stock1,stock2,window,week,correlation_date_value(df1,df2,date_start,date_end,week)]
            else:
                lista_both = [stock1,stock2,window,week,correlation_date_default(date_default)]
            table.append(lista_both)


#writes a dataframe from a table as above using all the stocks in the list_stocks
def table_of_all_corrs(dict_dfs, list_stocks, list_weeks, list_windows, date_end, date_default):
    table_all = []
    for stock1 in list_stocks:
        df1=dict_dfs[stock1]
        for stock2 in list_stocks:
            df2=dict_dfs[stock2]
            table_of_corrs(table_all, stock1, stock2, df1, df2, list_weeks, list_windows, date_end, date_default)
    df_table = table_to_df(table_all)
    return df_table




