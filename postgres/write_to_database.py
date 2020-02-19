def write_to_database(df, url, properties, mode, table):
    df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
