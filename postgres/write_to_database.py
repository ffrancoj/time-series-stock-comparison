def write_to_database(df, url, properties, mode, table):
    '''
    Takes a spark dataframe df with standard parameters
    
    url = "jdbc:postgresql://ec2-##-###-###-###.compute-1.amazonaws.com:5432/DATABASE_NAME"
    properties = {"user": USER_NAME, "password": USER_PASSWORD, "driver": "org.postgresql.Driver"}
    mode = "overwrite"
    table = TABLE_NAME

    and writes the dataframe as a table in postgres.
    '''
    
    df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
