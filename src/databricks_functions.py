

def get_username():
  """This function will get the current user's username. It does this by trying to write a table 
  to a database (it trys all of them) and then later deleting that table."""
  
  import uuid
  # first set some spark config - we want to be able 
  # spark.conf.set('spark.databricks.userInfoFunctions.enabled','true') # might be useful in newer versions of databases
  # username = spark.sql("SELECT current_user()").collect()[0][0] # not in our version of databricks
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true") # this allows us to overwrite damaged tables
  
  try:
    old_uuid_tbl_name = spark.table('uuid_tbl_name_temp').collect()[0][0]
    old_uuid_tbl_database = spark.table('uuid_tbl_database_temp').collect()[0][0]
    spark.sql(f"drop table if exists {old_uuid_tbl_database}.{old_uuid_tbl_name}") # drop the old table if anything failed from before, or if the function was stopped prematurely.
  except:
    pass
  
  uuid_tbl_name = ("temp_" + str(uuid.uuid4())).replace("-","_") # create a uuid for the table name, to avoid collisions if a few people do this at the same time.
  spark.sql(f"create or replace temp view uuid_tbl_name_temp AS SELECT '{uuid_tbl_name}' as temp_table_name")
  try:
    username = spark.table('username_temp').collect()[0][0] # first try and pull the username from the username_temp table if it exists... this saves time
  except: # if username_temp doesn't exist we need to get the username
    # ToDo: rather than cycling over all the databases, perhaps could use SHOW GRANT to find a database where you do have write access.... although that might involve knowning your username!
    for database in spark.sql("SHOW DATABASES").toPandas()['databaseName']: # we don't know which databases are writeable, so we can try them all
      temp_table_path = f"{database}.{uuid_tbl_name}"
      try:  
        spark.range(1).write.format('delta').mode('overwrite').saveAsTable(temp_table_path) # make a table
        
        spark.sql(f"create or replace temp view uuid_tbl_database_temp AS SELECT '{database}' as temp_table_database")
        
        username = spark.sql(f"DESCRIBE History {temp_table_path}").select('userName').collect()[0][0] # using the history we can get the username of the person who made it
        spark.sql(f"create or replace temp view username_temp as SELECT '{username}' as username") # save this to a temp table for easy access later
        break # once we have made a table we want to exist the loop.
      except:
        spark.sql(f"drop table if exists {temp_table_path}") # drop the table if anything fails
    
    # in anycase - drop the table once finished
    spark.sql(f"drop table if exists {temp_table_path}")
  return username
