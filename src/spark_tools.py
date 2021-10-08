import os
import pyspark.sql


def python_location():
    """work out the location of the python interpretter - this is needed for Pyspark to initialise"""
    import subprocess
    import pathlib
    with subprocess.Popen("where python", shell=True, stdout=subprocess.PIPE) as subprocess_return:
        python_exe_paths = subprocess_return.stdout.read().decode('utf-8').split('\r\n')
        env_path = pathlib.Path([x for x in python_exe_paths if '\\envs\\' in x][0])
    return str(env_path)


def initialise_spark():
    """This function creates a spark session if one doesn't already exist (i.e. within databricks this will do nothing)"""
    if 'spark' not in locals():
        os.environ["PYSPARK_PYTHON"] = python_location()
        spark = pyspark.sql.SparkSession.builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    return spark