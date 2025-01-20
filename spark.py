from pyspark.sql import SparkSession

def spark_things():
    PYTHON_PATH = r'.venv\Scripts\python.exe'
    # os.environ['PYSPARK_DRIVER_PYTHON'] = PYTHON_PATH
    # os.environ['%HADOOP_HOME%'] = r'C:\Users\JV\Downloads\spark-3.5.4-bin-hadoop3\spark-3.5.4-bin-hadoop3'

    # spark = SparkSession.builder.appName("demo").getOrCreate()
    spark = SparkSession.builder \
        .appName("demo") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.pyspark.python", PYTHON_PATH) \
        .config("spark.pyspark.driver.python", PYTHON_PATH) \
        .getOrCreate()

    print(dir(spark))

    df = spark.createDataFrame([(1, 'a'),
    (2, 'b')])
    print(df.head())
