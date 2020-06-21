#-------------------------------------------------------
#- Setting environment
#-------------------------------------------------------
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


#-------------------------------------------------------
#- pySpark
#- Creating environment in local machine
#- http://192.168.15.15:4040
#-------------------------------------------------------
import pyspark

number_cores = 1
memory_gb = 1
conf = (
    pyspark.SparkConf()
        .setMaster('local[{}]'.format(number_cores))
        .set('spark.driver.memory', '{}g'.format(memory_gb))
)
sc = pyspark.SparkContext(conf=conf)

# Ignoring all python warnings - for testing
import warnings

warnings.filterwarnings("ignore")


#-------------------------------------------------------
#- 1. Converting file - Colunar
#-------------------------------------------------------
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Init spark session
app_name = 'Learning Spark'
master   = 'local'

spark = SparkSession.builder \
	.master(master) \
	.appName(app_name) \
	.getOrCreate()

input_file  = './data/input/users/load.csv'

# Reading spark dataframe
df = spark.read.csv(input_file, header=True)

# Colunar table - Casting
def data_casting(df, column):
    _collection = df.select(column).rdd.map(lambda x: x).collect()
    _array = [str(row[column]) for row in _collection]
    return [column] + [str(row[column]) for row in _collection]


def df_casting(spark, df):
	# Get data
	_data = [data_casting(df,column) for column in df.columns]
	# Dataframe schema
	_schema = StructType([StructField('key', StringType(), True)] + \
		[StructField('idx %d' % i, StringType(), True) for i in range(df.count())])
	# Convert list to rdd
	_rdd = spark.sparkContext.parallelize(_data)
	_df = spark.createDataFrame(_rdd,_schema)
	return _df


# Save a CSV file from a dataframe
def save_csv(df, output_file):
	df.write.format('csv').mode('overwrite') \
		.option('header',True).option('sep',',') \
		.save(output_file)


new_df = df_casting(spark, df)
# Display
# new_df.show()
save_csv(new_df, './data/output/exp1')


#-------------------------------------------------------
#- 2. Removing duplicates
#-------------------------------------------------------
df = spark.read.csv(input_file, header=True)
clean_df = df.orderBy('update_date', ascending=False). \
	coalesce(1).dropDuplicates(subset = ['id'])
# Display
# clean_df.show()
save_csv(clean_df, './data/output/exp2')


#-------------------------------------------------------
#- 3. Casting types
#-------------------------------------------------------
import json

with open('config/types_mapping.json') as f:
	mapping = json.load(f)

mapping_to = {
	'integer': IntegerType,
	'timestamp': TimestampType
}

df = spark.read.csv(input_file, header=True)
map_df = df

for column, mapping_type in mapping.items():
	map_df = map_df.withColumn(column, map_df[column].cast(mapping_to[mapping_type]()))

# Display
# print(map_df.dtypes)
save_csv(map_df, './data/output/exp3')
