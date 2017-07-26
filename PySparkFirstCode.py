#PySparkFirstCode

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, IntegerType,
                              StringType, StructType)

# define our custom structure
spark = SparkSession.builder.appName('Basics').getOrCreate()
data_schema = [StructField('age',IntegerType(),True),
              StructField('name',StringType(),True)]
final_struc = StructType(fields=data_schema)

# assign custom structure to exported file
df = spark.read.json('people.json',schema=final_struc)
df.printSchema()
df.show()
df.columns
df.describe().show()