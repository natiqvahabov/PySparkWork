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

# select -> column, head -> row
df.select('age').show()
type(df.select('age'))
df.head(2)[0]
type(df.head(2)[1])
df.select(['age','name']).show()
df.withColumn('double_age',df['age']*2).show()
df.show()
df.withColumnRenamed('age','new_age').show()

# SQL with Spark
df.createOrReplaceTempView('people')
results = spark.sql('SELECT * FROM people')
results.show()
new_result = spark.sql('select * from people where age>20')
new_result.show()