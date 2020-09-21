# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
changes

# COMMAND ----------

# First - Updated records (update)
# Second - Same as Existing Records 
# Third - Exist in Target but not in incoming records (delte)
# Fourth - Inactive records of current active records (History)
# Fifth - New records (insert)

sourceRdd = sc.parallelize([
  Row(None,None,"2006-01-01","True","M","4000.60"),
  Row("Michael",None,"2004-01-01","True","F","3300.80"),
  Row("Robert","37","1992-01-01","False","M","5000.50")])

targetRdd = sc.parallelize([
  Row("James",34,"2006-01-01",True,"M",2500.60,12,True,"2010-01-01","2099-01-01"),
  Row("James",34,"2006-01-01",True,"M",3500.60,11,False,"2009-01-01","2010-01-01"),
  Row("Michael",33,"2004-01-01",True,"F",3300.80,13,True,"2010-01-01","2099-01-01"),
  Row("Madan",34,"2004-01-01",True,"F",3200.80,12,True,"2010-01-01","2099-01-01")
  ]
  )

# +---------+---+------------+-----------+------+------+-------+--------+-----------------+---------------+
# |firstName|age|jobStartDate|isGraduated|gender|salary|batchId|isActive|record_Start_Date|record_End_Date|
# +---------+---+------------+-----------+------+------+-------+--------+-----------------+---------------+
# |    James| 34|  2006-01-01|       true|     M|3000.6|      1|    true|       2018-01-01|   Still Active|
# |    James| 34|  2006-01-01|       true|     M|2500.6|     12|   false|       2010-01-01|     2018-01-01|
# |    James| 34|  2006-01-01|       true|     M|3500.6|     11|   false|       2009-01-01|     2018-01-01|
# |    Madan| 34|  2004-01-01|       true|     F|3200.8|     12|   false|       2010-01-01|     2018-01-01|
# |  Michael| 33|  2004-01-01|       true|     F|3300.8|     13|    true|       2010-01-01|     2018-01-01|
# |   Robert| 37|  1992-01-01|      false|     M|5000.5|      1|    true|       2018-01-01|   Still Active|
# +---------+---+------------+-----------+------+------+-------+--------+-----------------+---------------+

# Row("James",34,"2006-01-01",True,"M",2500.60,12,True,"2010-01-01","2099-01-01"),
# Row("James",34,"2006-01-01",True,"M",3500.60,11,False,"2009-01-01","2010-01-01"),
# Row("Michael",33,"2004-01-01",True,"F",3300.80,13,True,"2010-01-01","2099-01-01"),
# Row("Madan",34,"2004-01-01",True,"F",3200.80,12,True,"2010-01-01","2099-01-01")  

# Row("James",34,"2006-01-01",True,"M",3500.60,11),
# Row("Michael",33,"2004-01-01",True,"F",3300.80,12),
# Row("Madan",34,"2004-01-01",True,"F",3200.80,12)  
  
  
source_Schema  =  StructType([
    StructField("firstName",StringType(),True),
    StructField("age",StringType(),True),
    StructField("jobStartDate",StringType(),True),
    StructField("isGraduated", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True)
  ])
target_Schema  =  StructType([
    StructField("firstName",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("jobStartDate",StringType(),True),
    StructField("isGraduated", BooleanType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("batchId", IntegerType(), True),
    StructField("isActive", BooleanType(), True),
    StructField("record_Start_Date", StringType(), True),
    StructField("record_End_Date", StringType(), True)
  ])


# COMMAND ----------

# %sql
# MERGE INTO target
# USING (
#    -- These rows will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
#   SELECT source.customerId as mergeKey, source.*
#   FROM source
  
#   UNION ALL
  
#   -- These rows will INSERT new addresses of existing customers 
#   -- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
#   SELECT NULL as mergeKey, source.*
#   FROM source JOIN target
#   ON source.firstName = target.firstName 
#   WHERE target.isGraduated = true AND source.salary <> target.salary 
  
# ) staged_updates
# ON target.firstName = mergeKey
# WHEN MATCHED AND source.isGraduated = true AND target.salary <> staged_updates.salary THEN  
#   UPDATE SET current = false, endDate = staged_updates.effectiveDate    -- Set current to false and endDate to source's effective date.
# WHEN NOT MATCHED THEN 
#   INSERT(firstName, age, isGraduated,salary, effectivedate, enddate) 
#   VALUES(staged_updates.firstName, staged_updates.age,3000.80, true, staged_updates.effectiveDate, null) -- Set current to true along with the new address and its effective date.