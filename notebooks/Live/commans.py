# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ast import literal_eval
from delta.tables import *
from delta import *

# COMMAND ----------

etl_Columns = ["batchId","isActive","record_Start_Date","record_End_Date"]

# COMMAND ----------

#Config File Related Functions

# Read the config file to get the related values of MetaData, SCDType, DQC and convert values to MAP.
def getColumnsMap(colsType, rdd):
  return rdd.filter(lambda x : x[0] == colsType).map(lambda x : (x[1],x[2])).collectAsMap() 

def getAttributeMap(configFile):
  configRDD = sc.textFile(configFile)
  #Read the config file split the lines
  configRDD.map(lambda lines : lines.split(':='))
  myRDD =  configRDD.map(lambda lines : lines.split(':='))
  #Get the attributes and create map for further processing
  metaDataMap = getColumnsMap("MetaData",myRDD)
  scdTypeMap = getColumnsMap("scdType",myRDD)
  dqcMap     = getColumnsMap("DQC",myRDD)
  arr = literal_eval(dqcMap["missing_attributes"])
  globalDqDict = getdq_dict(*arr)
  return (metaDataMap,scdTypeMap,dqcMap,arr)

# COMMAND ----------

#DQ Related functions

from pyspark.sql.functions import udf,explode
@udf("string")

def getMissingCols(df,*a):
  return df.select("*",missing_cols_udf(*a).alias("Missing_Attributes"))

#Create a global dict to help in substituting the column names in error message
def getdq_dict(*dqCols):
  dict = {}
  for index,item in enumerate(dqCols):
    dict[index] = item
  return dict

def missing_cols_udf(*si):
  missingAttributes = []
  missingAttributes.append("The missing attributes are -> ")
  for index,item in enumerate(si):
    if item is None:
      missingAttributes.append(globalDqDict[index]) 
      
  return ": ".join(i for i in missingAttributes)


# COMMAND ----------

#SCD Type2 Related Helper functions

# Creating expr for type casting the columns to required datatype, ex: (CAST(col1 as integer),CAST(col2 as double))
def ConvertColumnTypes(columnsDict):
  typeCastedArr = []
  for col, dataType in columnsDict.items():
    val = dataType.strip().upper()
    if val != "STRING":
      expr = "CAST(" + col + " AS " + val +")"
      typeCastedArr.append(expr)
    else:
      typeCastedArr.append(col)
  return typeCastedArr

#To get the updated records from the source table
def jointables(sourceTable,target,expr,naturalCols,joinType,combined):
  return sourceTable.alias('a').join(target.alias('b'),naturalCols,joinType).where(expr)

# In order to generate SCD Type2 we need certain expressions
def GenerateScdType2Exressions(naturalCols,scdCols):
  joinClause = compositeKey = notEqualClause = equalClause =''
  
  for col in naturalCols:
    joinClause+= "a." + col + "=" + "b." + col + " AND "
    #To concat the natural columns
    compositeKey += "a."+col+","
  for col in scdCols:
    notEqualClause += "a." + col + "<>" + "b." +col+ " AND "   
    equalClause += "a." + col + "=" + "b." +col+ " AND "
    
  whereClause = " b.isActive = True"  
  deleteExpr = "a.isActive = True"
  updateExpr = joinClause + notEqualClause + whereClause
  similarExpr = joinClause + equalClause + whereClause
 
  
  expr2 = "concat("+compositeKey+") as MergeKey"
  compositeKeyExpr = ''.join(expr2.rsplit(',', 1))
  
  return (updateExpr,similarExpr,deleteExpr,compositeKeyExpr)

def scdexpr(columns,primaryKey,batch_id,start_time):
  dict={}
  compositeKey = ''
  for col in primaryKey:
    compositeKey += "t."+col+","
  expr2 = "concat("+compositeKey+")"
  compositeKeyExpr = ''.join(expr2.rsplit(',', 1))  
  condition = compositeKeyExpr + "=s.MergeKey"
  for col in columns:
    dict[col] = "s." + col
  dict["batchId"] = batch_id
  dict["record_Start_Date"]= start_time
  dict["record_End_Date"] = '2099-01-01'
  dict["isActive"] = "True"
  setTargetColumns={
    "record_End_Date": start_time,
    "isActive":"False"
    }
  return (dict,condition,setTargetColumns)

# COMMAND ----------

#SCD Type2 Related Functions...

def scdType2(source,finalTargetDF,metaDataMap,naturalColumns,scdColumns,fileName,batch_id,startDate):
  
  (updateExpr,similarExpr,deleteExpr,comExpr) = GenerateScdType2Exressions(naturalColumns,scdColumns)
  #Change the source datatypes to required datatype
  sourceSelExpr  = ConvertColumnTypes(metaDataMap)
  #Now apply sourceSelExpr on source dataframe to get the final source with typecasted dataframe.
  finalSourceDF  = source.selectExpr(sourceSelExpr)
  #Get the similar records which present in source and target
  similarRecords = jointables(finalSourceDF,finalTargetDF,similarExpr,naturalColumns,'inner',comExpr).select("a.*")
  # Remove the records which are presnt in target and active and similar or duplicate in source file.
  deltaRecords   = finalSourceDF.subtract(similarRecords)
  #Get the records which are updated and present in source file
  updatedRecords = jointables(deltaRecords,finalTargetDF,updateExpr,naturalColumns,'inner',comExpr).selectExpr(comExpr, "a.*")
  #Get the records which are present in target file and doesnt exist in source, which are considered as deleted from source
#   deltedRecords  = jointables(finalTargetDF,finalSourceDF,deleteExpr,naturalColumns,'left_anti',comExpr).selectExpr(comExpr, "a.*").drop(*etl_Columns)
  # Records in the source file except similar records appended with null as merge key for scd type2
  incRecords     = deltaRecords.alias("a").selectExpr("null as MergeKey", "a.*")
  # updated and delted record set needs to set its counter part in target as InActive Rest of the records needs to be inserted.
  sourceRecords  = incRecords.union(updatedRecords).union(deltedRecords)
  
  #Creeation of Target table as Delta Table 
  deltaPath = "/mnt/delta/" + fileName
  finalTargetDF.write.format("delta").mode("overwrite").save(deltaPath)
  targetDelta    = DeltaTable.forPath(spark,deltaPath)
  #Here Batch id needs to generated based on the logic of the project
  (columns,condition,setTargetColumns) =  scdexpr(finalTargetDF.columns,naturalColumns,batch_id,startDate)
  
  #Finally Generate the SCD Type2 
  targetDelta.alias("t").merge(sourceRecords.alias("s"), condition).whenMatchedUpdate(
    set = setTargetColumns
    ).whenNotMatchedInsert(
    values = columns
    ).execute()

# COMMAND ----------



# COMMAND ----------

import json
#Generate the schema from the dict of (col,type) 
def generateSchemaFromCols(columnsDict):
  json_type = {"fields": list(), "type": "struct"}
  for col,type in columnsDict.items():
    json_type['fields'].append({
      "metadata":{},
      "name":col,
      "nullable":True,
      "type":type.strip()
      })
  schemajson = json.dumps(json_type)
  return StructType.fromJson(json.loads(schemajson))
  
  # cols = {"firstName":"string",
# "age":"integer",
# "jobStartDate":"string",
# "isGraduated":"boolean",
# "gender":"string",
# "salary":"double"}

# # schema = generateSchemaFromCols(cols)
# expr   = ConvertColumnTypes(cols)
# df.selectExpr("concat(firstName,gender)").show()