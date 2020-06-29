from pyspark import SparkContext, SparkConf

from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from pyspark import SparkContext
from hashlib import md5
from pyspark.sql.functions import *
from pyspark.sql import Row

# Create  Spark Context
sc = SparkContext()
spark = SparkSession(sc)

# Create common db connection
def ReadFromDb(tbl_name):
    url = "jdbc:mysql://localhost:3306/test"
    driver = 'com.mysql.jdbc.Driver'
    # dbtable=' entity_counter'   # It will come dynamically
    user = 'root'
    password = 'mysql'
    sourceDataDF = spark.read.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", tbl_name) \
        .option("user", user) \
        .option("password", password).load()
    return sourceDataDF


def WriteToDb(dfname,tbl_name):
    url = "jdbc:mysql://localhost:3306/test"
    driver = 'com.mysql.jdbc.Driver'
    user = 'root'
    password = 'mysql'
    sourceDataDF = dfname.write.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", tbl_name) \
        .option("user", user) \
        .option("password", password).mode('append').save()
    return sourceDataDF


if __name__ == "__main__":
   # Retrive Source Table Data
   sourcetbl = ReadFromDb("source_tbl")
   sourcetbl.show()
   print("&&&&&& Source Table ***")
   # Retrive Target Table Data
   targettbl = ReadFromDb("target_tbl")
   targettbl.show()
   print("&&&&&& Target Table ***")

   # Prepare hashcode for source table
   sourcetbl_col_list = sourcetbl.columns
   sourcetbl_md5list = sourcetbl.select(concat(*sourcetbl_col_list).alias("source_data_in_row"),
                                        md5(concat(*sourcetbl_col_list)).alias("source_hash_code_for_row")).collect()
   print("Source MD5List :",sourcetbl_md5list)



   # Prepare hashcode for target table
   targettbl_col_list = targettbl.columns
   tagettbl_md5list = targettbl.select(concat(*targettbl_col_list).alias("target_data_in_row"),
                                       md5(concat(*targettbl_col_list)).alias("target_hash_code_for_row")).collect()
   print("Target MD5List :", tagettbl_md5list)

   output_table= []

   for rec_index  in range(0,len(sourcetbl_md5list)):
       rec_rel = Row((sourcetbl_md5list[rec_index][0]),
             (sourcetbl_md5list[rec_index][1]),
             (tagettbl_md5list[rec_index][0]),
             (tagettbl_md5list[rec_index][1]),
             str(sourcetbl_md5list[rec_index][1] == tagettbl_md5list[rec_index][1]))
       output_table.append(rec_rel)

   schemaList = ['SourceTblData','SourceTBLHashCode','TargetTBLData','TargetTBLHashCode','IsSame']
   validationTBlDF = spark.createDataFrame(output_table,schemaList)
   validationTBlDF.show()
   recordMismatchCount= validationTBlDF.filter(validationTBlDF.IsSame == 'False').count()
   print("Mismatch Record:",recordMismatchCount)
   # if recordMismatchCount ==0
   #    return
   # #WriteToDb(validationTBlDF,'audit_table')
