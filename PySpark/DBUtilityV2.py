from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_unixtime, unix_timestamp

# To be Enable when to record matching between two table
# from hashlib import md5
# from pyspark.sql import Row


# Create Spark Context And Spark Session
sc = SparkContext()
spark = SparkSession(sc)

current_date ='2020-06-25'


def ReadFromDb(db_name,tbl_name):
    url = "jdbc:mysql://localhost:3306/{}".format(db_name)
    print("URL:",url)
    print("Table Name:",tbl_name)
    driver = 'com.mysql.jdbc.Driver'
    user = 'root'
    password = 'mysql'
    sourceDataDF = spark.read.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", tbl_name) \
        .option("user", user) \
        .option("password", password)\
        .option("dateFormat","MM/dd/yyyy").load()
    return sourceDataDF



def WriteToDb(dfname,dbname, tbl_name):
    url = "jdbc:mysql://localhost:3306/{}".format(dbname)
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



# Redundant for now until getting clarity on filter condition
# def GetCurDataFromEntTbl(df):
    # return df.filter(df['STG1_TABLE_NAME'].contains('Dim') |
    #                  df['STG1_TABLE_NAME'].contains('Facts')) \
    #          #.filter(df['CURRENT_ROW_IND'] == True)
    #           #.filter(df['EXTRACTION_DATE']=='')     # DateInData = 6/25/2020  DateInSpark =6/25/2020-6-25


def CheckMetricesForStage(Record):
    entityCntDF = ReadFromDb('stg',Record.STG_TableName)
    STG_PrimaryKeyList= Record.STG_PrimaryKeyList.replace(' ', '').split(',')

    #Metrics :1 Check For Record Count In Stage Table
    stg_rec_cnt = entityCntDF.\
                  withColumn("VALID_FROM",F.to_date(from_unixtime(unix_timestamp('VALID_FROM', 'yyyy-MM-dd HH:mm:ss.SSS'))))\
                 .filter(F.to_date(entityCntDF["VALID_FROM"])==F.to_date(F.lit(current_date)))
    recordCountInStage = stg_rec_cnt.count()
    message =''
    if recordCountInStage == Record.SAP_RecordCount:
        message = 'Success'
    else:
        message = 'Mismatched Rec:>'+str(recordCountInStage- Record.SAP_RecordCount)


    #Metrics :2 Check For Duplicate record in Stage Table
    #primaryKeyList = Record.STG_PrimaryKeyList.replace(' ', '').split(',')
    entityCntDF.groupBy(*STG_PrimaryKeyList).count().filter("count >1").show()
    duplicateRecordCount = entityCntDF.groupBy(*STG_PrimaryKeyList).count()\
                                      .filter("count >1").count()
    print("Duplicate :",duplicateRecordCount)


    #Metrics :3  Check For Null Key In Primary Key columns of Stage Table
    filter_condition = ''
    size = 0
    for key in Record.STG_PrimaryKeyList.replace(' ', '').split(','):
        filter_condition += key + " IS NULL "
        if size < len(STG_PrimaryKeyList) - 1:
            filter_condition += "OR "
        size += 1
    print("Filter Condition : ", filter_condition)
    nullInPKCol = entityCntDF.filter(filter_condition).count()


    # Return Final Status To Main
    return (Record.SAP_TableName,
            Record.STG_TableName,
            Record.SAP_RecordCount,
            recordCountInStage,
            message,
            duplicateRecordCount,
            nullInPKCol)



# def StgTableDuplicateCheck(Record):
#     primaryKeyList = Record.STG_PrimaryKeyList.replace(' ','').split(',')
#     print(primaryKeyList)
#     entityCntDF = ReadFromDb('stg',Record.STG_TableName)
#
#
#     entityCntDF.select(*primaryKeyList).show()
#
#     entityCntDF.groupBy(*primaryKeyList).count().filter("count >1").show()
#     duplicateRecordCount = entityCntDF.groupBy(F.concat(*primaryKeyList)).count()\
#                                       .filter("count >1").count()
#     print("Duplicate :",duplicateRecordCount)



def ReadSTGToEDWTableMapping():
    csvDF =  spark.read\
                  .format("CSV")\
                  .option("header", "true")\
                  .option("inferSchema", "true")\
                  .load("STG_TO_EDW_TBL_MAPPING.csv")
    csvDF.show(20,False)



def main():
    # Read Entity Table
    entityCntDF = ReadFromDb('etl', 'entity_counter')

    reqColDf = entityCntDF.select(entityCntDF['DB'].alias("SAP_TableName"), \
                                              entityCntDF['COUNTER'].alias("SAP_RecordCount"), \
                                              entityCntDF['STG1_TABLE_NAME'].alias("STG_TableName"), \
                                              entityCntDF['PK_NAME_STG1'].alias("STG_PrimaryKeyList"), \
                                              entityCntDF['EXTRACTION_DATE'].alias("STG_RefreshDate")
                                              ) \
              .withColumn('STG_RefreshDate', F.to_date(from_unixtime(unix_timestamp('STG_RefreshDate', 'MM/dd/yyy'))))
    reqColDf.show()
    # Get Current Date record  # Change below Code when you want to run with Current date
    etlTblDataDF = reqColDf.filter((reqColDf['STG_RefreshDate']) == F.to_date(F.lit(current_date)))
    etlTblDataDF.show()



    # *** Code For STG TO EDW Maping  **#
    # *** Activate below only when have clear idea about EDW
    # Read From CSV Data
    # stgToEdwTblMapDF = ReadSTGToEDWTableMapping()
    # allDf = etlTblDataDF.join('stgToEdwTblMapDF',
    #                            etlTblDataDF['STG_TableName']==stgToEdwTblMapDF['STG_Table'],
    #                            how='left')
    # allDf.show()

    Result =[]
    ResultSchema =['ETL_TableName',
                   'STG_TableName',
                   'ETL_RecodCount',
                   'STG_RecordCount',
                   'ETL2STG_RecordCountCheck',
                   'STG_DuplicateRecordCount',
                   'No_Of_Record_In_STG_With_Null_In_PK_Column']

    for rec in etlTblDataDF.collect():
        print("Record In ETL :",rec)
        etlToSTGRecCount = CheckMetricesForStage(rec)
        Result.append(etlToSTGRecCount)

    etlToSTGFinalResult =spark.createDataFrame(Result,ResultSchema)
    etlToSTGFinalResult.show()
    
    # Write Final table to CSV/Table
    WriteToDb(etlToSTGFinalResult,'etl','audit')  # To Write Final Dataframe Into Table
    etlToSTGFinalResult.repartition(1)\           # To Write Final Dataframe Into CSV Final
                       .write \
                       .mode('overwrite')\
                       .csv("AuditTblFinalReport.csv")
    
    

if __name__ == "__main__":
    main()
