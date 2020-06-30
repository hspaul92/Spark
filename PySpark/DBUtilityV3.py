from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_unixtime, unix_timestamp

# from pyspark.sql import SaveMode
# To be Enable when to record matching between two table
# from hashlib import md5
from pyspark.sql import Row


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
        .option("header", "true") \
        .option("password", password).mode('overwrite').save()
    return sourceDataDF



def CheckMetricesForStage(Record):
    entityCntDF = ReadFromDb('stg2',Record.STG_TableName)
    STG_PrimaryKeyList= Record.STG_PrimaryKeyList.replace(' ', '').split(',')
    print("Before Cleaning :",Record.STG_PrimaryKeyList,"After Cleaning :" ,STG_PrimaryKeyList)

    # Record Count Check
    stg_rec_cnt = entityCntDF\
                  .withColumn("VALID_FROM",F.to_date(from_unixtime(unix_timestamp('VALID_FROM','MM/dd/yyy')))) \
                  .withColumn("CurrentDate", F.to_date(F.lit(current_date)))
    currDataInStgDF = stg_rec_cnt.filter(stg_rec_cnt['VALID_FROM']==stg_rec_cnt['CurrentDate'])
    recordCountInStage = currDataInStgDF.count()
    message =''

    if recordCountInStage == Record.SAP_RecordCount:
        message = 'Success'
    else:
        message = 'Mismatched Rec:>'+str(Record.SAP_RecordCount-recordCountInStage)

    #Duplicate Count
    duplicateRecordCount = entityCntDF.groupBy(*STG_PrimaryKeyList).count()\
                                      .filter("count >1").count()

    # Null Check
    filter_condition = ''
    size = 0
    for key in STG_PrimaryKeyList:
        filter_condition += key + " IS NULL "
        if size < len(STG_PrimaryKeyList) - 1:
            filter_condition += "OR "
        size += 1
    nullInPKCol = entityCntDF.filter(filter_condition).count()

    #Return Result
    return (Record.SAP_TableName,
            Record.STG_TableName,
            Record.EDW_TableName,
            Record.SAP_RecordCount,
            recordCountInStage,
            message,
            duplicateRecordCount,
            nullInPKCol)


def ReadSTGToEDWTableMapping():
    csvDF =  spark.read\
                  .format("CSV")\
                  .option("header", "true")\
                  .option("inferSchema", "true")\
                  .load("STG_TO_EDW_TBL_MAPPING.csv")
    reqColumnDF =  csvDF.select(csvDF['STG_Table'],
                               csvDF['EDW_Table'].alias('EDW_TableName'))
    # reqColumnDF.show(20,False)
    return reqColumnDF


def CheckMetricesForEDW(Record):
    recordCountInStg = Record[4]
    edwDataDF = ReadFromDb('edw2', Record[2])
    edwDF= edwDataDF.withColumn("effective_date",
                                F.to_date(from_unixtime(unix_timestamp('effective_date', 'MM/dd/yyy')))) \
                     .withColumn("CurrentDate", F.to_date(F.lit(current_date)))

    currDataInEDWDF = edwDF.filter(edwDF['effective_date']==edwDF['CurrentDate'])
    recordCountInEDW = currDataInEDWDF.count()

    edwmessage = ''
    if recordCountInStg == recordCountInEDW:
        edwmessage = 'Success'
    else:
        edwmessage = 'Mismatched Rec:>' + str(recordCountInStg - recordCountInEDW)

    return    (Record[0],
               Record[1],
               Record[2],
               Record[3],
               Record[4],
               Record[5],
               Record[6],
               edwmessage
            )



def main():
    # Read Entity Table
    entityCntDF = ReadFromDb('etl2', 'entity_counter')
    reqColDf = entityCntDF.select(entityCntDF['DB'].alias("SAP_TableName"), \
                                              entityCntDF['COUNTER'].alias("SAP_RecordCount"), \
                                              entityCntDF['STG1_TABLE_NAME'].alias("STG_TableName"), \
                                              entityCntDF['PK_NAME_STG1'].alias("STG_PrimaryKeyList"), \
                                              entityCntDF['EXTRACTION_DATE'].alias("STG_RefreshDate")
                                              ) \
              .withColumn('STG_RefreshDate', F.to_date(from_unixtime(unix_timestamp('STG_RefreshDate', 'MM/dd/yyy'))))

    # Get Current Date record  # Change below Code when you want to run with Current date
    etlTblDataDF = reqColDf.filter((reqColDf['STG_RefreshDate']) == F.to_date(F.lit(current_date)))

    # Get Stage To EDW maping data
    stgToEDWMapDF = ReadSTGToEDWTableMapping()

    # Create Master Dataframe by combining ETL-STG-EDW infortion
    etl_To_Stg_To_EDW_MapDF = etlTblDataDF.join(stgToEDWMapDF,
                                           etlTblDataDF['STG_TableName'] == stgToEDWMapDF['STG_Table'])\
                                          .select('SAP_TableName',
                                              'SAP_RecordCount',
                                              'STG_TableName',
                                              'STG_PrimaryKeyList',
                                              'STG_RefreshDate',
                                              'EDW_TableName')
    Result =[]
    ResultSchema =['ETL_TableName',
                   'STG_TableName',
                   'ETL_RecodCount',
                   'STG_RecordCount',
                   'ETL2STG_RecordCountCheck',
                   'STG_DuplicateRecordCount',
                   'No_Of_Record_In_STG_With_Null_In_PK_Column',
                   'STG_TO_EDW_Record_Count_Match']


    # Start processing each record in Master dataframe
    for rec in etl_To_Stg_To_EDW_MapDF.collect():
        etlToSTGCheck = CheckMetricesForStage(rec)
        stgToEDWCheck= CheckMetricesForEDW(etlToSTGCheck)
        Result.append(stgToEDWCheck)

    # Create Final Result Data frame
    etlToSTGFinalResult =spark.createDataFrame(Result,ResultSchema)

    # Write Final Result Data frame  to CSV/Table
    WriteToDb(etlToSTGFinalResult,'etl2','audit')
    etlToSTGFinalResult.repartition(1)\
                       .write \
                       .mode('overwrite')\
                       .option("header", "true")\
                       .csv("FinalAuditTblReport.csv")




if __name__ == "__main__":
    main()
