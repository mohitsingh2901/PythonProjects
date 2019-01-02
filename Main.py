from pyspark.sql import *
from pyspark import *
from QueryBuilder import *
from FileReaderFactory import FileReaderFactory
import sys
import os


if __name__ == '__main__':
    print("Starting main")

    #session = SparkSession.builder.appName(sys.argv[6]).enableHiveSupport().getOrCreate()
    #session.sparkContext.addFile(sys.argv[5])
    #confFilePath = SparkFiles.get(sys.argv[5])
    session = SparkSession.builder.appName(sys.argv[6]).config("spark.jars.packages", "com.databricks:spark-xml_2.11:0.4.1").getOrCreate()
    confFilePath = sys.argv[5]
    print("Spark session created")

    c = ConfigParser()
    s = c.conf_execute(confFilePath)
    qb = QueryBuilder()
    tempTableName = "tempTable_" + sys.argv[6].replace(" ", "")
    queries = qb.execute(sys.argv[3], s, tempTableName)
    for i in queries:
        for j in queries[i]:
            print(j, queries[i][j])

    if sys.argv[1] == "BATCH":
        reader = FileReaderFactory.get_reader(sys.argv[2])
        mList = reader.execute(session, queries, sys.argv[4], tempTableName, sys.argv[7],
                               sys.argv[8], sys.argv[9])
        #for m in mList:
            #session.sql("DESCRIBE FORMATTED " + m.statge_table).show()
            #session.sql("DESCRIBE FORMATTED "+m.target_table).show()
