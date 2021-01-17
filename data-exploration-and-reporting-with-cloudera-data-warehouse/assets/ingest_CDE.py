#****************************************************************************
# (C) Cloudera, Inc. 2020-2021
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
#  Source File Name: ingest_CDE.py
#  Description: This Spark job will ingest data from CSV files stored
#               on AWS S3 Cloud storage. Using basic data manipulation,
#               the following database objects will be created and populated:
#                         DATABASE: HR
#                           TABLES: EMPLOYEE
#                                   FACTORY
#                                   LEAVE_TIME
#                                   TIMESHEET
#
#                         DATABASE: FACTORY
#                           TABLES: AMBIENT_DATA
#                                   MACHINE_REVENUE
#                                   MACHINE_THROUGHPUT
#                                   MACHINE_UPTIME
#
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#***************************************************************************/
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------
spark = SparkSession.builder.appName('Ingest').getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")


#-----------------------------------------------------------------------------------
# LOAD DATA FROM .CSV FILES ON AWS S3 CLOUD STORAGE
#
# REQUIREMENT: Update variable s3BucketName
#              using storage.location.base attribute; defined by your environment.
#
#              For example, property storage.location.base
#                           has value 's3a://usermarketing-cdp-demo'
#                           Therefore, set variable as:
#                                 s3BucketName = "s3a://usermarketing-cdp-demo"
#-----------------------------------------------------------------------------------
s3BucketName = "s3a://usermarketing-cdp-demo/tutorial-data/data-warehouse"
employee_metadata   = spark.read.csv(s3BucketName + "/employee_metadata_export.csv",    header=True, inferSchema=True)
employee_sicktime   = spark.read.csv(s3BucketName + "/employee_sicktime_export.csv",    header=True, inferSchema=True)
employee_vacation   = spark.read.csv(s3BucketName + "/employee_vacation_export.csv",    header=True, inferSchema=True)
daily_employee_logs = spark.read.csv(s3BucketName + "/daily_employee_logs_export.csv",  header=True, inferSchema=True)
factory_ambient     = spark.read.csv(s3BucketName + "/factory_ambient_export.csv",      header=True, inferSchema=True)
factory_revenue     = spark.read.csv(s3BucketName + "/factory_revenue_export.csv",      header=True, inferSchema=True)
machine_throughput  = spark.read.csv(s3BucketName + "/machine_throughput_export.csv",   header=True, inferSchema=True)
machine_uptime      = spark.read.csv(s3BucketName + "/machine_uptime_export.csv",       header=True, inferSchema=True)

#---------------------------------------------------
#       SQL CLEANUP: DATABASES, TABLES, VIEWS
#---------------------------------------------------
spark.sql("DROP DATABASE IF EXISTS hr CASCADE")
spark.sql("DROP DATABASE IF EXISTS factory CASCADE")
print("DROP DATABASE(S) COMPLETED!\n")


#---------------------------------------------------
#                 CREATE DATABASES
#---------------------------------------------------
spark.sql("CREATE DATABASE hr")
spark.sql("CREATE DATABASE factory")
print("CREATE DATABASE(S) COMPLETED!\n")


#---------------------------------------------------
#         POPULATE TABLE: HR.EMPLOYEE TABLE
#---------------------------------------------------
employee_metadata.write.mode("overwrite").saveAsTable('hr.employee', format="parquet")
print("POPULATE TABLE HR.EMPLOYEE COMPLETED!\n")


#---------------------------------------------------
#            POPULATE TABLE: HR.FACTORY
#---------------------------------------------------
factory_ids = employee_metadata.select('factory_id').distinct().sort('factory_id')
factory_ids.write.mode("overwrite").saveAsTable('hr.factory', format="parquet")
print("POPULATE TABLE HR.FACTORY COMPLETED!\n")


#---------------------------------------------------
#           POPULATE TABLE: HR.LEAVE_TIME
#---------------------------------------------------
sick_days = employee_sicktime.filter(col('sick') == 1)
sick_days = sick_days.withColumn('leave_type',lit('sick'))
vaca_days = employee_vacation.filter(col('vacation') == 1)
vaca_days = vaca_days.withColumn('leave_type',lit('vacation'))
leave_time = sick_days.union(vaca_days)
leave_time = leave_time.select('factory_id','employee_id','date','leave_type')
leave_time.write.mode("overwrite").saveAsTable('hr.leave_time', format="parquet")
print("POPULATE TABLE HR.LEAVE_TIME COMPLETED!\n")


#---------------------------------------------------
#            POPULATE TABLE: HR.TIMESHEET
#---------------------------------------------------
timesheet = daily_employee_logs.withColumnRenamed('date','workday')
timesheet.write.mode("overwrite").saveAsTable('hr.timesheet', format="parquet")
print("POPULATE TABLE HR.TIMESHEET COMPLETED!\n")


#---------------------------------------------------
#       POPULATE TABLE: FACTORY.AMBIENT_DATA
#---------------------------------------------------
factory_ambient.write.mode("overwrite").saveAsTable('factory.ambient_data', format="parquet")
print("POPULATE TABLE FACTORY.AMBIENT_DATA COMPLETED!\n")


#---------------------------------------------------
#       POPULATE TABLE: FACTORY.MACHINE_REVENUE
#---------------------------------------------------
factory_revenue.write.mode("overwrite").saveAsTable('factory.machine_revenue', format="parquet")
print("POPULATE TABLE FACTORY.MACHINE_REVENUE COMPLETED!\n")


#---------------------------------------------------
#       POPULATE TABLE: FACTORY.MACHINE_THROUGHPUT
#---------------------------------------------------
machine_throughput.write.mode("overwrite").saveAsTable('factory.machine_throughput', format="parquet")
print("POPULATE TABLE FACTORY.MACHINE_THROUGHPUT COMPLETED!\n")


#---------------------------------------------------
#       POPULATE TABLE: FACTORY.MACHINE_UPTIME
#---------------------------------------------------
machine_uptime.write.mode("overwrite").saveAsTable('factory.machine_uptime', format="parquet")
print("POPULATE TABLE FACTORY.MACHINE_UPTIME COMPLETED!\n")


print("\n\n\nSPARK JOB COMPLETED!\n")
spark.stop()
