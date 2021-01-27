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
#  Source File Name: Data_Extraction_Sub_150k.py
#  Description: Extract and transform data for loans less than $150,000 and
#               filter out data relating to Texas.
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#***************************************************************************/
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import sys

spark = SparkSession \
    .builder \
    .appName("Pyspark PPP ETL") \
    .getOrCreate()

#Path of our file in S3
input_path ='s3a://usermarketing-cdp-demo/tutorial-data/data-engineering/PPP-Sub-150k-TX.csv'

#This is to deal with tables existing before running this code. Not needed if you're starting fresh.
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

#Bring data into Spark from S3 Bucket
base_df=spark.read.option("header","true").option("inferSchema","true").csv(input_path)
#Print schema so we can see what we're working with
print(f"printing schema")
base_df.printSchema()

#Filter out only the columns we actually care about
filtered_df = base_df.select("LoanAmount", "City", "State", "Zip", "BusinessType", "NonProfit", "JobsRetained", "DateApproved", "Lender")

#This is a Texas only dataset but lets do a quick count to feel good about it
print(f"How many TX records did we get?")
tx_cnt = filtered_df.count()
print(f"We got: %i " % tx_cnt)

#Create the database if it doesn't exist
print(f"Creating TexasPPP Database \n")
spark.sql("CREATE DATABASE IF NOT EXISTS TexasPPP")
spark.sql("SHOW databases").show()

print(f"Inserting Data into TexasPPP.loan_data table \n")

#insert the data
filtered_df.\
  write.\
  mode("append").\
  saveAsTable("TexasPPP"+'.'+"loan_data", format="parquet")

#Another sanity check to make sure we inserted the right amount of data
print(f"Number of records \n")
spark.sql("Select count(*) as RecordCount from TexasPPP.loan_data").show()

print(f"Retrieve 15 records for validation \n")
spark.sql("Select * from TexasPPP.loan_data limit 15").show()