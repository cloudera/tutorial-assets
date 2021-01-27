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
#  Source File Name: Create_Reports.py
#  Description: Create tables used for reporting purposes.
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#***************************************************************************/
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import sys

spark = SparkSession \
    .builder \
    .appName("Pyspark PPP ETL") \
    .getOrCreate()

#A simple script that runs aggregate queries to be used for reporting purposes.
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

print(f"Running report for Jobs Retained by City")

#Delete any reports that were previously run
spark.sql("drop table IF EXISTS texasppp.Jobs_Per_City_Report")
spark.sql("drop table IF EXISTS texasppp.Jobs_Per_Company_Type_Report")

#Create the Jobs Per City Report
cityReport = "create table texasppp.Jobs_Per_City_Report as \
select * from (Select \
  sum(jobsretained) as jobsretained, \
  city \
from \
  texasppp.loan_data \
group by \
  city \
) A order by A.jobsretained desc"

#Run the query to make the new table with the result data
print(f"Running - Jobs Per City Report \n")
spark.sql(cityReport)

#Show the top 10 results
print(f"Results - Jobs Per City Report \n")
cityReportResults = "select * from texasppp.Jobs_Per_City_Report limit 10"
spark.sql(cityReportResults).show()

#Create the Jobs Retained per Company Type Report
companyTypeReport = "create table texasppp.Jobs_Per_Company_Type_Report as \
select * from (Select \
  sum(jobsretained) as jobsretained, \
  businesstype \
from \
  texasppp.loan_data \
group by \
  businesstype \
) A order by A.jobsretained desc"

#Run the query to make the new table with the result data
print(f"Running - Jobs Per Company Type Report \n")
spark.sql(companyTypeReport)

#Show the top 10 results
print(f"Results - Jobs Per Company Type Report \n")
cityReportResults = "select * from texasppp.Jobs_Per_Company_Type_Report limit 10"
spark.sql(cityReportResults).show()