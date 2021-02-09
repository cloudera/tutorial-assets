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
#  Source File Name: EnrichData_ETL.py
#
#  Description: Create a new enriched table using filtered data
#               from existing data warehouse.
#               The following tables are used:
#                   DATABASE: SALES
#                     TABLES: CAR_SALES
#
#                   DATABASE: FACTORY
#                     TABLES: CAR_INSTALLS
#                             EXPERIMENTAL_MOTORS
#                             EXPERIMENTAL_MOTORS_ENRICHED (will be created)
#
#                   DATABASE: MARKETING
#                     TABLES: CUSTOMER_DATA
#                             GEO_DATA_XREF
#
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#***************************************************************************/
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------
spark = SparkSession.builder.appName('Ingest').getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
_DEBUG_ = False



#---------------------------------------------------
#                READ SOURCE TABLES
#---------------------------------------------------
print("JOB STARTED...")
car_sales     = spark.sql("SELECT * FROM sales.car_sales")
customer_data = spark.sql("SELECT * FROM marketing.customer_data")
car_installs  = spark.sql("SELECT * FROM factory.car_installs")
factory_data  = spark.sql("SELECT * FROM factory.experimental_motors")
geo_data      = spark.sql("SELECT postalcode as zip, latitude, longitude FROM marketing.geo_data_xref")
print("\tREAD TABLE(S) COMPLETED")



#---------------------------------------------------
#                  APPLY FILTERS
# - Remove under aged drivers (less than 16 yrs old)
#---------------------------------------------------
before = customer_data.count()
customer_data = customer_data.filter(col('birthdate') <= F.add_months(F.current_date(),-192))
after = customer_data.count()
print(f"\tFILTER DATA (CUSTOMER_DATA): Before({before}), After ({after}), Difference ({after - before}) rows")



#---------------------------------------------------
#             JOIN DATA INTO ONE TABLE
#---------------------------------------------------
# SQL way to do things
salesandcustomers_sql = "SELECT customers.*, sales.sale_date, sales.saleprice, sales.model, sales.VIN \
                            FROM sales.car_sales sales JOIN marketing.customer_data customers \
                             ON sales.customer_id = customers.customer_id "
tempTable = spark.sql(salesandcustomers_sql)
if (_DEBUG_):
    print("\tTABLE: CAR_SALES")
    car_sales.show(n=5)
    print("\tTABLE: CUSTOMER_DATA")
    customer_data.show(n=5)
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA")
    tempTable.show(n=5)


# Add geolocations based on ZIP
tempTable = tempTable.join(geo_data, "zip")
if (_DEBUG_):
    print("\tTABLE: GEO_DATA_XREF")
    geo_data.show(n=5)
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip)")
    tempTable.show(n=5)


# Add installation information (What part went into what car?)
tempTable = tempTable.join(car_installs, ["VIN","model"])
if (_DEBUG_):
    print("\tTABLE: CAR_INSTALLS")
    car_installs.show(n=5)
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip) x CAR_INSTALLS (vin, model)")
    tempTable.show(n=5)


# Add factory information (For each part, in what factory was it made, from what machine, and at what time)
tempTable = tempTable.join(factory_data, ["serial_no"])
if (_DEBUG_):
    print("\tTABLE: EXPERIMENTAL_MOTORS")
    factory_data.show(n=5)
    print("\tJOIN QUERY: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip) x CAR_INSTALLS (vin, model) x EXPERIMENTAL_MOTORS (serial_no)")
    tempTable.show(n=5)


#---------------------------------------------------
#             CREATE NEW HIVE TABLE
#---------------------------------------------------
tempTable.write.mode("overwrite").saveAsTable('factory.experimental_motors_enriched', format="parquet")
print("\tNEW ENRICHED TABLE CREATED: FACTORY.EXPERIMENTAL_MOTORS_ENRICHED")
tempTable.show(n=5)


spark.stop()
print("JOB COMPLETED!\n\n")