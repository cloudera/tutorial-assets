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
#  Source File Name: util.py
#
#  Description: Helper functions used in Part_Modeling.py
#
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#***************************************************************************/
import pandas as pd
import datetime
import time
from random import uniform

# Join datasets and get needed weekly aggregates
def collect_format_data(_part_no, _time_delta):
    car_sales_df = pd.read_csv("car_sales.csv")
    parts_production_df = pd.read_csv("parts_production_export.csv")
    surplus_parts_df = pd.read_csv("surplus_export.csv")
    time_delta = _time_delta
    dayfreq = str(time_delta) + 'D'

    car_sales_df['sale_date'] = pd.to_datetime(car_sales_df['sale_date']) - pd.to_timedelta(time_delta, unit='d')
    parts_production_df['timestamp'] = pd.to_datetime(parts_production_df['timestamp'], unit='s') - pd.to_timedelta(
        time_delta, unit='d')
    surplus_parts_df['timestamp'] = pd.to_datetime(surplus_parts_df['timestamp'], unit='s') - pd.to_timedelta(
        time_delta, unit='d')

    parts_production_df = parts_production_df[parts_production_df["part_no"] == _part_no]
    surplus_parts_df = surplus_parts_df[surplus_parts_df["part_no"] == _part_no]

    df_final_sales = (car_sales_df
                      .reset_index()
                      .set_index("sale_date")
                      .groupby(["model", pd.Grouper(freq=dayfreq)])["VIN"].count()
                      .astype(int)
                      .reset_index()
                      )

    df_final_parts_production = (parts_production_df
                                 .reset_index()
                                 .set_index("timestamp")
                                 .groupby(["part_no", pd.Grouper(freq=dayfreq)])["serial_no"].count()
                                 .astype(int)
                                 .reset_index()
                                 )

    df_final_surplus_parts = (surplus_parts_df
                              .reset_index()
                              .set_index("timestamp")
                              .groupby(["part_no", pd.Grouper(freq=dayfreq)])["serial_no"].count()
                              .astype(int)
                              .reset_index()
                              )

    df_final_sales = df_final_sales.pivot(index='sale_date', columns='model', values='VIN')

    add_prod = df_final_sales.join(df_final_parts_production.set_index('timestamp'))
    final_df = df_final_surplus_parts.set_index('timestamp').join(add_prod, lsuffix='_surplus', rsuffix='_prod')
    final_df = final_df.rename(columns={"serial_no_surplus": "surplus_count", "serial_no_prod": "prod_count"})
    final_df = final_df.drop(columns=['part_no_surplus', 'part_no_prod'])
    final_df['goal_parts'] = final_df.apply(lambda row: row.prod_count - row.surplus_count, axis=1)
    final_df = final_df.drop(columns=['surplus_count', 'prod_count'])

    return final_df



def getmachinedata(factory_id,machine_id,date_string,date_actual):
    factory_data = []
    timestamp_start = time.mktime(date_actual.timetuple())
    nextday = date_actual + datetime.timedelta(days=1)
    timestamp_end = time.mktime(nextday.timetuple())
    if(factory_id==2):
        reporting_slices = 18
    elif(factory_id==4):
        reporting_slices = 22
    else:
        reporting_slices = 20
    for reports in range(1,reporting_slices):
        part_no = chr(96 + machine_id) + '42CLDR'
        timestamp = uniform(timestamp_start,timestamp_end)
        serial_no = part_no + '01' + str(factory_id) + '666' + str(int(timestamp))
        factory_data.append([factory_id,machine_id,part_no,serial_no,timestamp])

    return factory_data
