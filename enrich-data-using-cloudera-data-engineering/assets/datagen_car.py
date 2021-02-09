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
#  Source File Name: datagen_car.py
#
#  Description: Generate data used in demo.
#       Output: car_sales.csv
#               car_installs.csv
#
#  Author(s): Nicolas Pelaez
#***************************************************************************/
from faker import Faker
from random import randint,uniform,randrange,choices
import numpy as np
import pandas as pd
import datetime,string
import os

#--------------------
# Variables Used
#--------------------
customer_id,sale_id = 1,1
total_customers = 10000
SALES,INSTALLS = [],[]
SALES_cols = ['customer_id','model','saleprice','sale_date','VIN']
INSTALLS_cols = ['model','VIN','serial_no']

outdir = './output/'
if not os.path.exists(outdir):
    os.mkdir(outdir)

df = pd.read_csv("parts_production_export.csv")



#Select only experimental motors
filtered_df = df.loc[(df['part_no'] == 'a42CLDR') & (df['factory_no'] == 5)]
final_df = filtered_df.groupby('part_no').first()

start_date = datetime.date(year=2020, month=1, day=1)
end_date   = datetime.date(year=2020, month=12,  day=31)

time_between_dates = end_date - start_date
days_between_dates = time_between_dates.days
random_number_of_days = randrange(days_between_dates)

faker = Faker()

def getsalesandinstalldata(customer_id,faker,sale_id,returntype,car_serial_no):
    SALES = []
    INSTALLS = []
    models = ['Model C','Model D','Model R']
    price_med = [65120,74200,92000]
    randomseed = randint(0,2)
    saleprice = price_med[randomseed] * uniform(1.0, 1.3)
    random_number_of_days = randrange(days_between_dates)
    sale_date = start_date + datetime.timedelta(days=random_number_of_days)

    INSTALLS.append([models[randomseed],car_serial_no,filtered_df['serial_no'].values[sale_id]])
    exportinstalls = np.array(INSTALLS).ravel()
    SALES.append([customer_id,models[randomseed], saleprice, sale_date,car_serial_no])
    exportsales = np.array(SALES).ravel()
    if returntype == 'SALES':
        return exportsales
    if returntype == 'INSTALLS':
        return exportinstalls
    else:
        return

while customer_id <= total_customers:
    if(customer_id % 3 != 0):       #this line is just here to not have a sale for every customer
        print('Processing Customer: ', customer_id, sale_id)
        car_serial_no = ''.join(choices(string.ascii_uppercase + string.digits, k=12))
        SALES.append(getsalesandinstalldata(customer_id,faker,sale_id,'SALES',car_serial_no))
        INSTALLS.append(getsalesandinstalldata(customer_id,faker,sale_id,'INSTALLS',car_serial_no))
        sale_id = sale_id + 1
    customer_id = customer_id + 1


sales_export = np.vstack(SALES)
installs_export = np.vstack(INSTALLS)
sales_export_csv = pd.DataFrame(sales_export, columns = SALES_cols)
installs_export_csv = pd.DataFrame(installs_export, columns = INSTALLS_cols)
sales_export_csv.to_csv(outdir + 'car_sales.csv', index =False)
installs_export_csv.to_csv(outdir + 'car_installs.csv', index =False)