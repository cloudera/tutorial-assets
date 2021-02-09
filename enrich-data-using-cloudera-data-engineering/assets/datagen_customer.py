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
#  Source File Name: datagen_customer.py
#
#  Description: Generate data used in demo.
#       Output: customer_data.csv
#
#  Author(s): Nicolas Pelaez
#***************************************************************************/
from faker import Faker
from random import randint,uniform,gauss
import numpy as np
import pandas as pd
import os

#--------------------
# Variables Used
#--------------------
customer_id = 1
total_customers = 10000
employee_metadata = []
employee_metadata_cols = ['customer_id','username','name','gender','email','occupation','birthdate','address','salary','zip']

outdir = './output/'
if not os.path.exists(outdir):
    os.mkdir(outdir)

zipcodes = pd.read_csv("postal_codes.csv")

faker = Faker()

def getemployeemetadata(customer_id,faker):
    employee_metadata = []
    employee = faker.profile()
    salary = round(uniform(30000,420000),2)
    zipcode = zipcodes['postalcode'].values[randint(0,len(zipcodes) - 1)]

    employee_metadata.append([ customer_id,             employee['username'],
                               employee['name'],        employee['sex'],
                               employee['mail'],        employee['job'],
                               employee['birthdate'],   employee['address'].replace('\n', ''),
                               salary,                  zipcode])

    exportdata = np.array(employee_metadata).ravel()
    return exportdata

while customer_id <= total_customers:
    print('Processing Customer: ', customer_id)
    employee_metadata.append(getemployeemetadata(customer_id,faker))
    customer_id = customer_id + 1

test = np.vstack(employee_metadata)
customer_export = pd.DataFrame(test, columns = employee_metadata_cols)
customer_export.to_csv(outdir + 'customer_data.csv', index =False)
