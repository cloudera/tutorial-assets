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
#  Source File Name: data_generator.py
#  Description: Generate data we're going to use as mock factory
#  Author(s): Nicolas Pelaez
#***************************************************************************/
import datetime
import util
import pandas as pd
from faker import Faker
import numpy as np

start_date = datetime.date(year=2020, month=1, day=1)
end_date   = datetime.date(year=2020, month=12,  day=31)

parts_production = []

parts_production_cols = ['factory_no','machine_no','part_no','serial_no','timestamp']

current_date = start_date
faker = Faker()

days_processed = 0
print('done')
# Iterating over all dates from start date until end date including end date ("inclusive")
while current_date <= end_date:
    for factory_id in range(1,6): #Iterate through the factories
        sql_date = current_date.isoformat()
        for machine_id in range(1,11):
            parts_production.append(util.getmachinedata(factory_id,machine_id,sql_date,current_date))
    days_processed = days_processed + 1
    print(days_processed)
    current_date += datetime.timedelta(days=1)

finalprod = np.vstack(parts_production)
parts_production_export = pd.DataFrame(finalprod, columns = parts_production_cols)

parts_production_export.to_csv('parts_production_export.csv', index =False)
