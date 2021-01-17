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
#  Description: Generate data we're going to use as mock factory and
#               employee data. It'll iterate through dates for us and
#               assign values at random. Each factory will have 10
#               machines and 110 employees. We'll simulate one year.
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#***************************************************************************/
import datetime
import util
import pandas as pd
from faker import Faker
import numpy as np
import multiprocessing as mp

start_date = datetime.date(year=2019, month=10, day=31)
end_date   = datetime.date(year=2020, month=10,  day=31)

columns = ["1","2"]
machine_uptime, machine_throughput, factory_ambient_conditions = [],[],[]
employee_metadata,daily_employee_logs,employee_vacation,employee_sicktime = [],[],[],[]


factory_ambient_cols = ['factory_id','date_measured','temp','humidity','pressure']
machine_uptime_cols = ['factory_id','machine_id','hours_operational','factory_date']
machine_throughput_cols = ['factory_id','machine_id','daily_units_produced','factory_date']
daily_employee_logs_cols = ['factory_id','employee_id','time_worked','date']
employee_vacation_cols = ['factory_id','employee_id','vacation','date']
employee_sicktime_cols = ['factory_id','employee_id','sick','date']
employee_metadata_cols = ['factory_id','employee_id','gender','email','occupation','birthdate','salary']


#To make things interesting, we're going to make factory 2 and 4 different
#in both terms of ambient conditions as well as employee throughput

#Factory 2 is going to be in Alaska (Much colder)
#Factory 4 is going to be in Texas (Much hotter)

current_date = start_date
faker = Faker()

#lets try parallel processing
#pool = mp.Pool(mp.cpu_count())
#results =  [pool.apply(util.getmachinedata, args=(1,machine_id,'18',"Uptime")) for machine_id in range(1,6)]

days_processed = 0
print('done')
# Iterating over all dates from start date until end date including end date ("inclusive")
while current_date <= end_date:
    for factory_id in range(1,6): #Iterate through the factories
        sql_date = current_date.isoformat()
        factory_ambient_conditions.append(util.getambientdata(factory_id,sql_date))
        for machine_id in range(1,11):
            machine_uptime.append(util.getmachinedata(factory_id,machine_id,sql_date,"Uptime"))
            machine_throughput.append(util.getmachinedata(factory_id,machine_id,sql_date,"Throughput"))
        #Do employee data
        for employee_id in range (1,111):
            daily_employee_logs.append(util.getemployeedata(factory_id,employee_id,sql_date,"Logs"))
            employee_vacation.append(util.getemployeedata(factory_id,employee_id,sql_date,"Vacation"))
            employee_sicktime.append(util.getemployeedata(factory_id,employee_id,sql_date,"Sick"))
            if(days_processed == 0):
                #Only want one set of metadata not one for every single day
                employee_metadata.append(util.getemployeemetadata(factory_id,employee_id,faker))
    days_processed = days_processed + 1
    print(days_processed)
    current_date += datetime.timedelta(days=1)


machine_uptime_export = pd.DataFrame(machine_uptime, columns = machine_uptime_cols)
machine_throughput_export = pd.DataFrame(machine_throughput, columns = machine_throughput_cols)
daily_employee_logs_export = pd.DataFrame(daily_employee_logs, columns = daily_employee_logs_cols)
employee_vacation_export = pd.DataFrame(employee_vacation, columns = employee_vacation_cols)
employee_sicktime_export = pd.DataFrame(employee_sicktime, columns = employee_sicktime_cols)
employee_metadata_export = pd.DataFrame(employee_metadata, columns = employee_metadata_cols)
factory_ambient_export = pd.DataFrame(factory_ambient_conditions, columns = factory_ambient_cols)

machine_uptime_export.to_csv('machine_uptime_export.csv', index =False)
machine_throughput_export.to_csv('machine_throughput_export.csv', index= False)
daily_employee_logs_export.to_csv('daily_employee_logs_export.csv', index = False)
employee_vacation_export.to_csv('employee_vacation_export.csv', index = False)
employee_sicktime_export.to_csv('employee_sicktime_export.csv', index= False)
employee_metadata_export.to_csv('employee_metadata_export.csv', index = False)
factory_ambient_export.to_csv('factory_ambient_export.csv', index = False)