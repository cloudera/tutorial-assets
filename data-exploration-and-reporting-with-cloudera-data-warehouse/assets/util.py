# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 13:43:11 2020

@author: Nico
"""

from random import randint,uniform,gauss
import numpy as np


#Factory 2 is going to be in Alaska (Much colder)
#Factory 4 is going to be in Texas (Much hotter)

def getambientdata(factory_id,date):
    ambient_data = []
    if(factory_id==2):
        temp = randint(-35,15) #Celcius
        humidity = randint(0,60) #Percent
        pressure = randint(1010,1040) #millibar
    elif(factory_id==4):
        temp = randint(0,30) #Celcius
        humidity = randint(0,100) #Percent
        pressure = randint(1010,1040) #millibar
    else:
        temp = randint(-10,25) #Celcius
        humidity = randint(0,100) #Percent
        pressure = randint(1010,1040) #millibar
    ambient_data.append([factory_id,date,temp,humidity,pressure])
    export_data = np.array(ambient_data).ravel()
    return export_data

def getmachinedata(factory_id,machine_id,date,sourcetype):
    factory_data = []
    if(factory_id==2):
        hours_operational = round(randint(17,24),2)
        daily_units_produced = randint(3200,3900)
    elif(factory_id==4):
        hours_operational = round(randint(22,24),2)
        daily_units_produced = randint(4500,5000)
    else:
        hours_operational = round(randint(20,24),2)
        daily_units_produced = randint(4000,5000)
    if(sourcetype == "Uptime"):
        factory_data.append([factory_id,machine_id,hours_operational,date])
    if(sourcetype == "Throughput"):
        factory_data.append([factory_id,machine_id,daily_units_produced,date])
    exportdata = np.array(factory_data).ravel()
    return exportdata

def getemployeedata(factory_id,employee_id,date,sourcetype):
    employee_data = []
    if(factory_id==2):
        time_worked = round(gauss(7,2),2)
        if(randint(0,1000) % 48 == 0):
            vacation = 1
        else:
            vacation = 0
        if(randint(0,1000) % 30 == 0):
            sick = 1
        else:
            sick = 0
    elif(factory_id==4):
        time_worked = round(gauss(8,1),2) #Change to gauss
        if(randint(0,1000) % 20 == 0):
            vacation = 1
        else:
            vacation = 0
        if(randint(0,1000) % 60 == 0):
            sick = 1
        else:
            sick = 0
    else:
        time_worked = round(gauss(8,2),2)
        if(randint(0,1000) % 30 == 0):
            vacation = 1
        else:
            vacation = 0
        if(randint(0,1000) % 48 == 0):
            sick = 1
        else:
            sick = 0
    if(sourcetype == "Logs"):
        employee_data.append([factory_id,employee_id,time_worked,date])
    if(sourcetype == "Vacation"):
        employee_data.append([factory_id,employee_id,vacation,date])
    if(sourcetype == "Sick"):
        employee_data.append([factory_id,employee_id,sick,date])
    exportdata = np.array(employee_data).ravel()
    return exportdata

def getemployeemetadata(factory_id,employee_id,faker):
    employee_metadata = []
    employee = faker.profile()
    salary = round(uniform(60000,120000),2)
    
    employee_metadata.append([factory_id,employee_id,employee['sex'],
                            employee['mail'],employee['job'],
                            employee['birthdate'],salary])
    exportdata = np.array(employee_metadata).ravel()
    return exportdata
    