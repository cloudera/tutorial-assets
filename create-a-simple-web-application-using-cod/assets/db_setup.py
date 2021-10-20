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
#  Source File Name: db_setup.py
#  Description: Create and populate table used in simple inventory
#               management system
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#****************************************************************************

import phoenixdb
import configparser

#---------------------------------------------------
#          CONFIGURATION - VALIDATE / SETUP
#---------------------------------------------------
REQUIRED_OPTS = ['Username', 'Password','Authentication', 'Url','Serialization']
config = configparser.ConfigParser()
config.read('config.ini')

if not 'COD' in config:
    raise Exception("Could not find section for COD in config.ini")

cod_config = config['COD']
opts = {}

for required_opt in REQUIRED_OPTS:
    if not required_opt in cod_config:
        raise Exception("Did not find %s in configuration" % (required_opt))

opts['avatica_user'] = cod_config['Username']
opts['avatica_password'] = cod_config['Password']
opts['authentication'] = cod_config['Authentication']
opts['serialization'] = cod_config['serialization']



#---------------------------------------------------
#              CREATE & POPULATE TABLE
#---------------------------------------------------
try:
    print("Database Connection")
    conn = phoenixdb.connect(cod_config['Url'], autocommit=True, **opts)

    print("Create Cursor")
    cursor = conn.cursor()

    print("Create Table")
    cursor.execute('CREATE TABLE IF NOT EXISTS part_inventory_app (part_no VARCHAR PRIMARY KEY, quant INTEGER)')

    print("Insert Data")
    cursor.execute('UPSERT INTO part_inventory_app VALUES (\'a42CLDR\',18194)')
    cursor.execute('UPSERT INTO part_inventory_app VALUES (\'b42CLDR\',18362)')
    cursor.execute('UPSERT INTO part_inventory_app VALUES (\'c42CLDR\',12362)')
    cursor.execute('UPSERT INTO part_inventory_app VALUES (\'d42CLDR\',128)')
    cursor.execute('UPSERT INTO part_inventory_app VALUES (\'e42CLDR\',1228)')

    print("Close Cursor")
    cursor.close()

    print("Close Connection")
    conn.close()
except Exception as e:
    print(f"\tERROR: {str(e)}", flush=True)
