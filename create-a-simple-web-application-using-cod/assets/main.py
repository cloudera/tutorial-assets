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
#  Source File Name: main.py
#  Description: Simple inventory management system using Flask
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#****************************************************************************

from flask import Flask, render_template, request, redirect, g
import logging
import os
import json
import phoenixdb
import configparser

#----------------------------------------------
#-               VARIABLE SETUP               -
#----------------------------------------------
app = Flask(__name__)
PARTLIST = []
MESSAGE  = ""



#----------------------------------------------
#-                SETUP LOGGING               -
#----------------------------------------------
logging.basicConfig(filename='opdb-app.log', \
                    format='%(asctime)s %(levelname)-8s [%(filename)-12s:%(lineno)d] %(message)s', \
                    datefmt='%Y-%m-%d:%H:%M:%S', level=logging.INFO)
log = logging.getLogger('ecc-inventory-app')



#--------------------------------------------------------
#-                       GETPARTS                       -
#- Retrieve current part inventory.                     -
#--------------------------------------------------------
def getparts():
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute(
            'select part_no, quant from part_inventory_app'
        )
        result = cursor.fetchall()

    log.info("GETPARTS() - CURRENT PART INVENTORY:\n\t\t%s", result)
    return result



#--------------------------------------------------------
#-                     REQUESTPARTS                     -
#- Update part quantity for given part number           -
#--------------------------------------------------------
@app.route('/requestparts', methods=['POST'])
def requestparts():
    global PARTLIST
    global MESSAGE

    part_no = request.form['part_requested']
    req_amt = request.form['amount_requested']
    log.info("RECEIVED FORM DATA:\n\t\tPART = %s\n\t\tQUANTITY = %i", part_no, req_amt)

    if part_no and req_amt:
      req_amt = int(req_amt)
      db = get_db()
      with db.cursor() as cursor:
          cursor.execute(
              "select quant from part_inventory_app where part_no = '" + part_no + "'"
          )

          result = cursor.fetchone()
          log.info("REQUESTPARTS().results = %s", result)
          if result != None:
              cur_val = result['QUANT']
              print(f"cur_val = {cur_val}")
              if cur_val >= req_amt:
                  new_amt = cur_val - req_amt
                  print('new amount is ' + str(new_amt))
                  cursor.execute(
                      'UPSERT INTO part_inventory_app VALUES (\'' +
                      part_no + '\',' + str(new_amt) + ')'
                  )
                  return redirect('/')
              else:
                  MESSAGE = f"INSUFFICIENT QUANTITY FOR {part_no}: inventory = {cur_val}, requested {req_amt}"
                  return render_template('index.html', partlist=PARTLIST, message=MESSAGE)
          else:
              MESSAGE = f"PART NOT FOUND: {part_no}"
              return render_template('index.html', partlist=PARTLIST, message=MESSAGE)
    else:
      MESSAGE = "INVALID PART NUMBER / QUANTITY"
      return render_template('index.html', partlist=PARTLIST, message=MESSAGE)



#--------------------------------------------------------
#-                         INDEX                        -
#- Render index.html using parts from the database.     -
#--------------------------------------------------------
@app.route('/')
def index():
    global PARTLIST
    global MESSAGE

    log.info("PAGE REFRESH")
    MESSAGE = ""
    PARTLIST = getparts()
    return render_template('index.html', partlist=PARTLIST, message=MESSAGE)



#--------------------------------------------------------
#-                  CONNECT_TO_DATABASE                 -
#- Connect to database using credentials provided in    -
#- config.ini.                                          -
#--------------------------------------------------------
def connect_to_database():
    log.info("Database Connection")
    REQUIRED_OPTS = ['Username', 'Password','Authentication', 'Url','Serialization']
    config = configparser.ConfigParser()
    config.read('config.ini')
    if not 'COD' in config:
        raise Exception("Could not find section for COD in config.ini")
    cod_config = config['COD']
    opts = {}

    # Validate the configuration
    for required_opt in REQUIRED_OPTS:
        if not required_opt in cod_config:
            raise Exception("Did not find %s in configuration" % (required_opt))

    # Read required options
    opts['avatica_user'] = cod_config['Username']
    opts['avatica_password'] = cod_config['Password']
    opts['authentication'] = cod_config['Authentication']
    opts['serialization'] = cod_config['serialization']

    try:
        db = phoenixdb.connect(cod_config['Url'], autocommit=True, **opts)
    except Exception as e:
        print(f"\tDATABASE CONNECTION ERROR: {str(e)}")
        log.error(e)
        raise

    return db



#--------------------------------------------------------
#-                         GET_DB                       -
#- Opens a new database connection if there is none yet -
#- for the current application context.                 -
#--------------------------------------------------------
def get_db():
    if 'db' not in g:
        try:
            g.db = connect_to_database()
            g.db.cursor_factory = phoenixdb.cursor.DictCursor
        except Exception as e:
            raise

    return g.db



#--------------------------------------------------------
#-                      TEARDOWN_DB                     -
#- Close database connection.                           -
#--------------------------------------------------------
@app.teardown_appcontext
def teardown_db(exception):
    print(exception)
    db = g.pop('db', None)

    if db is not None:
        db.close()
        log.info("Closed DB Connection")



#--------------------------------------------------------
#-                          MAIN                        -
#--------------------------------------------------------
if __name__ == '__main__':
    log.info("BEGIN PROGRAM")
    app.static_folder = os.environ['HOME'] + '/static'
    app.template_folder = os.environ['HOME'] + '/templates'

    try:
        app.run(host = '127.0.0.1', port = int(os.environ['CDSW_APP_PORT']))
    except Exception as e:
        print(f"ERROR: unable to run application:\n {str(e)}")

    log.info("END PROGRAM")
