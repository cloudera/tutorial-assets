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
#  Source File Name: Part_Modeling.py
#
#  Description: The purpose of this script is to take production part data
#               from the factories and subtract surplus part data to attain
#               a "goal" for optimal part production for a given part.
#
#               It then takes car sales data to build a model that predicts
#               what the optimal weekly production of a specific part should
#               be given the sales of the three different models the company
#               produces.
#
#               Note: this code makes one model per part number.
#
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#***************************************************************************/
import util
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
import sys
import pickle

#-----------------------------------------------------------------------------
#             INPUT PARAMETER HANDLING
#
# Five (5), order dependent, parameters are required to build model:
#     model_C_sales = daily MODEL-C sales           [default = 12]
#     model_D_sales = daily MODEL-D sales           [default = 15]
#     model_R_sales = daily MODEL-R sales           [default = 18]
#     part_no       = part number                   [default = 'a42CLDR']
#     time_delta    = number of days for prediction [default = 21]
#-----------------------------------------------------------------------------
if (len(sys.argv) == 6):
    model_C_sales = int(sys.argv[1])
    model_D_sales = int(sys.argv[2])
    model_R_sales = int(sys.argv[3])
    part_no = sys.argv[4]
    time_delta = int(sys.argv[5])
else:
    model_C_sales = 12
    model_D_sales = 15
    model_R_sales = 18
    part_no = 'a42CLDR'
    time_delta = 21

model_C_sales = model_C_sales * time_delta
model_D_sales = model_D_sales * time_delta
model_R_sales = model_R_sales * time_delta



#---------------------------------------------------
#                    FORCASTING
# Put together next week's forecast so we can predict
# the part quantity needed later.
#---------------------------------------------------
sales_forecast = {'Model C': [model_C_sales], 'Model D': [model_D_sales], 'Model R': [model_R_sales]}
df_forecast = pd.DataFrame(sales_forecast)



#---------------------------------------------------
#              CREATE TRAINING DATASET
# Collects all data and combines datasets to create
# a training dataset in the format we want.
#---------------------------------------------------
print('Collecting last year worth of data to build model')
final_df = util.collect_format_data(part_no, time_delta)

# Split into test/train sets
X = final_df.drop(columns="goal_parts", axis=1, inplace=False)
y = final_df.goal_parts.values
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
data_dmatrix = xgb.DMatrix(data=X, label=y)



#---------------------------------------------------
#              CREATE REGRESSION MODEL
#---------------------------------------------------
params = {"objective": "reg:squarederror", 'colsample_bytree': 0.3, 'learning_rate': 0.1,
          'max_depth': 5, 'alpha': 10}
xg_reg = xgb.train(params=params, dtrain=data_dmatrix, num_boost_round=50)



#---------------------------------------------------
#                  VALIDATE MODEL
# Do some cross-validation to make sure model
# is not terrible.
#---------------------------------------------------
cv_results = xgb.cv(dtrain=data_dmatrix, params=params, nfold=10,
                    num_boost_round=50, early_stopping_rounds=10, metrics="rmse", as_pandas=True, seed=123)



#---------------------------------------------------
#              PLOT FEATURE IMPORTANCE
#---------------------------------------------------
xgb.plot_importance(xg_reg)



#---------------------------------------------------
#           ROOT MEAN SQUARE ERROR (RMSE)
#---------------------------------------------------
final_rmse = cv_results["test-rmse-mean"].iat[-1]
print("Root Mean Std. Err : ", final_rmse)



#---------------------------------------------------
#                  FINAL PREDICTION
# Show predictions for time range using forecasted
# car production
#---------------------------------------------------
data_newforecast = xgb.DMatrix(data=df_forecast)
new_preds = xg_reg.predict(data_newforecast)
print("Predicted weekly production for Part No ", part_no, ": ", new_preds[0])



#---------------------------------------------------
#                 SAVE PICKLE FILE
#---------------------------------------------------
picklefile = part_no + '-' + str(time_delta) + '.pickle'
pickle.dump(xg_reg, open(picklefile, 'wb'))
