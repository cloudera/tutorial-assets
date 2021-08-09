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
#
#  Description:
#
#  Author(s): Nicolas Pelaez, George Rueda de Leon
#***************************************************************************
import tensorflow.keras as keras
import tensorflow as tf
import os
import time, datetime
import shutil
import os.path
# CDSW library is only needed when code is ran within CML-Experiments.
# Comment out when running outside of CML-Experiments.
import cdsw

#---------------------------------------------------
#                COPY LIBRARY FILE
# Allows us to use cuDNN 8.0.5 installation, which
# is the default in this runtime
#---------------------------------------------------
if os.path.isfile('/usr/local/cuda/lib64/libcusolver.so.10') == False:
  shutil.copy2('/usr/local/cuda/lib64/libcusolver.so.11', '/usr/local/cuda/lib64/libcusolver.so.10')



#---------------------------------------------------
#                 GPU AVAILABILITY
# Display GPUs that are available to Tensorflow
#---------------------------------------------------
print(tf.config.list_physical_devices('GPU'))



#---------------------------------------------------
#                 MODEL TRAINING
# Use CIFAR dataset to augment the pre-existing ResNet model
# and split it into appropriate testing and training datasets.
#
# CIFAR homepage: https://www.cs.toronto.edu/~kriz/cifar.html
#---------------------------------------------------
(x_train, y_train), (x_test, y_test) = keras.datasets.cifar10.load_data()
x_train = keras.applications.resnet50.preprocess_input(x_train)
x_test = keras.applications.resnet50.preprocess_input(x_test)
y_train = keras.utils.to_categorical(y_train, 10)
y_test = keras.utils.to_categorical(y_test, 10)

# Input size we want for the ResNet model
t_size = keras.Input(shape=(32, 32, 3))

# Important: Mirrored Strategy is what allows us to automatically
#            leverage CPUs and/or GPUs that are available on the system.
mirrored_strategy = tf.distribute.MirroredStrategy()

with mirrored_strategy.scope():
  rnv2_model = keras.applications.ResNet50V2(include_top=False,
                                             weights="imagenet",
                                             input_tensor=t_size)

  # For now, we only want to train the last 10 layers of
  # the model - freeze all other layers
  for layer in rnv2_model.layers[:143]:
    layer.trainable = False

  # Create definitions of the new layers (this could be sub-optimal)
  model = keras.models.Sequential()
  model.add(keras.layers.Lambda(lambda image: tf.image.resize_with_pad(image, 32, 32)))
  model.add(rnv2_model)
  model.add(keras.layers.Flatten())
  model.add(keras.layers.BatchNormalization())
  model.add(keras.layers.Dense(256, activation='relu'))
  model.add(keras.layers.Dropout(0.5))
  model.add(keras.layers.BatchNormalization())
  model.add(keras.layers.Dense(128, activation='relu'))
  model.add(keras.layers.Dropout(0.5))
  model.add(keras.layers.BatchNormalization())
  model.add(keras.layers.Dense(64, activation='relu'))
  model.add(keras.layers.Dropout(0.5))
  model.add(keras.layers.BatchNormalization())
  model.add(keras.layers.Dense(10, activation='softmax'))

  check_point = keras.callbacks.ModelCheckpoint(filepath="cifar10.h5",
                                              monitor="val_acc",
                                              mode="max",
                                              save_best_only=True,
                                              )

  model.compile(loss='categorical_crossentropy',
                optimizer=keras.optimizers.RMSprop(learning_rate=2e-5),
                metrics=['accuracy'])

  # Adjust batch size to make use of extra replicas
  # when doing multi GPU training
  BATCH_SIZE_PER_REPLICA = 32
  BATCH_SIZE = BATCH_SIZE_PER_REPLICA * mirrored_strategy.num_replicas_in_sync

  # Start timer: begin model training
  train_time_start = time.perf_counter()

  # Train the model
  history = model.fit(x_train, y_train, batch_size=BATCH_SIZE, epochs=5, verbose=1,
                        validation_data=(x_test, y_test),
                        callbacks=[check_point])

  # Stop timer: end model training
  train_time_end = time.perf_counter()

  # Elapsed time for training model
  train_time = train_time_end - train_time_start



#---------------------------------------------------
#             PERFORMACE METRICS
# - display model summary
# - save the model
# - display elapsed time for training the model
#---------------------------------------------------
model.summary()
model.save("cifar10.h5")

totalTime = datetime.timedelta(seconds = train_time)
print(f"Training model took {totalTime}")

# CDSW library is only needed when code is ran within CML-Experiments.
# Comment out when running outside of CML-Experiments.
cdsw.track_metric('Train Time', str(totalTime))
