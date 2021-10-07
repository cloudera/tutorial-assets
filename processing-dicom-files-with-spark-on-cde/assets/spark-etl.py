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
#  Source File Name: spark-etl.py
#
#  Description: Transform the DICOM files produced by the MRI and into
#               PNG images.
#
#  Author(s): Michael Ridley
#***************************************************************************
import botocore
import boto3
import pydicom
from pydicom.pixel_data_handlers.util import apply_voi_lut
from pydicom.errors import InvalidDicomError
import PIL
import numpy as np
import pyspark
import io
import sys
import os

IMG_PX_SIZE = 224

S3_BUCKET = 's3BucketName'
S3_INPUT_KEY_PREFIX = 's3FolderName'
S3_INPUT_PATH = 's3a://' + S3_BUCKET + '/' + S3_INPUT_KEY_PREFIX
S3_OUTPUT_KEY_PREFIX = S3_INPUT_KEY_PREFIX + '_processed_images'

INPUT_PATH = S3_INPUT_PATH
OUTPUT_PATH = S3_OUTPUT_KEY_PREFIX


def process_image(rdd_image):
    image_bytes = io.BytesIO(rdd_image.content)
    data = b''
    png_image = io.BytesIO()
    try:
        dicom_conversion_result = "SUCCESS"
        ds = pydicom.dcmread(image_bytes, force=True)
        data = apply_voi_lut(ds.pixel_array, ds)
        data = np.amax(data) - data
        data = data - np.min(data)
        data = (data * 255).astype(np.uint8)
    except InvalidDicomError as err:
        dicom_conversion_result = err
    except:
        dicom_conversion_result = sys.exc_info()[0]
    try:
        image_conversion_result = "SUCCESS"
        height_px = len(data)
        width_px = len(data[0])
        PIL.Image.frombytes("L", (height_px, width_px), data).resize((IMG_PX_SIZE, IMG_PX_SIZE)).save(
            png_image, 'png')
    except OSError:
        image_conversion_result = "FAIL"
    except IndexError:
        image_conversion_result = "FAIL"
    image_png_rdd_element = {"path": rdd_image.path,
                             "modificationTime": rdd_image.modificationTime,
                             'image_conversion_result': image_conversion_result,
                             'dicom_conversion_result': dicom_conversion_result,
                             "content": png_image}
    return image_png_rdd_element


def write_processed_image(rdd_image):
    png_image = rdd_image['content']
    scan_type = os.path.split(os.path.split(rdd_image['path'])[0])[1]
    patient_dir = os.path.split(os.path.split(os.path.split(rdd_image['path'])[0])[0])[1]
    output_key = OUTPUT_PATH + '/' + patient_dir + '/' + scan_type + '/' + os.path.basename(rdd_image['path']) + '.png'
    try:
        s3 = boto3.resource('s3')
        put_result = "SUCCESS"
        image_object = s3.Object(S3_BUCKET, output_key)
        png_image.seek(0, 0)
        image_object.put(Body=png_image)
    except botocore.exceptions.ClientError as err:
        put_result = err
    return {'output_key': output_key,
            'image_size': sys.getsizeof(png_image),
            'image_conversion_result': rdd_image['image_conversion_result'],
            'dicom_conversion_result': rdd_image['dicom_conversion_result'],
            'put_result': put_result }


sc = pyspark.sql.SparkSession.builder.getOrCreate()
dicom_images = sc.read.format('binaryFile').option("recursiveFileLookup", "true").load(INPUT_PATH)

print("Dicom image count " + str(dicom_images.count()))
processed_images = dicom_images.rdd.map(process_image)
filenames = processed_images.map(write_processed_image).collect()
for filename in filenames:
    print(filename)
exit(0)
