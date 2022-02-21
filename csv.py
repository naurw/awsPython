#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 21 16:40:02 2022

@author: William
"""

import pandas as pd 
import boto3 
from boto3 import client 
import csv 

s3_client = boto3.client("s3", aws_access_key_id='insert_here', aws_secret_access_key='insert_here')
s3_resource = boto3.resource('s3', aws_access_key_id='insert_here', aws_secret_access_key='insert_here')
s3 = boto3.resource('s3')
s3bucket = s3.Bucket('myBucket')

csvList = []
for file in s3_resource.Bucket('myBucket').objects.all(): 
    tempList = file.key
    #print(f'"{tempList}"', end= ',\n')
    print('"{}"'.format(tempList), end=',\n')
    csvList.append(tempList)

obj = s3_resource.Object('myBucket', csvList[0])
obj_data =obj.get()['Body'].read().decode('utf-8').splitlines()
lines = csv.reader(obj_data)
headers = next(lines)
df = pd.DataFrame(lines, columns=headers).sample(200)
print(csvList[0])
df.info()
    
for i in csvList:
    fileName = i 
    fileNamenoCSV = fileName.replace(".csv","")
    obj = s3_resource.Object('myBucket', fileName)
    obj_data =obj.get()['Body'].read().decode('utf-8').splitlines()
    lines = csv.reader(obj_data)
    headers = next(lines)
    temp = pd.DataFrame(lines, columns=headers)
    temp
    
