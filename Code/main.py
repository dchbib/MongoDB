#!/usr/bin/python3

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from MongoDB import ManageDataset
from PipelineStages import pipeline


pathFile = 'OnlineRetail.xlsx'

#Definition of the DataBase class and his objects 
DataBase = ManageDataset(pathFile, username='User', password='Password')

dataSet = DataBase.prepareFilesToMongoDB()

dbOnServer = DataBase.insertDataToMongoDB(projectName='OnlineRetail', datasetSerie=dataSet)

# The customer spent the most money // CustomerId : 16446 with Amount = 168469.6
information_lookingFor = "aggre_product"  # Possible values : "aggre_product", "aggre_customer",  "aggre_price", "aggre_invoice"
pipelines = pipeline(information_lookingFor) 
groupedata = DataBase.aggregations(dbOnServer, pipelines)  

print(groupedata[1][0])

# The product sold the most // 'PAPER CRAFT , LITTLE BIRDIE'
information_lookingFor = "aggre_customer" #   
pipelines = pipeline(information_lookingFor) 
groupedata = DataBase.aggregations(dbOnServer, pipelines)  

# The average of unit price // avg = 66.8968
information_lookingFor = "aggre_price" #   
pipelines = pipeline(information_lookingFor) 
groupedata = DataBase.aggregations(dbOnServer, pipelines)  

# Groupby invoice and plot the charts 
information_lookingFor = "aggre_invoice"
pipelines = pipeline(information_lookingFor) 
groupedata = DataBase.aggregations(dbOnServer, pipelines)  
DataBase.graphics(groupedata[1][0])

#To delete data 
DataBase.drop_db(projectName='OnlineRetail')
