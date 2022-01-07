#!/usr/bin/python3

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from MongoDB import ManageDataset
from PipelineStages import pipeline


pathFile = 'OnlineRetail.xlsx'

#Definition of the DataBase class and his objects 
DataBase = ManageDataset(pathFile, username='dchbib', password='q7VmFR79HCFkUTKP')

dataSet = DataBase.prepareFilesToMongoDB()

dbOnServer = DataBase.insertDataToMongoDB(projectName='OnlineRetail', datasetSerie=dataSet)

# Groupby invoice and plot the charts 
information_lookingFor = "" # "aggre_invoice" by default   
pipelines = pipeline(information_lookingFor) 
groupedata = DataBase.aggregations(dbOnServer, pipelines)  

print(groupedata)
DataBase.graphics(groupedata[1][0])

# To obtain the average of unit price // avg = 3.2370318148
information_lookingFor = "aggre_price" #  "aggre_customer", "aggre_product", "aggre_price"
pipelines = pipeline(information_lookingFor) 
groupedata = DataBase.aggregations(dbOnServer, pipelines)  


#To delete data 
DataBase.drop_db(projectName='OnlineRetail')
