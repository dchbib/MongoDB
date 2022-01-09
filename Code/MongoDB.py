#!/usr/bin/python3

import matplotlib.pyplot as plt
import pandas as pd
import pymongo
from pymongo import MongoClient
import numpy as np


class ManageDataset:
    """
    It's a class to manage Dataset into/from the MongoDB 
    """
    def __init__(self, pathFile: str, username: str, password: str)->None:
        self.pathFile = pathFile
        self.username = username
        self.password = password
        
        
    def prepareFilesToMongoDB(self)->list:
        """
        Function to make dataset in the MongoDB format 
        """
        dataSet = [] # It is a list containing series of DataSet for MongoDB. 
        sheets = pd.ExcelFile(self.pathFile).sheet_names # Obtaining the entire of sheets in the excel file. If there is one sheet, The list dataSet will contain only one Dataset for MongoDB.
        for e in sheets:
            dfr = pd.read_excel(self.pathFile, sheet_name=e) # reading the sheet 
            dataSet.append([ {dfr.columns[i] : dfr[dfr.columns[i]][j] for i in range(len(dfr.columns))}  for j in range(len(dfr.values)) ]) # Make the data in json/MongoDB format with respect to the number of columns and their values in the each one. Using a comprehension list to avoid time complexity
            
        return dataSet
    
    @staticmethod
    def correct_encoding(dictionary: dict)->dict:
        """Correct the encoding of python dictionaries so they can be encoded to mongodb
        inputs
        -------
        dictionary : dictionary instance to add as document
        output
        -------
        new : new dictionary with corrected encodings"""
        new = {}
        for key1, val1 in dictionary.items():
            # Nested dictionaries
            if isinstance(val1, dict):
                val1 = correct_encoding(val1)

            if isinstance(val1, np.bool_):
                val1 = bool(val1)

            if isinstance(val1, np.int64):
                val1 = int(val1)

            if isinstance(val1, np.float64):
                val1 = float(val1)

            new[key1] = val1

        return new

    def connectionToMongoDB(function):
        """
        The decorator to check the connection to the Atlas user on the MongoDB server
        """
        def connexion(self, *arg1, **arg2):
            try:
                self.client = MongoClient('mongodb+srv://'+self.username+':'+self.password+\
                    '@cluster0.mj0kf.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
                print("Connected successfully!!!")
                return function(self, *arg1, **arg2)
            except Exception as e:
                print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        
        return connexion


    @connectionToMongoDB
    def insertDataToMongoDB(self, projectName: str, datasetSerie : list)->pymongo.database.Database:
        """
        Insert data on the project with respect to the projectName. 
        Creation of the collections with respect to the number of sheets in the excel file.
        """
        db = self.client.get_database(projectName)
        for mongoDBFormat,i in zip(datasetSerie, range(len(datasetSerie))): #Start with a mongoDBFormat (a sheet of excel file) as an element of the dataset series
            #for i in range(len(datasetSerie)): # Make a loop with respect to the number of sheet (mongoDBFormat is a list contain several datasets). Each sheet (dataset) corresponds to a collection
            collection = db["collection"+str(i)]
            try:
                collection.insert_many(mongoDBFormat)
            except pymongo.errors.InvalidDocument:
                # If we have several data types and you have to convert them
                dataAfterConversion = []
                for e in mongoDBFormat: # convert each dictionary in the dataset of the collection 
                    new_e = self.correct_encoding(e)
                    dataAfterConversion.append(new_e)    
                        
                collection.insert_many(dataAfterConversion)

            return db
            

    @connectionToMongoDB
    def drop_db(self, projectName: str,)->None:
        """
        drop data from a cluster MongoDB
        """
        self.client.drop_database(projectName)
        
        
    @connectionToMongoDB
    def aggregations(self, databaseOfProject: pymongo.collection.Collection, 
                     pipeline: list)->tuple:
        """
        Make groubby on the dataset with respect to given condition 
        Input: Project of mongoDB with several collections
        Output: a tuple that contain two elements. The first one is an aggregation pipeline and the second one is a list of dataFrame for each cursor.  
        """
        collections = databaseOfProject.list_collection_names()
        result_cursors, result_dataFrame, listToConvert_df = [], [], []
        for coll in collections:
            mycollection = databaseOfProject[coll]
            cursor = mycollection.aggregate(pipeline, allowDiskUse=True)    
            for e in cursor:
                print(e)
                listToConvert_df.append(e)

            dataFrame = pd.DataFrame(listToConvert_df) # convert the cursor result to a dataFrame
            cursor = mycollection.aggregate(pipeline, allowDiskUse=True) # apply the aggregate function a second time because we have already performed an operation (print) on the first.
                
            result_dataFrame.append(dataFrame)          
            result_cursors.append(cursor)
            
        return result_cursors, result_dataFrame



    def graphics(self, df):
        """
        Give a set of charts
        """       
        plt.figure()
        df["TotalAmount"].plot.hist(orientation="vertical", bins=100, cumulative=False, title="Price distribution")
        plt.xlabel("Price")
        plt.savefig("Price_distribution.png")
        #---------------plot products distribution----------------
        
        dff = df.groupby(["_id"]) 
        ratio = df["TotalAmount"] / df['TotalQuantity']
        
        ddf = pd.DataFrame({'ratio': ratio })
        Id = [str(e[0]) for e in dff["_id"]]
        
        fig, ax = plt.subplots()
        ddf.ratio.plot(color='b', style='.-') 
         
        ax.set_xticks(ddf.ratio.index)
        ax.set_xticklabels([], rotation=90)
        ax.set_ylabel('Ratio', fontsize=14)
        ax.set_xlabel('Invoice', fontsize=14)
        
        fig.tight_layout()
        plt.savefig("Ratio_price_quantity.png")
        
        """
        fig, ax = plt.subplots()
        listLabels = []
        df_pivot = pd.pivot_table(
	df.groupby(["product", "country"]).sum(),
	values="TotalQuantity",
	index="product",
	columns="country",
	aggfunc=np.sum)
        
        ax = df_pivot.plot(kind="bar", width=14)
        ax.set_xticklabels([t if not i%30 else "" for i,t in enumerate(ax.get_xticklabels())])
        plt.legend(ncol=3)
        ax.set_xlabel("Product")
        ax.set_ylabel("Frequency")
        fig.tight_layout()
        plt.savefig("Frequency_by_product_and_country.png", bbox_inches='tight')
        """