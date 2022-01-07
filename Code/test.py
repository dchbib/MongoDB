#!/usr/bin/python3

import unittest
from io import BytesIO
import pandas as pd 
import pymongo
from pymongo import MongoClient

from MongoDB import ManageDataset

mongoclient = MongoClient('mongodb+srv://dchbib:q7VmFR79HCFkUTKP@cluster0.mj0kf.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')


class MongoDBTest(unittest.TestCase):
            
        def test_prepareFilesToMongoDB(self):
            """
            Test the function that creates a dataset in MongoDB format
            """
            # Define a DF as the contents for your excel file.
            dfr = pd.DataFrame({'A': [1.1, 2.7, 5.3], 'B': [2, 10, 9], 'C': [3.3, 5.4, 1.5], 
                                'D': [4, 7, 15]}, columns = ['A', 'B', 'C', 'D']) 
            # Create your in memory BytesIO file.
            output = BytesIO()
            writer = pd.ExcelWriter(output, engine="xlsxwriter")
            dfr.to_excel(writer, sheet_name="Sheet1", index=False)
            writer.save()
            output.seek(0)  # Contains the Excel file in memory file.

            with open("testout.xlsx",'wb') as out: ## Open temporary file as bytes
                out.write(output.read())   

            result_data = ManageDataset(pathFile="testout.xlsx", username='dchbib', password='q7VmFR79HCFkUTKP').prepareFilesToMongoDB()
            excpected_data = [[{'A': 1.1, 'B': 2, 'C': 3.3, 'D': 4},
                          {'A': 2.7, 'B': 10, 'C': 5.4, 'D': 7},
                          {'A': 5.3, 'B': 9, 'C': 1.5, 'D': 15}
                         ]]
            
            self.assertEqual(excpected_data, result_data)
            self.assertEqual(len(result_data), 1)
        
        
        def test_insertDataToMongoDB(self):
            """
            Test the function that inserts data on the project
            """
            data = [[{'A': 1.1, 'B': 2, 'C': 3.3, 'D': 4},
                     {'A': 2.7, 'B': 10, 'C': 5.4, 'D': 7},
                     {'A': 5.3, 'B': 9, 'C': 1.5, 'D': 15}
                    ]]
            
            result_data = ManageDataset(pathFile="testout.xlsx", username='dchbib', password='q7VmFR79HCFkUTKP').insertDataToMongoDB(projectName='Test_OnlineRetail', datasetSerie=data)
            
            collection = result_data['collection0']
            nbOfDocuements= collection.count_documents({})

            self.assertIsInstance(result_data, pymongo.database.Database)
            self.assertEqual(nbOfDocuements, 3)


        def test_drop_db(self):
            """
            Test the function that delete data 
            """
            #drop the data used in the previous test
            ManageDataset(pathFile="testout.xlsx", username='dchbib', password='q7VmFR79HCFkUTKP').drop_db(projectName='Test_OnlineRetail')
                       
            data_db_after_delete = mongoclient.list_databases()
            data_db_after_delete_list = [e['name'] for e in data_db_after_delete]
            
            testValue = 'Test_OnlineRetail' in data_db_after_delete_list
            
            self.assertFalse(testValue)
            



if __name__=="__main__":

    unittest.main()
    
    