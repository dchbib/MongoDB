#!/usr/bin/python3

from numpy import nan

def pipeline(aggregationModel: str)->list: 
    """
    return a list as pipeline for mongoDB
    """

    aggre_filter = [
                        { "$match": {
                                "InvoiceNo": { 
                                    "$exists": True,
                                   
                                },
                                "Description": { 
                                    "$exists": True, 
                                },
                                "CustomerID": { 
                                    "$ne" : nan,
                                },
                                "Country": { 
                                    "$ne" : nan,
                                },
                                "Quantity": { 
                                    "$gt" : 0,
                                }, 
                            }    
                        }
                    ]
                
    aggre_invoice = [
        
                        {"$group": 
                             
                             { "_id": {"InvoiceNo" : "$InvoiceNo", "product" : "$Description", "country" : "$Country"},
                             "TotalQuantity" : {"$sum" : "$Quantity"},
                             "TotalAmount": { "$sum": { "$multiply": [ "$UnitPrice", "$Quantity" ] } },
                        
                          }},                      
                    
                     ]
                                

    aggre_product = [                
                         {"$group": 
                             {
                             "_id": "$InvoiceNo",
                             "country": { "$push": "$Country" },
                             "CustomerID": { "$push": "$CustomerID" },
                             "unitPrice": { "$push": "$UnitPrice" },
                             "product": { "$push": "$Description" },

                             "TotalQuantity" : {"$sum" : "$Quantity"},
                             "TotalAmount": { "$sum": { "$multiply": [ "$UnitPrice", "$Quantity" ] } },
                                                          
                          }},
                             
                         {"$sort":{"TotalQuantity":-1}},
                         {"$limit": 1},

                    ]


    aggre_customer = [
                        {"$group": 
                             {
                             "_id": "$InvoiceNo",
                             "country": { "$push": "$Country" },
                             "CustomerID": { "$push": "$CustomerID" },
                             "unitPrice": { "$push": "$UnitPrice" },
                             "product": { "$push": "$Description" },

                             "TotalQuantity" : {"$sum" : "$Quantity"},
                             "TotalAmount": { "$sum": { "$multiply": [ "$UnitPrice", "$Quantity" ] } },
                                                          
                          }},
                             
                        {"$sort":{"TotalAmount":-1}},
                         {"$limit": 1},
                    ]
    
    
    aggre_price = [                    
                    
                    {"$group": 
                             {
                             "_id": "$InvoiceNo",
                             "country": { "$push": "$Country" },
                             "CustomerID": { "$push": "$CustomerID" },
                             "unitPrice": { "$push": "$UnitPrice" },
                             "product": { "$push": "$Description" },

                             "TotalQuantity" : {"$sum" : "$Quantity"},
                             "TotalAmount": { "$sum": { "$multiply": [ "$UnitPrice", "$Quantity" ] } },
                                                          
                          }},
                             
                    {
                    "$group" : 
                        {"_id" : "null",
            
                        "AverageUnitPrice" : {"$avg" : {"$sum" : "$unitPrice"}},
                        
                        }},  
                    ]
      
    pipeline = aggre_filter

    listOfModels = ["aggre_invoice", "aggre_customer", "aggre_product", "aggre_price"]
    models = [aggre_invoice, aggre_customer, aggre_product, aggre_price]
    
    for i in range(len(listOfModels)):
        if aggregationModel == listOfModels[i]:
            pipeline += models[i]
            break
        else:
            pass
        
    return pipeline
                    