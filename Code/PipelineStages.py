#!/usr/bin/python3

from numpy import nan

def pipeline(aggregationModel: str)->list: 
    """
    return a list as pipeline for mongoDB
    """

    aggre_filter = [
                        { "$match": {
                                "Quantity": { 
                                    "$gt": 0, 
                                },
                                "InvoiceNo": { 
                                    "$exists": True, 
                                },
                                "Description": { 
                                    "$exists": True, 
                                },
                                "CustomerID": { 
                                    "$ne" : nan,
                                }
                            }    
                        }
                        ]
                
    aggre_invoice = [
        
                        {"$group": 
                             
                             { "_id": {"InvoiceNo" : "$InvoiceNo", "customerID" : "$CustomerID", "unitPrice": "$UnitPrice", "product" : "$Description", "country" : "$Country"},
                             "TotalQuantity" : {"$sum" : "$Quantity"},
                             "TotalAmount": { "$sum": { "$multiply": [ "$UnitPrice", "$Quantity" ] } },
                        
                          }},                      
                    
                    {"$match" : {"TotalQuantity" : {"$lte" : 100 } }},
                    
                    {"$project": {"_id" : "$_id.InvoiceNo", 
                                
                                "product" : "$_id.product",
                                "CustomerID" : "$_id.customerID",
                                "unitPrice" : "$_id.unitPrice",
                                "TotalAmount" : "$TotalAmount",
                                "TotalQuantity" :  "$TotalQuantity",
                                "country" : "$_id.country"

                                }
                    },
                        
                    ]
                                
                            
    aggre_product = [                
                   
                    {"$match" : {"TotalQuantity" : {"$gte" : 80 } }},
                    
                    {"$group" : 
                            {"_id" :  "$product", 
                             
                             "MostProductSold" :  { "$max": "$TotalQuantity" },
                    }},      
                    
                ]

    aggre_customer = [
                            
                    {"$group" : 
                            {"_id" :  "$CustomerID", 
                             
                             "MostCustomerSpent" :  { "$max": "$TotalAmount" },
                    }},
                            
        
                ]
    
    
    aggre_price = [{
                "$group" : 
                    {"_id" : "$product", 
        
                     "AverageUnitPrice" : {"$min" : "$unitPrice"},
                    
                    }},
                    
                {
                "$group" : 
                    {"_id" : "null",
        
                     "AverageUnitPrice" : {"$avg" : "$AverageUnitPrice"},
                    
                    }},  
                    
                ]
      
    pipeline = aggre_filter + aggre_invoice

    listOfModels = ["aggre_customer", "aggre_product", "aggre_price"]
    models = [aggre_customer, aggre_product, aggre_price]
    
    for i in range(len(listOfModels)):
        if aggregationModel == listOfModels[i]:
            pipeline += models[i]
            break
        else:
            pass
        
    return pipeline
                    