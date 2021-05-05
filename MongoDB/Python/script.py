import pymongo
from datetime import datetime
from bson.objectid import ObjectId

def insert_row(collection):
    mydict = { "name": "EL Tonsey", "address": "Lowstreet 27", "company": "AnalyticaBI" , "createdat" : datetime.now()}

    flag = collection.insert_one(mydict)
    
    print(flag.inserted_id)


def insert_rows(collection):
    mylist = [
      { "name": "Amy", "address": "Apple st 652", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Hannah", "address": "Mountain 21", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Michael", "address": "Valley 345", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Sandy", "address": "Ocean blvd 2", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Betty", "address": "Green Grass 1", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Richard", "address": "Sky st 331", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Susan", "address": "One way 98", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Vicky", "address": "Yellow Garden 2", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Ben", "address": "Park Lane 38", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "William", "address": "Central st 954", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Chuck", "address": "Main Road 989", "company": "AnalyticaBI" , "createdat" : datetime.now()},
      { "name": "Viola", "address": "Sideway 1633", "company": "AnalyticaBI" , "createdat" : datetime.now()}
    ]
    
    flag = collection.insert_many(mylist)
    
    print(flag.inserted_ids)

def find_first_occurence(collection):
    row = collection.find_one()
    print(row);
    
    
def find_all_occurences(collection):
    for row in collection.find():
        print(row);
    
def find_record_by_id(collection, id):
    myquery = {  "_id": ObjectId(id) }

    result = collection.find_one(myquery)
    print(result)

def find_data_sorted(collection):
    result = collection.find().sort("createdat")
    #result = collection.find().sort("createdat", -1) #desending
    for row in result:
        print(row)

def delete_record_by_id(collection, id):
     myquery = {  "_id": ObjectId(id) }
     result = collection.delete_one(myquery)
    
     print(result.deleted_count)

def delete_collection_data(collection):
    result = collection.delete_many({});
    print(result.deleted_count)
    
def drop_collection(collection):
    collection.drop()
    

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Datawarehouse"]

facebook_collection = mydb["Facebook"]

#insert_row(facebook_collection)
#insert_rows(facebook_collection) 
#find_first_occurence(facebook_collection)
#find_all_occurences(facebook_collection)
#find_record_by_id(facebook_collection, "id")
#find_data_sorted(facebook_collection)
#delete_record_by_id(facebook_collection, "6086a2aa76387161df2fdc12")
#delete_collection_data(facebook_collection)
#collection_ist = mydb.list_collection_names();





