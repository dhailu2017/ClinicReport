# -*- coding: utf-8 -*-
'''*************************************************************************--
-- Desc: This script moves data from a CSV file to MongoDB Atlas cloud
-- Change Log: When,Who,What
-- 2020-05-15,RRoot,Created File

NOTE: Make sure to Create the Database and Collection manually
      on Atlas for best results!
--**************************************************************************'''
import pymongo
import csv
import datetime as dt
print('Start:', dt.datetime.now())  # Takes about 2 minutes to run!

strFilePath = "..C:\\_BISolutions\\FinalC2_DejeneHailu\\DataFile\\DoctoreShiftReportData"

strCon = "mongodb+srv://test3:test3@cluster0-omzbi.azure.mongodb.net/ClinicReportDB?retryWrites=true&w=majority"

objCon = pymongo.MongoClient(strCon)
db = objCon["ClinicReportDB"]  # Creates or Connects a database
if db["DoctorsSchedule"].drop():
    print('collection dropped')  # Without this you get duplicates when ran twice
objCol = db['DoctorsSchedule']  # creates a new collection if it does not exist

# Selecting the last _id in the document
intLastID = 0
curLastRow = objCol.find().sort([("_id", -1)]).limit(1)  # -1 get last and +1 gets first
for row in curLastRow:
    intLastID = row["_id"]
    print("Last ID Was >>> ", intLastID)

# Import with Insert (and a new _id added!)
try:
    fPatientVisits = open(strFilePath, 'r', encoding='utf-8-sig')
    rPatientVisits = csv.DictReader(fPatientVisits)
    for row in rPatientVisits:
        intLastID += 1
        row["_id"] = intLastID
        objCol.insert_one(row)
except Exception as e:
    print('LineID', intLastID, e)  # Adding the line number help with troubleshooting
print('Done:', dt.datetime.now())
