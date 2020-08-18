# -*- coding: utf-8 -*-
'''*************************************************************************--
-- Desc: This script moves data from a CSV file to MongoDB Atlas cloud
-- Change Log: When,Who,What
-- 2020-05-15,RRoot,Created File
--**************************************************************************'''
import pymongo

strCon = "mongodb+srv://test3:test3@cluster0-omzbi.azure.mongodb.net/ClinicReportDB?retryWrites=true&w=majority"

objCon = pymongo.MongoClient(strCon)
db = objCon["ClinicReportDB"]  # database
objCol = db['PatientVisits']  # collection

# Verify Data Was Uploaded
curData = objCol.find({"ClinicName": "Bellevue"})

objF = open('C:\_BISolutions\FinalC2_DejeneHailu\DataFile\BellevuePatientVisitsReportData.csv', 'w')
tplCol = ("VisitDate","PatientFullName","PatientCity","DoctorFullName","ProcedureName","ProcedureVisitCharge")
objF.write("%s,%s,%s,%s,%s,%s\n" % tplCol)
for val in curData:
    objF.write("%s,%s,%s,%s,%s,%d\n" % (val["VisitDate"],
                                         val["PatientFullName"],
                                         val["PatientCity"],
                                         val["DoctorFullName"],
                                         val["ProcedureName"],
                                         float(val["ProcedureVisitCharge"])))
objF.close()


