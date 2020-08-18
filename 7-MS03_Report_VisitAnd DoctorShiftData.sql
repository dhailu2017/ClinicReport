--*************************************************************************--
-- Title: Final
-- Author: RRoot
-- Desc: Incremental loading data from [a4psqldbsever.database.windows.net].[Patients] database and [LAPTOP-GRDPVG28\SQL2019].[Doctors] databse into 
---[a4psqldbsever.database.windows.net] server in DWClinicReportDataDejeneHail datawarehouse database with SQL code
-- Change Log: When,Who,What
-- 2020-06-08, DejenHailu, Created SQL code
--**************************************************************************--
USE  DWClinicReportDataDejeneHail;
go
SET NoCount ON;
go

 
CREATE or ALTER VIEW vReportVisitData
/* Author: RRoot
** Desc: Extracts and transforms data for vReportVisitData
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
As
SELECT [VisitKey] = ID
             , [ClinicName]
			 , [ClinicCity]
			 , [ClinicZip]
			 , VisitsDate =  V.VisitsDate
			 , [FullDateName]
			 ,[MonthName]
            , [YearName]
			, [DoctorFullName]
			, [PatientFullName]
			, [PatientCity]
			,[ProcedureName]
			, [ProcedureVistCharge] 
           
FROM (SELECT  ID, [ClinicID] = Clinic
          , VisitsDate =  SUBSTRING(CONVERT(nvarchar(50), [date], 112), 1, 8) 
		  , PatientID = [Patient]
		  , DoctorID = [Doctor]
		  , ProcedureID = [Procedure]
		  ,[ProcedureVistCharge] = [Charge]
          FROM [StagingDWClinicReportDataDHailu]..[ETLStagingVisits]) AS V 
JOIN [dbo].[DimClinics] AS DC
ON V.ClinicID = DC.[ClinicID] 
JOIN  [dbo].[DimPatients] AS Pt
ON V.PatientID = Pt.[PatientID]
JOIN [dbo].[DimDoctors] AS DDr
ON V.DoctorID = DDr.DoctorID
JOIN [dbo].[DimProcedures] AS P
ON V.[ProcedureID] = P.[ProcedureID]
JOIN [dbo].[DimDates]  AS D
ON V.[VisitsDate] = D.[FullDate]
GO 
/* Testing Code: 
 Select * From vReportVisitData;
*/



CREATE or ALTER VIEW vReportDoctorShiftData
/* Author: RRoot
** Desc: Extracts and transforms data for vReportDoctorShiftData
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
AS
SELECT [DoctorsShiftID]
             , [ClinicName]
			 , [ClinicCity]
			 , [FullDateName]
			 , [MonthName]
             , [YearName]
			 , [DoctorFullName]
			 , [ShiftStart]
			 , [ShiftEnd] 
			, [HoursWorked] = Abs(DATEDIFF(HOUR, [ShiftStart], [ShiftEnd]))
FROM (SELECT DoctorsShiftID, ShiftDate, ShiftID, DoctorID, ClinicID = CASE WHEN ClinicID = 1 THEN 100 WHEN  ClinicID = 2 THEN 200 WHEN  ClinicID =3 THEN 300 ELSE 'uknown' END
        FROM [StagingDWClinicReportDataDHailu]..[ETLStagingDoctorShifts]) AS DS
JOIN [dbo].[DimDoctors] AS Dr
ON DS.DoctorID = Dr.DoctorID 
JOIN [dbo].[DimClinics] AS DCc 
ON DS.ClinicID= DCc.ClinicID
JOIN [dbo].[DimShifts] AS DSt
ON DS.ShiftID = DSt.ShiftID
JOIN [dbo].[DimDates] AS DDt
ON DS.ShiftDate = DDt.FullDate
GO 
/* Testing Code:
 Select * From vReportDoctorShiftData;
*/





CREATE or ALTER PROCEDURE pETLFactDoctorShifts
/* Author: RRoot
** Desc: Inserts data into DimShifts
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 BEGIN
  Declare @RC int = 0;
  BEGIN Try

		With ChangedFactDoctorShift 
		As(
			    SELECT [DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked] FROM [DWClinicReportDataDejeneHail]..vETLFactDoctorShifts
 				Except            			
			    SELECT [DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked] From DWClinicReportDataDejeneHail..[FactDoctorShifts]
		   )UPDATE DWClinicReportDataDejeneHail.[dbo].[FactDoctorShifts]
		  SET [DoctorsShiftID] = [DoctorsShiftID], [ShiftDateKey] = [ShiftDateKey], [ClinicKey] = [ClinicKey], [ShiftKey] = [ShiftKey],
		  [DoctorKey] = [DoctorKey]
		   WHERE [DoctorsShiftID] IN (Select [DoctorsShiftID] From ChangedFactDoctorShift);
		

    --; 2)For INSERT or UPDATES: Add new rows to the table
		With AddedORChangedDShifts 
		As(
			    Select [DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked] FROM [DWClinicReportDataDejeneHail].dbo.vETLFactDoctorShifts
 				Except            			
			    Select [DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked] From DWClinicReportDataDejeneHail..[FactDoctorShifts]
		)INSERT INTO DWClinicReportDataDejeneHail.[dbo].[FactDoctorShifts]
        ([DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked])
         SELECT 
		        [DoctorsShiftID]
				, [ShiftDateKey]
				, [ClinicKey]
				, [ShiftKey]
				, [DoctorKey]
				, [HoursWorked] 

FROM DWClinicReportDataDejeneHail..vETLFactDoctorShifts
WHERE [DoctorsShiftID] IN (Select [DoctorsShiftID] From AddedORChangedDShifts);
      
    -- ; 3) For Delete: Change the IsCurrent status to zero
		With DeletedShifts 
			As(
			    Select [DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked] FROM [DWClinicReportDataDejeneHail]..[FactDoctorShifts] 
 				Except            			
			    Select [DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked] From DWClinicReportDataDejeneHail..vETLFactDoctorShifts
   		)DELETE DWClinicReportDataDejeneHail..[FactDoctorShifts]
		   WHERE [DoctorsShiftID] IN (Select [DoctorsShiftID] From DeletedShifts) ---=BUT DELETE COMMAND NOT RECOMENDED IN dATA WAREHOUSE DATABASE
	   ;
      Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message() 
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFactDoctorShifts;
 Print @Status;
 Select * From [DWClinicReportDataDejeneHail]..[FactDoctorShifts]
*/



