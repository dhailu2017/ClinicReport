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

CREATE or ALTER PROCEDURE pETLDropViewsProcedures
/* Author: RRoot
** Desc: Inserts data into DimPatients
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 BEGIN
  DECLARE @RC int = 0;
  BEGIN TRY
	If Exists(Select * from Sys.objects where Name = 'vETLDimPatients')
   Drop View vETLDimPatients;

	If Exists(Select * from Sys.objects where Name = 'pETLDimPatients')
   Drop Procedure pETLDimPatients;

	If Exists(Select * from Sys.objects where Name = 'vETLDimProcedures')
   Drop View vETLDimProcedures;

	If Exists(Select * from Sys.objects where Name = 'pETLDimProcedures')
   Drop Procedure pETLDimProcedures;

	If Exists(Select * from Sys.objects where Name = 'vETLFactVisits') 
   Drop View vETLFactOrders;

	If Exists(Select * from Sys.objects where Name = 'pETLFactVisits')
   Drop Procedure pETLFactVisits;

	If Exists(Select * from Sys.objects where Name = 'vETLDimClinics')
   Drop View vETLDimProducts;

	If Exists(Select * from Sys.objects where Name = 'pETLDimClinics')
   Drop Procedure pETLDimClinics;

	If Exists(Select * from Sys.objects where Name = 'vETLDimDoctors')
   Drop View vETLDimCustomers;

	If Exists(Select * from Sys.objects where Name = 'pETLDimDoctors')
   Drop Procedure pETLSyncDimCustomers;

	If Exists(Select * from Sys.objects where Name = 'vETLDimShifts')
   Drop View vETLDimCustomers;

	If Exists(Select * from Sys.objects where Name = 'pETLDimShifts')
   Drop Procedure pETLSyncDimCustomers;

	If Exists(Select * from Sys.objects where Name = 'vETLFactDoctorShifts')
   Drop View vETLDimCustomers;

	If Exists(Select * from Sys.objects where Name = 'pETLFactDoctorShifts')
   Drop Procedure pETLSyncDimCustomers;

	If Exists(Select * from Sys.objects where Name = 'pETLDimDates')
   DROP PROCEDURE pETLDimDates;
      SET @RC = +1
  END 
  TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLDropViewsProcedures;
 Print @Status;
 Select * From DWClinicReportDataDejeneHail..DimPatients
*/
GO

/****** [dbo].[DimPatients] ******/

CREATE or ALTER VIEW vETLDimPatients
/* Author: RRoot
** Desc: Extracts and transforms data for DimPatients
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
As
SELECT [PatientID] = [ID]
		   , [PatientFullName] = ISNULL([FName], 'Not entered Name')  + ' ' + ISNULL([LName], 'Not Entered Name')
		   , [PatientEmail] = LOWER([Email]) + '  ' + '(' + IIF(Patindex('%_@_%._%', LOWER([Email])) <> 0, 'Valid', 'InValid') + ')'
		   , [PatientCity] = ISNULL([City], 'City not entered')
		   , [PatientState] = ISNULL([State], 'State not given')
		   , [PatientZipCode] = ISNULL(CAST([ZipCode] AS nvarchar(10)), 'ZipCode not given') 
FROM [StagingDWClinicReportDataDHailu].[dbo].[ETLStagingPatients]
GO
/* Testing Code:
 Select * From vETLDimPatients;
*/

CREATE or ALTER PROCEDURE pETLDimPatients
/* Author: RRoot
** Desc: Inserts data into DimPatients
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 BEGIN
  DECLARE @RC int = 0;
  BEGIN TRY
    -- ETL Processing Code --
    -- NOTE: Performing the Update before an Insert makes the coding eaiser since there is only one current version of the data
    -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows 
		WITH ChangedPatients 
		AS(
			SELECT [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode]From DWClinicReportDataDejeneHail..vETLDimPatients
			EXCEPT
			SELECT [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DWClinicReportDataDejeneHail..DimPatients
             WHERE IsCurrent = 1 -- Needed if the value is changed back to previous value
		)UPDATE DWClinicReportDataDejeneHail.[dbo].DimPatients
		  SET EndDate = GetDate()
			 ,IsCurrent = 0
		   WHERE [PatientID] IN (SELECT [PatientID] FROM ChangedPatients);
		

    --; 2)For INSERT or UPDATES: Add new rows to the table
		WITH AddedORChangedPatients 
		AS(
			SELECT [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DWClinicReportDataDejeneHail..vETLDimPatients
			EXCEPT
			SELECT [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DWClinicReportDataDejeneHail..DimPatients
             WHERE IsCurrent = 1 -- Needed if the value is changed back to previous value
		)INSERT INTO DWClinicReportDataDejeneHail.[dbo].DimPatients
        ([PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode], [StartDate], [EndDate], [IsCurrent])
         SELECT
		      [PatientID]
			  , [PatientFullName]
			  , [PatientCity]
			  , [PatientState]
			  , [PatientZipCode]
              ,[StartDate] = GetDate()
              ,[EndDate] = Null
              ,[IsCurrent] = 1
FROM [DWClinicReportDataDejeneHail]..vETLDimPatients
WHERE [PatientID] IN (SELECT [PatientID] FROM AddedORChangedPatients);
      
    -- ; 3) For Delete: Change the IsCurrent status to zero
		WITH DeletedPatients 
			AS(
			    SELECT [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DWClinicReportDataDejeneHail..DimPatients
				 WHERE IsCurrent = 1 -- We do not care about row already marked zero!
 				EXCEPT            			
			    SELECT [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From vETLDimPatients
   		)UPDATE DWClinicReportDataDejeneHail.[dbo].DimPatients
		  SET EndDate = GetDate()
			 ,IsCurrent = 0
		   WHERE [PatientID] IN (Select [PatientID] From DeletedPatients)
	   ;
      SET @RC = +1
  END TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLDimPatients;
 Print @Status;
 Select * From DWClinicReportDataDejeneHail..DimPatients
*/
/* UPDATE [StagingDWClinicReportDataDHailu]..[ETLStagingPatients]
SET City = 'Seattle' WHERE ID = 2;
SELECT * FROM [StagingDWClinicReportDataDHailu]..[ETLStagingPatients];*/
GO


/****** [dbo].[DimProcedures] ******/
go 
CREATE or ALTER VIEW vETLDimProcedures
/* Author: RRoot
** Desc: Extracts and transforms data for DimProcedures
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
AS
SELECT  [ProcedureID] = [ID]
			, [ProcedureName] = ISNULL([Name], 'Name not entered')
			, [ProcedureDesc] = ISNULL([Desc], 'Not descriped')
			, [ProcedureCharge] = CAST(ISNULL([Charge], 0) AS money)
FROM [StagingDWClinicReportDataDHailu].[dbo].[ETLStagingProcedures]
GO
/* Testing Code:
 Select * From vETLDimProcedures;
*/

CREATE or ALTER PROCEDURE pETLDimProcedures
/* Author: RRoot
** Desc: Inserts data into DimPatients
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

		With ChangedProcedure 
		As(
			Select[ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From DWClinicReportDataDejeneHail..vETLDimProcedures AS VP
			Except
			Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From DWClinicReportDataDejeneHail..DimProcedures AS DP
		)UPDATE DWClinicReportDataDejeneHail.[dbo].DimProcedures
		  SET [ProcedureID] = [ProcedureID], [ProcedureName] = [ProcedureName], [ProcedureDesc] = [ProcedureDesc], [ProcedureCharge] = [ProcedureCharge]
		   WHERE [ProcedureID] IN (Select [ProcedureID] From ChangedProcedure);
		

    --; 2)For INSERT or UPDATES: Add new rows to the table
		With AddedORChangedProcedure 
		As(
            Select[ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From DWClinicReportDataDejeneHail..vETLDimProcedures 
			Except
			Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From DWClinicReportDataDejeneHail..DimProcedures
		)INSERT INTO DWClinicReportDataDejeneHail.[dbo].DimProcedures
        ([ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge])
         SELECT
		      [ProcedureID]
			  , [ProcedureName]
			  , [ProcedureDesc]
			  , [ProcedureCharge]
FROM DWClinicReportDataDejeneHail..vETLDimProcedures
WHERE [ProcedureID] IN (Select [ProcedureID] From AddedORChangedProcedure);
      
    -- ; 3) For Delete: Change the IsCurrent status to zero
		With DeletedProcedures 
			As(
			    Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From DWClinicReportDataDejeneHail..DimProcedures
 				Except            			
			    Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge] From DWClinicReportDataDejeneHail..vETLDimProcedures
   		)DELETE DWClinicReportDataDejeneHail..DimProcedures
		   WHERE [ProcedureID] IN (Select [ProcedureID] From DeletedProcedures) ---=BUT DELETE COMMAND NOT RECOMENDED IN dATA WAREHOUSE DATABASE
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
 Exec @Status = pETLDimProcedures;
 Print @Status;
 Select * From [DWClinicReportDataDejeneHail]..DimProcedures
*/
GO



/****** [dbo].[DimClinics] ******/
 
CREATE or ALTER VIEW vETLDimClinics
/* Author: RRoot
** Desc: Extracts and transforms data for DimProcedures
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
As
SELECT  [ClinicID] = CASE 
                          WHEN [ClinicID] = 1 THEN 100
                          WHEN [ClinicID] = 2 THEN 200
						  WHEN [ClinicID] = 3 THEN 300
						  ELSE 'ClinicID not Entered' END
           , [ClinicName] = ISNULL([ClinicName], 'Name not entered')
		   , [ClinicCity] = ISNULL([City], 'City not entered')
		   , [ClinicState] = ISNULL([State], 'State not entered')
		   , [ClinicZip] = ISNULL([Zip], 'zipCode not entered')
FROM [StagingDWClinicReportDataDHailu].[dbo].[ETLStagingClinic]
GO
/* Testing Code:
 Select * From [DWClinicReportDataDejeneHail]..vETLDimClinics;
*/

CREATE or ALTER PROCEDURE pETLDimClinics
/* Author: RRoot
** Desc: Inserts data into DimClinics
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 BEGIN
  DECLARE @RC int = 0;
  BEGIN TRY
		WITH ChangedClinic 
		AS(
			SELECT [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From DWClinicReportDataDejeneHail..vETLDimClinics AS VP
			EXCEPT
			SELECT [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From DWClinicReportDataDejeneHail..DimClinics AS DP
		)UPDATE [DWClinicReportDataDejeneHail].[dbo].[DimClinics]
		  SET [ClinicID] = [ClinicID], [ClinicName] = [ClinicName], [ClinicCity] = [ClinicCity], [ClinicState] = [ClinicState], [ClinicZip] = [ClinicZip]
		   WHERE [ClinicID] IN (Select [ClinicID] From ChangedClinic);
		

    --; 2)For INSERT or UPDATES: Add new rows to the table
		WITH AddedORChangedClinic
		AS(
            SELECT [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From DWClinicReportDataDejeneHail..vETLDimClinics 
			EXCEPT
			SELECT [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From DWClinicReportDataDejeneHail..DimClinics
		)INSERT INTO [DWClinicReportDataDejeneHail].[dbo].[DimClinics]
        ([ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip])
         SELECT
		      [ClinicID]
			  , [ClinicName]
			  , [ClinicCity]
			  , [ClinicState]
			  , [ClinicZip]

FROM DWClinicReportDataDejeneHail..vETLDimClinics
WHERE [ClinicID] IN (SELECT  [ClinicID] FROM AddedORChangedClinic);
      
    -- ; 3) For Delete: Change the IsCurrent status to zero
		WITH DeletedClinic 
			AS(
			    SELECT [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] FROM DWClinicReportDataDejeneHail..DimClinics
 				EXCEPT            			
			    SELECT [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] FROM DWClinicReportDataDejeneHail..vETLDimClinics
   		)DELETE DWClinicReportDataDejeneHail..DimClinics
		   WHERE [ClinicID] IN (SELECT [ClinicID] FROM DeletedClinic) ---=BUT DELETE COMMAND NOT RECOMENDED IN dATA WAREHOUSE DATABASE
	   ;
      SET @RC = +1
  END TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLDimClinics;
 Print @Status;
 Select * From [DWClinicReportDataDejeneHail]..DimClinics
*/
GO



/****** [dbo].[DimDates] ******/

CREATE or ALTER VIEW vETLDimDate
/* Author: RRoot
** Desc: Extracts and transforms data for DimProcedures
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
As
SELECT  ---[FullDate] = CAST([FullDate] AS datetime)
                [FullDate]    = CAST(SUBSTRING(CONVERT(nvarchar(50), [FullDate], 112), 1, 8) AS date)
            , [FullDateName] = DateName(weekday, [FullDate]) + ', ' + Convert(nVarchar(132), [FullDate], 110) -- [FullDateName]  
			, [MonthID] = Cast(Left(Convert(nVarchar(50), [FullDate], 112), 6) as int)
			, [MonthName] = CAST(DateName(month, [FullDate]) + ' - ' + DateName(YYYY,[FullDate]) AS nvarchar(100))
			, [YearID] = CAST(Year([FullDate]) AS int)
			, [YearName] = Cast(Year([FullDate] ) as nVarchar(50))                     
FROM [StagingDWClinicReportDataDHailu].[dbo].[ETLStagingDates]
GO
/* Testing Code:
 Select * From [DWClinicReportDataDejeneHail]..[vETLDimDate];
 select * from [StagingDWClinicReportDataDHailu].[dbo].[ETLStagingDates]
*/


CREATE or ALTER PROCEDURE pETLDimDates
/* Author: RRoot
** Desc: Inserts data into DimDates
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 BEGIN
  DECLARE @RC int = 0;
  BEGIN TRY
    ---TRUNCATE THE TABLE AND FILL
	--TRUNCATE TABLE [dbo].[DimDates];
	INSERT INTO [dbo].[DimDates] ([FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName])
	SELECT [FullDateName] = CONVERT(nvarchar(100), [FullDateName])
			   , [MonthID] = CAST([MonthID] AS int) 
			   , [MonthName] = CONVERT(nvarchar(100), [MonthName])
			   , [YearID] = CAST([YearID] AS int)
			   , [YearName] = CONVERT(nvarchar(50), [YearName])
			   , [FullDate] = [FullDate] 
FROM [DWClinicReportDataDejeneHail]..[vETLDimDate]
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
GO  
/* Testing Code:
 Declare @Status int; 
 Exec @Status = pETLDimDates;
 Print @Status; 
 Select * From [DWClinicReportDataDejeneHail].[dbo].[DimDates] order by DateKey; 
*/
GO




/****** [dbo].[FactVisits] ******/
 
CREATE or ALTER VIEW vETLFactVisits
/* Author: RRoot
** Desc: Extracts and transforms data for FactVisits
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
As
SELECT [VisitKey] = ID
          , VisitsDate =  V.VisitsDate 
		   --CAST(CONVERT(nvarchar(50), [date], 112) AS Date)
		  , [DateKey] = D.DateKey
		  , [ClinicKey] = DC.[ClinicKey]
		  , [PatientKey] = Pt. PatientKey
		  , [DoctorKey] = DDr.DoctorKey
		  , [ProcedureKey] = p.ProcedureKey
		  ,[ProcedureVistCharge] = v.[ProcedureVistCharge] 
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
 Select * From vETLFactVisits;
 SELECT * FROM [dbo].[DimDates]
 SELECT * FROM [DWClinicReportDataDejeneHail].[dbo].[FactVisits]
 SELECT * FROM [StagingDWClinicReportDataDHailu].[dbo].[ETLStagingVisits]
*/


CREATE or ALTER PROCEDURE pETLFactVisits
/* Author: RRoot
** Desc: Inserts data into FactVisits
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 BEGIN
  DECLARE @RC int = 0;
  BEGIN TRY
		WITH ChangedFactVisits 
		AS(
			SELECT [VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge] From DWClinicReportDataDejeneHail..vETLFactVisits 
			EXCEPT
			SELECT [VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge] From DWClinicReportDataDejeneHail..FactVisits 
		
		)UPDATE DWClinicReportDataDejeneHail.[dbo].[FactVisits]
		  SET [VisitKey] = [VisitKey], [DateKey] = [DateKey], [ClinicKey] = [ClinicKey], [PatientKey] = [PatientKey], [DoctorKey] = [DoctorKey], [ProcedureKey] = [ProcedureKey], [ProcedureVistCharge] = [ProcedureVistCharge]
		   WHERE [VisitKey] IN (Select [VisitKey] From ChangedFactVisits);
		

    --; 2)For INSERT or UPDATES: Add new rows to the table
		WITH AddedORChangedFactVisits
		AS(
 			SELECT [VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge] From DWClinicReportDataDejeneHail..vETLFactVisits 
			EXCEPT
			SELECT [VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge] From DWClinicReportDataDejeneHail..FactVisits 
		
		)INSERT INTO DWClinicReportDataDejeneHail.[dbo].[FactVisits]
        ([VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge])
         SELECT [VisitKey]
		           ,[DateKey]
				   , [ClinicKey]
				   , [PatientKey]
				   , [DoctorKey]
				   , [ProcedureKey]
				   , [ProcedureVistCharge]

FROM DWClinicReportDataDejeneHail..vETLFactVisits
WHERE [VisitKey] IN (SELECT  [VisitKey] FROM AddedORChangedFactVisits);
      
    -- ; 3) For Delete: Change the IsCurrent status to zero
		WITH DeletedFactVisits 
			AS(
			    SELECT [VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge] FROM DWClinicReportDataDejeneHail..FactVisits
 				EXCEPT            			
			    SELECT [VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge] FROM DWClinicReportDataDejeneHail..vETLFactVisits
   		
		)DELETE DWClinicReportDataDejeneHail..FactVisits
		   WHERE [VisitKey] IN (SELECT [VisitKey] FROM DeletedFactVisits) ---=BUT DELETE COMMAND NOT RECOMENDED IN dATA WAREHOUSE DATABASE
	   ;
      SET @RC = +1
  END TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLDimClinics;
 Print @Status;
 Select * From FactVisits
*/
GO




---------------------------------------------------------------
--SECOND METADA
---------------------------------------------------------------
/****** [dbo].[Dimdoctors] ******/
go 
CREATE or ALTER VIEW vETLDimdoctors
/* Author: RRoot
** Desc: Extracts and transforms data for Dimdoctors
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
As
SELECT [DoctorID]
           , [DoctorFullName] = cast([FirstName] + '  ' + [LastName] AS nvarchar(250))
		   , [DoctorEmailAddress] = CASE WHEN [EmailAddress] like '__%@%__.com' then LOWER([EmailAddress])
		                            ELSE 'Invalide Email' END	                        
		   , [DoctorCity] = ISNULL([City], 'City not Entered')
		   , [DoctorState] = CASE WHEN LEN(LTRIM([State])) = 2 THEN UPPER([State])
		                   ELSE 'Invalid Zip' END
		   , [DoctorZip] = ISNULL(Zip, 'Zip not Entered')
FROM [StagingDWClinicReportDataDHailu]..[ETLStagingDoctors]
GO
/* Testing Code:
 Select * From vETLDimdoctors;
*/

CREATE or ALTER PROCEDURE pETLDimdoctors
/* Author: RRoot
** Desc: Inserts data into Dimdoctors
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 BEGIN
  DECLARE @RC int = 0;
  BEGIN TRY
    -- ETL Processing Code --
    -- NOTE: Performing the Update before an Insert makes the coding eaiser since there is only one current version of the data
    -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows 
		WITH ChangedPatients 
		AS(
			SELECT [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState],[DoctorZip] From DWClinicReportDataDejeneHail..vETLDimdoctors
			EXCEPT
			SELECT [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState],[DoctorZip] From DWClinicReportDataDejeneHail..Dimdoctors 
		)UPDATE DWClinicReportDataDejeneHail.[dbo].Dimdoctors
		SET [DoctorID] = [DoctorID],  [DoctorFullName] = [DoctorFullName], [DoctorEmailAddress] = [DoctorEmailAddress],
		[DoctorCity] = [DoctorCity], [DoctorState] = [DoctorState], [DoctorZip] = [DoctorZip]
		   WHERE [DoctorID] IN (SELECT [DoctorID] FROM ChangedPatients);
		

    --; 2)For INSERT or UPDATES: Add new rows to the table
		WITH AddedORChangedDoctors 
		AS(
			SELECT [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState],[DoctorZip] From DWClinicReportDataDejeneHail..vETLDimdoctors
			EXCEPT
			SELECT [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState],[DoctorZip] From DWClinicReportDataDejeneHail..Dimdoctors

		)INSERT INTO DWClinicReportDataDejeneHail.[dbo].DimDoctors
        ([DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState],[DoctorZip])
         SELECT
		      [DoctorID]
			  , [DoctorFullName]
			  , [DoctorEmailAddress]
			  , [DoctorCity]
			  , [DoctorState]
			  ,[DoctorZip]
FROM [DWClinicReportDataDejeneHail]..vETLDimdoctors
WHERE [DoctorID] IN (SELECT [DoctorID] FROM AddedORChangedDoctors);
      
    -- ; 3) For Delete: Change the IsCurrent status to zero
		WITH DeletedDoctors 
			AS(
			    SELECT [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState],[DoctorZip] From DWClinicReportDataDejeneHail..Dimdoctors
 				EXCEPT            			
			    SELECT [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState],[DoctorZip] From vETLDimdoctors
   		)DELETE FROM DWClinicReportDataDejeneHail.[dbo].Dimdoctors
		   WHERE [DoctorID] IN (Select [DoctorID] From DeletedDoctors)
	   ;
      SET @RC = +1
  END TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLDimDoctors;
 Print @Status;
 Select * From DWClinicReportDataDejeneHail..DimDoctors
*/
GO


/****** [dbo].[DimShifts] ******/
go 
CREATE or ALTER VIEW vETLDimShifts
/* Author: RRoot
** Desc: Extracts and transforms data for DimShifts
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
AS
SELECT [ShiftID]
          ,ShiftStart
= Case ShiftStart
				 When '01:00' Then '13:00'
				 When '05:00' Then '17:00'
				 Else ShiftStart
				 End
,ShiftEnd
 = Case ShiftEnd
				When '01:00' Then '12:00'
				When '05:00' Then '17:00'
				Else ShiftEnd
				End
FROM [StagingDWClinicReportDataDHailu]..[ETLStagingShifts]
GO
/* Testing Code:
 Select * From vETLDimShifts;
*/

CREATE or ALTER PROCEDURE pETLDimShifts
/* Author: RRoot
** Desc: Inserts data into DimShifts
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
 
		With ChangedShifts 
		As(
			Select [ShiftID], [ShiftStart], [ShiftEnd] From DWClinicReportDataDejeneHail..vETLDimShifts 
			Except
			Select [ShiftID], [ShiftStart], [ShiftEnd] From DWClinicReportDataDejeneHail..DimShifts 
		)UPDATE DWClinicReportDataDejeneHail.[dbo].DimShifts
		  SET [ShiftID] = [ShiftID], [ShiftStart] = [ShiftStart], [ShiftEnd] = [ShiftEnd]
		   WHERE [ShiftID] IN (Select [ShiftID] From ChangedShifts);

    --; 2)For INSERT or UPDATES: Add new rows to the table
		With AddedORChangedShifts 
		As(
            Select [ShiftID], [ShiftStart], [ShiftEnd]  From DWClinicReportDataDejeneHail..vETLDimShifts 
			Except
			Select [ShiftID], [ShiftStart], [ShiftEnd]  From DWClinicReportDataDejeneHail..DimShifts
		)INSERT INTO DWClinicReportDataDejeneHail.[dbo].DimShifts
        ([ShiftID], [ShiftStart], [ShiftEnd])
         SELECT 
		          [ShiftID]
		          , [ShiftStart]
		          , [ShiftEnd]

FROM DWClinicReportDataDejeneHail..vETLDimShifts
WHERE [ShiftID] IN (Select [ShiftID] From AddedORChangedShifts);
      
    -- ; 3) For Delete: Change the IsCurrent status to zero
		With DeletedShifts 
			As(
			    Select [ShiftID], [ShiftStart], [ShiftEnd]From DWClinicReportDataDejeneHail..DimShifts
 				Except            			
			    Select [ShiftID], [ShiftStart], [ShiftEnd] From DWClinicReportDataDejeneHail..vETLDimShifts
   		)DELETE DWClinicReportDataDejeneHail..DimShifts
		   WHERE [ShiftID] IN (Select [ShiftID] From DeletedShifts) ---=BUT DELETE COMMAND NOT RECOMENDED IN dATA WAREHOUSE DATABASE
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
 Exec @Status = pETLDimShifts;
 Print @Status;
 Select * From [DWClinicReportDataDejeneHail]..DimShifts
*/



/****** [dbo].[FactDoctorShifts]] ******/
go 
CREATE or ALTER VIEW vETLFactDoctorShifts
/* Author: RRoot
** Desc: Extracts and transforms data for FactDoctorShifts
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
AS
SELECT [DoctorsShiftID]
               , [ShiftDateKey] = [DateKey]
				, [ClinicKey]
				, [ShiftKey] 
				, [DoctorKey]
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
 Select * From vETLFactDoctorShifts;
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



CREATE or ALTER PROCEDURE pETLFillDWClinicReportData
/* Author: RRoot
** Desc: Inserts data intoDimention and Fact Tables
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
EXEC pETLDimDates;
EXEC pETLDimPatients;
EXEC pETLDimProcedures;
EXEC pETLDimClinics;
EXEC pETLFactVisits;

EXEC pETLDimdoctors
EXEC pETLDimShifts
EXEC pETLFactDoctorShifts
      Set @RC = +1
  END TRY
  BEGIN CATCH
   Print Error_Message()
   Set @RC = -1
  END CATCH
  RETURN @RC;
 END
GO
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDWClinicReportData;
 Print @Status;
 Select * From [DWClinicReportDataDejeneHail]..DimShifts
*/