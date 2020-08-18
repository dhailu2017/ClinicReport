USE [DWClinicReportDataDejeneHail]
GO

/****** Object:  View [dbo].[vReportVisitData]    Script Date: 6/20/2020 4:34:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE or ALTER  VIEW [dbo].[vReportVisitData]
/* Author: RRoot
** Desc: Extracts and transforms data for vReportVisitData
** Change Log: When,Who,What
** 2020-06-08,DHailu,Created view.
*/
As
SELECT Top 1000 [VisitKey] = ID
             , [ClinicName]
			 , [ClinicCity]
			 , [ClinicZip]
			 , VisitsDate =  V.VisitsDate
			 , [FullDateName] = REPLACE([FullDateName], ',',   ' - ')
			 ,[MonthName]
			 ,[MonthID]
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
Where [IsCurrent] = 1
Order By [VisitSDate];
GO


