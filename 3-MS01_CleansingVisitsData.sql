--*************************************************************************--
-- Title: Authomate new patient and visits daily regestration from CSV file into patient database
-- Author: RRoot
-- Desc: Pull row data and store it at temporary patient database
-- Change Log: When,Who,What
-- 2018-01-17,Dejene Hailu,File cleansing
--*************************************************************************--
USE [Patients]
GO

---Drop foreign key constraints and to add identity columns 
CREATE PROCEDURE pDropConstraints
AS
BEGIN
  Declare @RC int = 0;
  Begin Try
ALTER TABLE [dbo].[Visits] DROP CONSTRAINT [FK_Visits_Clinics];
ALTER TABLE [dbo].[Visits] DROP CONSTRAINT [fkDoctors];
ALTER TABLE [dbo].[Visits] DROP CONSTRAINT [fkPatients];
ALTER TABLE  [dbo].[Visits] DROP CONSTRAINT [fkProcedures];
ALTER TABLE [dbo].[Visits] DROP CONSTRAINT [PK__Visits__3214EC278C6EF430];
  SET @RC = +1
  END TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO

ALTER TABLE [dbo].[Visits] DROP COLUMN ID;
GO
ALTER TABLE [dbo].[Visits] ADD ID int Not Null IDENTITY (1,1);
GO
---CREATE PROCEDURE TO ADD FORIEN KEY CONSTRAINTS
CREATE PROCEDURE pAddConstraints
AS
BEGIN
  Declare @RC int = 0;
  Begin Try
ALTER TABLE [dbo].[Visits] with nocheck ADD CONSTRAINT [FK_Visits_Clinics] FOREIGN KEY (Clinic) REFERENCES dbo.Clinics (ID); 
ALTER TABLE [dbo].[Visits] ADD CONSTRAINT [fk_Visits_Doctors] FOREIGN KEY ([Doctor]) REFERENCES [dbo].[Doctors] (ID);
ALTER TABLE [dbo].[Visits] ADD CONSTRAINT [fkPatients] FOREIGN KEY ([Patient]) REFERENCES [dbo].[Patients] (ID);
ALTER TABLE  [dbo].[Visits] ADD CONSTRAINT [fkProcedures] FOREIGN KEY ([Procedure]) REFERENCES [dbo].[Procedures] (ID);
ALTER TABLE [dbo].[Visits] ADD CONSTRAINT [PK__Visits__3214EC278C6EF430] PRIMARY KEY (ID);
  SET @RC = +1
  END TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO
--If (object_id('pResetDemo') is not null) Drop Procedure pResetDemo;
--Go

ALTER TABLE [dbo].[tempVisits] ADD [Date] datetime
---CREATE VIEW TO CLEAN AND ORGANIZE ROW DATA
IF Exists(SELECT * FROM Sys.objects WHERE NAME = 'vETLVisits')
   DROP VIEW vETLVisits;

CREATE OR ALTER VIEW vETLVisits 
/* Author: DHailu
** Desc: Create temporary table
** Change Log: When,Who,What
** 2020-05-30,DHailu,Created TempTable.*/
AS 
SELECT 
	  --[date] =CONCAT(ISNULL([date], '20100102'), '  ',  [time])
	  [date] = ISNULL([date], '20100102') + FORMAT(CAST(CONCAT('20100102', '  ',  [time]) AS datetime2), N'HH:mm')
	, [Clinic]
	, [Patient]
	, [Doctor]
	, [Procedure]
	, [Charge] 
FROM [dbo].[tempVisits]
GO


---CREATE PROCEDURE TO POPULATE VISITS TABLE WITH TEMPvISITS TABLE (Incremental loading)
If (object_id('pETLVisits') is not null) Drop Procedure pETLVisits;
Go

CREATE OR ALTER PROCEDURE pETLVisits 
/* Author: DHailu
** Desc: Create temporary table
** Change Log: When,Who,What
** 2020-05-30,DHailu,Created TempTable.*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

WITH AddVisitsData
AS
(SELECT [date], [Clinic], [Patient], [Doctor], [Procedure], [Charge] FROM dbo.vETLVisits
EXCEPT
SELECT [Date], [Clinic], [Patient], [Doctor], [Procedure], [Charge] FROM  [dbo].[Visits]
)

INSERT INTO  [dbo].[Visits] ([Date], [Clinic], [Patient], [Doctor], [Procedure], [Charge])
SELECT [date], [Clinic], [Patient], [Doctor], [Procedure], [Charge] FROM AddVisitsData;

  SET @RC = +1
  END TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO
--Testing Code:
 --Declare @Status int;
 --Exec @Status = pETLVisits;
 --Print @Status;
-- Select * From Visits Order By ID

--Truncate the temporarly tebl to make ready for the new incoming data
CREATE PROCEDURE pTruncateTempVisits
AS 
BEGIN
 DECLARE @RC int = 0;
  BEGIN TRY
TRUNCATE TABLE [dbo].[tempVisits]
  SET @RC = +1
  END TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO


---CHECK THE CODE
--SELECT * FROM [dbo].[Visits] ORDER BY ID DESC
--DELETE FROM [dbo].[Visits] WHERE ID > 40150
--SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM [dbo].[Visits]
--SELECT COUNT(*) FROM [dbo].[Visits] --40304
--SELECT * FROM [dbo].[vETLVisits] 
--SELECT COUNT(*) FROM [vETLVisits] --462
--SELECT COUNT(*) FROM [dbo].[tempVisits] --462
--SELECT * FROM [dbo].[tempVisits]
--EXEC pETLVisits 
--SELECT * FROM [dbo].[tempVisits]



	
  

