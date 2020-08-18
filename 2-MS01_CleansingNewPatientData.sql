--*************************************************************************--
-- Title: Authomate new patient and visits daily regestration from CSV file into patient database
-- Author: RRoot
-- Desc: Pull row data and store it at temporary patient database
-- Change Log: When,Who,What
-- 2020-05-31,Dejene Hailu,File cleansing
--*************************************************************************--
USE [Patients]
GO

ALTER TABLE [dbo].[Patients] DROP COLUMN ID;
GO
ALTER TABLE [dbo].[Patients] ADD ID int Not Null IDENTITY (1,1);
GO

---CREATE VIEW FOR ETL FROM PATIENT TEMPORARY TABLE
CREATE VIEW vETLPatient 
/* Author: DHailu
** Desc: Create temporary table
** Change Log: When,Who,What
** 2020-05-31,DHailu,Created view.*/
CREATE VIEW vETLPatient 

AS 
SELECT 
	  [FName] = CAST(FName AS varchar(28))
	  ,[LName] = CAST(LName AS varchar(29))
	  ,[Email] = LOWER(CAST(Email AS varchar(100)))
	  ,[Email Validation] = CASE WHEN Email like '__%@%__.com' THEN 1
	                   ELSE 0 END
	  , [Address]
	  , [City]
	  , [State]
	  , [ZipCode]
FROM [dbo].[tempPaitient]

---CREATE PROCEDURE TO FILL PATIENT TABLE
CREATE OR ALTER PROCEDURE pETLPatient 
/* Author: DHailu
** Desc: Create temporary table
** Change Log: When,Who,What
** 2020-05-31,DHailu,Created procedure.*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
WITH FillPatient
AS
(
SELECT [FName], [LName], [Email], [Address], [City], [State], [ZipCode] FROM [dbo].[vETLPatient]
EXCEPT
SELECT [FName], [LName], [Email], [Address], [City], [State], [ZipCode] FROM [dbo].[Patients]
)
INSERT INTO [dbo].[Patients]
SELECT [FName], [LName], [Email], [Address], [City], [State], [ZipCode] FROM FillPatient
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
 --Exec @Status = pETLPatient;
 --Print @Status;
 --Select * From patients Order By ID

---PROCEDURE TO TRUNCATE AND MAKE READY FOR THE INCOMING NEW PATIENT DATA
CREATE OR ALTER PROCEDURE pTruncateTempPaitient 
/* Author: DHailu
** Desc: Create temporary table
** Change Log: When,Who,What
** 2020-05-31,DHailu,Created procedure to truncate temp table.*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

  TRUNCATE TABLE [dbo].[tempPaitient]
  SET @RC = +1
  END TRY
  BEGIN CATCH
   PRINT Error_Message()
   SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO


---CHECK YOUR CODE
--SELECT COUNT(*) FROM  [dbo].[tempPaitient]
--SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM [dbo].[Patients]
--DELETE FROM [dbo].[tempPaitient]
--SELECT COUNT(*) FROM [dbo].[vETLPatient]
--SELECT COUNT(*) FROM [dbo].[Patients]
--DELETE FROM [dbo].[Patients] WHERE ID > 999

