USE [MonitoracaoSystemSAT]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- ################ BACKUP FULL #######################################

-- exec [MonitoracaoSystemsat].[Backup].[BackupFull] 'F:\AXIS_CORE\BACKUP\' , 'DbaStuff' -- Executa o backup de uma base específica
-- exec [MonitoracaoSystemsat].[Backup].[BackupFull] 'F:\AXIS_CORE\BACKUP\FULL\' , NULL       -- Executa o backup para todas as bases da instância

-- =============================================
-- Author:		Alef Rodrigues
-- Create date: 15/02/2024
-- Description:	Rotina de backup FULL 
-- =============================================

CREATE PROCEDURE [Backup].[BackupFull]
	@path varchar(500),
	@DatabaseName varchar(150)
AS
BEGIN

	SET NOCOUNT ON;

DECLARE @name VARCHAR(50), -- database name
--@path VARCHAR(256), -- path for backup files
@fileName VARCHAR(256), -- filename for backup
@createpath VARCHAR(256), --  -- Quando deseja criar subpastar dentro do caminho PATH
@fileDate VARCHAR(20), -- used for file name
@backupCount INT

CREATE TABLE [dbo].#tempBackup (intID INT IDENTITY (1, 1), name VARCHAR(200))

-- Includes the date in the filename
SET @fileDate = CONVERT(VARCHAR(20), GETDATE(), 112)

-- Includes the date and time in the filename
SET @fileDate = CONVERT(VARCHAR(20), GETDATE(), 112) + '_' + REPLACE(CONVERT(VARCHAR(20), GETDATE(), 108), ':', '')

	If (select @DatabaseName) IS NOT NULL
	Begin
		insert into [dbo].#tempBackup (name) select @DatabaseName
	end
	Else
	begin
		INSERT INTO [dbo].#tempBackup (name) 
		SELECT name
		FROM master.dbo.sysdatabases
		WHERE 1=1
		and	name not in ('model','tempdb','distribution','ReportServer$MSSQL2008','ReportServer$MSSQL2008TempDB')   
		and name not like '%_Teste%'   
		and name not like '%_Bak%'   
		and name not like '%_Mig%'   
		and name not like '%_Homolog%'   
		and DATABASEPROPERTYEX(name, 'status') != 'OFFLINE'
		order by name
	END

SELECT TOP 1 @backupCount = intID FROM [dbo].#tempBackup ORDER BY intID DESC
IF ((@backupCount IS NOT NULL) AND (@backupCount > 0))
BEGIN
DECLARE @currentBackup INT

SET @currentBackup = 1
WHILE (@currentBackup <= @backupCount)
BEGIN
SELECT
@name = [name]
,@fileName = @path + @name + '.BAK' -- Non-Unique Filename
FROM [dbo].#tempBackup
WHERE intID = @currentBackup

BACKUP DATABASE @name TO DISK = @fileName WITH INIT,FORMAT,COMPRESSION

SET @currentBackup = @currentBackup + 1
END
END

DROP TABLE [dbo].#tempBackup

END
GO

