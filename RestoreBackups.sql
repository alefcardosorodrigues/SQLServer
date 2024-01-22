USE master;
GO

CREATE PROCEDURE sp_RestoreBackups
    @BackupDirectory NVARCHAR(255),
    @DataDirectory NVARCHAR(255),
    @LogDirectory NVARCHAR(255)
AS
BEGIN
    DECLARE @BackupFile NVARCHAR(255);
    DECLARE @DatabaseName NVARCHAR(255);
    DECLARE @SQLCommand NVARCHAR(MAX);

    -- Criar uma tabela temporária para armazenar os detalhes do backup
    CREATE TABLE #BackupList
    (
        BackupFile NVARCHAR(255),
        DatabaseName NVARCHAR(255)
    );

    -- Popula a tabela temporária com os arquivos de backup no diretório especificado
    INSERT INTO #BackupList (BackupFile, DatabaseName)
    EXEC xp_cmdshell 'dir /B "E:\BACKUP\"';
    --EXEC xp_cmdshell 'for %i in ("E:\BACKUP\*") do @echo %~ni'; possibilidade de código para pegar somente o nome da base

    -- Loop através dos backups e executa o script de restore
    DECLARE BackupCursor CURSOR FOR
    SELECT BackupFile, DatabaseName
    FROM #BackupList
    WHERE BackupFile LIKE '%.bak';

    OPEN BackupCursor;
    FETCH NEXT FROM BackupCursor INTO @BackupFile, @DatabaseName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @SQLCommand = '
            RESTORE DATABASE [' + @DatabaseName + ']
            FROM DISK = ''' + @BackupDirectory + '\' + @BackupFile + '''
            WITH
                MOVE N''' + @DatabaseName + ''' TO N''' + @DataDirectory + '\' + @DatabaseName + '.mdf'',
                MOVE N''' + @DatabaseName + '_log'' TO N''' + @LogDirectory + '\' + @DatabaseName + '_log.ldf'',
                REPLACE;';

        EXEC sp_executesql @SQLCommand;

        FETCH NEXT FROM BackupCursor INTO @BackupFile, @DatabaseName;
    END

    CLOSE BackupCursor;
    DEALLOCATE BackupCursor;

    -- Drop da tabela temporária
    DROP TABLE #BackupList;
END;