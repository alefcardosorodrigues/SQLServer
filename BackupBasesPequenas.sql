CREATE PROCEDURE Backup.backupfullSmallDatabases
AS
BEGIN

    CREATE TABLE medium_databases
    (
        DatabaseName NVARCHAR(255),
        Category NVARCHAR(50)
    );

    DECLARE @DatabaseName NVARCHAR(255)
    DECLARE @Category NVARCHAR(50)

DECLARE databaseCursor CURSOR FOR
    SELECT name
    FROM sys.databases
    WHERE database_id > 4
      AND state_desc = 'ONLINE'
	--AND name NOT IN ('MonitoracaoSystemSAT', 'RelatoriosSystemSAT')

    -- Abrir o cursor
    OPEN databaseCursor

    -- Inicializar o primeiro registro
    FETCH NEXT FROM databaseCursor INTO @DatabaseName

    -- Loop para calcular o tamanho de cada base de dados
    WHILE @@FETCH_STATUS = 0
            BEGIN
            -- Calcular o tamanho total da base de dados
            DECLARE @SizeMB DECIMAL(18, 2)
            SELECT @SizeMB = SUM(size / 128.0)
            FROM sys.master_files
            WHERE database_id = DB_ID(@DatabaseName)

            -- Categorizar o tamanho
            IF @SizeMB < 8000
                SET @Category = 'Pequena'
                INSERT INTO small_databases (DatabaseName, Category)
                VALUES (@DatabaseName, @Category)


    -- Obter o próximo registro
    FETCH NEXT FROM databaseCursor INTO @DatabaseName
END

    -- Fechar o cursor
    CLOSE databaseCursor
    DEALLOCATE databaseCursor

    DECLARE @NomeBanco VARCHAR(100)
    DECLARE @ComandoBackup NVARCHAR(1000)

    PRINT 'Fazendo backup dos bancos de dados na categoria: ' + @Category

    -- Cursor para obter os nomes dos bancos de dados na categoria especificada
    DECLARE bancos_cursor CURSOR FOR
    SELECT NomeBanco
    FROM all_databases
    WHERE Category = @Category

    -- Abre o cursor
    OPEN bancos_cursor

    -- Loop pelos bancos de dados
    FETCH NEXT FROM bancos_cursor INTO @NomeBanco
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @ComandoBackup = 'BACKUP DATABASE [' + @NomeBanco + '] TO DISK = ''C:\Backup\' + @NomeBanco + '_' + CONVERT(VARCHAR(8), GETDATE(), 112) + '.bak'' WITH COMPRESSION'
        EXEC sp_executesql @ComandoBackup

        PRINT 'Backup completo do banco de dados ' + @NomeBanco + ' feito com sucesso.'

        FETCH NEXT FROM bancos_cursor INTO @NomeBanco
    END

    -- Fecha o cursor dos bancos de dados
    CLOSE bancos_cursor
    DEALLOCATE bancos_cursor
    TRUNCATE TABLE  all_databases
END