CREATE PROCEDURE dbo.ManutencaoIndicesPorCategoria
    @Category NVARCHAR(50)
AS
BEGIN

    -- Tabela temporária para categorizar os bancos de dados
    CREATE TABLE #DatabaseSizes
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
	AND name NOT IN ('MonitoracaoSystemSAT', 'RelatoriosSystemSAT')

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
            ELSE IF @SizeMB BETWEEN 8000 AND 50000
                SET @Category = 'Média'
            ELSE IF @SizeMB BETWEEN 50000 AND 225000
                SET @Category = 'Grande'
            ELSE
                SET @Category = 'Gigante'

    -- Inserir o resultado na tabela temporária
    INSERT INTO #DatabaseSizes (DatabaseName, Category)
    VALUES (@DatabaseName, @Category)

    -- Obter o próximo registro
    FETCH NEXT FROM databaseCursor INTO @DatabaseName
END

    -- Fechar o cursor
    CLOSE databaseCursor
    DEALLOCATE databaseCursor

    -- Tabela temporária para armazenar os resultados finais
    CREATE TABLE #Resultados
    (
        DatabaseName NVARCHAR(255),
        SchemaName NVARCHAR(255),
        TableName NVARCHAR(255),
        FragmentationPercent FLOAT
    );

 -- Loop pelos bancos de dados da categoria especificada
--DECLARE @DatabaseName NVARCHAR(255);
DECLARE db_cursor CURSOR FOR
SELECT DatabaseName
FROM #DatabaseSizes
WHERE Category = @Category;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @DatabaseName;

WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @SqlStatement NVARCHAR(MAX);

-- Executa a operação no índice
DECLARE @InnerSchemaName NVARCHAR(255), @InnerTableName NVARCHAR(255); -- Adicionado
DECLARE @InnerIndexName NVARCHAR(255); -- Adicionado
DECLARE @InnerFragmentationPercent FLOAT; -- Adicionado

DECLARE index_cursor CURSOR FOR
    SELECT
        s.name AS SchemaName,
        t.name AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent AS FragmentationPercent
    FROM sys.indexes i
    INNER JOIN sys.tables t 
        ON i.object_id = t.object_id
    INNER JOIN sys.schemas s 
        ON t.schema_id = s.schema_id
    INNER JOIN sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) ips
        ON i.object_id = ips.object_id AND i.index_id = ips.index_id
    WHERE i.index_id > 0
        AND ips.avg_fragmentation_in_percent >= 60;

EXEC sp_executesql @SqlStatement;

-- Executa a operação no índice
OPEN index_cursor;
FETCH NEXT FROM index_cursor INTO @InnerSchemaName, @InnerTableName, @InnerIndexName, @InnerFragmentationPercent;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Reorganize se a fragmentação estiver entre 5 e 59, caso contrário, reconstrua
    IF @InnerFragmentationPercent BETWEEN 5 AND 59
        SET @SqlStatement = 'ALTER INDEX [' + @InnerIndexName + '] ON [' + @DatabaseName + '].[' + @InnerSchemaName + '].[' + @InnerTableName + '] REORGANIZE;';
    ELSE
        SET @SqlStatement = 'ALTER INDEX [' + @InnerIndexName + '] ON [' + @DatabaseName + '].[' + @InnerSchemaName + '].[' + @InnerTableName + '] REBUILD;';

    -- Executa a operação no índice
    EXEC sp_executesql @SqlStatement;

    FETCH NEXT FROM index_cursor INTO @InnerSchemaName, @InnerTableName, @InnerIndexName, @InnerFragmentationPercent;
END

CLOSE index_cursor;
DEALLOCATE index_cursor;


    -- Atualiza estatísticas
    SET @SqlStatement = '
    USE [' + @DatabaseName + '];
    EXEC sp_updatestats;
    ';

    -- Executa o comando
    EXEC sp_executesql @SqlStatement;

    FETCH NEXT FROM db_cursor INTO @DatabaseName;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

-- Limpa as tabelas temporárias
DROP TABLE #DatabaseSizes;
DROP TABLE #Resultados;
END