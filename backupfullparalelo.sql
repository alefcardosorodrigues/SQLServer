CREATE PROCEDURE BackupBancosPorCategoria
    @Categoria VARCHAR(50)
AS
BEGIN
    DECLARE @NomeBanco VARCHAR(100)
    DECLARE @ComandoBackup NVARCHAR(1000)

    PRINT 'Fazendo backup dos bancos de dados na categoria: ' + @Categoria

    -- Cursor para obter os nomes dos bancos de dados na categoria especificada
    DECLARE bancos_cursor CURSOR FOR
    SELECT NomeBanco
    FROM Bancos
    WHERE TamanhoCategoria = @Categoria

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
END