USE <databasename, sysname, queuedb>
GO
TRUNCATE TABLE [SBQ].[MessageHistory]
GO
TRUNCATE TABLE [SBQ].[OutgoingHistory]
GO

DECLARE @queueName VARCHAR(255)

DECLARE queuesToCheck CURSOR FAST_FORWARD READ_ONLY FOR
SELECT REPLACE(sq.[name], '/queue', '')
FROM sys.[services] s
JOIN sys.[service_queues] sq ON s.[service_queue_id] = sq.[object_id]
JOIN sys.[service_contract_usages] scu ON scu.[service_id] = s.[service_id]
JOIN sys.[service_contracts] sc ON scu.[service_contract_id] = sc.[service_contract_id]
WHERE sc.[name] = 'http://servicebroker.queues.com/servicebroker/2009/09/ServiceBusContract'

OPEN queuesToCheck

FETCH NEXT FROM queuesToCheck INTO @queueName

WHILE @@FETCH_STATUS = 0
BEGIN
	WHILE (1=1)
	BEGIN
		DECLARE @messageCount INT;
		DECLARE @messages TABLE
		(
			[conversationHandle] UNIQUEIDENTIFIER,
			[data] VARBINARY(MAX)
		)
		
		INSERT INTO @messages ([conversationHandle], [data])
		EXEC [SBQ].[Dequeue] @queueName
		PRINT @queueName
		IF NOT EXISTS (SELECT * FROM @messages)
			BREAK
		DELETE FROM @messages
	END

FETCH NEXT FROM queuesToCheck INTO @queueName
END

CLOSE queuesToCheck
DEALLOCATE queuesToCheck
GO
