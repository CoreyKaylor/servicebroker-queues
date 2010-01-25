USE [<databasename, sysname, queuedb>]
GO

CREATE SCHEMA [SBQ]
GO

CREATE SCHEMA [SBUQ]
GO

CREATE TABLE [SBQ].[Detail]
(
	id UNIQUEIDENTIFIER CONSTRAINT PK_Details_id PRIMARY KEY CLUSTERED,
	schemaVersion VARCHAR(16) NOT NULL,
)
GO

CREATE MESSAGE TYPE
[http://servicebroker.queues.com/servicebroker/2009/09/Message]
VALIDATION = NONE
GO

CREATE CONTRACT [http://servicebroker.queues.com/servicebroker/2009/09/ServiceBusContract]
(
	[http://servicebroker.queues.com/servicebroker/2009/09/Message] SENT BY ANY
)
GO

IF NOT EXISTS (SELECT * FROM sys.[endpoints] e WHERE e.[name] = 'ServiceBusEndpoint')
BEGIN
	CREATE ENDPOINT ServiceBusEndpoint
	STATE = STARTED
	AS TCP 
	(
		LISTENER_PORT = <port, , 2204>
	)
	FOR SERVICE_BROKER 
	(
		AUTHENTICATION = WINDOWS
	)
END
GO

USE MASTER;
GO

GRANT CONNECT ON ENDPOINT::ServiceBusEndpoint TO [public];
GO 

USE [<databasename, sysname, queuedb>];
GO

CREATE TABLE [SBQ].[OutgoingHistory]
(
	[messageId] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_OutgoingHistory_messageId PRIMARY KEY CLUSTERED,
	[address] VARCHAR(255) NOT NULL,
	[conversationHandle] UNIQUEIDENTIFIER NOT NULL,
	[route] VARCHAR(255) NOT NULL,
	[sizeOfData] INT NOT NULL,
	[sentAt] DATETIME2(7) NOT NULL,
	[deferProcessingUntilTime] DATETIME2(7),
	[data] VARBINARY(MAX),
	[sent] BIT
)
GO

CREATE TABLE [SBQ].[ConversationDialogs]
(
	[conversationHandle] UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_ConversationDialogs_conversationHandle PRIMARY KEY CLUSTERED,
	[fromService] VARCHAR(255) NOT NULL,
	[toService] VARCHAR(255) NOT NULL,
	[createdAt] DATETIME2(7) NOT NULL
)
GO

CREATE TABLE [SBQ].[MessageHistory]
(
	[localId] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_MessageHistory_localId PRIMARY KEY CLUSTERED,
	[queueName] VARCHAR(255) NOT NULL,
	[receivedTimestamp] DATETIME2(7) NOT NULL,
	[data] VARBINARY(MAX)
)
GO

CREATE PROCEDURE [SBQ].[Dequeue]
(
	@queueName AS VARCHAR(255)
)
AS 
BEGIN
WHILE(1=1)
BEGIN
	DECLARE @ch UNIQUEIDENTIFIER = NULL
	DECLARE @messageTypeName VARCHAR(256)
	DECLARE @data VARBINARY(MAX)
	DECLARE @sql NVARCHAR(MAX)
	DECLARE @param_def NVARCHAR(MAX)
	
	-- Creating the parameter substitution
	SET @param_def = '
	@ch UNIQUEIDENTIFIER OUTPUT,
	@messagetypename VARCHAR(256) OUTPUT,
	@data VARBINARY(MAX) OUTPUT'

	SET @sql = '
	RECEIVE TOP(1)
	@ch = conversation_handle,
	@messagetypename = message_type_name,
	@data = message_body
	FROM [SBUQ].[' + @queueName + '/queue]'
	
	BEGIN TRY
	EXEC sp_executesql
		@sql, 
		@param_def,
		@ch = @ch OUTPUT,
		@messageTypeName = @messagetypename OUTPUT,
		@data = @data OUTPUT
	END TRY
	BEGIN CATCH
	END CATCH
	
	IF(@ch IS NULL)
		BREAK
	ELSE
	BEGIN
		IF (@messageTypeName = 'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog' OR @messageTypeName = 'http://schemas.microsoft.com/SQL/ServiceBroker/Error')
		BEGIN
			BEGIN TRY
				END CONVERSATION @ch
			END TRY
			BEGIN CATCH
			END CATCH
		END
		ELSE IF (@messageTypeName = 'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer')
		BEGIN
			DECLARE @outgoingHistoryId INT
			DECLARE @conversationHandle UNIQUEIDENTIFIER
			DECLARE @messageData VARBINARY(MAX)

			DECLARE deferredMessages CURSOR FAST_FORWARD READ_ONLY FOR
			SELECT oh.[messageId], oh.[conversationHandle], oh.[data]
			FROM [SBQ].[OutgoingHistory] oh WITH(READPAST)
			JOIN sys.conversation_endpoints ce with(nolock) on ce.[conversation_handle] = oh.[conversationHandle]
			WHERE oh.[deferProcessingUntilTime] <= sysutcdatetime() AND oh.[sent] = 0

			OPEN deferredMessages

			FETCH NEXT FROM deferredMessages INTO @outgoingHistoryId, @conversationHandle, @messageData

			WHILE @@FETCH_STATUS = 0
			BEGIN

			BEGIN TRY
			BEGIN
				SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [http://servicebroker.queues.com/servicebroker/2009/09/Message] (@messageData);
			END
			END TRY
			BEGIN CATCH
			END CATCH
			
			UPDATE [SBQ].[OutgoingHistory] SET [sent] = 1 WHERE [messageId] = @outgoingHistoryId
			FETCH NEXT FROM deferredMessages INTO @outgoingHistoryId, @conversationHandle, @messageData

			END

			CLOSE deferredMessages
			DEALLOCATE deferredMessages
			
			UPDATE oh SET oh.[sent] = 1
			FROM [SBQ].[OutgoingHistory] oh
			LEFT JOIN sys.conversation_endpoints ce on oh.[conversationHandle] = ce.[conversation_handle]
			WHERE ce.[conversation_handle] is null
			AND oh.[sent] = 0
		END
		ELSE
		BEGIN 
			INSERT INTO [SBQ].[MessageHistory] (
			[queueName],
			[receivedTimestamp],
			[data]
			)
			VALUES(@queueName, SYSUTCDATETIME(), @data)
			
			SELECT @ch, @data
			BREAK
		END
	END
END
END
GO

CREATE PROCEDURE [SBQ].[RegisterToSend]
(
	@localServiceName VARCHAR(255),
	@address VARCHAR(255),
	@route VARCHAR(255),
	@sizeOfData INT,
	@deferProcessingUntilTime DATETIME2(7) = NULL,
	@sentAt DATETIME2(7),
	@data VARBINARY(MAX) = NULL
)
AS 
BEGIN
	DECLARE @conversationHandle UNIQUEIDENTIFIER
	
	SET @conversationHandle = (SELECT TOP 1 [conversationHandle] FROM [SBQ].[ConversationDialogs] WITH(READPAST,XLOCK,ROWLOCK) WHERE [toService] = @address AND [fromService] = @localServiceName)

	IF(@conversationHandle IS NULL)
	BEGIN
		BEGIN DIALOG CONVERSATION @conversationHandle
			FROM SERVICE @localServiceName
			TO SERVICE @address
			ON CONTRACT [http://servicebroker.queues.com/servicebroker/2009/09/ServiceBusContract]
			WITH ENCRYPTION = OFF;
			
		INSERT INTO [SBQ].[ConversationDialogs] (
			[conversationHandle],
			[fromService],
			[toService],
			[createdAt]
		) VALUES ( 
			/* conversationHandle - UNIQUEIDENTIFIER */ @conversationHandle,
			/* fromService - VARCHAR(255) */ @localServiceName,
			/* toService - VARCHAR(255) */ @address,
			/* createdAt - DATETIME2(7) */ SYSUTCDATETIME() ) 
	END
		
	IF (@deferProcessingUntilTime IS NULL)
	BEGIN 
		INSERT INTO [SBQ].[OutgoingHistory] (
		[address],
		[route],
		[conversationHandle],
		[sizeOfData],
		[deferProcessingUntilTime],
		[sentAt],
		[data],
		[sent]
		) VALUES ( 
		/* address - varchar(255) */ @address,
		/* route - varchar(255) */ @route,
		/* conversationHandle uniqueidentifier */ @conversationHandle,
		/* sizeOfData - int */ @sizeOfData,
		/* deferProcessingUntilTime - datetime2 */ @deferProcessingUntilTime,
		/* sentAt - datetime2 */ @sentAt,
		/* data - varbinary(max) */ @data, 
		/* sent - bit */ 1);
		SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [http://servicebroker.queues.com/servicebroker/2009/09/Message] (@data);
	END
	ELSE
	BEGIN
		INSERT INTO [SBQ].[OutgoingHistory] (
		[address],
		[route],
		[conversationHandle],
		[sizeOfData],
		[deferProcessingUntilTime],
		[sentAt],
		[data],
		[sent]
		) VALUES ( 
		/* address - varchar(255) */ @address,
		/* route - varchar(255) */ @route,
		/* conversationHandle uniqueidentifier */ @conversationHandle,
		/* sizeOfData - int */ @sizeOfData,
		/* deferProcessingUntilTime - datetime2 */ @deferProcessingUntilTime,
		/* sentAt - datetime2 */ @sentAt,
		/* data - varbinary(max) */ @data,
		/* sent - bit */ 0)
		BEGIN CONVERSATION TIMER (@conversationHandle)
		TIMEOUT = DATEDIFF(SECOND, SYSUTCDATETIME(), @deferProcessingUntilTime);
	END
END
GO

CREATE PROCEDURE [SBQ].[CreateQueueIfDoesNotExist]
(
      @address VARCHAR(255)
)
AS 
BEGIN
	IF NOT EXISTS(SELECT * FROM sys.[service_queues] q WHERE q.[name] = @address + '/queue')
	BEGIN
		DECLARE @cmd NVARCHAR(MAX) = 
		N'CREATE QUEUE [SBUQ].' + QUOTENAME(@address + N'/queue') + N'
		WITH STATUS = ON';
		EXECUTE(@cmd);

		SET @cmd = N'CREATE SERVICE ' + QUOTENAME(@address) + N'
		ON QUEUE [SBUQ].' + QUOTENAME(@address + N'/queue') + N'
		(
			[http://servicebroker.queues.com/servicebroker/2009/09/ServiceBusContract]
		)';
		EXECUTE(@cmd);

		SET @cmd = N'GRANT SEND ON SERVICE::' + QUOTENAME(@address) + N' TO PUBLIC;';
		EXECUTE(@cmd);

		-- Creating the dynamic T-SQL statement, that inserts the configured route into the sys.routes catalog view
		SET @cmd = 'IF NOT EXISTS (SELECT * FROM sys.routes WHERE name = ' + QUOTENAME(@address, '''') + ') '
		SET @cmd = @cmd + 'BEGIN ';

		SET @cmd = @cmd + 'CREATE ROUTE ' + QUOTENAME(@address) +' WITH SERVICE_NAME = ' + QUOTENAME(@address, '''') + ', ADDRESS = ''LOCAL''';
		SET @cmd = @cmd + ' END';

		-- Execute the dynamic T-SQL statement
		EXECUTE(@cmd);
	END
END
GO

CREATE PROCEDURE [SBQ].[ProcessConfigurationNoticeRequestMessages]
AS
BEGIN
	DECLARE @ch UNIQUEIDENTIFIER;
	DECLARE @messagetypename NVARCHAR(256);
	DECLARE	@messagebody XML;
	DECLARE @responsemessage XML;

	WHILE (1=1)
	BEGIN
		SET @ch = NULL;
		WAITFOR (
			RECEIVE TOP(1)
				@ch = conversation_handle,
				@messagetypename = message_type_name,
				@messagebody = CAST(message_body AS XML)
			FROM
				[SBQ].[SQL/ServiceBroker/BrokerConfiguration/Queue]
		), TIMEOUT 1000

		IF (@@ROWCOUNT = 0)
		BEGIN
			BREAK
		END

		IF (@messagetypename = 'http://schemas.microsoft.com/SQL/ServiceBroker/BrokerConfigurationNotice/MissingRoute')
		BEGIN
			DECLARE @serviceName NVARCHAR(256);
			DECLARE @route NVARCHAR(256);
			DECLARE @sql NVARCHAR(MAX);

			-- Extract the service name from the received message
			WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/SQL/ServiceBroker/BrokerConfigurationNotice/MissingRoute')
			SELECT @serviceName = @messagebody.value(
			'/MissingRoute[1]/SERVICE_NAME[1]', 'nvarchar(max)');

			-- Extract the route from the history table
			SELECT @route = 'tcp://' + SUBSTRING(@serviceName, 0, CHARINDEX('/', @serviceName))

			-- Creating the dynamic T-SQL statement, that inserts the configured route into the sys.routes catalog view
			SET @sql = 'IF NOT EXISTS (SELECT * FROM sys.routes WHERE name = ''' + @serviceName + ''') '
			SET @sql = @sql + 'BEGIN ';

			SET @sql = @sql + 'CREATE ROUTE ' + QUOTENAME(@serviceName) + ' WITH SERVICE_NAME = ''' + @serviceName + ''', ADDRESS = ''' + @route + '''';
			SET @sql = @sql + ' END';

			-- Execute the dynamic T-SQL statement
			EXEC sp_executesql @sql;
		
			-- End the conversation
			BEGIN TRY
				END CONVERSATION @ch;
			END TRY
			BEGIN CATCH
			END CATCH
		END

		IF (@messagetypename = 'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog' OR @messageTypeName = 'http://schemas.microsoft.com/SQL/ServiceBroker/Error')
		BEGIN
			-- End the conversation
			BEGIN TRY
				END CONVERSATION @ch;
			END TRY
			BEGIN CATCH
			END CATCH
		END

		IF (@messagetypename = 'http://schemas.microsoft.com/SQL/ServiceBroker/BrokerConfigurationNotice/MissingRemoteServiceBinding')
		BEGIN
			-- End the conversation
			BEGIN TRY
				END CONVERSATION @ch;
			END TRY
			BEGIN CATCH
			END CATCH
		END
	END
END
GO

CREATE QUEUE [SBQ].[SQL/ServiceBroker/BrokerConfiguration/Queue]
WITH ACTIVATION 
(
	STATUS = ON,
	PROCEDURE_NAME = [SBQ].[ProcessConfigurationNoticeRequestMessages],
	MAX_QUEUE_READERS = 1,
	EXECUTE AS SELF
)
GO

CREATE SERVICE [SQL/ServiceBroker/BrokerConfiguration]
ON QUEUE [SBQ].[SQL/ServiceBroker/BrokerConfiguration/Queue]
(
	[http://schemas.microsoft.com/SQL/ServiceBroker/BrokerConfigurationNotice]
)
GO

CREATE ROUTE [SQL/ServiceBroker/BrokerConfiguration]
	WITH SERVICE_NAME = N'SQL/ServiceBroker/BrokerConfiguration',
	ADDRESS	= N'LOCAL'
GO

CREATE ROUTE [http://schemas.microsoft.com/SQL/ServiceBroker/ServiceBroker]
	WITH SERVICE_NAME = 'http://schemas.microsoft.com/SQL/ServiceBroker/ServiceBroker',
	ADDRESS = 'LOCAL'
GO

CREATE PROCEDURE [SBQ].[PurgeHistoric]
AS
BEGIN
	DELETE oh FROM [SBQ].[OutgoingHistory] oh WITH(READPAST)
	WHERE oh.[messageId] < ((SELECT MAX(messageId) FROM [SBQ].[OutgoingHistory] WITH(READPAST)) - 1000)
	
	DELETE mh FROM [SBQ].[MessageHistory] mh WITH(READPAST)
	WHERE mh.[localId] < ((SELECT MAX(localId) FROM [SBQ].[MessageHistory] WITH(READPAST)) - 1000)
	
	DECLARE @conversationHandle UNIQUEIDENTIFIER

	DECLARE oldConversations CURSOR FOR
	SELECT [conversationHandle]
	FROM [SBQ].[ConversationDialogs] WITH(READPAST)
	WHERE [createdAt] >= DATEADD(hh, -2, sysutcdatetime())

	OPEN oldConversations

	FETCH NEXT FROM oldConversations INTO @conversationHandle

	WHILE @@FETCH_STATUS = 0
	BEGIN

	BEGIN TRY
		BEGIN
			END CONVERSATION @conversationHandle
			DELETE FROM [SBQ].[ConversationDialogs]
			WHERE [conversationHandle] = @conversationHandle
		END
	END TRY
	BEGIN CATCH
	END CATCH

	FETCH NEXT FROM oldConversations INTO @conversationHandle

	END

	CLOSE oldConversations
	DEALLOCATE oldConversations
END
GO

INSERT INTO [SBQ].[Detail](id, schemaVersion)
SELECT NEWID(), '1.0'
GO

CREATE ROLE [ServiceBusOwner] AUTHORIZATION [dbo];
GO

EXEC sys.sp_addrolemember N'db_datareader', N'ServiceBusOwner';
GO

EXEC sys.sp_addrolemember N'db_datawriter', N'ServiceBusOwner';
GO

GRANT EXECUTE ON [SBQ].[PurgeHistoric] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [SBQ].[CreateQueueIfDoesNotExist] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [SBQ].[RegisterToSend] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [SBQ].[Dequeue] TO [ServiceBusOwner];
GO

GRANT TAKE OWNERSHIP ON SCHEMA::[SBUQ] TO [ServiceBusOwner];