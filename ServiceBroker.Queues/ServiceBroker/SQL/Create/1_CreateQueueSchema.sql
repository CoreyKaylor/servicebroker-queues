USE [<databasename, sysname, queuedb>]
GO

CREATE SCHEMA [Queue]
GO

CREATE TABLE [Queue].[Detail]
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

CREATE TABLE [Queue].[OutgoingHistory]
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

CREATE TABLE [Queue].[ConversationDialogs]
(
	[conversationHandle] UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_ConversationDialogs_conversationHandle PRIMARY KEY CLUSTERED,
	[fromService] VARCHAR(255) NOT NULL,
	[toService] VARCHAR(255) NOT NULL,
	[createdAt] DATETIME2(7) NOT NULL
)
GO

CREATE TABLE [Queue].[MessageHistory]
(
	[localId] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_MessageHistory_localId PRIMARY KEY CLUSTERED,
	[queueName] VARCHAR(255) NOT NULL,
	[receivedTimestamp] DATETIME2(7) NOT NULL,
	[data] VARBINARY(MAX)
)
GO

CREATE PROCEDURE [Queue].[Dequeue]
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
	FROM [' + @queueName + '/queue]'
	
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
		IF (@messageTypeName = 'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog')
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
			FROM [Queue].[OutgoingHistory] oh WITH(READPAST)
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
			
			UPDATE [Queue].[OutgoingHistory] SET [sent] = 1 WHERE [messageId] = @outgoingHistoryId
			FETCH NEXT FROM deferredMessages INTO @outgoingHistoryId, @conversationHandle, @messageData

			END

			CLOSE deferredMessages
			DEALLOCATE deferredMessages
		END
		ELSE
		BEGIN 
			INSERT INTO [Queue].[MessageHistory] (
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

CREATE PROCEDURE [Queue].[RegisterToSend]
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
	
	SET @conversationHandle = (SELECT TOP 1 [conversationHandle] FROM [Queue].[ConversationDialogs] WITH(READPAST,XLOCK,ROWLOCK) WHERE [toService] = @address AND [fromService] = @localServiceName)

	IF(@conversationHandle IS NULL)
	BEGIN
		BEGIN DIALOG CONVERSATION @conversationHandle
			FROM SERVICE @localServiceName
			TO SERVICE @address
			ON CONTRACT [http://servicebroker.queues.com/servicebroker/2009/09/ServiceBusContract]
			WITH ENCRYPTION = OFF;
			
		INSERT INTO [Queue].[ConversationDialogs] (
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
		INSERT INTO [Queue].[OutgoingHistory] (
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
		INSERT INTO [Queue].[OutgoingHistory] (
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

CREATE PROCEDURE [Queue].[CreateQueueIfDoesNotExist]
(
	@address VARCHAR(255)
)
AS 
BEGIN
	IF NOT EXISTS(SELECT * FROM sys.[service_queues] q WHERE q.[name] = @address + '/queue')
	BEGIN
		EXECUTE( N'CREATE QUEUE [' + @address + N'/queue]
		WITH STATUS = ON')
		
		EXECUTE(N'CREATE SERVICE [' + @address + N']
		ON QUEUE [' + @address + N'/queue]
		(
			[http://servicebroker.queues.com/servicebroker/2009/09/ServiceBusContract]
		)')
		
		EXECUTE(N'GRANT SEND ON SERVICE::[' + @address + N'] TO PUBLIC')
		
		DECLARE @sql NVARCHAR(MAX);
		
		-- Creating the dynamic T-SQL statement, that inserts the configured route into the sys.routes catalog view
		SET @sql = 'IF NOT EXISTS (SELECT * FROM sys.routes WHERE name = ''' + @address + ''') '
		SET @sql = @sql + 'BEGIN ';

		SET @sql = @sql + 'CREATE ROUTE [' + @address +'] WITH SERVICE_NAME = ''' + @address + ''', ADDRESS = ''LOCAL''';
		SET @sql = @sql + ' END';

		-- Execute the dynamic T-SQL statement
		EXECUTE(@sql);
	END
END
GO

CREATE PROCEDURE ProcessConfigurationNoticeRequestMessages
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
				[SQL/ServiceBroker/BrokerConfiguration/Queue]
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

			SET @sql = @sql + 'CREATE ROUTE [' + @serviceName +'] WITH SERVICE_NAME = ''' + @serviceName + ''', ADDRESS = ''' + @route + '''';
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

		IF (@messagetypename = 'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog')
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

CREATE QUEUE [SQL/ServiceBroker/BrokerConfiguration/Queue]
WITH ACTIVATION 
(
	STATUS = ON,
	PROCEDURE_NAME = ProcessConfigurationNoticeRequestMessages,
	MAX_QUEUE_READERS = 1,
	EXECUTE AS SELF
)
GO

CREATE SERVICE [SQL/ServiceBroker/BrokerConfiguration]
ON QUEUE [SQL/ServiceBroker/BrokerConfiguration/Queue]
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

CREATE PROCEDURE [Queue].[PurgeHistoric]
AS
BEGIN
	DELETE oh FROM [Queue].[OutgoingHistory] oh WITH(READPAST)
	WHERE oh.[messageId] < ((SELECT MAX(messageId) FROM [Queue].[OutgoingHistory] WITH(READPAST)) - 1000)
	
	DELETE mh FROM [Queue].[MessageHistory] mh WITH(READPAST)
	WHERE mh.[localId] < ((SELECT MAX(localId) FROM [Queue].[MessageHistory] WITH(READPAST)) - 1000)
	
	DECLARE @conversationHandle UNIQUEIDENTIFIER

	DECLARE oldConversations CURSOR FOR
	SELECT [conversationHandle]
	FROM [Queue].[ConversationDialogs] WITH(READPAST)
	WHERE [createdAt] >= DATEADD(hh, -2, sysutcdatetime())

	OPEN oldConversations

	FETCH NEXT FROM oldConversations INTO @conversationHandle

	WHILE @@FETCH_STATUS = 0
	BEGIN

	BEGIN TRY
		BEGIN
			END CONVERSATION @conversationHandle
			DELETE FROM [Queue].[ConversationDialogs]
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

INSERT INTO [Queue].[Detail](id, schemaVersion)
SELECT NEWID(), '1.0'
GO

CREATE ROLE [ServiceBusOwner] AUTHORIZATION [dbo];
GO

EXEC sys.sp_addrolemember N'db_datareader', N'ServiceBusOwner';
GO

EXEC sys.sp_addrolemember N'db_datawriter', N'ServiceBusOwner';
GO

GRANT EXECUTE ON [Queue].[PurgeHistoric] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [Queue].[RegisterToSend] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [Queue].[Dequeue] TO [ServiceBusOwner];