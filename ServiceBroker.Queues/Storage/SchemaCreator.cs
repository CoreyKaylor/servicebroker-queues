using System;
using System.Data.SqlClient;
using System.Configuration;
using System.Threading;
using System.Data;

namespace ServiceBroker.Queues.Storage
{
    public class SchemaCreator
    {
        #region Create SQL
        private const string CREATE_SQL = @"
USE master;
GO

SET ANSI_NULL_DFLT_ON ON;
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET XACT_ABORT ON;

DECLARE @databaseName SYSNAME = '{0}';

IF DB_ID (@databaseName) IS NOT NULL
BEGIN
	ALTER DATABASE {0}
	SET single_user WITH ROLLBACK immediate 
	DROP DATABASE {0};
END 
GO

DECLARE @databaseName SYSNAME = '{0}';

DECLARE @dataDir NVARCHAR(260);
EXEC master.sys.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultData', @dataDir OUTPUT, 'no_output';
IF ((@dataDir IS NOT NULL) AND (LEN(@dataDir) > 0))
BEGIN
	IF (CHARINDEX(N'\', @dataDir, LEN(@dataDir)) = 0)
	BEGIN
		SET @dataDir = @dataDir + N'\';
	END
END
ELSE
BEGIN
	SET @dataDir =
	(
		SELECT SUBSTRING(physical_name, 1, LEN(physical_name) - (CHARINDEX(N'\', REVERSE(physical_name)) - 1))
		FROM master.sys.database_files
		WHERE type = 0
	);
END

DECLARE @logDir NVARCHAR(260);
EXEC master.sys.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultLog', @logDir OUTPUT, 'no_output';
IF ((@logDir IS NOT NULL) AND (LEN(@logDir) > 0))
BEGIN
	IF (CHARINDEX(N'\', @logDir, LEN(@logDir)) = 0)
	BEGIN
		SET @logDir = @logDir + N'\';
	END
END
ELSE
BEGIN
	SET @logDir =
	(
		SELECT SUBSTRING(physical_name, 1, LEN(physical_name) - (CHARINDEX(N'\', REVERSE(physical_name)) - 1))
		FROM master.sys.database_files
		WHERE type = 1
	);
END

DECLARE @dataPath NVARCHAR(260) = @dataDir + @databaseName + N'.mdf';
DECLARE @logName NVARCHAR(260) = @databaseName + N'_log';
DECLARE @logPath NVARCHAR(260) = @logDir + @logName + N'.ldf';

DECLARE @setupDatabaseCommand NVARCHAR(MAX) =
N'CREATE DATABASE ' + QUOTENAME(@databaseName, N'[') + N'
ON (NAME = ' + QUOTENAME(@databaseName, N'[') + N', FILENAME = N''' + @dataPath + N''', SIZE = 64 MB, MAXSIZE = UNLIMITED, FILEGROWTH = 10%)
LOG ON (NAME = ' + QUOTENAME(@logName, N'[') + N', FILENAME = N''' + @logPath + N''', SIZE = 128 MB, MAXSIZE = UNLIMITED, FILEGROWTH = 10%)
COLLATE Latin1_General_100_CI_AS
WITH DB_CHAINING OFF, TRUSTWORTHY OFF;

ALTER DATABASE ' + QUOTENAME(@databaseName, N'[') + '
SET
	AUTO_CREATE_STATISTICS ON,
	AUTO_SHRINK OFF,
	AUTO_UPDATE_STATISTICS_ASYNC ON,
	DATE_CORRELATION_OPTIMIZATION OFF,
	RECOVERY SIMPLE,
	PAGE_VERIFY CHECKSUM,
	ALLOW_SNAPSHOT_ISOLATION OFF, -- The queues require specific locking semantics that are not compatible with snapshot isolation.
	ANSI_NULL_DEFAULT ON,
	ANSI_NULLS ON,
	ANSI_PADDING ON,
	ANSI_WARNINGS ON,
	ARITHABORT ON,
	COMPATIBILITY_LEVEL = 100,
	CONCAT_NULL_YIELDS_NULL ON,
	QUOTED_IDENTIFIER ON,
	NUMERIC_ROUNDABORT OFF;';

IF(@setupDatabaseCommand IS NULL)
BEGIN
	RAISERROR('Setup command is null and cannot continue', 16, 1)
END 
EXECUTE (@setupDatabaseCommand);

GO

	USE {0}
	GO

CREATE SCHEMA [Bus]
GO

CREATE TABLE [Bus].[Detail]
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

IF EXISTS (SELECT * FROM sys.[endpoints] e WHERE e.[name] = 'ServiceBusEndpoint')
BEGIN
	DROP ENDPOINT ServiceBusEndpoint
END
GO

CREATE ENDPOINT ServiceBusEndpoint
STATE = STARTED
AS TCP 
(
	LISTENER_PORT = {1}
)
FOR SERVICE_BROKER 
(
	AUTHENTICATION = WINDOWS
)
GO

CREATE QUEUE NotificationQueue
GO

CREATE SERVICE NotificationService
ON QUEUE NotificationQueue
(
	[http://schemas.microsoft.com/SQL/Notifications/PostEventNotification]
)
GO

CREATE ROUTE [NotificationService]
	WITH SERVICE_NAME = 'NotificationService',
	ADDRESS = 'LOCAL'
GO

USE MASTER;
GO

GRANT CONNECT ON ENDPOINT::ServiceBusEndpoint TO [public];
GO 

USE [{0}];
GO

CREATE TABLE [Bus].[OutgoingHistory]
(
	[messageId] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_OutgoingHistory_messageId PRIMARY KEY CLUSTERED,
	[address] VARCHAR(255) NOT NULL,
	[conversationHandle] UNIQUEIDENTIFIER NOT NULL,
	[route] VARCHAR(255) NOT NULL,
	[sizeOfData] INT NOT NULL,
	[sentAt] DATETIME2(7) NOT NULL,
	[deferProcessingUntilTime] DATETIME2(7),
	[data] VARBINARY(MAX)
)
GO

CREATE TABLE [Bus].[ConversationDialogs]
(
	[conversationHandle] UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_ConversationDialogs_conversationHandle PRIMARY KEY CLUSTERED,
	[fromService] VARCHAR(255) NOT NULL,
	[toService] VARCHAR(255) NOT NULL,
	[createdAt] DATETIME2(7) NOT NULL
)
GO

CREATE TABLE [Bus].[MessageHistory]
(
	[localId] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_MessageHistory_localId PRIMARY KEY CLUSTERED,
	[queueName] VARCHAR(255) NOT NULL,
	[receivedTimestamp] DATETIME2(7) NOT NULL,
	[data] VARBINARY(MAX)
)
GO

CREATE PROCEDURE [Bus].[Dequeue]
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
			DECLARE @conversationHandle UNIQUEIDENTIFIER
			DECLARE @messageData VARBINARY(MAX)

			DECLARE deferredMessages CURSOR FAST_FORWARD READ_ONLY FOR
			SELECT oh.[conversationHandle], oh.[data]
			FROM [Bus].[OutgoingHistory] oh WITH(READPAST)
			WHERE oh.[deferProcessingUntilTime] <= sysutcdatetime()

			OPEN deferredMessages

			FETCH NEXT FROM deferredMessages INTO @conversationHandle, @messageData

			WHILE @@FETCH_STATUS = 0
			BEGIN

			SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [http://servicebroker.queues.com/servicebroker/2009/09/Message] (@messageData);

			FETCH NEXT FROM deferredMessages INTO @conversationHandle, @messageData

			END

			CLOSE deferredMessages
			DEALLOCATE deferredMessages
		END
		ELSE
		BEGIN 
			INSERT INTO [Bus].[MessageHistory] (
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

CREATE PROCEDURE GetQueueWithMessages
AS
BEGIN

DECLARE @cg UNIQUEIDENTIFIER
DECLARE @ch UNIQUEIDENTIFIER
DECLARE @messagetypename NVARCHAR(256)
DECLARE	@messagebody XML;

	WAITFOR (
			RECEIVE TOP (1)
				@cg = conversation_group_id,
				@ch = conversation_handle,
				@messagetypename = message_type_name,
				@messagebody = CAST(message_body AS XML)
			FROM NotificationQueue
		), TIMEOUT 15000
	
	--select the queue name from the xml message to indicate which queue needs activation
	SELECT @messagebody.value('/EVENT_INSTANCE[1]/ObjectName[1]', 'NVARCHAR(MAX)')
END
GO

CREATE PROCEDURE [Bus].[RegisterToSend]
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
	
	SET @conversationHandle = (SELECT TOP 1 [conversationHandle] FROM [Bus].[ConversationDialogs] WITH(READPAST) WHERE [toService] = @address AND [fromService] = @localServiceName)

	IF(@conversationHandle IS NULL)
	BEGIN
		BEGIN DIALOG CONVERSATION @conversationHandle
			FROM SERVICE @localServiceName
			TO SERVICE @address
			ON CONTRACT [http://servicebroker.queues.com/servicebroker/2009/09/ServiceBusContract]
			WITH ENCRYPTION = OFF;
			
		INSERT INTO [Bus].[ConversationDialogs] (
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
		
	INSERT INTO [Bus].[OutgoingHistory] (
		[address],
		[route],
		[conversationHandle],
		[sizeOfData],
		[deferProcessingUntilTime],
		[sentAt],
		[data]
	) VALUES ( 
		/* address - varchar(255) */ @address,
		/* route - varchar(255) */ @route,
		/* conversationHandle uniqueidentifier */ @conversationHandle,
		/* sizeOfData - int */ @sizeOfData,
		/* deferProcessingUntilTime - datetime2 */ @deferProcessingUntilTime,
		/* sentAt - datetime2 */ @sentAt,
		/* data - varbinary(max) */ @data )
		
	IF (@deferProcessingUntilTime IS NULL)
	BEGIN 
		SEND ON CONVERSATION @conversationHandle MESSAGE TYPE [http://servicebroker.queues.com/servicebroker/2009/09/Message] (@data);
	END
	ELSE
	BEGIN
		BEGIN CONVERSATION TIMER (@conversationHandle)
		TIMEOUT = DATEDIFF(SECOND, SYSUTCDATETIME(), @deferProcessingUntilTime);
	END
END
GO

CREATE PROCEDURE [Bus].[CreateQueueIfDoesNotExist]
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
		
		SET @sql = 'CREATE EVENT NOTIFICATION [' + @address + N'/notification]
		ON QUEUE [' + @address + N'/queue]
		FOR QUEUE_ACTIVATION
		TO SERVICE ''NotificationService'', ''current database'''
		
		--create a notification for activation used to determine when to start new processing threads
		--avoids the need for waitfor i.e. not compatible with MSDTC
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

CREATE PROCEDURE [Bus].[PurgeHistoric]
AS
BEGIN
	DELETE oh FROM [Bus].[OutgoingHistory] oh WITH(READPAST)
	WHERE oh.[messageId] < ((SELECT MAX(messageId) FROM [Bus].[OutgoingHistory] WITH(READPAST)) - 1000)
	
	DELETE mh FROM [Bus].[MessageHistory] mh WITH(READPAST)
	WHERE mh.[localId] < ((SELECT MAX(localId) FROM [Bus].[MessageHistory] WITH(READPAST)) - 1000)
	
	DECLARE @conversationHandle UNIQUEIDENTIFIER

	DECLARE oldConversations CURSOR FOR
	SELECT [conversationHandle]
	FROM [Bus].[ConversationDialogs] WITH(READPAST)
	WHERE [createdAt] >= DATEADD(hh, -2, sysutcdatetime())

	OPEN oldConversations

	FETCH NEXT FROM oldConversations INTO @conversationHandle

	WHILE @@FETCH_STATUS = 0
	BEGIN

	BEGIN TRY
		BEGIN
			END CONVERSATION @conversationHandle
			DELETE FROM [Bus].[ConversationDialogs]
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

INSERT INTO [Bus].[Detail](id, schemaVersion)
SELECT NEWID(), '1.0'
GO

CREATE TABLE [Bus].[Subscription]
(
	[messageTypeName] VARCHAR(255) NOT NULL,
	[subscriberUri] VARCHAR(255) NOT NULL,
	CONSTRAINT PK_Subscription_messageTypeName_subscriberUri PRIMARY KEY CLUSTERED ([messageTypeName], [subscriberUri])
)
GO

CREATE TABLE [Bus].[InstanceSubscription]
(
	[instanceId] UNIQUEIDENTIFIER NOT NULL,
	[messageTypeName] VARCHAR(255) NOT NULL,
	[subscriberUri] VARCHAR(255) NOT NULL,
	CONSTRAINT PK_InstanceSubscription_instanceId_messageTypeName_subscriberUri PRIMARY KEY CLUSTERED ([instanceId],[messageTypeName],[subscriberUri])
)
GO

CREATE PROCEDURE [Bus].[AddSubscriptionIfDoesNotExist]
(
	@messageTypeName VARCHAR(255),
	@subscriberUri VARCHAR(255)
)
AS 
BEGIN
	IF NOT EXISTS (SELECT * FROM [Bus].[Subscription] s WITH(NOLOCK) WHERE s.[messageTypeName] = @messageTypeName AND s.[subscriberUri] = @subscriberUri)
	BEGIN
		INSERT INTO [Bus].[Subscription] (
			[messageTypeName],
			[subscriberUri]
		) VALUES ( 
			/* messageTypeName - varchar(255) */ @messageTypeName,
			/* subscriberUri - varchar(255) */ @subscriberUri )
		SELECT 1
	END
	ELSE 
	BEGIN
		SELECT 0
	END
END
GO

CREATE PROCEDURE [Bus].[AddInstanceSubscriptionIfDoesNotExist]
(
	@instanceId UNIQUEIDENTIFIER,
	@messageTypeName VARCHAR(255),
	@subscriberUri VARCHAR(255)
)
AS 
BEGIN
	IF NOT EXISTS (SELECT * FROM [Bus].[InstanceSubscription] s WITH(NOLOCK) WHERE s.[instanceId] = @instanceId)
	BEGIN
		INSERT INTO [Bus].[InstanceSubscription] (
			[instanceId],
			[messageTypeName],
			[subscriberUri]
		) VALUES ( 
			/* instanceId - uniqueidentifier */ @instanceId,
			/* messageTypeName - varchar(255) */ @messageTypeName,
			/* subscriberUri - varchar(255) */ @subscriberUri ) 	
	END
END
GO

CREATE PROCEDURE [Bus].[GetAllSubscriptionsForType]
(
	@messageTypeName VARCHAR(255)
)
AS 
BEGIN
	SELECT s.[subscriberUri]
	FROM [Bus].[InstanceSubscription] s WITH(NOLOCK)
	WHERE s.[messageTypeName] = @messageTypeName
	UNION
	SELECT s2.[subscriberUri] 
	FROM [Bus].[Subscription] s2 WITH(NOLOCK)
	WHERE s2.[messageTypeName] = @messageTypeName
END
GO

CREATE ROLE [ServiceBusOwner] AUTHORIZATION [dbo];
GO

EXEC sys.sp_addrolemember N'db_datareader', N'ServiceBusOwner';
GO

EXEC sys.sp_addrolemember N'db_datawriter', N'ServiceBusOwner';
GO

GRANT EXECUTE ON [Bus].[PurgeHistoric] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [Bus].[RegisterToSend] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [Bus].[Dequeue] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [Bus].[AddInstanceSubscriptionIfDoesNotExist] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [Bus].[GetAllSubscriptionsForType] TO [ServiceBusOwner];
GO

GRANT EXECUTE ON [Bus].[AddSubscriptionIfDoesNotExist] TO [ServiceBusOwner];
GO

";
        #endregion //Create SQL

        public static string SchemaVersion
        {
            get { return "1.0"; }
        }

        public void Create(string database, int port)
        {
            var connectionStringSettings = ConfigurationManager.ConnectionStrings[database];
            var connectionString = connectionStringSettings.ConnectionString;

            database = database.GetUserDatabaseName();
            var sql = string.Format(CREATE_SQL, database, port).Split(new[] { "GO" }, StringSplitOptions.None);

            using (var localConnection = new SqlConnection(connectionString))
            {
                if (localConnection.State == ConnectionState.Closed)
                    localConnection.Open();
                foreach (var cmd in sql)
                {
                    ExecuteCommand(cmd, localConnection, createCommand => createCommand.ExecuteNonQuery());
                }
            }
            SqlConnection.ClearAllPools();
            bool notFinishedCreating = true;
            int retryCount = 0;
            

            while(notFinishedCreating && retryCount < 20)
            {
                using (var connection = new SqlConnection(string.Format("database={0};{1}",
                                                                        database, connectionString)))
                {
                    try
                    {
                        connection.Open();
                        notFinishedCreating = false;
                    }
                    catch (SqlException)
                    {
                        retryCount++;
                        Thread.Sleep(1000);
                        continue;
                    }
                    finally
                    {
                        connection.Close();
                    }
                }
            }
        }

        private static void ExecuteCommand(string commandText, SqlConnection connection, Action<SqlCommand> command)
        {
            using(var cmd = new SqlCommand(commandText, connection))
            {
                command(cmd);
            }
        }
    }
}