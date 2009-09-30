using System;
using System.Configuration;
using System.Data.SqlClient;

namespace ServiceBroker.Queues.Storage
{
    public class StorageUtil
    {
        #region Purge SQL
        private static string PURGE_SQL = @"
USE {0}
GO
TRUNCATE TABLE [Bus].[InstanceSubscription]
GO
TRUNCATE TABLE [Bus].[Subscription]
GO
TRUNCATE TABLE [Bus].[MessageHistory]
GO
TRUNCATE TABLE [Bus].[OutgoingHistory]
GO

";

        private static string PURGE_QUEUE = @"
USE {0}
GO

declare @conversation uniqueidentifier

while exists (select top 1 conversation_handle from [{1}/queue])
begin
    exec [Bus].[Dequeue] @queueName = '{1}'
end
GO

";

        #endregion //Purge SQL

        public static void PurgeNonQueueInformation(string database, bool isUserInstance)
        {
            var connectionStringName = database;
            database = GetDatabase(database, isUserInstance);

            ExecuteScript(connectionStringName, database, string.Format(PURGE_SQL, database));
        }

        private static string GetDatabase(string database, bool isUserInstance)
        {
            if (isUserInstance)
                database = database.GetUserDatabaseName();
            return database;
        }

        public static void PurgeQueueData(string database, string queue, bool isUserInstance)
        {
            var connectionStringName = database;
            database = GetDatabase(database, isUserInstance);
            ExecuteScript(connectionStringName, database, string.Format(PURGE_QUEUE, database, queue));
        }

        private static void ExecuteScript(string connectionStringName, string database, string script)
        {
            var connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
            try
            {
                var sql = string.Format(script, database).Split(new[] { "GO" }, StringSplitOptions.None);
                using (var localConnection = new SqlConnection(connectionStringSettings.ConnectionString))
                {
                    localConnection.Open();
                    foreach (var cmd in sql)
                    {
                        using (var createCommand = new SqlCommand(cmd, localConnection))
                            createCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (SqlException)
            {

            }
        }
    }
}