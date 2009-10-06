using System;

namespace ServiceBroker.Queues.Storage
{
    public class StorageUtil
    {
        public static void PurgeAll(string database)
        {
        	SqlFileCommandExecutor.ExecuteSqlScripts(database, Environment.CurrentDirectory + "\\ServiceBroker\\SQL\\Purge",
													 sql =>
													 {
														 sql = sql.Replace("{databasename}", database.GetUserDatabaseName());
														 return sql;
													 });
        }
    }
}