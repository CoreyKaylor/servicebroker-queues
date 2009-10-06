using System;
using System.Data.SqlClient;

namespace ServiceBroker.Queues.Storage
{
    public class SchemaCreator
    {
        public static string SchemaVersion
        {
            get { return "1.0"; }
        }

        public void Create(string database, int port)
        {
        	var dbName = database.GetUserDatabaseName();
			SqlFileCommandExecutor.ExecuteSqlScripts(database, Environment.CurrentDirectory + "\\ServiceBroker\\SQL\\Create",
				sql =>
				{
					sql = sql.Replace("{databasename}", dbName);
					sql = sql.Replace("{port}", port.ToString());
					return sql;
				});
           
            SqlConnection.ClearAllPools();
        }
    }
}