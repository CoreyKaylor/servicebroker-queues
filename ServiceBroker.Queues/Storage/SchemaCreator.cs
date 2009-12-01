using System;
using System.Data.SqlClient;
using System.Diagnostics;

namespace ServiceBroker.Queues.Storage
{
    public class SchemaCreator
    {
        public static string SchemaVersion
        {
            get { return "1.0"; }
        }

		[Conditional("QUEUE_MODIFY")]
        public void Create(string connectionStringName, int port)
        {
			SqlFileCommandExecutor.ExecuteSqlScripts(connectionStringName, Environment.CurrentDirectory + "\\ServiceBroker\\SQL\\Create",
				sql =>
				{
					sql = sql.Replace("<port, , 2204>", port.ToString());
					return sql;
				});
           
            SqlConnection.ClearAllPools();
        }
    }
}