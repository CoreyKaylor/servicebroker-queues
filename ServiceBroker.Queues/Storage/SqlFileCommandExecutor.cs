using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace ServiceBroker.Queues.Storage
{
	public static class SqlFileCommandExecutor
	{
		public static void ExecuteSqlScripts(string database, string directory, Func<string, string> replacementFieldAction)
		{
			var connectionStringSettings = ConfigurationManager.ConnectionStrings[database];
			var connectionString = connectionStringSettings.ConnectionString;

			foreach (var file in Directory.GetFiles(directory, "*.sql").OrderBy(f => f))
			{
				var sql = File.ReadAllText(file);
				sql = replacementFieldAction(sql);
				var sqlStatements = sql.Split(new[] { "GO" }, StringSplitOptions.None);
				using (var localConnection = new SqlConnection(connectionString))
				{
					if (localConnection.State == ConnectionState.Closed)
						localConnection.Open();
					foreach (var cmd in sqlStatements)
					{
						ExecuteCommand(cmd, localConnection, createCommand => createCommand.ExecuteNonQuery());
					}
				}
			}
		}

		private static void ExecuteCommand(string commandText, SqlConnection connection, Action<SqlCommand> command)
		{
			using (var cmd = new SqlCommand(commandText, connection))
			{
				command(cmd);
			}
		}
	}
}