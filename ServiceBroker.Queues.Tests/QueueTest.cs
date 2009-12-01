using System.Configuration;
using ServiceBroker.Queues.Storage;
using System.Data.SqlClient;
using System.Data;

namespace ServiceBroker.Queues.Tests
{
	public abstract class QueueTest
	{
		protected QueueTest(string connectionStringName)
		{
			var connectionStringBuilder =
				new SqlConnectionStringBuilder(ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString);
			var databaseName = connectionStringBuilder.InitialCatalog;
			connectionStringBuilder.InitialCatalog = "master";
			using(var connection = new SqlConnection(connectionStringBuilder.ConnectionString))
			{
				connection.Open();
				using(var createCommand = new SqlCommand(
				@"
				IF ((SELECT DB_ID ('<databasename, sysname, queuedb>')) IS NULL)
				BEGIN
					CREATE DATABASE [<databasename, sysname, queuedb>]
				END".Replace("<databasename, sysname, queuedb>", databaseName), connection))
				{
					createCommand.CommandType = CommandType.Text;
					createCommand.ExecuteNonQuery();
				}
			}
			try
			{
				var storage = new QueueStorage(connectionStringName);
				storage.Initialize();
			}
			catch(SqlException)
			{
				new SchemaCreator().Create(connectionStringName, 2204);
			}
			StorageUtil.PurgeAll(connectionStringName);
		}
	}
}