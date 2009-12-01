using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;

namespace ServiceBroker.Queues.Storage
{
    public class QueueStorage
    {
        private readonly ConnectionStringSettings connectionStringSettings;

        public QueueStorage(string connectionStringName)
        {
            connectionStringSettings = ConfigurationManager.ConnectionStrings[connectionStringName];
        }

        public Guid Id { get; private set; }

        public void Initialize()
        {
			SetIdFromDb();
		}

        private void SetIdFromDb()
        {
			using (var connection = new SqlConnection(connectionStringSettings.ConnectionString))
			{
				connection.Open();
				using (var sqlCommand = new SqlCommand("select * from [Queue].[Detail]", connection))
				using (var reader = sqlCommand.ExecuteReader(CommandBehavior.SingleRow))
				{
					if (reader == null || !reader.Read())
						throw new InvalidOperationException("No version detail found in the queue storage");

					Id = reader.GetGuid(reader.GetOrdinal("id"));
					var schemaVersion = reader.GetString(reader.GetOrdinal("schemaVersion"));
					if (schemaVersion != SchemaCreator.SchemaVersion)
					{
						throw new InvalidOperationException("The version on disk (" + schemaVersion +
															") is different that the version supported by this library: " +
															SchemaCreator.SchemaVersion + Environment.NewLine +
															"You need to migrate the database version to the library version, alternatively, if the data isn't important, you can drop the items in the [Queue] schema and run the scripts to create it.");
					}
				}
			}
        }

        public void Global(Action<GlobalActions> action)
        {
            using (var connection = new SqlConnection(connectionStringSettings.ConnectionString))
            {
                connection.Open();
                var qa = new GlobalActions(connection);
                action(qa);
            }
        }
    }
}