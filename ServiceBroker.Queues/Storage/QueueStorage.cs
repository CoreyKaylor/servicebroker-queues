using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading;
using System.Data;

namespace ServiceBroker.Queues.Storage
{
    public class QueueStorage
    {
        private readonly string database;
        private readonly bool isUserInstance;
        private readonly int port;
        private readonly string connectionString;
        private readonly ConnectionStringSettings connectionStringSettings;

        public QueueStorage(string database, bool isUserInstance, int port)
        {
            connectionStringSettings = ConfigurationManager.ConnectionStrings[database];
            this.database = database;
            this.isUserInstance = isUserInstance;
            this.port = port;

            connectionString = string.Format("database={0};{1}",
                                             isUserInstance ? database.GetUserDatabaseName() : database, ConfigurationManager.ConnectionStrings[database]);
        }

        public Guid Id { get; private set; }

        public void Initialize()
        {
            try
            {
                SqlConnection.ClearAllPools();
                EnsureDatabaseIsCreated();
                SetIdFromDb();
				if (isUserInstance)
					StorageUtil.PurgeAll(database);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not open queue: " + database, e);
            }
        }

        private void SetIdFromDb()
        {
            int retryCount = 0;
            while (retryCount < 5)
            {
                try
                {
                    using (var connection = new SqlConnection(connectionString))
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
                                if (isUserInstance)
                                    new SchemaCreator().Create(database, port);
                                else
                                    throw new InvalidOperationException("The version on disk (" + schemaVersion +
                                                                        ") is different that the version supported by this library: " +
                                                                        SchemaCreator.SchemaVersion + Environment.NewLine +
                                                                        "You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can drop the database and it will be re-created (with no data) with the library version.");
                            }
                            break;
                        }
                    }
                }
                catch (SqlException e)
                {
                    retryCount++;
                    Thread.Sleep(1000);
                    if (retryCount == 5)
                        throw new InvalidOperationException(
                            "Could not read db details from disk. It is likely that there is a version difference between the library and the db on the disk." +
                            Environment.NewLine +
                            "You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.",
                            e);
                }
            }
        }

        private void EnsureDatabaseIsCreated()
        {
            try
            {
				
                using(var connection = new SqlConnection(connectionStringSettings.ConnectionString))
                {
                    connection.Open();
                    using (var sqlCommand = new SqlCommand(string.Format("SELECT DB_ID ('{0}')", isUserInstance ? database.GetUserDatabaseName() : database), connection)
                    {
                        CommandType = CommandType.Text
                    })
                    {
                        var dbExists = sqlCommand.ExecuteScalar();
                        if (dbExists != DBNull.Value) return;
                    }
                    if (isUserInstance)
                        new SchemaCreator().Create(database, port);
                }
            }
            catch(SqlException)
            {
                if (isUserInstance)
                    new SchemaCreator().Create(database, port);
            }
        }

        public void Global(Action<GlobalActions> action)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var qa = new GlobalActions(connection);
                action(qa);
            }
        }

		public void ExecuteNonQueryOnDbConnection(SqlCommand command, Action<SqlCommand> action)
		{
			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();
				using (command)
				{
					command.Connection = connection;
					action(command);
				}
			}
		}
    }
}