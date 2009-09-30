using System;
using System.Data;
using System.Data.SqlClient;

namespace ServiceBroker.Queues.Storage
{
    public abstract class AbstractActions
    {
        protected readonly SqlConnection connection;
        protected SqlTransaction transaction;

        protected AbstractActions(SqlConnection connection)
        {
            this.connection = connection;
        }

        public QueueActions GetQueue(Uri queueUri)
        {
            return new QueueActions(queueUri, this);
        }

        public void BeginTransaction()
        {
            transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
        }

        public void Commit()
        {
            if(transaction == null)
                return;
            transaction.Commit();
        }

        internal void ExecuteCommand(string commandText, Action<SqlCommand> command)
        {
            using(var sqlCommand = new SqlCommand(commandText, connection))
            {
                sqlCommand.Transaction = transaction;
                command(sqlCommand);
            }
        }
    }
}