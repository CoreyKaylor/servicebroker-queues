using System;
using System.Data;
using System.Web;
using log4net;
using System.Data.SqlClient;

namespace ServiceBroker.Queues.Storage
{
    public class GlobalActions : AbstractActions
    {
        private readonly ILog logger = LogManager.GetLogger(typeof (GlobalActions));

        public GlobalActions(SqlConnection connection) : base(connection)
        {
        }

        public Uri PollForMessage()
        {
            string queueName = null;

            ExecuteCommand("[dbo].[GetQueueWithMessages]", cmd =>
            {
                cmd.CommandType = CommandType.StoredProcedure;
                queueName = Convert.ToString(cmd.ExecuteScalar());
            });

            return string.IsNullOrEmpty(queueName) ? null : new Uri("tcp://" + queueName.Replace("/queue", string.Empty));
        }

        public void CreateQueue(Uri queueUri)
        {
            ExecuteCommand("[SBQ].[CreateQueueIfDoesNotExist]", cmd =>
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@address", queueUri.ToServiceName());
                cmd.ExecuteNonQuery();
            });
        }

        public void DeleteHistoric()
        {
            ExecuteCommand("[SBQ].[PurgeHistoric]", cmd =>
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.ExecuteNonQuery();
            });
        }
    }
}