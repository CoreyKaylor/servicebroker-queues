using System;
using System.Data;
using log4net;
using System.IO;
using System.Runtime.Serialization;

namespace ServiceBroker.Queues.Storage
{
    public class QueueActions
    {
        private readonly Uri queueUri;
        private AbstractActions actions;
        private readonly ILog logger = LogManager.GetLogger(typeof (QueueActions));

        public QueueActions(Uri queueUri, AbstractActions actions)
        {
            this.queueUri = queueUri;
            this.actions = actions;
        }

        public AbstractActions Actions
        {
            get { return actions; }
            set{ actions = value; }
        }

        public MessageEnvelope Dequeue()
        {
            MessageEnvelope message = null;
            actions.ExecuteCommand("[Bus].[Dequeue]", cmd =>
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@queueName", queueUri.ToServiceName());
                using (var reader = cmd.ExecuteReader(CommandBehavior.Default))
                {
                    if (reader == null || !reader.Read())
                    {
                        message = null;
                        return;
                    }

                    message = Fill(reader);
                }
            });
            return message;
        }

        public void RegisterToSend(Uri destination, MessageEnvelope payload)
        {
            byte[] data;
            using(var ms = new MemoryStream())
            {
                var serializer = new NetDataContractSerializer();
                serializer.Serialize(ms, payload);
                data = ms.ToArray();
            }
            actions.ExecuteCommand("[Bus].[RegisterToSend]", cmd =>
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@localServiceName", queueUri.ToServiceName());
                cmd.Parameters.AddWithValue("@address", destination.ToServiceName());
                cmd.Parameters.AddWithValue("@route", string.Format("{0}://{1}", destination.Scheme, destination.Authority));
                cmd.Parameters.AddWithValue("@sizeOfData", payload.Data.Length);
                cmd.Parameters.AddWithValue("@deferProcessingUntilTime",
                                            (object)payload.DeferProcessingUntilTime ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@sentAt", DateTime.UtcNow);
                cmd.Parameters.AddWithValue("@data", data);
                cmd.ExecuteNonQuery();
            });
            logger.DebugFormat("Created output message for 'net.tcp://{0}:{1}'",
                               destination.Host,
                               destination.Port
                );
        }

        private static MessageEnvelope Fill(IDataRecord reader)
        {
            var conversationId = reader.GetGuid(0);
            using (var stream = new MemoryStream((byte[])reader.GetValue(1)))
            {
                var serializer = new NetDataContractSerializer();
                var message = (MessageEnvelope)serializer.ReadObject(stream);
                message.ConversationId = conversationId;
                return message;
            }
        }
    }
}