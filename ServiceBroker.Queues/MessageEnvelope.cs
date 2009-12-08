using System;
using System.Collections.Specialized;

namespace ServiceBroker.Queues
{
    public class MessageEnvelope
    {
        public MessageEnvelope()
        {
            Headers = new NameValueCollection();
        }

        public Guid ConversationId { get; set; }
        public DateTime? DeferProcessingUntilUtcTime { get; set; }
        public byte[] Data { get; set; }
        public NameValueCollection Headers { get; set; }
    }
}