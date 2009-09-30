using System;

namespace ServiceBroker.Queues
{
    public class Queue : IQueue
    {
        private readonly QueueManager queueManager;
        private readonly Uri queueUri;

        public Queue(QueueManager queueManager, Uri queueUri)
        {
            this.queueManager = queueManager;
            this.queueUri = queueUri;
        }

        public MessageEnvelope Receive()
        {
            return queueManager.Receive(queueUri);
        }

        public MessageEnvelope Receive(TimeSpan timeout)
        {
            return queueManager.Receive(queueUri, timeout);
        }
    }
}