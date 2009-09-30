using System;

namespace ServiceBroker.Queues
{
    public interface IQueue
    {
        MessageEnvelope Receive();
        MessageEnvelope Receive(TimeSpan timeout);
    }
}