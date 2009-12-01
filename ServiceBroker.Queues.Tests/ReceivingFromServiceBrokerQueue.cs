using System;
using System.Text;
using System.Threading;
using System.Transactions;
using Xunit;

namespace ServiceBroker.Queues.Tests
{
    public class ReceivingFromServiceBrokerQueue : QueueTest, IDisposable
    {
        private readonly QueueManager queueManager;
        private readonly Uri queueUri = new Uri("tcp://localhost:2204/h");

        public ReceivingFromServiceBrokerQueue() : base("testqueue")
        {
            queueManager = new QueueManager("testqueue");
			queueManager.CreateQueues(queueUri);
        }

        public void Dispose()
        {
            queueManager.Dispose();
        }

    	[Fact]
        public void CanReceiveFromQueue()
        {
            using (var tx = new TransactionScope())
            {
                queueManager.Send(queueUri, queueUri,
                                   new MessageEnvelope
                                   {
                                       Data = Encoding.Unicode.GetBytes("hello"),
                                   });
                tx.Complete();
            }
            Thread.Sleep(50);
            using(var tx = new TransactionScope())
            {
                var message = queueManager.GetQueue(queueUri).Receive();
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = queueManager.GetQueue(queueUri).Receive();
                Assert.Null(message);
                tx.Complete();
            }
        }

        [Fact]
        public void WhenRevertingTransactionMessageGoesBackToQueue()
        {
            using (var tx = new TransactionScope())
            {
                queueManager.Send(queueUri, queueUri,
                                   new MessageEnvelope
                                   {
                                       Data = Encoding.Unicode.GetBytes("hello"),
                                   });
                tx.Complete();
            }
            Thread.Sleep(30);

            using (new TransactionScope())
            {
                var message = queueManager.GetQueue(queueUri).Receive();
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            }
            using (new TransactionScope())
            {
                var message = queueManager.GetQueue(queueUri).Receive();
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            }
        }
    }
}