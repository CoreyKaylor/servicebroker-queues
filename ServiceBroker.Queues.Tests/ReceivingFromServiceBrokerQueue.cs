using System;
using System.Text;
using System.Threading;
using System.Transactions;
using ServiceBroker.Queues.Storage;
using Xunit;

namespace ServiceBroker.Queues.Tests
{
    public class ReceivingFromServiceBrokerQueue : IDisposable
    {
        private readonly QueueManager queueManager;
        private readonly Uri queueUri = new Uri("tcp://localhost:2204/h");

        public ReceivingFromServiceBrokerQueue()
        {
            StorageUtil.PurgeNonQueueInformation("testqueue", true);
            StorageUtil.PurgeQueueData("testqueue", "localhost:2204/h", true);
            queueManager = new QueueManager("testqueue", true, queueUri.Port);
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
            var queueWithMessage = queueManager.WaitForQueueWithMessageNotification();
            using(var tx = new TransactionScope())
            {
                var message = queueManager.GetQueue(queueWithMessage).Receive();
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
                tx.Complete();
            }

            using (var tx = new TransactionScope())
            {
                var message = queueManager.GetQueue(queueWithMessage).Receive();
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
            var queueWithMessage = queueManager.WaitForQueueWithMessageNotification();

            using (new TransactionScope())
            {
                var message = queueManager.GetQueue(queueWithMessage).Receive();
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            }
            using (new TransactionScope())
            {
                var message = queueManager.GetQueue(queueWithMessage).Receive();
                Assert.Equal("hello", Encoding.Unicode.GetString(message.Data));
            }
        }
    }
}