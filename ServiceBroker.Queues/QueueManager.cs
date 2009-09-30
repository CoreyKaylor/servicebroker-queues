using System;
using System.Threading;
using System.Transactions;
using log4net;
using ServiceBroker.Queues.Storage;

namespace ServiceBroker.Queues
{
    public class QueueManager : IDisposable
    {
        private volatile bool wasDisposed;
        private readonly string databaseName;
        private readonly Timer purgeOldDataTimer;
        private readonly QueueStorage queueStorage;
        private readonly ILog logger = LogManager.GetLogger(typeof(QueueManager));

        public QueueManager(string databaseName, bool isUserInstance, int port)
        {
            this.databaseName = databaseName;
            queueStorage = new QueueStorage(databaseName, isUserInstance, port);
            queueStorage.Initialize();

            purgeOldDataTimer = new Timer(PurgeOldData, null, TimeSpan.FromMinutes(3), TimeSpan.FromMinutes(3));
        }

        private void PurgeOldData(object ignored)
        {
            logger.DebugFormat("Starting to purge old data");
            try
            {
                queueStorage.Global(actions =>
                {
                    actions.BeginTransaction();
                    actions.DeleteHistoric();
                    actions.Commit();
                });
            }
            catch (Exception exception)
            {
                logger.Warn("Failed to purge old data from the system", exception);
            }
        }

        public string DatabaseName
        {
            get { return databaseName; }
        }

        public void Dispose()
        {
            if(wasDisposed)
                return;

            if (purgeOldDataTimer != null)
            {
                purgeOldDataTimer.Dispose();
            }

            wasDisposed = true;
        }

        private void AssertNotDisposed()
        {
            if (wasDisposed)
                throw new ObjectDisposedException("QueueManager");
        }

        public IQueue GetQueue(Uri queueUri)
        {
            return new Queue(this, queueUri);
        }

        public MessageEnvelope Receive(Uri queueUri)
        {
            return Receive(queueUri, TimeSpan.FromDays(1));
        }

        public MessageEnvelope Receive(Uri queueUri, TimeSpan timeout)
        {
            EnsureEnslistment();
         
            var message = GetMessageFromQueue(queueUri);
            return message;
        }

        public Uri WaitForQueueWithMessageNotification()
        {
            if(Transaction.Current != null)
                throw new InvalidOperationException("You cannot find queue with messages with an ambient transaction, this method is not MSDTC friendly");

            Uri queueUri = null;
            queueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                queueUri = actions.PollForMessage();
                actions.Commit();
            });
            return queueUri;
        }

        public void CreateQueues(params Uri[] queues)
        {
            foreach (var queue in queues)
            {
                Uri uri = queue;
                queueStorage.Global(actions =>
                {
                    actions.BeginTransaction();
                    actions.CreateQueue(uri);
                    actions.Commit();
                });
            }
        }

        public void Send(Uri fromQueue, Uri toQueue, MessageEnvelope payload)
        {
            EnsureEnslistment();

            queueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                actions.GetQueue(fromQueue)
                    .RegisterToSend(toQueue, payload);
                actions.Commit();
            });
        }

        private void EnsureEnslistment()
        {
            AssertNotDisposed();

            if (Transaction.Current == null)
                throw new InvalidOperationException("You must use TransactionScope when using ServiceBroker.Queues");
        }

        private MessageEnvelope GetMessageFromQueue(Uri queueUri)
        {
            AssertNotDisposed();
            MessageEnvelope message = null;
            queueStorage.Global(actions =>
            {
                actions.BeginTransaction();
                message = actions.GetQueue(queueUri).Dequeue();
                actions.Commit();
            });
            return message;
        }
    }
}