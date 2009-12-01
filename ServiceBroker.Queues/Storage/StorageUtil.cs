using System;
using System.Diagnostics;

namespace ServiceBroker.Queues.Storage
{
    public class StorageUtil
    {
		[Conditional("QUEUE_MODIFY")]
        public static void PurgeAll(string connectionStringName)
        {
			SqlFileCommandExecutor.ExecuteSqlScripts(connectionStringName, Environment.CurrentDirectory + "\\ServiceBroker\\SQL\\Purge");
        }
    }
}