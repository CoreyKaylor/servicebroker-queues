using System;
using System.Security.Principal;

namespace ServiceBroker.Queues.Storage
{
    public static class ExtensionMethods
    {
        public static string GetUserDatabaseName(this string database)
        {
            return database + WindowsIdentity.GetCurrent().User.Value.Replace('-', '_');
        }

        public static string ToServiceName(this Uri uri)
        {
            return uri.Authority + uri.PathAndQuery;
        }
    }
}