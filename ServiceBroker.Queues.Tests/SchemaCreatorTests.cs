using ServiceBroker.Queues.Storage;
using Xunit;

namespace ServiceBroker.Queues.Tests
{
    public class SchemaCreatorTests
    {
        [Fact]
        public void CanCreateQueueDb()
        {
            var schemaCreator = new SchemaCreator();
            schemaCreator.Create("testqueue", 2204);
        }
    }
}