using ServiceBroker.Queues.Storage;
using Xunit;
using System;

namespace ServiceBroker.Queues.Tests
{
    public class UtilityTests
    {
        [Fact]
        public void CanCorrectlyParseUriIntoServiceName()
        {
            var uri = new Uri("tcp://something.fake.com:2204/corey/devqueue");
            Assert.Equal(uri.ToServiceName(), "something.fake.com:2204/corey/devqueue");
        }
    }
}