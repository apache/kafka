/*
 * Copyright 2011 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

namespace Kafka.Client.Tests.Producers
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Producers.Partitioning;
    using NUnit.Framework;

    [TestFixture]
    public class PartitioningTests
    {
        private ProducerConfig config;

        [TestFixtureSetUp]
        public void SetUp()
        {
            config = new ProducerConfig();
            config.BrokerPartitionInfo = "1:192.168.0.1:1234,2:192.168.0.2:3456";
        }

        [Test]
        public void BrokerPartitionInfoGetAllBrokerInfoTest()
        {
            IBrokerPartitionInfo brokerPartitionInfo = new ConfigBrokerPartitionInfo(config);
            var allInfo = brokerPartitionInfo.GetAllBrokerInfo();
            this.MakeAssertionsForBroker(allInfo[1], 1, "192.168.0.1", 1234);
            this.MakeAssertionsForBroker(allInfo[2], 2, "192.168.0.2", 3456);
        }

        [Test]
        public void BrokerPartitionInfoGetPartitionInfo()
        {
            IBrokerPartitionInfo brokerPartitionInfo = new ConfigBrokerPartitionInfo(config);
            var broker = brokerPartitionInfo.GetBrokerInfo(1);
            this.MakeAssertionsForBroker(broker, 1, "192.168.0.1", 1234);
        }

        [Test]
        public void BrokerPartitionInfoGetPartitionInfoReturnsNullOnNonexistingBrokerId()
        {
            IBrokerPartitionInfo brokerPartitionInfo = new ConfigBrokerPartitionInfo(config);
            var broker = brokerPartitionInfo.GetBrokerInfo(45);
            Assert.IsNull(broker);
        }

        private void MakeAssertionsForBroker(Broker broker, int expectedId, string expectedHost, int expectedPort)
        {
            Assert.IsNotNull(broker);
            Assert.AreEqual(expectedId, broker.Id);
            Assert.AreEqual(expectedHost, broker.Host);
            Assert.AreEqual(expectedPort, broker.Port);
        }
    }
}
