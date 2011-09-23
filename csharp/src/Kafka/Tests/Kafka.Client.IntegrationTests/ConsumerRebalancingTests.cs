/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Kafka.Client.IntegrationTests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Consumers;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using NUnit.Framework;

    [TestFixture]
    public class ConsumerRebalancingTests : IntegrationFixtureBase
    {
        /// <summary>
        /// Kafka Client configuration
        /// </summary>
        private static KafkaClientConfiguration clientConfig;

        [TestFixtureSetUp]
        public void SetUp()
        {
            clientConfig = KafkaClientConfiguration.GetConfiguration();
        }

        [Test]
        public void ConsumerPorformsRebalancingOnStart()
        {
            var config = new ConsumerConfig(clientConfig) { AutoCommit = false, GroupId = "group1" };
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                ZooKeeperClient client = ReflectionHelper.GetInstanceField<ZooKeeperClient>("zkClient", consumerConnector);
                Assert.IsNotNull(client);
                client.DeleteRecursive("/consumers/group1");
                var topicCount = new Dictionary<string, int> { { "test", 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);
                IList<string> children = client.GetChildren("/consumers", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);
                Assert.That(children, Contains.Item("group1"));
                children = client.GetChildren("/consumers/group1", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);
                Assert.That(children, Contains.Item("ids"));
                Assert.That(children, Contains.Item("owners"));
                children = client.GetChildren("/consumers/group1/ids", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);
                string consumerId = children[0];
                children = client.GetChildren("/consumers/group1/owners", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);
                Assert.That(children.Count, Is.EqualTo(1));
                Assert.That(children, Contains.Item("test"));
                children = client.GetChildren("/consumers/group1/owners/test", false);
                Assert.That(children, Is.Not.Null.And.Not.Empty);
                Assert.That(children.Count, Is.EqualTo(2));
                string partId = children[0];
                string data = client.ReadData<string>("/consumers/group1/owners/test/" + partId);
                Assert.That(data, Is.Not.Null.And.Not.Empty);
                Assert.That(data, Contains.Substring(consumerId));
                data = client.ReadData<string>("/consumers/group1/ids/" + consumerId);
                Assert.That(data, Is.Not.Null.And.Not.Empty);
                Assert.That(data, Is.EqualTo("{ \"test\": 1 }"));
            }

            using (var client = new ZooKeeperClient(config.ZkConnect, config.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                //// Should be created as ephemeral
                IList<string> children = client.GetChildren("/consumers/group1/ids");
                Assert.That(children, Is.Null.Or.Empty);
                //// Should be created as ephemeral
                children = client.GetChildren("/consumers/group1/owners/test");
                Assert.That(children, Is.Null.Or.Empty);
            }
        }

        [Test]
        public void ConsumerPorformsRebalancingWhenNewBrokerIsAddedToTopic()
        {
            var config = new ConsumerConfig(clientConfig)
                             {
                                 AutoCommit = false,
                                 GroupId = "group1",
                                 ZkSessionTimeoutMs = 60000,
                                 ZkConnectionTimeoutMs = 60000
                             };
            string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + 2345;
            string brokerTopicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/test/" + 2345;
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var client = ReflectionHelper.GetInstanceField<ZooKeeperClient>(
                    "zkClient", consumerConnector);
                Assert.IsNotNull(client);
                client.DeleteRecursive("/consumers/group1");
                var topicCount = new Dictionary<string, int> { { "test", 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);
                IList<string> children = client.GetChildren("/consumers/group1/ids", false);
                string consumerId = children[0];
                client.CreateEphemeral(brokerPath, "192.168.1.39-1310449279123:192.168.1.39:9102");
                client.CreateEphemeral(brokerTopicPath, 1);
                WaitUntillIdle(client, 500);
                children = client.GetChildren("/consumers/group1/owners/test", false);
                Assert.That(children.Count, Is.EqualTo(3));
                Assert.That(children, Contains.Item("2345-0"));
                string data = client.ReadData<string>("/consumers/group1/owners/test/2345-0");
                Assert.That(data, Is.Not.Null);
                Assert.That(data, Contains.Substring(consumerId));
                var topicRegistry =
                    ReflectionHelper.GetInstanceField<IDictionary<string, IDictionary<Partition, PartitionTopicInfo>>>(
                        "topicRegistry", consumerConnector);
                Assert.That(topicRegistry, Is.Not.Null.And.Not.Empty);
                Assert.That(topicRegistry.Count, Is.EqualTo(1));
                var item = topicRegistry["test"];
                Assert.That(item.Count, Is.EqualTo(3));
                var broker = topicRegistry["test"].SingleOrDefault(x => x.Key.BrokerId == 2345);
                Assert.That(broker, Is.Not.Null);
            }
        }

        [Test]
        public void ConsumerPorformsRebalancingWhenBrokerIsRemovedFromTopic()
        {
            var config = new ConsumerConfig(clientConfig) { AutoCommit = false, GroupId = "group1", ZkSessionTimeoutMs = 60000, ZkConnectionTimeoutMs = 60000 };
            string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + 2345;
            string brokerTopicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/test/" + 2345;
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var client = ReflectionHelper.GetInstanceField<ZooKeeperClient>("zkClient", consumerConnector);
                Assert.IsNotNull(client);
                client.DeleteRecursive("/consumers/group1");
                var topicCount = new Dictionary<string, int> { { "test", 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);
                client.CreateEphemeral(brokerPath, "192.168.1.39-1310449279123:192.168.1.39:9102");
                client.CreateEphemeral(brokerTopicPath, 1);
                WaitUntillIdle(client, 1000);
                client.DeleteRecursive(brokerTopicPath);
                WaitUntillIdle(client, 1000);

                IList<string> children = client.GetChildren("/consumers/group1/owners/test", false);
                Assert.That(children.Count, Is.EqualTo(2));
                Assert.That(children, Has.None.EqualTo("2345-0"));
                var topicRegistry = ReflectionHelper.GetInstanceField<IDictionary<string, IDictionary<Partition, PartitionTopicInfo>>>("topicRegistry", consumerConnector);
                Assert.That(topicRegistry, Is.Not.Null.And.Not.Empty);
                Assert.That(topicRegistry.Count, Is.EqualTo(1));
                var item = topicRegistry["test"];
                Assert.That(item.Count, Is.EqualTo(2));
                Assert.That(item.Where(x => x.Value.BrokerId == 2345).Count(), Is.EqualTo(0));
            }
        }

        [Test]
        public void ConsumerPerformsRebalancingWhenNewConsumerIsAddedAndTheyDividePartitions()
        {
            var config = new ConsumerConfig(clientConfig)
            {
                AutoCommit = false,
                GroupId = "group1",
                ZkSessionTimeoutMs = 60000,
                ZkConnectionTimeoutMs = 60000
            };

            IList<string> ids;
            IList<string> owners;
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var client = ReflectionHelper.GetInstanceField<ZooKeeperClient>(
                    "zkClient", consumerConnector);
                Assert.IsNotNull(client);
                client.DeleteRecursive("/consumers/group1");
                var topicCount = new Dictionary<string, int> { { "test", 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);
                using (var consumerConnector2 = new ZookeeperConsumerConnector(config, true))
                {
                    consumerConnector2.CreateMessageStreams(topicCount);
                    WaitUntillIdle(client, 1000);
                    ids = client.GetChildren("/consumers/group1/ids", false).ToList();
                    owners = client.GetChildren("/consumers/group1/owners/test", false).ToList();

                    Assert.That(ids, Is.Not.Null.And.Not.Empty);
                    Assert.That(ids.Count, Is.EqualTo(2));
                    Assert.That(owners, Is.Not.Null.And.Not.Empty);
                    Assert.That(owners.Count, Is.EqualTo(2));

                    var data1 = client.ReadData<string>("/consumers/group1/owners/test/" + owners[0], false);
                    var data2 = client.ReadData<string>("/consumers/group1/owners/test/" + owners[1], false);

                    Assert.That(data1, Is.Not.Null.And.Not.Empty);
                    Assert.That(data2, Is.Not.Null.And.Not.Empty);
                    Assert.That(data1, Is.Not.EqualTo(data2));
                    Assert.That(data1, Is.StringStarting(ids[0]).Or.StringStarting(ids[1]));
                    Assert.That(data2, Is.StringStarting(ids[0]).Or.StringStarting(ids[1]));
                }
            }
        }

        [Test]
        public void ConsumerPerformsRebalancingWhenConsumerIsRemovedAndTakesItsPartitions()
        {
            var config = new ConsumerConfig(clientConfig)
            {
                AutoCommit = false,
                GroupId = "group1",
                ZkSessionTimeoutMs = 60000,
                ZkConnectionTimeoutMs = 60000
            };

            IList<string> ids;
            IList<string> owners;
            using (var consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var client = ReflectionHelper.GetInstanceField<ZooKeeperClient>("zkClient", consumerConnector);
                Assert.IsNotNull(client);
                client.DeleteRecursive("/consumers/group1");
                var topicCount = new Dictionary<string, int> { { "test", 1 } };
                consumerConnector.CreateMessageStreams(topicCount);
                WaitUntillIdle(client, 1000);
                using (var consumerConnector2 = new ZookeeperConsumerConnector(config, true))
                {
                    consumerConnector2.CreateMessageStreams(topicCount);
                    WaitUntillIdle(client, 1000);
                    ids = client.GetChildren("/consumers/group1/ids", false).ToList();
                    owners = client.GetChildren("/consumers/group1/owners/test", false).ToList();
                    Assert.That(ids, Is.Not.Null.And.Not.Empty);
                    Assert.That(ids.Count, Is.EqualTo(2));
                    Assert.That(owners, Is.Not.Null.And.Not.Empty);
                    Assert.That(owners.Count, Is.EqualTo(2));
                }

                WaitUntillIdle(client, 1000);
                ids = client.GetChildren("/consumers/group1/ids", false).ToList();
                owners = client.GetChildren("/consumers/group1/owners/test", false).ToList();

                Assert.That(ids, Is.Not.Null.And.Not.Empty);
                Assert.That(ids.Count, Is.EqualTo(1));
                Assert.That(owners, Is.Not.Null.And.Not.Empty);
                Assert.That(owners.Count, Is.EqualTo(2));

                var data1 = client.ReadData<string>("/consumers/group1/owners/test/" + owners[0], false);
                var data2 = client.ReadData<string>("/consumers/group1/owners/test/" + owners[1], false);

                Assert.That(data1, Is.Not.Null.And.Not.Empty);
                Assert.That(data2, Is.Not.Null.And.Not.Empty);
                Assert.That(data1, Is.EqualTo(data2));
                Assert.That(data1, Is.StringStarting(ids[0]));
            }
        }
    }
}
