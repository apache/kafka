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
    using Kafka.Client.Cluster;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using Kafka.Client.ZooKeeperIntegration.Listeners;
    using NUnit.Framework;
    using ZooKeeperNet;

    [TestFixture]
    public class ZkBrokerPartitionInfoTests : IntegrationFixtureBase
    {
        [Test]
        public void ZkBrokerPartitionInfoGetsAllBrokerInfo()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;
            var prodConfigNotZk = this.ConfigBasedSyncProdConfig;

            IDictionary<int, Broker> allBrokerInfo;
            using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(prodConfig, null))
            {
                allBrokerInfo = brokerPartitionInfo.GetAllBrokerInfo();
            }

            Assert.AreEqual(prodConfigNotZk.Brokers.Count, allBrokerInfo.Count);
            allBrokerInfo.Values.All(x => prodConfigNotZk.Brokers.Any(
                y => x.Id == y.BrokerId
                && x.Host == y.Host
                && x.Port == y.Port));
        }

        [Test]
        public void ZkBrokerPartitionInfoGetsBrokerPartitionInfo()
        {
            var prodconfig = this.ZooKeeperBasedSyncProdConfig;
            SortedSet<Partition> partitions;
            using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(prodconfig, null))
            {
                partitions = brokerPartitionInfo.GetBrokerPartitionInfo("test");
            }

            Assert.NotNull(partitions);
            Assert.GreaterOrEqual(partitions.Count, 2);
        }

        [Test]
        public void ZkBrokerPartitionInfoGetsBrokerInfo()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;
            var prodConfigNotZk = this.ConfigBasedSyncProdConfig;

            using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(prodConfig, null))
            {
                var testBroker = prodConfigNotZk.Brokers[0];
                Broker broker = brokerPartitionInfo.GetBrokerInfo(testBroker.BrokerId);
                Assert.NotNull(broker);
                Assert.AreEqual(testBroker.Host, broker.Host);
                Assert.AreEqual(testBroker.Port, broker.Port);
            }
        }

        [Test]
        public void WhenNewTopicIsAddedBrokerTopicsListenerCreatesNewMapping()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            IDictionary<string, SortedSet<Partition>> mappings;
            IDictionary<int, Broker> brokers;
            string topicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + CurrentTestTopic;

            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(client))
                {
                    brokers = brokerPartitionInfo.GetAllBrokerInfo();
                    mappings =
                        ReflectionHelper.GetInstanceField<IDictionary<string, SortedSet<Partition>>>(
                            "topicBrokerPartitions", brokerPartitionInfo);
                }
            }

            Assert.NotNull(brokers);
            Assert.Greater(brokers.Count, 0);
            Assert.NotNull(mappings);
            Assert.Greater(mappings.Count, 0);
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                WaitUntillIdle(client, 500);
                var brokerTopicsListener = new BrokerTopicsListener(client, mappings, brokers, null);
                client.Subscribe(ZooKeeperClient.DefaultBrokerTopicsPath, brokerTopicsListener);
                client.CreatePersistent(topicPath, true);
                WaitUntillIdle(client, 500);
                client.UnsubscribeAll();
                WaitUntillIdle(client, 500);
                client.DeleteRecursive(topicPath);
            }

            Assert.IsTrue(mappings.ContainsKey(CurrentTestTopic));
        }

        [Test]
        public void WhenNewBrokerIsAddedBrokerTopicsListenerUpdatesBrokersList()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            IDictionary<string, SortedSet<Partition>> mappings;
            IDictionary<int, Broker> brokers;
            string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + 2345;
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(client))
                {
                    brokers = brokerPartitionInfo.GetAllBrokerInfo();
                    mappings =
                        ReflectionHelper.GetInstanceField<IDictionary<string, SortedSet<Partition>>>(
                            "topicBrokerPartitions", brokerPartitionInfo);
                }
            }

            Assert.NotNull(brokers);
            Assert.Greater(brokers.Count, 0);
            Assert.NotNull(mappings);
            Assert.Greater(mappings.Count, 0);
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                WaitUntillIdle(client, 500);
                var brokerTopicsListener = new BrokerTopicsListener(client, mappings, brokers, null);
                client.Subscribe(ZooKeeperClient.DefaultBrokerIdsPath, brokerTopicsListener);
                WaitUntillIdle(client, 500);
                client.CreatePersistent(brokerPath, true);
                client.WriteData(brokerPath, "192.168.1.39-1310449279123:192.168.1.39:9102");
                WaitUntillIdle(client, 500);
                client.UnsubscribeAll();
                WaitUntillIdle(client, 500);
                client.DeleteRecursive(brokerPath);
            }

            Assert.IsTrue(brokers.ContainsKey(2345));
            Assert.AreEqual("192.168.1.39", brokers[2345].Host);
            Assert.AreEqual(9102, brokers[2345].Port);
            Assert.AreEqual(2345, brokers[2345].Id);
        }

        [Test]
        public void WhenBrokerIsRemovedBrokerTopicsListenerUpdatesBrokersList()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            IDictionary<string, SortedSet<Partition>> mappings;
            IDictionary<int, Broker> brokers;
            string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + 2345;
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(client))
                {
                    brokers = brokerPartitionInfo.GetAllBrokerInfo();
                    mappings =
                        ReflectionHelper.GetInstanceField<IDictionary<string, SortedSet<Partition>>>(
                            "topicBrokerPartitions", brokerPartitionInfo);
                }
            }

            Assert.NotNull(brokers);
            Assert.Greater(brokers.Count, 0);
            Assert.NotNull(mappings);
            Assert.Greater(mappings.Count, 0);
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                WaitUntillIdle(client, 500); 
                var brokerTopicsListener = new BrokerTopicsListener(client, mappings, brokers, null);
                client.Subscribe(ZooKeeperClient.DefaultBrokerIdsPath, brokerTopicsListener);
                client.CreatePersistent(brokerPath, true);
                client.WriteData(brokerPath, "192.168.1.39-1310449279123:192.168.1.39:9102");
                WaitUntillIdle(client, 500); 
                Assert.IsTrue(brokers.ContainsKey(2345));
                client.DeleteRecursive(brokerPath);
                WaitUntillIdle(client, 500); 
                Assert.IsFalse(brokers.ContainsKey(2345));
            }
        }

        [Test]
        public void WhenNewBrokerInTopicIsAddedBrokerTopicsListenerUpdatesMappings()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            IDictionary<string, SortedSet<Partition>> mappings;
            IDictionary<int, Broker> brokers;
            string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + 2345;
            string topicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + CurrentTestTopic;
            string topicBrokerPath = topicPath + "/" + 2345;
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(client))
                {
                    brokers = brokerPartitionInfo.GetAllBrokerInfo();
                    mappings =
                        ReflectionHelper.GetInstanceField<IDictionary<string, SortedSet<Partition>>>(
                            "topicBrokerPartitions", brokerPartitionInfo);
                }
            }

            Assert.NotNull(brokers);
            Assert.Greater(brokers.Count, 0);
            Assert.NotNull(mappings);
            Assert.Greater(mappings.Count, 0);
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                WaitUntillIdle(client, 500);
                var brokerTopicsListener = new BrokerTopicsListener(client, mappings, brokers, null);
                client.Subscribe(ZooKeeperClient.DefaultBrokerIdsPath, brokerTopicsListener);
                client.Subscribe(ZooKeeperClient.DefaultBrokerTopicsPath, brokerTopicsListener);
                client.CreatePersistent(brokerPath, true);
                client.WriteData(brokerPath, "192.168.1.39-1310449279123:192.168.1.39:9102");
                client.CreatePersistent(topicPath, true);
                WaitUntillIdle(client, 500);
                Assert.IsTrue(brokers.ContainsKey(2345));
                Assert.IsTrue(mappings.ContainsKey(CurrentTestTopic));
                client.CreatePersistent(topicBrokerPath, true);
                client.WriteData(topicBrokerPath, 5);
                WaitUntillIdle(client, 500);
                client.UnsubscribeAll();
                WaitUntillIdle(client, 500);
                client.DeleteRecursive(brokerPath);
                client.DeleteRecursive(topicPath);
            }

            Assert.IsTrue(brokers.ContainsKey(2345));
            Assert.IsTrue(mappings.Keys.Contains(CurrentTestTopic));
            Assert.AreEqual(5, mappings[CurrentTestTopic].Count);
        }

        [Test]
        public void WhenSessionIsExpiredListenerRecreatesEphemeralNodes()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            IDictionary<string, SortedSet<Partition>> mappings;
            IDictionary<int, Broker> brokers;
            IDictionary<string, SortedSet<Partition>> mappings2;
            IDictionary<int, Broker> brokers2;
            using (
                IZooKeeperClient client = new ZooKeeperClient(
                    prodConfig.ZooKeeper.ZkConnect,
                    prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                    ZooKeeperStringSerializer.Serializer))
            {
                using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(client))
                {
                    brokers = brokerPartitionInfo.GetAllBrokerInfo();
                    mappings =
                        ReflectionHelper.GetInstanceField<IDictionary<string, SortedSet<Partition>>>(
                            "topicBrokerPartitions", brokerPartitionInfo);
                    Assert.NotNull(brokers);
                    Assert.Greater(brokers.Count, 0);
                    Assert.NotNull(mappings);
                    Assert.Greater(mappings.Count, 0);
                    client.Process(new WatchedEvent(KeeperState.Expired, EventType.None, null));
                    WaitUntillIdle(client, 3000);
                    brokers2 = brokerPartitionInfo.GetAllBrokerInfo();
                    mappings2 =
                        ReflectionHelper.GetInstanceField<IDictionary<string, SortedSet<Partition>>>(
                            "topicBrokerPartitions", brokerPartitionInfo);
                }
            }

            Assert.NotNull(brokers2);
            Assert.Greater(brokers2.Count, 0);
            Assert.NotNull(mappings2);
            Assert.Greater(mappings2.Count, 0);
            Assert.AreEqual(brokers.Count, brokers2.Count);
            Assert.AreEqual(mappings.Count, mappings2.Count);
        }

        [Test]
        public void WhenNewTopicIsAddedZkBrokerPartitionInfoUpdatesMappings()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            IDictionary<string, SortedSet<Partition>> mappings;
            string topicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + CurrentTestTopic;

            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(client))
                {
                    mappings =
                        ReflectionHelper.GetInstanceField<IDictionary<string, SortedSet<Partition>>>(
                            "topicBrokerPartitions", brokerPartitionInfo);
                    client.CreatePersistent(topicPath, true);
                    WaitUntillIdle(client, 500);
                    client.UnsubscribeAll();
                    WaitUntillIdle(client, 500);
                    client.DeleteRecursive(topicPath);
                }
            }

            Assert.NotNull(mappings);
            Assert.Greater(mappings.Count, 0);
            Assert.IsTrue(mappings.ContainsKey(CurrentTestTopic));
        }

        [Test]
        public void WhenNewBrokerIsAddedZkBrokerPartitionInfoUpdatesBrokersList()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            IDictionary<int, Broker> brokers;
            string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + 2345;
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(client))
                {
                    brokers = brokerPartitionInfo.GetAllBrokerInfo();
                    client.CreatePersistent(brokerPath, true);
                    client.WriteData(brokerPath, "192.168.1.39-1310449279123:192.168.1.39:9102");
                    WaitUntillIdle(client, 500);
                    client.UnsubscribeAll();
                    WaitUntillIdle(client, 500);
                    client.DeleteRecursive(brokerPath);
                }
            }

            Assert.NotNull(brokers);
            Assert.Greater(brokers.Count, 0);
            Assert.IsTrue(brokers.ContainsKey(2345));
            Assert.AreEqual("192.168.1.39", brokers[2345].Host);
            Assert.AreEqual(9102, brokers[2345].Port);
            Assert.AreEqual(2345, brokers[2345].Id);
        }

        [Test]
        public void WhenBrokerIsRemovedZkBrokerPartitionInfoUpdatesBrokersList()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            IDictionary<int, Broker> brokers;
            string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + 2345;
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(client))
                {
                    WaitUntillIdle(client, 500);
                    brokers = brokerPartitionInfo.GetAllBrokerInfo();
                    client.CreatePersistent(brokerPath, true);
                    client.WriteData(brokerPath, "192.168.1.39-1310449279123:192.168.1.39:9102");
                    WaitUntillIdle(client, 500);
                    Assert.NotNull(brokers);
                    Assert.Greater(brokers.Count, 0);
                    Assert.IsTrue(brokers.ContainsKey(2345));
                    client.DeleteRecursive(brokerPath);
                    WaitUntillIdle(client, 500);
                }
            }

            Assert.NotNull(brokers);
            Assert.Greater(brokers.Count, 0);
            Assert.IsFalse(brokers.ContainsKey(2345));
        }

        [Test]
        public void WhenNewBrokerInTopicIsAddedZkBrokerPartitionInfoUpdatesMappings()
        {
            var prodConfig = this.ZooKeeperBasedSyncProdConfig;

            IDictionary<string, SortedSet<Partition>> mappings;
            IDictionary<int, Broker> brokers;
            string brokerPath = ZooKeeperClient.DefaultBrokerIdsPath + "/" + 2345;
            string topicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + CurrentTestTopic;
            string topicBrokerPath = topicPath + "/" + 2345;
            using (IZooKeeperClient client = new ZooKeeperClient(
                prodConfig.ZooKeeper.ZkConnect,
                prodConfig.ZooKeeper.ZkSessionTimeoutMs,
                ZooKeeperStringSerializer.Serializer))
            {
                using (var brokerPartitionInfo = new ZKBrokerPartitionInfo(client))
                {
                    brokers = brokerPartitionInfo.GetAllBrokerInfo();
                    mappings =
                        ReflectionHelper.GetInstanceField<IDictionary<string, SortedSet<Partition>>>(
                            "topicBrokerPartitions", brokerPartitionInfo);
                    client.CreatePersistent(brokerPath, true);
                    client.WriteData(brokerPath, "192.168.1.39-1310449279123:192.168.1.39:9102");
                    client.CreatePersistent(topicPath, true);
                    WaitUntillIdle(client, 500);
                    Assert.IsTrue(brokers.ContainsKey(2345));
                    Assert.IsTrue(mappings.ContainsKey(CurrentTestTopic));
                    client.CreatePersistent(topicBrokerPath, true);
                    client.WriteData(topicBrokerPath, 5);
                    WaitUntillIdle(client, 500);
                    client.UnsubscribeAll();
                    WaitUntillIdle(client, 500);
                    client.DeleteRecursive(brokerPath);
                    client.DeleteRecursive(topicPath);
                }
            }

            Assert.NotNull(brokers);
            Assert.Greater(brokers.Count, 0);
            Assert.NotNull(mappings);
            Assert.Greater(mappings.Count, 0);
            Assert.IsTrue(brokers.ContainsKey(2345));
            Assert.IsTrue(mappings.Keys.Contains(CurrentTestTopic));
            Assert.AreEqual(5, mappings[CurrentTestTopic].Count);
        }
    }
}
