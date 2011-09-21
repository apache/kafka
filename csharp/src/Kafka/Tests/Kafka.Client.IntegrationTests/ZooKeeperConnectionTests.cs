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

namespace Kafka.Client.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using Kafka.Client.Cfg;
    using Kafka.Client.ZooKeeperIntegration;
    using NUnit.Framework;
    using ZooKeeperNet;

    [TestFixture]
    public class ZooKeeperConnectionTests
    {
        private KafkaClientConfiguration clientConfig;

        [TestFixtureSetUp]
        public void SetUp()
        {
            clientConfig = KafkaClientConfiguration.GetConfiguration();
        }

        [Test]
        public void ZooKeeperConnectionCreatesAndDeletesPath()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperConnection connection = new ZooKeeperConnection(producerConfig.ZkConnect))
            {
                connection.Connect(null);
                string pathName = "/" + Guid.NewGuid();
                connection.Create(pathName, null, CreateMode.Persistent);
                Assert.IsTrue(connection.Exists(pathName, false));
                connection.Delete(pathName);
                Assert.IsFalse(connection.Exists(pathName, false));
            }
        }

        [Test]
        public void ZooKeeperConnectionConnectsAndDisposes()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            IZooKeeperConnection connection;
            using (connection = new ZooKeeperConnection(producerConfig.ZkConnect))
            {
                Assert.IsNull(connection.ClientState);
                connection.Connect(null);
                Assert.NotNull(connection.Client);
                Assert.AreEqual(ZooKeeper.States.CONNECTING, connection.ClientState);
            }

            Assert.Null(connection.Client);
        }

        [Test]
        public void ZooKeeperConnectionCreatesAndGetsCreateTime()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperConnection connection = new ZooKeeperConnection(producerConfig.ZkConnect))
            {
                connection.Connect(null);
                string pathName = "/" + Guid.NewGuid();
                connection.Create(pathName, null, CreateMode.Persistent);
                long createTime = connection.GetCreateTime(pathName);
                Assert.Greater(createTime, 0);
                connection.Delete(pathName);
            }
        }

        [Test]
        public void ZooKeeperConnectionCreatesAndGetsChildren()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperConnection connection = new ZooKeeperConnection(producerConfig.ZkConnect))
            {
                connection.Connect(null);
                string child = Guid.NewGuid().ToString();
                string pathName = "/" + child;
                connection.Create(pathName, null, CreateMode.Persistent);
                IList<string> children = connection.GetChildren("/", false);
                Assert.Greater(children.Count, 0);
                Assert.IsTrue(children.Contains(child));
                connection.Delete(pathName);
            }
        }

        [Test]
        public void ZooKeeperConnectionWritesAndReadsData()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperConnection connection = new ZooKeeperConnection(producerConfig.ZkConnect))
            {
                connection.Connect(null);
                string child = Guid.NewGuid().ToString();
                string pathName = "/" + child;
                connection.Create(pathName, null, CreateMode.Persistent);
                var sourceData = new byte[2] { 1, 2 };
                connection.WriteData(pathName, sourceData);
                byte[] resultData = connection.ReadData(pathName, null, false);
                Assert.IsNotNull(resultData);
                Assert.AreEqual(sourceData[0], resultData[0]);
                Assert.AreEqual(sourceData[1], resultData[1]);
                connection.Delete(pathName);
            }
        }
    }
}
