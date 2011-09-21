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
    using System.Reflection;
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using Kafka.Client.ZooKeeperIntegration.Events;
    using Kafka.Client.ZooKeeperIntegration.Listeners;
    using log4net;
    using NUnit.Framework;
    using ZooKeeperNet;

    [TestFixture]
    internal class ZooKeeperClientTests : IntegrationFixtureBase, IZooKeeperDataListener, IZooKeeperStateListener, IZooKeeperChildListener
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private KafkaClientConfiguration clientConfig;
        private readonly IList<ZooKeeperEventArgs> events = new List<ZooKeeperEventArgs>();

        [TestFixtureSetUp]
        public void SetUp()
        {
            clientConfig = KafkaClientConfiguration.GetConfiguration();
        }

        [SetUp]
        public void TestSetup()
        {
            this.events.Clear();
        }

        [Test]
        public void ZooKeeperClientCreateWorkerThreadsOnBeingCreated()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                var eventWorker = ReflectionHelper.GetInstanceField<Thread>("eventWorker", client);
                var zooKeeperWorker = ReflectionHelper.GetInstanceField<Thread>("zooKeeperEventWorker", client);
                Assert.NotNull(eventWorker);
                Assert.NotNull(zooKeeperWorker);
            }
        }

        [Test]
        public void ZooKeeperClientFailsWhenCreatedWithWrongConnectionInfo()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient("random text", producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                Assert.Throws<FormatException>(client.Connect);
            }
        }

        [Test]
        public void WhenStateChangedToConnectedStateListenerFires()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Subscribe(this);
                client.Connect();
                WaitUntillIdle(client, 500);
            }

            Assert.AreEqual(1, this.events.Count);
            ZooKeeperEventArgs e = this.events[0];
            Assert.AreEqual(ZooKeeperEventTypes.StateChanged, e.Type);
            Assert.IsInstanceOf<ZooKeeperStateChangedEventArgs>(e);
            Assert.AreEqual(((ZooKeeperStateChangedEventArgs)e).State, KeeperState.SyncConnected);
        }

        [Test]
        public void WhenStateChangedToDisconnectedStateListenerFires()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Subscribe(this);
                client.Connect();
                WaitUntillIdle(client, 500);
                client.Process(new WatchedEvent(KeeperState.Disconnected, EventType.None, null));
                WaitUntillIdle(client, 500);
            }

            Assert.AreEqual(2, this.events.Count);
            ZooKeeperEventArgs e = this.events[1];
            Assert.AreEqual(ZooKeeperEventTypes.StateChanged, e.Type);
            Assert.IsInstanceOf<ZooKeeperStateChangedEventArgs>(e);
            Assert.AreEqual(((ZooKeeperStateChangedEventArgs)e).State, KeeperState.Disconnected);
        }

        [Test]
        public void WhenStateChangedToExpiredStateAndSessionListenersFire()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Subscribe(this);
                client.Connect();
                WaitUntillIdle(client, 500);
                client.Process(new WatchedEvent(KeeperState.Expired, EventType.None, null));
                WaitUntillIdle(client, 3000);
            }

            Assert.AreEqual(4, this.events.Count);
            ZooKeeperEventArgs e = this.events[1];
            Assert.AreEqual(ZooKeeperEventTypes.StateChanged, e.Type);
            Assert.IsInstanceOf<ZooKeeperStateChangedEventArgs>(e);
            Assert.AreEqual(((ZooKeeperStateChangedEventArgs)e).State, KeeperState.Expired);
            e = this.events[2];
            Assert.AreEqual(ZooKeeperEventTypes.SessionCreated, e.Type);
            Assert.IsInstanceOf<ZooKeeperSessionCreatedEventArgs>(e);
            e = this.events[3];
            Assert.AreEqual(ZooKeeperEventTypes.StateChanged, e.Type);
            Assert.IsInstanceOf<ZooKeeperStateChangedEventArgs>(e);
            Assert.AreEqual(((ZooKeeperStateChangedEventArgs)e).State, KeeperState.SyncConnected);
        }

        [Test]
        public void WhenSessionExpiredClientReconnects()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            IZooKeeperConnection conn1;
            IZooKeeperConnection conn2;
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                conn1 = ReflectionHelper.GetInstanceField<ZooKeeperConnection>("connection", client);
                client.Process(new WatchedEvent(KeeperState.Expired, EventType.None, null));
                WaitUntillIdle(client, 1000);
                conn2 = ReflectionHelper.GetInstanceField<ZooKeeperConnection>("connection", client);
            }

            Assert.AreNotEqual(conn1, conn2);
        }

        [Test]
        public void ZooKeeperClientChecksIfPathExists()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                Assert.IsTrue(client.Exists(ZooKeeperClient.DefaultBrokerTopicsPath, false));
            }
        }

        [Test]
        public void ZooKeeperClientCreatesANewPathAndDeletesIt()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                string myPath = "/" + Guid.NewGuid();
                client.CreatePersistent(myPath, false);
                Assert.IsTrue(client.Exists(myPath));
                client.Delete(myPath);
                Assert.IsFalse(client.Exists(myPath));
            }
        }

        [Test]
        public void WhenChildIsCreatedChilListenerOnParentFires()
        {
            string myPath = "/" + Guid.NewGuid();
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                WaitUntillIdle(client, 500);
                client.Subscribe("/", this as IZooKeeperChildListener);
                client.CreatePersistent(myPath, true);
                WaitUntillIdle(client, 500);
                client.UnsubscribeAll();
                client.Delete(myPath);
            }

            Assert.AreEqual(1, this.events.Count);
            ZooKeeperEventArgs e = this.events[0];
            Assert.AreEqual(ZooKeeperEventTypes.ChildChanged, e.Type);
            Assert.IsInstanceOf<ZooKeeperChildChangedEventArgs>(e);
            Assert.AreEqual(((ZooKeeperChildChangedEventArgs)e).Path, "/");
            Assert.Greater(((ZooKeeperChildChangedEventArgs)e).Children.Count, 0);
            Assert.IsTrue(((ZooKeeperChildChangedEventArgs)e).Children.Contains(myPath.Replace("/", string.Empty)));
        }

        [Test]
        public void WhenChildIsDeletedChildListenerOnParentFires()
        {
            string myPath = "/" + Guid.NewGuid();
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                client.CreatePersistent(myPath, true);
                WaitUntillIdle(client, 500);
                client.Subscribe("/", this as IZooKeeperChildListener);
                client.Delete(myPath);
                WaitUntillIdle(client, 500);
            }

            Assert.AreEqual(1, this.events.Count);
            ZooKeeperEventArgs e = this.events[0];
            Assert.AreEqual(ZooKeeperEventTypes.ChildChanged, e.Type);
            Assert.IsInstanceOf<ZooKeeperChildChangedEventArgs>(e);
            Assert.AreEqual(((ZooKeeperChildChangedEventArgs)e).Path, "/");
            Assert.Greater(((ZooKeeperChildChangedEventArgs)e).Children.Count, 0);
            Assert.IsFalse(((ZooKeeperChildChangedEventArgs)e).Children.Contains(myPath.Replace("/", string.Empty)));
        }

        [Test]
        public void WhenZNodeIsDeletedChildAndDataDeletedListenersFire()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            string myPath = "/" + Guid.NewGuid();
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                client.CreatePersistent(myPath, true);
                WaitUntillIdle(client, 500);
                client.Subscribe(myPath, this as IZooKeeperChildListener);
                client.Subscribe(myPath, this as IZooKeeperDataListener);
                client.Delete(myPath);
                WaitUntillIdle(client, 500);
            }

            Assert.AreEqual(2, this.events.Count);
            ZooKeeperEventArgs e = this.events[0];
            Assert.AreEqual(ZooKeeperEventTypes.ChildChanged, e.Type);
            Assert.IsInstanceOf<ZooKeeperChildChangedEventArgs>(e);
            Assert.AreEqual(((ZooKeeperChildChangedEventArgs)e).Path, myPath);
            Assert.IsNull(((ZooKeeperChildChangedEventArgs)e).Children);
            e = this.events[1];
            Assert.AreEqual(ZooKeeperEventTypes.DataChanged, e.Type);
            Assert.IsInstanceOf<ZooKeeperDataChangedEventArgs>(e);
            Assert.AreEqual(((ZooKeeperDataChangedEventArgs)e).Path, myPath);
            Assert.IsNull(((ZooKeeperDataChangedEventArgs)e).Data);
        }

        [Test]
        public void ZooKeeperClientCreatesAChildAndGetsChildren()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                string child = Guid.NewGuid().ToString();
                string myPath = "/" + child;
                client.CreatePersistent(myPath, false);
                IList<string> children = client.GetChildren("/", false);
                int countChildren = client.CountChildren("/");
                Assert.Greater(children.Count, 0);
                Assert.AreEqual(children.Count, countChildren);
                Assert.IsTrue(children.Contains(child));
                client.Delete(myPath);
            }
        }

        [Test]
        public void WhenDataChangedDataListenerFires()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            string myPath = "/" + Guid.NewGuid();
            string sourceData = "my test data";
            string resultData;
            using (IZooKeeperClient client = new ZooKeeperClient(producerConfig.ZkConnect, producerConfig.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
            {
                client.Connect();
                client.CreatePersistent(myPath, true);
                WaitUntillIdle(client, 500);
                client.Subscribe(myPath, this as IZooKeeperDataListener);
                client.Subscribe(myPath, this as IZooKeeperChildListener);
                client.WriteData(myPath, sourceData);
                WaitUntillIdle(client, 500);
                client.UnsubscribeAll();
                resultData = client.ReadData<string>(myPath);
                client.Delete(myPath);
            }

            Assert.IsTrue(!string.IsNullOrEmpty(resultData));
            Assert.AreEqual(sourceData, resultData);
            Assert.AreEqual(1, this.events.Count);
            ZooKeeperEventArgs e = this.events[0];
            Assert.AreEqual(ZooKeeperEventTypes.DataChanged, e.Type);
            Assert.IsInstanceOf<ZooKeeperDataChangedEventArgs>(e);
            Assert.AreEqual(((ZooKeeperDataChangedEventArgs)e).Path, myPath);
            Assert.IsNotNull(((ZooKeeperDataChangedEventArgs)e).Data);
            Assert.AreEqual(((ZooKeeperDataChangedEventArgs)e).Data, sourceData);
        }

        [Test]
        [ExpectedException(typeof(ZooKeeperException))]
        public void WhenClientWillNotConnectWithinGivenTimeThrows()
        {
            var producerConfig = new ProducerConfig(clientConfig);
            using (IZooKeeperClient client = 
                new ZooKeeperClient(
                    producerConfig.ZkConnect, 
                    producerConfig.ZkSessionTimeoutMs, 
                    ZooKeeperStringSerializer.Serializer,
                    1))
            {
                client.Connect();
            }
        }

        public void HandleDataChange(ZooKeeperDataChangedEventArgs args)
        {
            Logger.Debug(args + " reach test event handler");
            this.events.Add(args);
        }

        public void HandleDataDelete(ZooKeeperDataChangedEventArgs args)
        {
            Logger.Debug(args + " reach test event handler");
            this.events.Add(args);
        }

        public void HandleStateChanged(ZooKeeperStateChangedEventArgs args)
        {
            Logger.Debug(args + " reach test event handler");
            this.events.Add(args);
        }

        public void HandleSessionCreated(ZooKeeperSessionCreatedEventArgs args)
        {
            Logger.Debug(args + " reach test event handler");
            this.events.Add(args);
        }

        public void HandleChildChange(ZooKeeperChildChangedEventArgs args)
        {
            Logger.Debug(args + " reach test event handler");
            this.events.Add(args);
        }

        public void ResetState()
        {
        }
    }
}
