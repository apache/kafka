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

namespace Kafka.Client.Producers.Partitioning
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using Kafka.Client.ZooKeeperIntegration.Events;
    using Kafka.Client.ZooKeeperIntegration.Listeners;
    using log4net;
    using ZooKeeperNet;

    /// <summary>
    /// Fetch broker info like ID, host, port and number of partitions from ZooKeeper.
    /// </summary>
    /// <remarks>
    /// Used when zookeeper based auto partition discovery is enabled
    /// </remarks>
    internal class ZKBrokerPartitionInfo : IBrokerPartitionInfo, IZooKeeperStateListener
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);   
        private readonly Action<int, string, int> callback;
        private IDictionary<int, Broker> brokers;
        private IDictionary<string, SortedSet<Partition>> topicBrokerPartitions;
        private readonly IZooKeeperClient zkclient;
        private readonly BrokerTopicsListener brokerTopicsListener;
        private volatile bool disposed;
        private readonly object shuttingDownLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="ZKBrokerPartitionInfo"/> class.
        /// </summary>
        /// <param name="zkclient">The wrapper above ZooKeeper client.</param>
        public ZKBrokerPartitionInfo(IZooKeeperClient zkclient)
        {
            this.zkclient = zkclient;
            this.zkclient.Connect();
            this.InitializeBrokers();
            this.InitializeTopicBrokerPartitions();
            this.brokerTopicsListener = new BrokerTopicsListener(this.zkclient, this.topicBrokerPartitions, this.brokers, this.callback);
            this.RegisterListeners();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ZKBrokerPartitionInfo"/> class.
        /// </summary>
        /// <param name="config">The config.</param>
        /// <param name="callback">The callback invoked when new broker is added.</param>
        public ZKBrokerPartitionInfo(ZKConfig config, Action<int, string, int> callback)
            : this(new ZooKeeperClient(config.ZkConnect, config.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer))
        {
            this.callback = callback;
        }

        /// <summary>
        /// Gets a mapping from broker ID to the host and port for all brokers
        /// </summary>
        /// <returns>
        /// Mapping from broker ID to the host and port for all brokers
        /// </returns>
        public IDictionary<int, Broker> GetAllBrokerInfo()
        {
            this.EnsuresNotDisposed();
            return this.brokers;
        }

        /// <summary>
        /// Gets a mapping from broker ID to partition IDs
        /// </summary>
        /// <param name="topic">The topic for which this information is to be returned</param>
        /// <returns>
        /// Mapping from broker ID to partition IDs
        /// </returns>
        public SortedSet<Partition> GetBrokerPartitionInfo(string topic)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(topic));

            this.EnsuresNotDisposed();
            SortedSet<Partition> brokerPartitions = null;
            if (this.topicBrokerPartitions.ContainsKey(topic))
            {
                brokerPartitions = this.topicBrokerPartitions[topic];
            }

            if (brokerPartitions == null || brokerPartitions.Count == 0)
            {
                var numBrokerPartitions = this.BootstrapWithExistingBrokers(topic);
                this.topicBrokerPartitions.Add(topic, numBrokerPartitions);
                return numBrokerPartitions;
            }

            return brokerPartitions;
        }

        /// <summary>
        /// Gets the host and port information for the broker identified by the given broker ID
        /// </summary>
        /// <param name="brokerId">The broker ID.</param>
        /// <returns>
        /// Host and port of broker
        /// </returns>
        public Broker GetBrokerInfo(int brokerId)
        {
            this.EnsuresNotDisposed();
            return this.brokers.ContainsKey(brokerId) ? this.brokers[brokerId] : null;
        }

        /// <summary>
        /// Closes underlying connection to ZooKeeper
        /// </summary>
        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            lock (this.shuttingDownLock)
            {
                if (this.disposed)
                {
                    return;
                }

                this.disposed = true;
            }

            try
            {
                if (this.zkclient != null)
                {
                    this.zkclient.Dispose();
                }
            }
            catch (Exception exc)
            {
                Logger.Warn("Ignoring unexpected errors on closing", exc);
            }
        }

        /// <summary>
        /// Initializes the list of brokers.
        /// </summary>
        private void InitializeBrokers()
        {
            if (this.brokers != null)
            {
                return;
            }

            this.brokers = new Dictionary<int, Broker>();
            IList<string> brokerIds = this.zkclient.GetChildrenParentMayNotExist(ZooKeeperClient.DefaultBrokerIdsPath);
            foreach (var brokerId in brokerIds)
            {
                string path = ZooKeeperClient.DefaultBrokerIdsPath + "/" + brokerId;
                int id = int.Parse(brokerId, CultureInfo.InvariantCulture);
                var info = this.zkclient.ReadData<string>(path, null);
                string[] parts = info.Split(':');
                int port = int.Parse(parts[2], CultureInfo.InvariantCulture);
                this.brokers.Add(id, new Broker(id, parts[0], parts[1], port));
            }
        }

        /// <summary>
        /// Initializes the topic - broker's partitions mappings.
        /// </summary>
        private void InitializeTopicBrokerPartitions()
        {
            if (this.topicBrokerPartitions != null)
            {
                return;
            }

            this.topicBrokerPartitions = new Dictionary<string, SortedSet<Partition>>();
            this.zkclient.MakeSurePersistentPathExists(ZooKeeperClient.DefaultBrokerTopicsPath);
            IList<string> topics = this.zkclient.GetChildrenParentMayNotExist(ZooKeeperClient.DefaultBrokerTopicsPath);
            foreach (string topic in topics)
            {
                string brokerTopicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + topic;
                IList<string> brokersPerTopic = this.zkclient.GetChildrenParentMayNotExist(brokerTopicPath);
                var brokerPartitions = new SortedDictionary<int, int>();
                foreach (string brokerId in brokersPerTopic)
                {
                    string path = brokerTopicPath + "/" + brokerId;
                    var numPartitionsPerBrokerAndTopic = this.zkclient.ReadData<string>(path);
                    brokerPartitions.Add(int.Parse(brokerId, CultureInfo.InvariantCulture), int.Parse(numPartitionsPerBrokerAndTopic, CultureInfo.CurrentCulture));
                }              

                var brokerParts = new SortedSet<Partition>();
                foreach (var brokerPartition in brokerPartitions)
                {
                    for (int i = 0; i < brokerPartition.Value; i++)
                    {
                        var bidPid = new Partition(brokerPartition.Key, i);
                        brokerParts.Add(bidPid);
                    }
                }

                this.topicBrokerPartitions.Add(topic, brokerParts);
            }
        }

        /// <summary>
        /// Add the all available brokers with default one partition for new topic, so all of the brokers
        /// participate in hosting this topic
        /// </summary>
        /// <param name="topic">The new topic.</param>
        /// <returns>Default partitions for new broker</returns>
        /// <remarks>
        /// Since we do not have the in formation about number of partitions on these brokers, just assume single partition
        /// just pick partition 0 from each broker as a candidate
        /// </remarks>
        private SortedSet<Partition> BootstrapWithExistingBrokers(string topic)
        {
            Logger.Debug("Currently, no brokers are registered under topic: " + topic);
            Logger.Debug("Bootstrapping topic: " + topic + " with available brokers in the cluster with default "
                + "number of partitions = 1");
            var numBrokerPartitions = new SortedSet<Partition>();
            var allBrokers = this.zkclient.GetChildrenParentMayNotExist(ZooKeeperClient.DefaultBrokerIdsPath);
            Logger.Debug("List of all brokers currently registered in zookeeper -> " + string.Join(", ", allBrokers));
            foreach (var broker in allBrokers)
            {
                numBrokerPartitions.Add(new Partition(int.Parse(broker, CultureInfo.InvariantCulture), 0));
            }

            Logger.Debug("Adding following broker id, partition id for NEW topic: " + topic + " -> " + string.Join(", ", numBrokerPartitions));
            return numBrokerPartitions;
        }

        /// <summary>
        /// Registers the listeners under several path in ZooKeeper 
        /// to keep related data structures updated.
        /// </summary>
        /// <remarks>
        /// Watch on following path:
        /// /broker/topics
        /// /broker/topics/[topic]
        /// /broker/ids
        /// </remarks>
        private void RegisterListeners()
        {
            this.zkclient.Subscribe(ZooKeeperClient.DefaultBrokerTopicsPath, this.brokerTopicsListener);
            Logger.Debug("Registering listener on path: " + ZooKeeperClient.DefaultBrokerTopicsPath);
            foreach (string topic in this.topicBrokerPartitions.Keys)
            {
                string path = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + topic;
                this.zkclient.Subscribe(path, this.brokerTopicsListener);
                Logger.Debug("Registering listener on path: " + path);
            }

            this.zkclient.Subscribe(ZooKeeperClient.DefaultBrokerIdsPath, this.brokerTopicsListener);
            Logger.Debug("Registering listener on path: " + ZooKeeperClient.DefaultBrokerIdsPath);

            this.zkclient.Subscribe(this);
            Logger.Debug("Registering listener on state changed event");
        }

        /// <summary>
        /// Resets the related data structures
        /// </summary>
        private void Reset()
        {
            this.topicBrokerPartitions = null;
            this.brokers = null;
            this.InitializeBrokers();
            this.InitializeTopicBrokerPartitions();
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }

        /// <summary>
        /// Called when the ZooKeeper connection state has changed.
        /// </summary>
        /// <param name="args">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperStateChangedEventArgs"/> instance containing the event data.</param>
        /// <remarks>
        /// Do nothing, since zkclient will do reconnect for us.
        /// </remarks>
        public void HandleStateChanged(ZooKeeperStateChangedEventArgs args)
        {
            Guard.Assert<ArgumentNullException>(() => args != null);
            Guard.Assert<ArgumentException>(() => args.State != KeeperState.Unknown);

            this.EnsuresNotDisposed();
            Logger.Debug("Handle state change: do nothing, since zkclient will do reconnect for us.");
        }

        /// <summary>
        /// Called after the ZooKeeper session has expired and a new session has been created.
        /// </summary>
        /// <param name="args">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperSessionCreatedEventArgs"/> instance containing the event data.</param>
        /// <remarks>
        /// We would have to re-create any ephemeral nodes here.
        /// </remarks>
        public void HandleSessionCreated(ZooKeeperSessionCreatedEventArgs args)
        {
            Guard.Assert<ArgumentNullException>(() => args != null);

            this.EnsuresNotDisposed();
            Logger.Debug("ZK expired; release old list of broker partitions for topics ");
            this.Reset();
            this.brokerTopicsListener.ResetState();
            foreach (var topic in this.topicBrokerPartitions.Keys)
            {
                this.zkclient.Subscribe(ZooKeeperClient.DefaultBrokerTopicsPath + "/" + topic, this.brokerTopicsListener);   
            }
        }
    }
}
