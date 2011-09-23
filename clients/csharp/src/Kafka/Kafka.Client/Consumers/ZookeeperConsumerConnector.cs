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

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using Kafka.Client.ZooKeeperIntegration.Listeners;
    using log4net;

    /// <summary>
    /// The consumer high-level API, that hides the details of brokers from the consumer. 
    /// It also maintains the state of what has been consumed. 
    /// </summary>
    public class ZookeeperConsumerConnector : ZooKeeperAwareKafkaClientBase, IConsumerConnector
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        
        public static readonly int MaxNRetries = 4;
        
        internal static readonly FetchedDataChunk ShutdownCommand = new FetchedDataChunk(null, null, -1);

        private readonly ConsumerConfig config;
       
        private IZooKeeperClient zkClient;
       
        private readonly object shuttingDownLock = new object();
       
        private readonly bool enableFetcher;
        
        private Fetcher fetcher;
        
        private readonly KafkaScheduler scheduler = new KafkaScheduler();
        
        private readonly IDictionary<string, IDictionary<Partition, PartitionTopicInfo>> topicRegistry = new ConcurrentDictionary<string, IDictionary<Partition, PartitionTopicInfo>>();
        
        private readonly IDictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>> queues = new Dictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>>();

        private readonly object syncLock = new object();

        private volatile bool disposed;

        /// <summary>
        /// Gets the consumer group ID.
        /// </summary>
        public string ConsumerGroup
        {
            get { return this.config.GroupId; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ZookeeperConsumerConnector"/> class.
        /// </summary>
        /// <param name="config">
        /// The consumer configuration. At the minimum, need to specify the group ID 
        /// of the consumer and the ZooKeeper connection string.
        /// </param>
        /// <param name="enableFetcher">
        /// Indicates whether fetchers should be enabled
        /// </param>
        public ZookeeperConsumerConnector(ConsumerConfig config, bool enableFetcher)
            : base(config)
        {
            this.config = config;
            this.enableFetcher = enableFetcher;
            this.ConnectZk();
            this.CreateFetcher();

            if (this.config.AutoCommit)
            {
                Logger.InfoFormat(CultureInfo.CurrentCulture, "starting auto committer every {0} ms", this.config.AutoCommitIntervalMs);
                scheduler.ScheduleWithRate(this.AutoCommit, this.config.AutoCommitIntervalMs, this.config.AutoCommitIntervalMs);
            }
        }

        /// <summary>
        /// Commits the offsets of all messages consumed so far.
        /// </summary>
        public void CommitOffsets()
        {
            this.EnsuresNotDisposed();
            if (this.zkClient == null)
            {
                return;
            }

            foreach (KeyValuePair<string, IDictionary<Partition, PartitionTopicInfo>> topic in topicRegistry)
            {
                var topicDirs = new ZKGroupTopicDirs(this.config.GroupId, topic.Key);
                foreach (KeyValuePair<Partition, PartitionTopicInfo> partition in topic.Value)
                {
                    var newOffset = partition.Value.GetConsumeOffset();
                    try
                    {
                        ZkUtils.UpdatePersistentPath(zkClient, topicDirs.ConsumerOffsetDir + "/" + partition.Value.Partition.Name, newOffset.ToString());
                    }
                    catch (Exception ex)
                    {
                        Logger.WarnFormat(CultureInfo.CurrentCulture, "exception during CommitOffsets: {0}", ex);
                    }

                    if (Logger.IsDebugEnabled)
                    {
                        Logger.DebugFormat(CultureInfo.CurrentCulture, "Commited offset {0} for topic {1}", newOffset, partition);
                    }
                }
            }
        }

        public void AutoCommit()
        {
            this.EnsuresNotDisposed();
            try
            {
                this.CommitOffsets();
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat(CultureInfo.CurrentCulture, "exception during AutoCommit: {0}", ex);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

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

                Logger.Info("ZookeeperConsumerConnector shutting down");
                this.disposed = true;
            }

            try
            {
                if (this.scheduler != null)
                {
                    this.scheduler.Dispose();
                }

                if (this.fetcher != null)
                {
                    this.fetcher.Dispose();
                }

                this.SendShutdownToAllQueues();
                if (this.zkClient != null)
                {
                    this.zkClient.Dispose();
                }
            }
            catch (Exception exc)
            {
                Logger.Debug("Ignoring unexpected errors on shutting down", exc);
            }

            Logger.Info("ZookeeperConsumerConnector shut down completed");
        }

        /// <summary>
        /// Creates a list of message streams for each topic.
        /// </summary>
        /// <param name="topicCountDict">
        /// The map of topic on number of streams
        /// </param>
        /// <returns>
        /// The list of <see cref="KafkaMessageStream"/>, which are iterators over topic.
        /// </returns>
        /// <remarks>
        /// Explicitly triggers load balancing for this consumer
        /// </remarks>
        public IDictionary<string, IList<KafkaMessageStream>> CreateMessageStreams(IDictionary<string, int> topicCountDict)
        {
            this.EnsuresNotDisposed();
            return this.Consume(topicCountDict);
        }

        private void ConnectZk()
        {
            Logger.InfoFormat(CultureInfo.CurrentCulture, "Connecting to zookeeper instance at {0}", this.config.ZkConnect);
            this.zkClient = new ZooKeeperClient(this.config.ZkConnect, this.config.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer);
            this.zkClient.Connect();
        }

        private void CreateFetcher()
        {
            if (this.enableFetcher)
            {
                this.fetcher = new Fetcher(this.config, this.zkClient);
            }
        }

        private IDictionary<string, IList<KafkaMessageStream>> Consume(IDictionary<string, int> topicCountDict)
        {
            Logger.Debug("entering consume");

            if (topicCountDict == null)
            {
                throw new ArgumentNullException();
            }

            var dirs = new ZKGroupDirs(this.config.GroupId);
            var result = new Dictionary<string, IList<KafkaMessageStream>>();

            string consumerUuid = Environment.MachineName + "-" + DateTime.Now.Millisecond;
            string consumerIdString = this.config.GroupId + "_" + consumerUuid;
            var topicCount = new TopicCount(consumerIdString, topicCountDict);

            // listener to consumer and partition changes
            var loadBalancerListener = new ZKRebalancerListener(
                this.config, 
                consumerIdString, 
                this.topicRegistry, 
                this.zkClient, 
                this, 
                queues, 
                this.fetcher, 
                this.syncLock);
            this.RegisterConsumerInZk(dirs, consumerIdString, topicCount);
            this.zkClient.Subscribe(dirs.ConsumerRegistryDir, loadBalancerListener);

            //// create a queue per topic per consumer thread
            var consumerThreadIdsPerTopicMap = topicCount.GetConsumerThreadIdsPerTopic();
            foreach (var topic in consumerThreadIdsPerTopicMap.Keys)
            {
                var streamList = new List<KafkaMessageStream>();
                foreach (string threadId in consumerThreadIdsPerTopicMap[topic])
                {
                    var stream = new BlockingCollection<FetchedDataChunk>(new ConcurrentQueue<FetchedDataChunk>());
                    this.queues.Add(new Tuple<string, string>(topic, threadId), stream);
                    streamList.Add(new KafkaMessageStream(stream, this.config.Timeout));
                }

                result.Add(topic, streamList);
                Logger.DebugFormat(CultureInfo.CurrentCulture, "adding topic {0} and stream to map...", topic);

                // register on broker partition path changes
                string partitionPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + topic;
                this.zkClient.MakeSurePersistentPathExists(partitionPath);
                this.zkClient.Subscribe(partitionPath, loadBalancerListener);
            }

            //// register listener for session expired event
            this.zkClient.Subscribe(new ZKSessionExpireListener(dirs, consumerIdString, topicCount, loadBalancerListener, this));

            //// explicitly trigger load balancing for this consumer
            lock (this.syncLock)
            {
                loadBalancerListener.SyncedRebalance();
            }

            return result;
        }

        private void SendShutdownToAllQueues()
        {
            foreach (var queue in this.queues)
            {
                Logger.Debug("Clearing up queue");
                //// clear the queue
                while (queue.Value.Count > 0)
                {
                    queue.Value.Take();
                }

                queue.Value.Add(ShutdownCommand);
                Logger.Debug("Cleared queue and sent shutdown command");
            }
        }

        internal void RegisterConsumerInZk(ZKGroupDirs dirs, string consumerIdString, TopicCount topicCount)
        {
            this.EnsuresNotDisposed();
            Logger.InfoFormat(CultureInfo.CurrentCulture, "begin registering consumer {0} in ZK", consumerIdString);
            ZkUtils.CreateEphemeralPathExpectConflict(this.zkClient, dirs.ConsumerRegistryDir + "/" + consumerIdString, topicCount.ToJsonString());
            Logger.InfoFormat(CultureInfo.CurrentCulture, "end registering consumer {0} in ZK", consumerIdString);
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
    }
}
