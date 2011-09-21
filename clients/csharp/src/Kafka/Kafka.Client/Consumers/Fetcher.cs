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

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.ZooKeeperIntegration;
    using log4net;

    /// <summary>
    /// Background thread that fetches data from a set of servers
    /// </summary>
    internal class Fetcher : IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly ConsumerConfig config;
        private readonly IZooKeeperClient zkClient;
        private FetcherRunnable[] fetcherWorkerObjects;
        private volatile bool disposed;
        private readonly object shuttingDownLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="Fetcher"/> class.
        /// </summary>
        /// <param name="config">
        /// The consumer configuration.
        /// </param>
        /// <param name="zkClient">
        /// The wrapper above ZooKeeper client.
        /// </param>
        public Fetcher(ConsumerConfig config, IZooKeeperClient zkClient)
        {
            this.config = config;
            this.zkClient = zkClient;
        }

        /// <summary>
        /// Shuts down all fetch threads
        /// </summary>
        private void Shutdown()
        {
            if (fetcherWorkerObjects != null)
            {
                foreach (FetcherRunnable fetcherRunnable in fetcherWorkerObjects)
                {
                    fetcherRunnable.Shutdown();
                }

                fetcherWorkerObjects = null;
            }
        }

        /// <summary>
        /// Opens connections to brokers.
        /// </summary>
        /// <param name="topicInfos">
        /// The topic infos.
        /// </param>
        /// <param name="cluster">
        /// The cluster.
        /// </param>
        /// <param name="queuesToBeCleared">
        /// The queues to be cleared.
        /// </param>
        public void InitConnections(IEnumerable<PartitionTopicInfo> topicInfos, Cluster cluster, IEnumerable<BlockingCollection<FetchedDataChunk>> queuesToBeCleared)
        {
            this.EnsuresNotDisposed();
            this.Shutdown();
            if (topicInfos == null)
            {
                return;
            }

            foreach (var queueToBeCleared in queuesToBeCleared)
            {
                while (queueToBeCleared.Count > 0)
                {
                    queueToBeCleared.Take();
                }
            }

            var partitionTopicInfoMap = new Dictionary<int, List<PartitionTopicInfo>>();

            //// re-arrange by broker id
            foreach (var topicInfo in topicInfos)
            {
                if (!partitionTopicInfoMap.ContainsKey(topicInfo.BrokerId))
                {
                    partitionTopicInfoMap.Add(topicInfo.BrokerId, new List<PartitionTopicInfo>() { topicInfo });
                }
                else
                {
                    partitionTopicInfoMap[topicInfo.BrokerId].Add(topicInfo);
                } 
            }

            //// open a new fetcher thread for each broker
            fetcherWorkerObjects = new FetcherRunnable[partitionTopicInfoMap.Count];
            int i = 0;
            foreach (KeyValuePair<int, List<PartitionTopicInfo>> item in partitionTopicInfoMap)
            {
                Broker broker = cluster.GetBroker(item.Key);
                var fetcherRunnable = new FetcherRunnable("FetcherRunnable-" + i, zkClient, config, broker, item.Value);
                var threadStart = new ThreadStart(fetcherRunnable.Run);
                var fetcherThread = new Thread(threadStart);
                fetcherWorkerObjects[i] = fetcherRunnable;
                fetcherThread.Start();
                i++;
            }
        }

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
                this.Shutdown();
            }
            catch (Exception exc)
            {
                Logger.Warn("Ignoring unexpected errors on closing", exc);
            }
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
