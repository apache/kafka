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

namespace Kafka.Client.Producers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using log4net;

    /// <summary>
    /// High-level Producer API that exposes all the producer functionality to the client
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TData">The type of the data.</typeparam>
    /// <remarks>
    /// Provides serialization of data through a user-specified encoder, zookeeper based automatic broker discovery
    /// and software load balancing through an optionally user-specified partitioner
    /// </remarks>
    public class Producer<TKey, TData> : ZooKeeperAwareKafkaClientBase, IProducer<TKey, TData>
        where TKey : class 
        where TData : class 
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);       
        private static readonly Random Randomizer = new Random();
        private readonly ProducerConfig config;
        private readonly IProducerPool<TData> producerPool;
        private readonly IPartitioner<TKey> partitioner;
        private readonly bool populateProducerPool;
        private readonly IBrokerPartitionInfo brokerPartitionInfo;
        private volatile bool disposed;
        private readonly object shuttingDownLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="Producer&lt;TKey, TData&gt;"/> class.
        /// </summary>
        /// <param name="config">The config object.</param>
        /// <param name="partitioner">The partitioner that implements <see cref="IPartitioner&lt;TKey&gt;" /> 
        /// used to supply a custom partitioning strategy based on the message key.</param>
        /// <param name="producerPool">Pool of producers, one per broker.</param>
        /// <param name="populateProducerPool">if set to <c>true</c>, producers should be populated.</param>
        /// <remarks>
        /// Should be used for testing purpose only.
        /// </remarks>
        internal Producer(
            ProducerConfig config,
            IPartitioner<TKey> partitioner,
            IProducerPool<TData> producerPool,
            bool populateProducerPool = true)
            : base(config)
        {
            Guard.Assert<ArgumentNullException>(() => config != null);
            Guard.Assert<ArgumentNullException>(() => producerPool != null);
            this.config = config;
            this.partitioner = partitioner ?? new DefaultPartitioner<TKey>();
            this.populateProducerPool = populateProducerPool;
            this.producerPool = producerPool;
            if (this.IsZooKeeperEnabled)
            {
                this.brokerPartitionInfo = new ZKBrokerPartitionInfo(this.config, this.Callback);
            }
            else
            {
                this.brokerPartitionInfo = new ConfigBrokerPartitionInfo(this.config);   
            }

            if (this.populateProducerPool)
            {
                IDictionary<int, Broker> allBrokers = this.brokerPartitionInfo.GetAllBrokerInfo();
                foreach (var broker in allBrokers)
                {
                    this.producerPool.AddProducer(
                        new Broker(broker.Key, broker.Value.Host, broker.Value.Host, broker.Value.Port));
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Producer&lt;TKey, TData&gt;"/> class.
        /// </summary>
        /// <param name="config">The config object.</param>
        /// <remarks>
        /// Can be used when all config parameters will be specified through the config object
        /// and will be instantiated via reflection
        /// </remarks>
        public Producer(ProducerConfig config)
            : this(
                config, 
                ReflectionHelper.Instantiate<IPartitioner<TKey>>(config.PartitionerClass),
                ProducerPool<TData>.CreatePool(config, ReflectionHelper.Instantiate<IEncoder<TData>>(config.SerializerClass)))
        {
            Guard.Assert<ArgumentNullException>(() => config != null);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Producer&lt;TKey, TData&gt;"/> class.
        /// </summary>
        /// <param name="config">The config object.</param>
        /// <param name="partitioner">The partitioner that implements <see cref="IPartitioner&lt;TKey&gt;" /> 
        /// used to supply a custom partitioning strategy based on the message key.</param>
        /// <param name="encoder">The encoder that implements <see cref="IEncoder&lt;TData&gt;" /> 
        /// used to convert an object of type TData to <see cref="Message" />.</param>
        /// <param name="callbackHandler">The callback handler that implements <see cref="ICallbackHandler" />, used 
        /// to supply callback invoked when sending asynchronous request is completed.</param>
        /// <remarks>
        /// Can be used to provide pre-instantiated objects for all config parameters
        /// that would otherwise be instantiated via reflection.
        /// </remarks>
        public Producer(
            ProducerConfig config,
            IPartitioner<TKey> partitioner,
            IEncoder<TData> encoder,
            ICallbackHandler callbackHandler)
            : this(
                config, 
                partitioner,
                ProducerPool<TData>.CreatePool(config, encoder, callbackHandler))
        {
            Guard.Assert<ArgumentNullException>(() => config != null);
            Guard.Assert<ArgumentNullException>(() => encoder != null);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Producer&lt;TKey, TData&gt;"/> class.
        /// </summary>
        /// <param name="config">The config object.</param>
        /// <param name="partitioner">The partitioner that implements <see cref="IPartitioner&lt;TKey&gt;" /> 
        /// used to supply a custom partitioning strategy based on the message key.</param>
        /// <param name="encoder">The encoder that implements <see cref="IEncoder&lt;TData&gt;" /> 
        /// used to convert an object of type TData to <see cref="Message" />.</param>
        /// <remarks>
        /// Can be used to provide pre-instantiated objects for all config parameters
        /// that would otherwise be instantiated via reflection.
        /// </remarks>
        public Producer(
            ProducerConfig config,
            IPartitioner<TKey> partitioner,
            IEncoder<TData> encoder)
            : this(
                config, 
                partitioner,
                ProducerPool<TData>.CreatePool(config, encoder, null))
        {
            Guard.Assert<ArgumentNullException>(() => config != null);
            Guard.Assert<ArgumentNullException>(() => encoder != null);
        }

        /// <summary>
        /// Sends the data to a multiple topics, partitioned by key, using either the
        /// synchronous or the asynchronous producer.
        /// </summary>
        /// <param name="data">The producer data objects that encapsulate the topic, key and message data.</param>
        public void Send(IEnumerable<ProducerData<TKey, TData>> data)
        {
            Guard.Assert<ArgumentNullException>(() => data != null);
            Guard.Assert<ArgumentException>(() => data.Count() > 0);
            this.EnsuresNotDisposed();
            var poolRequests = new List<ProducerPoolData<TData>>();
            foreach (var dataItem in data)
            {
                Partition partition = this.GetPartition(dataItem);
                var poolRequest = new ProducerPoolData<TData>(dataItem.Topic, partition, dataItem.Data);
                poolRequests.Add(poolRequest);
            }

            this.producerPool.Send(poolRequests);
        }

        /// <summary>
        /// Sends the data to a single topic, partitioned by key, using either the
        /// synchronous or the asynchronous producer.
        /// </summary>
        /// <param name="data">The producer data object that encapsulates the topic, key and message data.</param>
        public void Send(ProducerData<TKey, TData> data)
        {
            Guard.Assert<ArgumentNullException>(() => data != null);
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(data.Topic));
            Guard.Assert<ArgumentNullException>(() => data.Data != null);
            Guard.Assert<ArgumentException>(() => data.Data.Count() > 0);
            this.EnsuresNotDisposed();
            this.Send(new[] { data });
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

                this.disposed = true;
            }

            try
            {
                if (this.brokerPartitionInfo != null)
                {
                    this.brokerPartitionInfo.Dispose();
                }
            }
            catch (Exception exc)
            {
                Logger.Warn("Ignoring unexpected errors on closing", exc);
            }
        }

        /// <summary>
        /// Callback to add a new producer to the producer pool.
        /// Used by <see cref="ZKBrokerPartitionInfo" /> on registration of new broker in ZooKeeper
        /// </summary>
        /// <param name="bid">The broker Id.</param>
        /// <param name="host">The broker host address.</param>
        /// <param name="port">The broker port.</param>
        private void Callback(int bid, string host, int port)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(host));
            Guard.Assert<ArgumentOutOfRangeException>(() => port > 0);

            if (this.populateProducerPool)
            {
                this.producerPool.AddProducer(new Broker(bid, host, host, port));
            }
            else
            {
                Logger.Debug("Skipping the callback since populating producers is off");
            }
        }

        /// <summary>
        /// Retrieves the partition id based on key using given partitioner or select random partition if key is null
        /// </summary>
        /// <param name="key">The partition key.</param>
        /// <param name="numPartitions">The total number of available partitions.</param>
        /// <returns>Partition Id</returns>
        private int GetPartitionId(TKey key, int numPartitions)
        {
            Guard.Assert<ArgumentOutOfRangeException>(() => numPartitions > 0);
            return key == null 
                ? Randomizer.Next(numPartitions) 
                : this.partitioner.Partition(key, numPartitions);
        }

        /// <summary>
        /// Gets the partition for topic.
        /// </summary>
        /// <param name="dataItem">The producer data object that encapsulates the topic, key and message data.</param>
        /// <returns>Partition for topic</returns>
        private Partition GetPartition(ProducerData<TKey, TData> dataItem)
        {
            Logger.DebugFormat(
                CultureInfo.CurrentCulture,
                "Getting the number of broker partitions registered for topic: {0}",
                dataItem.Topic);
            SortedSet<Partition> brokerPartitions = this.brokerPartitionInfo.GetBrokerPartitionInfo(dataItem.Topic);
            int totalNumPartitions = brokerPartitions.Count;
            Logger.DebugFormat(
                CultureInfo.CurrentCulture,
                "Broker partitions registered for topic: {0} = {1}",
                dataItem.Topic,
                totalNumPartitions);
            int partitionId = this.GetPartitionId(dataItem.Key, totalNumPartitions);
            Partition brokerIdPartition = brokerPartitions.ToList()[partitionId];
            Broker brokerInfo = this.brokerPartitionInfo.GetBrokerInfo(brokerIdPartition.BrokerId);
            if (this.IsZooKeeperEnabled)
            {
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture,
                    "Sending message to broker {0}:{1} on partition {2}",
                    brokerInfo.Host,
                    brokerInfo.Port,
                    brokerIdPartition.PartId);
                return new Partition(brokerIdPartition.BrokerId, brokerIdPartition.PartId);
            }

            Logger.DebugFormat(
                CultureInfo.CurrentCulture,
                "Sending message to broker {0}:{1} on a randomly chosen partition",
                brokerInfo.Host,
                brokerInfo.Port);
            return new Partition(brokerIdPartition.BrokerId, ProducerRequest.RandomPartition);
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
