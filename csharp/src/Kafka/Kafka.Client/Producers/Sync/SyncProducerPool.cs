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

namespace Kafka.Client.Producers.Sync
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
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using log4net;

    /// <summary>
    /// Pool of synchronous producers used by high-level API
    /// </summary>
    /// <typeparam name="TData">The type of the data.</typeparam>
    internal class SyncProducerPool<TData> : ProducerPool<TData>
        where TData : class 
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly IDictionary<int, ISyncProducer> syncProducers;

        /// <summary>
        /// Factory method used to instantiating synchronous producer pool
        /// </summary>
        /// <param name="config">
        /// The synchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <returns>
        /// Instantiated synchronous producer pool
        /// </returns>
        public static SyncProducerPool<TData> CreateSyncPool(ProducerConfig config, IEncoder<TData> serializer)
        {
            return new SyncProducerPool<TData>(config, serializer);
        }

        /// <summary>
        /// Factory method used to instantiating synchronous producer pool
        /// </summary>
        /// <param name="config">
        /// The synchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <param name="callbackHandler">
        /// The callback invoked after new broker is added.
        /// </param>
        /// <returns>
        /// Instantiated synchronous producer pool
        /// </returns>
        public static SyncProducerPool<TData> CreateSyncPool(ProducerConfig config, IEncoder<TData> serializer, ICallbackHandler callbackHandler)
        {
            return new SyncProducerPool<TData>(config, serializer, callbackHandler);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncProducerPool{TData}"/> class. 
        /// </summary>
        /// <param name="config">
        /// The synchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <param name="syncProducers">
        /// The list of synchronous producers.
        /// </param>
        /// <param name="cbkHandler">
        /// The callback invoked after new broker is added.
        /// </param>
        /// <remarks>
        /// Should be used for testing purpose only
        /// </remarks>
        private SyncProducerPool(
            ProducerConfig config, 
            IEncoder<TData> serializer,
            IDictionary<int, ISyncProducer> syncProducers,
            ICallbackHandler cbkHandler)
            : base(config, serializer, cbkHandler)
        {
            this.syncProducers = syncProducers;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncProducerPool{TData}"/> class. 
        /// </summary>
        /// <param name="config">
        /// The synchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <param name="cbkHandler">
        /// The callback invoked after new broker is added.
        /// </param>
        /// <remarks>
        /// Should be used for testing purpose only
        /// </remarks>
        private SyncProducerPool(
            ProducerConfig config,
            IEncoder<TData> serializer,
            ICallbackHandler cbkHandler)
            : this(config, serializer, new Dictionary<int, ISyncProducer>(), cbkHandler)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncProducerPool{TData}"/> class. 
        /// </summary>
        /// <param name="config">
        /// The synchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        private SyncProducerPool(ProducerConfig config, IEncoder<TData> serializer)
            : this(
                config,
                serializer,
                new Dictionary<int, ISyncProducer>(),
                ReflectionHelper.Instantiate<ICallbackHandler>(config.CallbackHandler))
        {
        }

        /// <summary>
        /// Selects a synchronous producer, for
        /// the specified broker id and calls the send API on the selected
        /// producer to publish the data to the specified broker partition.
        /// </summary>
        /// <param name="poolData">The producer pool request object.</param>
        /// <remarks>
        /// Used for multi-topic request
        /// </remarks>
        public override void Send(IEnumerable<ProducerPoolData<TData>> poolData)
        {
            Guard.Assert<ArgumentNullException>(() => poolData != null);
            Dictionary<int, List<ProducerPoolData<TData>>> distinctBrokers = poolData.GroupBy(
                x => x.BidPid.BrokerId, x => x)
                .ToDictionary(x => x.Key, x => x.ToList());
            foreach (var broker in distinctBrokers)
            {
                Logger.DebugFormat(CultureInfo.CurrentCulture, "Fetching sync producer for broker id: {0}", broker.Key);
                ISyncProducer producer = this.syncProducers[broker.Key];
                IEnumerable<ProducerRequest> requests = broker.Value.Select(x => new ProducerRequest(
                    x.Topic,
                    x.BidPid.PartId,
                    new BufferedMessageSet(x.Data.Select(y => this.Serializer.ToMessage(y)))));
                Logger.DebugFormat(CultureInfo.CurrentCulture, "Sending message to broker {0}", broker.Key);
                if (requests.Count() > 1)
                {
                    producer.MultiSend(requests);
                }
                else
                {
                    producer.Send(requests.First());
                }
            }
        }

        /// <summary>
        /// Add a new synchronous producer to the pool
        /// </summary>
        /// <param name="broker">The broker informations.</param>
        public override void AddProducer(Broker broker)
        {
            Guard.Assert<ArgumentNullException>(() => broker != null);
            var syncConfig = new SyncProducerConfig
            {
                Host = broker.Host,
                Port = broker.Port,
                BufferSize = this.Config.BufferSize,
                ConnectTimeout = this.Config.ConnectTimeout,
                ReconnectInterval = this.Config.ReconnectInterval
            };
            var syncProducer = new SyncProducer(syncConfig);
            Logger.InfoFormat(
                CultureInfo.CurrentCulture,
                "Creating sync producer for broker id = {0} at {1}:{2}",
                broker.Id,
                broker.Host,
                broker.Port);
            this.syncProducers.Add(broker.Id, syncProducer);
        }
    }
}
