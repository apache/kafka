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

namespace Kafka.Client.Producers.Async
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using log4net;

    /// <summary>
    /// Pool of asynchronous producers used by high-level API
    /// </summary>
    /// <typeparam name="TData">The type of the data.</typeparam>
    internal class AsyncProducerPool<TData> : ProducerPool<TData>
        where TData : class 
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly IDictionary<int, IAsyncProducer> asyncProducers;
        private volatile bool disposed;
        
        /// <summary>
        /// Factory method used to instantiating asynchronous producer pool
        /// </summary>
        /// <param name="config">
        /// The asynchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <param name="cbkHandler">
        /// The callback invoked after new broker is added.
        /// </param>
        /// <returns>
        /// Instantiated asynchronous producer pool
        /// </returns>
        public static AsyncProducerPool<TData> CreateAsyncPool(
            ProducerConfiguration config, 
            IEncoder<TData> serializer, 
            ICallbackHandler cbkHandler)
        {
            return new AsyncProducerPool<TData>(config, serializer, cbkHandler);
        }

        /// <summary>
        /// Factory method used to instantiating asynchronous producer pool
        /// </summary>
        /// <param name="config">
        /// The asynchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <returns>
        /// Instantiated asynchronous producer pool
        /// </returns>
        public static AsyncProducerPool<TData> CreateAsyncPool(
            ProducerConfiguration config,
            IEncoder<TData> serializer)
        {
            return new AsyncProducerPool<TData>(config, serializer);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncProducerPool{TData}"/> class. 
        /// </summary>
        /// <param name="config">
        /// The asynchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <param name="asyncProducers">
        /// The list of asynchronous producers.
        /// </param>
        /// <param name="cbkHandler">
        /// The callback invoked after new broker is added.
        /// </param>
        /// <remarks>
        /// Should be used for testing purpose only
        /// </remarks>
        private AsyncProducerPool(
            ProducerConfiguration config, 
            IEncoder<TData> serializer, 
            IDictionary<int, IAsyncProducer> asyncProducers, 
            ICallbackHandler cbkHandler)
            : base(config, serializer, cbkHandler)
        {
            this.asyncProducers = asyncProducers;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncProducerPool{TData}"/> class. 
        /// </summary>
        /// <param name="config">
        /// The asynchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <param name="cbkHandler">
        /// The callback invoked after new broker is added.
        /// </param>
        private AsyncProducerPool(
            ProducerConfiguration config, 
            IEncoder<TData> serializer, 
            ICallbackHandler cbkHandler)
            : this(config, serializer, new Dictionary<int, IAsyncProducer>(), cbkHandler)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncProducerPool{TData}"/> class. 
        /// </summary>
        /// <param name="config">
        /// The asynchronous producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        private AsyncProducerPool(ProducerConfiguration config, IEncoder<TData> serializer)
            : this(
            config,
            serializer,
            new Dictionary<int, IAsyncProducer>(),
            ReflectionHelper.Instantiate<ICallbackHandler>(config.CallbackHandlerClass))
        {
        }

        /// <summary>
        /// Selects an asynchronous producer, for
        /// the specified broker id and calls the send API on the selected
        /// producer to publish the data to the specified broker partition.
        /// </summary>
        /// <param name="poolData">The producer pool request object.</param>
        /// <remarks>
        /// Used for multi-topic request
        /// </remarks>
        public override void Send(IEnumerable<ProducerPoolData<TData>> poolData)
        {
            this.EnsuresNotDisposed();
            Guard.NotNull(poolData, "poolData");
            Dictionary<int, List<ProducerPoolData<TData>>> distinctBrokers = poolData.GroupBy(
                x => x.BidPid.BrokerId, x => x)
                .ToDictionary(x => x.Key, x => x.ToList());
            foreach (var broker in distinctBrokers)
            {
                Logger.DebugFormat(CultureInfo.CurrentCulture, "Fetching async producer for broker id: {0}", broker.Key);
                var producer = this.asyncProducers[broker.Key];
                IEnumerable<ProducerRequest> requests = broker.Value.Select(x => new ProducerRequest(
                    x.Topic,
                    x.BidPid.PartId,
                    new BufferedMessageSet(x.Data.Select(y => this.Serializer.ToMessage(y)))));
                foreach (var request in requests)
                {
                    producer.Send(request);
                }
            }
        }

        /// <summary>
        /// Add a new asynchronous producer to the pool.
        /// </summary>
        /// <param name="broker">The broker informations.</param>
        public override void AddProducer(Broker broker)
        {
            this.EnsuresNotDisposed();
            Guard.NotNull(broker, "broker");
            var asyncConfig = new AsyncProducerConfiguration(this.Config, broker.Id, broker.Host, broker.Port)
                {
                    SerializerClass = this.Config.SerializerClass
                };
            var asyncProducer = new AsyncProducer(asyncConfig, this.CallbackHandler);
            Logger.InfoFormat(
                CultureInfo.CurrentCulture,
                "Creating async producer for broker id = {0} at {1}:{2}",
                broker.Id,
                broker.Host,
                broker.Port);
            this.asyncProducers.Add(broker.Id, asyncProducer);
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

            this.disposed = true;
            foreach (var asyncProducer in this.asyncProducers.Values)
            {
                asyncProducer.Dispose();
            }
        }
    }
}
