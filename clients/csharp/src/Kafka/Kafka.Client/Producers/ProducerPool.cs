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
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// The base for all classes that represents pool of producers used by high-level API
    /// </summary>
    /// <typeparam name="TData">The type of the data.</typeparam>
    internal abstract class ProducerPool<TData> : IProducerPool<TData>
        where TData : class 
    {
        /// <summary>
        /// Factory method used to instantiating either, 
        /// synchronous or asynchronous, producer pool based on configuration.
        /// </summary>
        /// <param name="config">
        /// The producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <returns>
        /// Instantiated either, synchronous or asynchronous, producer pool
        /// </returns>
        public static ProducerPool<TData> CreatePool(ProducerConfig config, IEncoder<TData> serializer)
        {
            if (config.ProducerType == ProducerTypes.Async)
            {
                return AsyncProducerPool<TData>.CreateAsyncPool(config, serializer);
            }

            if (config.ProducerType == ProducerTypes.Sync)
            {
                return SyncProducerPool<TData>.CreateSyncPool(config, serializer);
            }

            throw new InvalidOperationException("Not supported producer type " + config.ProducerType);
        }

        /// <summary>
        /// Factory method used to instantiating either, 
        /// synchronous or asynchronous, producer pool based on configuration.
        /// </summary>
        /// <param name="config">
        /// The producer pool configuration.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <param name="cbkHandler">
        /// The callback invoked after new broker is added.
        /// </param>
        /// <returns>
        /// Instantiated either, synchronous or asynchronous, producer pool
        /// </returns>
        public static ProducerPool<TData> CreatePool(
            ProducerConfig config,
            IEncoder<TData> serializer,
            ICallbackHandler cbkHandler)
        {
            if (config.ProducerType == ProducerTypes.Async)
            {
                return AsyncProducerPool<TData>.CreateAsyncPool(config, serializer, cbkHandler);
            }

            if (config.ProducerType == ProducerTypes.Sync)
            {
                return SyncProducerPool<TData>.CreateSyncPool(config, serializer, cbkHandler);
            }

            throw new InvalidOperationException("Not supported producer type " + config.ProducerType);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProducerPool&lt;TData&gt;"/> class.
        /// </summary>
        /// <param name="config">The config.</param>
        /// <param name="serializer">The serializer.</param>
        /// <remarks>
        /// Should be used for testing purpose only
        /// </remarks>
        protected ProducerPool(
            ProducerConfig config,
            IEncoder<TData> serializer)
        {
            Guard.Assert<ArgumentNullException>(() => config != null);
            Guard.Assert<ArgumentNullException>(() => serializer != null);

            this.Config = config;
            this.Serializer = serializer;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProducerPool&lt;TData&gt;"/> class.
        /// </summary>
        /// <param name="config">
        /// The config.
        /// </param>
        /// <param name="serializer">
        /// The serializer.
        /// </param>
        /// <param name="callbackHandler">
        /// The callback invoked after new broker is added.
        /// </param>
        protected ProducerPool(
            ProducerConfig config,
            IEncoder<TData> serializer,
            ICallbackHandler callbackHandler)
        {
            Guard.Assert<ArgumentNullException>(() => config != null);
            Guard.Assert<ArgumentNullException>(() => serializer != null);

            this.Config = config;
            this.Serializer = serializer;
            this.CallbackHandler = callbackHandler;
        }

        protected ProducerConfig Config { get; private set; }

        protected IEncoder<TData> Serializer { get; private set; }

        protected ICallbackHandler CallbackHandler { get; private set; }

        /// <summary>
        /// Add a new producer, either synchronous or asynchronous, to the pool
        /// </summary>
        /// <param name="broker">The broker informations.</param>
        public abstract void AddProducer(Broker broker);

        /// <summary>
        /// Selects either a synchronous or an asynchronous producer, for
        /// the specified broker id and calls the send API on the selected
        /// producer to publish the data to the specified broker partition.
        /// </summary>
        /// <param name="poolData">The producer pool request object.</param>
        /// <remarks>
        /// Used for single-topic request
        /// </remarks>
        public void Send(ProducerPoolData<TData> poolData)
        {
            Guard.Assert<ArgumentNullException>(() => poolData != null);
            this.Send(new[] { poolData });
        }

        /// <summary>
        /// Selects either a synchronous or an asynchronous producer, for
        /// the specified broker id and calls the send API on the selected
        /// producer to publish the data to the specified broker partition.
        /// </summary>
        /// <param name="poolData">The producer pool request object.</param>
        /// <remarks>
        /// Used for multi-topic request
        /// </remarks>
        public abstract void Send(IEnumerable<ProducerPoolData<TData>> poolData);
    }
}
