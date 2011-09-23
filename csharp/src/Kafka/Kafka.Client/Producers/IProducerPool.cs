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
    using System.Collections.Generic;
    using Kafka.Client.Cluster;

    /// <summary>
    /// Pool of producers used by producer high-level API
    /// </summary>
    /// <typeparam name="TData">The type of the data.</typeparam>
    internal interface IProducerPool<TData>
    {
        /// <summary>
        /// Selects either a synchronous or an asynchronous producer, for
        /// the specified broker id and calls the send API on the selected
        /// producer to publish the data to the specified broker partition.
        /// </summary>
        /// <param name="poolData">The producer pool request object.</param>
        /// <remarks>
        /// Used for single-topic request
        /// </remarks>
        void Send(ProducerPoolData<TData> poolData);

        /// <summary>
        /// Selects either a synchronous or an asynchronous producer, for
        /// the specified broker id and calls the send API on the selected
        /// producer to publish the data to the specified broker partition.
        /// </summary>
        /// <param name="poolData">The producer pool request object.</param>
        /// <remarks>
        /// Used for multi-topic request
        /// </remarks>
        void Send(IEnumerable<ProducerPoolData<TData>> poolData);

        /// <summary>
        /// Add a new producer, either synchronous or asynchronous, to the pool
        /// </summary>
        /// <param name="broker">The broker informations.</param>
        void AddProducer(Broker broker);
    }
}
