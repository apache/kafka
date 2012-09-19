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

    /// <summary>
    /// High-level Producer API that exposing all the producer functionality through a single API to the client
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TData">The type of the data.</typeparam>
    public interface IProducer<TKey, TData> : IDisposable
        where TKey : class
        where TData : class 
    {
        /// <summary>
        /// Sends the data to a single topic, partitioned by key, using either the
        /// synchronous or the asynchronous producer.
        /// </summary>
        /// <param name="data">The producer data object that encapsulates the topic, key and message data.</param>
        void Send(ProducerData<TKey, TData> data);

        /// <summary>
        /// Sends the data to a multiple topics, partitioned by key, using either the
        /// synchronous or the asynchronous producer.
        /// </summary>
        /// <param name="data">The producer data object that encapsulates the topic, key and message data.</param>
        void Send(IEnumerable<ProducerData<TKey, TData>> data);
    }
}
