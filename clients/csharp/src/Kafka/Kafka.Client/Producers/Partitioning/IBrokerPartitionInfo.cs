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
    using Kafka.Client.Cluster;

    /// <summary>
    /// Retrieves brokers and partitions info
    /// </summary>
    internal interface IBrokerPartitionInfo : IDisposable
    {
        /// <summary>
        /// Gets a mapping from broker ID to the host and port for all brokers
        /// </summary>
        /// <returns>Mapping from broker ID to the host and port for all brokers</returns>
        IDictionary<int, Broker> GetAllBrokerInfo();

        /// <summary>
        /// Gets a mapping from broker ID to partition IDs
        /// </summary>
        /// <param name="topic">The topic for which this information is to be returned</param>
        /// <returns>Mapping from broker ID to partition IDs</returns>
        SortedSet<Partition> GetBrokerPartitionInfo(string topic);

        /// <summary>
        /// Gets the host and port information for the broker identified by the given broker ID
        /// </summary>
        /// <param name="brokerId">The broker ID.</param>
        /// <returns>Host and port of broker</returns>
        Broker GetBrokerInfo(int brokerId);
    }
}
