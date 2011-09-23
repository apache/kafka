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
    /// Encapsulates data to be send on chosen partition
    /// </summary>
    /// <typeparam name="TData">
    /// Type of data
    /// </typeparam>
    internal class ProducerPoolData<TData>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProducerPoolData{TData}"/> class.
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="bidPid">
        /// The chosen partition.
        /// </param>
        /// <param name="data">
        /// The data.
        /// </param>
        public ProducerPoolData(string topic, Partition bidPid, IEnumerable<TData> data)
        {
            this.Topic = topic;
            this.BidPid = bidPid;
            this.Data = data;
        }

        /// <summary>
        /// Gets the topic.
        /// </summary>
        public string Topic { get; private set; }

        /// <summary>
        /// Gets the chosen partition.
        /// </summary>
        public Partition BidPid { get; private set; }

        /// <summary>
        /// Gets the data.
        /// </summary>
        public IEnumerable<TData> Data { get; private set; }
    }
}
