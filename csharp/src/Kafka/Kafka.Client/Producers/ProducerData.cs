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

namespace Kafka.Client.Producers
{
    using System.Collections.Generic;

    /// <summary>
    /// Encapsulates data to be send on topic
    /// </summary>
    /// <typeparam name="TKey">
    /// Type of partitioning key
    /// </typeparam>
    /// <typeparam name="TData">
    /// Type of data
    /// </typeparam>
    public class ProducerData<TKey, TData>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProducerData{TKey,TData}"/> class.
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="key">
        /// The partitioning key.
        /// </param>
        /// <param name="data">
        /// The list of data to send on the same topic.
        /// </param>
        public ProducerData(string topic, TKey key, IEnumerable<TData> data)
            : this(topic, data)
        {
            this.Key = key;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProducerData{TKey,TData}"/> class.
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="data">
        /// The list of data to send on the same topic.
        /// </param>
        public ProducerData(string topic, IEnumerable<TData> data)
        {
            this.Topic = topic;
            this.Data = data;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProducerData{TKey,TData}"/> class.
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="data">
        /// The data to send on the topic.
        /// </param>
        public ProducerData(string topic, TData data)
            : this(topic, new[] { data })
        {
        }

        /// <summary>
        /// Gets topic.
        /// </summary>
        public string Topic { get; private set; }

        /// <summary>
        /// Gets the partitioning key.
        /// </summary>
        public TKey Key { get; private set; }

        /// <summary>
        /// Gets the data.
        /// </summary>
        public IEnumerable<TData> Data { get; private set; }
    }
}
