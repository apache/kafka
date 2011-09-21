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
    using System.Collections.Generic;

    /// <summary>
    /// The consumer high-level API, that hides the details of brokers from the consumer 
    /// It also maintains the state of what has been consumed. 
    /// </summary>
    public interface IConsumerConnector : IDisposable
    {
        /// <summary>
        /// Creates a list of message streams for each topic.
        /// </summary>
        /// <param name="topicCountDict">
        /// The map of topic on number of streams
        /// </param>
        /// <returns>
        /// The list of <see cref="KafkaMessageStream"/>, which are iterators over topic.
        /// </returns>
        IDictionary<string, IList<KafkaMessageStream>> CreateMessageStreams(IDictionary<string, int> topicCountDict);

        /// <summary>
        /// Commits the offsets of all messages consumed so far.
        /// </summary>
        void CommitOffsets();
    }
}
