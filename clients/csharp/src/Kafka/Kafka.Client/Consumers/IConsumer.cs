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

namespace Kafka.Client.Consumers
{
    using System.Collections.Generic;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;

    /// <summary>
    /// The low-level API of consumer of Kafka messages
    /// </summary>
    /// <remarks>
    /// Maintains a connection to a single broker and has a close correspondence 
    /// to the network requests sent to the server.
    /// </remarks>
    public interface IConsumer
    {
        /// <summary>
        /// Gets the server to which the connection is to be established.
        /// </summary>
        string Host { get; }

        /// <summary>
        /// Gets the port to which the connection is to be established.
        /// </summary>
        int Port { get; }

        /// <summary>
        /// Fetch a set of messages from a topic.
        /// </summary>
        /// <param name="request">
        /// Specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
        /// </param>
        /// <returns>
        /// A set of fetched messages.
        /// </returns>
        /// <remarks>
        /// Offset is passed in on every request, allowing the user to maintain this metadata 
        /// however they choose.
        /// </remarks>
        BufferedMessageSet Fetch(FetchRequest request);

        /// <summary>
        /// Combine multiple fetch requests in one call.
        /// </summary>
        /// <param name="request">
        /// The list of fetch requests.
        /// </param>
        /// <returns>
        /// A list of sets of fetched messages.
        /// </returns>
        /// <remarks>
        /// Offset is passed in on every request, allowing the user to maintain this metadata 
        /// however they choose.
        /// </remarks>
        IList<BufferedMessageSet> MultiFetch(MultiFetchRequest request);

        /// <summary>
        /// Gets a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="request">
        /// The offset request.
        /// </param>
        /// <returns>
        /// The list of offsets, in descending order.
        /// </returns>
        IList<long> GetOffsetsBefore(OffsetRequest request);
    }
}
