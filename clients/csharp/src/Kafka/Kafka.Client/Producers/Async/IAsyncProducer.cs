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
    using System.Collections.Generic;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;

    /// <summary>
    /// Sends messages encapsulated in request to Kafka server asynchronously
    /// </summary>
    public interface IAsyncProducer
    {
        /// <summary>
        /// Sends request to Kafka server asynchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        void Send(ProducerRequest request);

        /// <summary>
        /// Sends request to Kafka server asynchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        /// <param name="callback">
        /// The callback invoked when a request is finished being sent.
        /// </param>
        void Send(ProducerRequest request, MessageSent<ProducerRequest> callback);

        /// <summary>
        /// Constructs request and sent it to Kafka server asynchronously
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="partition">
        /// The partition.
        /// </param>
        /// <param name="messages">
        /// The list of messages to sent.
        /// </param>
        void Send(string topic, int partition, IEnumerable<Message> messages);

        /// <summary>
        /// Constructs request and sent it to Kafka server asynchronously
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="partition">
        /// The partition.
        /// </param>
        /// <param name="messages">
        /// The list of messages to sent.
        /// </param>
        /// <param name="callback">
        /// The callback invoked when a request is finished being sent.
        /// </param>
        void Send(string topic, int partition, IEnumerable<Message> messages, MessageSent<ProducerRequest> callback);
    }
}
