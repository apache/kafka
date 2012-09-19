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

namespace Kafka.Client
{
    using System.Net.Sockets;
    using Kafka.Client.Requests;

    /// <summary>
    /// The context of a request made to Kafka.
    /// </summary>
    /// <typeparam name="T">
    /// Must be of type <see cref="AbstractRequest"/> and represents the type of request
    /// sent to Kafka.
    /// </typeparam>
    public class RequestContext<T> where T : AbstractRequest
    {
        /// <summary>
        /// Initializes a new instance of the RequestContext class.
        /// </summary>
        /// <param name="networkStream">The network stream that sent the message.</param>
        /// <param name="request">The request sent over the stream.</param>
        public RequestContext(NetworkStream networkStream, T request)
        {
            NetworkStream = networkStream;
            Request = request;
        }

        /// <summary>
        /// Gets the <see cref="NetworkStream"/> instance of the request.
        /// </summary>
        public NetworkStream NetworkStream { get; private set; }

        /// <summary>
        /// Gets the <see cref="FetchRequest"/> or <see cref="ProducerRequest"/> object
        /// associated with the <see cref="RequestContext"/>.
        /// </summary>
        public T Request { get; private set; }
    }
}
