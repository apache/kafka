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

using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Client.Request;
using Kafka.Client.Util;

namespace Kafka.Client
{
    /// <summary>
    /// Sends message to Kafka.
    /// </summary>
    public class Producer
    {
        /// <summary>
        /// Initializes a new instance of the Producer class.
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        public Producer(string server, int port)
        {
            Server = server;
            Port = port;
        }

        /// <summary>
        /// Gets the server to which the connection is to be established.
        /// </summary>
        public string Server { get; private set; }

        /// <summary>
        /// Gets the port to which the connection is to be established.
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// Sends a message to Kafka.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="msg">The message to send.</param>
        public void Send(string topic, int partition, Message msg)
        {
            Send(topic, partition, new List<Message> { msg });
        }

        /// <summary>
        /// Sends a list of messages to Kafka.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="messages">The list of messages to send.</param>
        public void Send(string topic, int partition, IList<Message> messages)
        {
            Send(new ProducerRequest(topic, partition, messages));
        }

        /// <summary>
        /// Sends a request to Kafka.
        /// </summary>
        /// <param name="request">The request to send to Kafka.</param>
        public void Send(ProducerRequest request)
        {
            if (request.IsValid())
            {
                using (KafkaConnection connection = new KafkaConnection(Server, Port))
                {
                    connection.Write(request);
                }
            }
        }

        /// <summary>
        /// Sends a request to Kafka.
        /// </summary>
        /// <param name="request">The request to send to Kafka.</param>
        public void Send(MultiProducerRequest request)
        {
            if (request.IsValid())
            {
                using (KafkaConnection connection = new KafkaConnection(Server, Port))
                {
                    connection.Write(request);
                }
            }
        }

        /// <summary>
        /// Sends a list of messages to Kafka.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="messages">The list of messages to send.</param>
        /// <param name="callback">
        /// A block of code to execute once the request has been sent to Kafka.  This value may 
        /// be set to null.
        /// </param>
        public void SendAsync(string topic, int partition, IList<Message> messages, MessageSent<ProducerRequest> callback)
        {
            SendAsync(new ProducerRequest(topic, partition, messages), callback);
        }

        /// <summary>
        /// Send a request to Kafka asynchronously.
        /// </summary>
        /// <remarks>
        /// If the callback is not specified then the method behaves as a fire-and-forget call
        /// with the callback being ignored.  By the time this callback is executed, the 
        /// <see cref="RequestContext.NetworkStream"/> will already have been closed given an 
        /// internal call <see cref="NetworkStream.EndWrite"/>.
        /// </remarks>
        /// <param name="request">The request to send to Kafka.</param>
        /// <param name="callback">
        /// A block of code to execute once the request has been sent to Kafka.  This value may 
        /// be set to null.
        /// </param>
        public void SendAsync(ProducerRequest request, MessageSent<ProducerRequest> callback)
        {
            if (request.IsValid())
            {
                KafkaConnection connection = new KafkaConnection(Server, Port);

                if (callback == null)
                {
                    // fire and forget
                    connection.BeginWrite(request.GetBytes());
                }
                else
                {
                    // execute with callback
                    connection.BeginWrite(request, callback);
                }
            }
        }
    }
}
