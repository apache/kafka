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

namespace Kafka.Client.Producers.Sync
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Kafka.Client.Cfg;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;

    /// <summary>
    /// Sends messages encapsulated in request to Kafka server synchronously
    /// </summary>
    public class SyncProducer : ISyncProducer
    {
        private readonly KafkaConnection connection;

        private volatile bool disposed;

        /// <summary>
        /// Gets producer config
        /// </summary>
        public SyncProducerConfiguration Config { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncProducer"/> class.
        /// </summary>
        /// <param name="config">
        /// The producer config.
        /// </param>
        public SyncProducer(SyncProducerConfiguration config)
        {
            Guard.NotNull(config, "config");
            this.Config = config;
            this.connection = new KafkaConnection(
                this.Config.Host, 
                this.Config.Port,
                config.BufferSize,
                config.SocketTimeout);
        }

        /// <summary>
        /// Constructs producer request and sends it to given broker partition synchronously
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="partition">
        /// The partition.
        /// </param>
        /// <param name="messages">
        /// The list of messages messages.
        /// </param>
        public void Send(string topic, int partition, IEnumerable<Message> messages)
        {
            Guard.NotNullNorEmpty(topic, "topic");
            Guard.NotNull(messages, "messages");
            Guard.AllNotNull(messages, "messages.items");
            Guard.Assert<ArgumentOutOfRangeException>(
                () => messages.All(
                    x => x.PayloadSize <= this.Config.MaxMessageSize));
            this.EnsuresNotDisposed();
            this.Send(new ProducerRequest(topic, partition, messages));
        }

        /// <summary>
        /// Sends request to Kafka server synchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        public void Send(ProducerRequest request)
        {
            this.EnsuresNotDisposed();
            this.connection.Write(request);
        }

        /// <summary>
        /// Sends the data to a multiple topics on Kafka server synchronously
        /// </summary>
        /// <param name="requests">
        /// The requests.
        /// </param>
        public void MultiSend(IEnumerable<ProducerRequest> requests)
        {
            Guard.NotNull(requests, "requests");
            Guard.Assert<ArgumentNullException>(
                () => requests.All(
                    x => x != null && x.MessageSet != null && x.MessageSet.Messages != null));
            Guard.Assert<ArgumentNullException>(
                () => requests.All(
                    x => x.MessageSet.Messages.All(
                        y => y != null && y.PayloadSize <= this.Config.MaxMessageSize)));
            this.EnsuresNotDisposed();
            var multiRequest = new MultiProducerRequest(requests);
            this.connection.Write(multiRequest);
        }

        /// <summary>
        /// Releases all unmanaged and managed resources
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
            if (this.connection != null)
            {
                this.connection.Dispose();
            }
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }
    }
}
