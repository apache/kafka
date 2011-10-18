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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Kafka.Client.Cfg;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;

    /// <summary>
    /// Sends messages encapsulated in request to Kafka server asynchronously
    /// </summary>
    public class AsyncProducer : IAsyncProducer
    {
        private readonly ICallbackHandler callbackHandler;
        private readonly KafkaConnection connection;
        private volatile bool disposed;

        /// <summary>
        /// Gets producer config
        /// </summary>
        public AsyncProducerConfiguration Config { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncProducer"/> class.
        /// </summary>
        /// <param name="config">
        /// The producer config.
        /// </param>
        public AsyncProducer(AsyncProducerConfiguration config)
            : this(
                config,
                ReflectionHelper.Instantiate<ICallbackHandler>(config.CallbackHandlerClass))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncProducer"/> class.
        /// </summary>
        /// <param name="config">
        /// The producer config.
        /// </param>
        /// <param name="callbackHandler">
        /// The callback invoked when a request is finished being sent.
        /// </param>
        public AsyncProducer(
            AsyncProducerConfiguration config,
            ICallbackHandler callbackHandler)
        {
            Guard.NotNull(config, "config");

            this.Config = config;
            this.callbackHandler = callbackHandler;
            this.connection = new KafkaConnection(
                this.Config.Host,
                this.Config.Port,
                this.Config.BufferSize,
                this.Config.SocketTimeout);
        }

        /// <summary>
        /// Sends request to Kafka server asynchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        public void Send(ProducerRequest request)
        {
            this.EnsuresNotDisposed();
            Guard.NotNull(request, "request");
            Guard.Assert<ArgumentException>(() => request.MessageSet.Messages.All(x => x.PayloadSize <= this.Config.MaxMessageSize));
            if (this.callbackHandler != null)
            {
                this.Send(request, this.callbackHandler.Handle);
            }
            else
            {
                this.connection.BeginWrite(request);
            }
        }

        /// <summary>
        /// Sends request to Kafka server asynchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        /// <param name="callback">
        /// The callback invoked when a request is finished being sent.
        /// </param>
        public void Send(ProducerRequest request, MessageSent<ProducerRequest> callback)
        {
            this.EnsuresNotDisposed();
            Guard.NotNull(request, "request");
            Guard.NotNull(request.MessageSet, "request.MessageSet");
            Guard.NotNull(request.MessageSet.Messages, "request.MessageSet.Messages");
            Guard.Assert<ArgumentException>(
                () => request.MessageSet.Messages.All(x => x.PayloadSize <= this.Config.MaxMessageSize));

            connection.BeginWrite(request, callback);
        }

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
        public void Send(string topic, int partition, IEnumerable<Message> messages)
        {
            this.EnsuresNotDisposed();
            Guard.NotNullNorEmpty(topic, "topic");
            Guard.NotNull(messages, "messages");
            Guard.Assert<ArgumentException>(() => messages.All(x => x.PayloadSize <= this.Config.MaxMessageSize));

            this.Send(new ProducerRequest(topic, partition, messages));
        }

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
        public void Send(string topic, int partition, IEnumerable<Message> messages, MessageSent<ProducerRequest> callback)
        {
            this.EnsuresNotDisposed();
            Guard.NotNullNorEmpty(topic, "topic");
            Guard.NotNull(messages, "messages");
            Guard.Assert<ArgumentException>(() => messages.All(x => x.PayloadSize <= this.Config.MaxMessageSize));

            this.Send(new ProducerRequest(topic, partition, messages), callback);
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
