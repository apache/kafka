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
    public class AsyncProducer : IAsyncProducer, IDisposable
    {
        private readonly AsyncProducerConfig config;
        private readonly ICallbackHandler callbackHandler;
        private KafkaConnection connection = null;

        /// <summary>
        /// Gets producer config
        /// </summary>
        public AsyncProducerConfig Config
        {
            get { return config; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncProducer"/> class.
        /// </summary>
        /// <param name="config">
        /// The producer config.
        /// </param>
        public AsyncProducer(AsyncProducerConfig config)
            : this(
                config,
                ReflectionHelper.Instantiate<ICallbackHandler>(config.CallbackHandler))
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
            AsyncProducerConfig config,
            ICallbackHandler callbackHandler)
        {
            Guard.Assert<ArgumentNullException>(() => config != null);

            this.config = config;
            this.callbackHandler = callbackHandler;
            this.connection = new KafkaConnection(this.config.Host, this.config.Port);
        }

        /// <summary>
        /// Sends request to Kafka server asynchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        public void Send(ProducerRequest request)
        {
            Guard.Assert<ArgumentNullException>(() => request != null);
            Guard.Assert<ArgumentException>(() => request.MessageSet.Messages.All(x => x.PayloadSize <= this.Config.MaxMessageSize));
            if (this.callbackHandler != null)
            {
                this.Send(request, this.callbackHandler.Handle);
            }
            else
            {
                connection.BeginWrite(request);
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
            Guard.Assert<ArgumentNullException>(() => request != null);
            Guard.Assert<ArgumentNullException>(() => request.MessageSet != null);
            Guard.Assert<ArgumentNullException>(() => request.MessageSet.Messages != null);
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
            Guard.Assert<ArgumentNullException>(() => !string.IsNullOrEmpty(topic));
            Guard.Assert<ArgumentNullException>(() => messages != null);
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
            Guard.Assert<ArgumentNullException>(() => !string.IsNullOrEmpty(topic));
            Guard.Assert<ArgumentNullException>(() => messages != null);
            Guard.Assert<ArgumentException>(() => messages.All(x => x.PayloadSize <= this.Config.MaxMessageSize));
            
            this.Send(new ProducerRequest(topic, partition, messages), callback);
        }

        public void Dispose()
        {
            if (connection != null)
            {
                connection.Dispose();
            }
        }
    }
}
