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
        private readonly SyncProducerConfig config;

        /// <summary>
        /// Gets producer config
        /// </summary>
        public SyncProducerConfig Config
        {
            get { return config; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncProducer"/> class.
        /// </summary>
        /// <param name="config">
        /// The producer config.
        /// </param>
        public SyncProducer(SyncProducerConfig config)
        {
            Guard.Assert<ArgumentNullException>(() => config != null);
            this.config = config;
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
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(topic));
            Guard.Assert<ArgumentNullException>(() => messages != null);
            Guard.Assert<ArgumentNullException>(
                () => messages.All(x => x != null));
            Guard.Assert<ArgumentOutOfRangeException>(
                () => messages.All(
                    x => x.PayloadSize <= this.Config.MaxMessageSize));
            
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
            Guard.Assert<ArgumentNullException>(() => request != null);
            using (var conn = new KafkaConnection(this.config.Host, this.config.Port))
            {
                conn.Write(request);
            }
        }

        /// <summary>
        /// Sends the data to a multiple topics on Kafka server synchronously
        /// </summary>
        /// <param name="requests">
        /// The requests.
        /// </param>
        public void MultiSend(IEnumerable<ProducerRequest> requests)
        {
            Guard.Assert<ArgumentNullException>(() => requests != null);
            Guard.Assert<ArgumentNullException>(
                () => requests.All(
                    x => x != null && x.MessageSet != null && x.MessageSet.Messages != null));
            Guard.Assert<ArgumentNullException>(
                () => requests.All(
                    x => x.MessageSet.Messages.All(
                        y => y != null && y.PayloadSize <= this.Config.MaxMessageSize)));

            var multiRequest = new MultiProducerRequest(requests);
            using (var conn = new KafkaConnection(this.config.Host, this.config.Port))
            {
                conn.Write(multiRequest);
            }
        }
    }
}
