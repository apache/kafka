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
    using Kafka.Client.Cfg;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Serialization;

    /// <summary>
    /// High-level Producer API that exposes all the producer functionality to the client 
    /// using <see cref="System.String" /> as type of key and <see cref="Message" /> as type of data
    /// </summary>
    public class Producer : Producer<string, Message>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Producer"/> class.
        /// </summary>
        /// <param name="config">The config object.</param>
        /// <param name="partitioner">The partitioner that implements <see cref="IPartitioner&lt;String&gt;" /> 
        /// used to supply a custom partitioning strategy based on the message key.</param>
        /// <param name="producerPool">Pool of producers, one per broker.</param>
        /// <param name="populateProducerPool">if set to <c>true</c>, producers should be populated.</param>
        /// <remarks>
        /// Should be used for testing purpose only.
        /// </remarks>
        internal Producer(ProducerConfig config, IPartitioner<string> partitioner, IProducerPool<Message> producerPool, bool populateProducerPool)
            : base(config, partitioner, producerPool, populateProducerPool)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Producer"/> class.
        /// </summary>
        /// <param name="config">The config object.</param>
        /// <remarks>
        /// Can be used when all config parameters will be specified through the config object
        /// and will be instantiated via reflection
        /// </remarks>
        public Producer(ProducerConfig config)
            : base(config)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Producer"/> class.
        /// </summary>
        /// <param name="config">The config object.</param>
        /// <param name="partitioner">The partitioner that implements <see cref="IPartitioner&lt;String&gt;" /> 
        /// used to supply a custom partitioning strategy based on the message key.</param>
        /// <param name="encoder">The encoder that implements <see cref="IEncoder&lt;Message&gt;" /></param>
        /// <param name="callbackHandler">The callback handler that implements <see cref="ICallbackHandler" />, used 
        /// to supply callback invoked when sending asynchronous request is completed.</param>
        /// <remarks>
        /// Can be used to provide pre-instantiated objects for all config parameters
        /// that would otherwise be instantiated via reflection.
        /// </remarks>
        public Producer(ProducerConfig config, IPartitioner<string> partitioner, IEncoder<Message> encoder, ICallbackHandler callbackHandler)
            : base(config, partitioner, encoder, callbackHandler)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Producer"/> class.
        /// </summary>
        /// <param name="config">The config object.</param>
        /// <param name="partitioner">The partitioner that implements <see cref="IPartitioner&lt;TKey&gt;" /> 
        /// used to supply a custom partitioning strategy based on the message key.</param>
        /// <param name="encoder">The encoder that implements <see cref="IEncoder&lt;Message&gt;" /> 
        /// </param>
        /// <remarks>
        /// Can be used to provide pre-instantiated objects for all config parameters
        /// that would otherwise be instantiated via reflection.
        /// </remarks>
        public Producer(ProducerConfig config, IPartitioner<string> partitioner, IEncoder<Message> encoder)
            : base(config, partitioner, encoder)
        {
        }
    }
}
