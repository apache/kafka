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
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using Kafka.Client.Messages;

    /// <summary>
    /// This class is a thread-safe IEnumerable of <see cref="Message"/> that can be enumerated to get messages.
    /// </summary>
    public class KafkaMessageStream : IEnumerable<Message>
    {
        private readonly BlockingCollection<FetchedDataChunk> queue;

        private readonly int consumerTimeoutMs;

        private readonly ConsumerIterator iterator;

        internal KafkaMessageStream(BlockingCollection<FetchedDataChunk> queue, int consumerTimeoutMs)
        {
            this.consumerTimeoutMs = consumerTimeoutMs;
            this.queue = queue;
            this.iterator = new ConsumerIterator(this.queue, this.consumerTimeoutMs);
        }

        public IEnumerator<Message> GetEnumerator()
        {
            return this.iterator;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
