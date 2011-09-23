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
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using log4net;

    /// <summary>
    /// An iterator that blocks until a value can be read from the supplied queue.
    /// </summary>
    /// <remarks>
    /// The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
    /// </remarks>
    internal class ConsumerIterator : IEnumerator<Message>
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private readonly BlockingCollection<FetchedDataChunk> channel;
        private readonly int consumerTimeoutMs;
        private PartitionTopicInfo currentTopicInfo;
        private ConsumerIteratorState state = ConsumerIteratorState.NotReady;
        private IEnumerator<Message> current;
        private Message nextItem;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConsumerIterator"/> class.
        /// </summary>
        /// <param name="channel">
        /// The queue containing 
        /// </param>
        /// <param name="consumerTimeoutMs">
        /// The consumer timeout in ms.
        /// </param>
        public ConsumerIterator(BlockingCollection<FetchedDataChunk> channel, int consumerTimeoutMs)
        {
            this.channel = channel;
            this.consumerTimeoutMs = consumerTimeoutMs;
        }

        /// <summary>
        /// Gets the element in the collection at the current position of the enumerator.
        /// </summary>
        /// <returns>
        /// The element in the collection at the current position of the enumerator.
        /// </returns>
        public Message Current
        {
            get
            {
                if (!MoveNext())
                {
                    throw new Exception("No element");
                }

                state = ConsumerIteratorState.NotReady;
                if (nextItem != null)
                {
                    currentTopicInfo.Consumed(MessageSet.GetEntrySize(nextItem));
                    return nextItem;
                }

                throw new Exception("Expected item but none found.");
            }
        }

        /// <summary>
        /// Gets the current element in the collection.
        /// </summary>
        /// <returns>
        /// The current element in the collection.
        /// </returns>
        object IEnumerator.Current
        {
            get { return this.Current; }
        }

        /// <summary>
        /// Advances the enumerator to the next element of the collection.
        /// </summary>
        /// <returns>
        /// true if the enumerator was successfully advanced to the next element; false if the enumerator has passed the end of the collection.
        /// </returns>
        public bool MoveNext()
        {
            if (state == ConsumerIteratorState.Failed)
            {
                throw new Exception("Iterator is in failed state");
            }
            
            switch (state)
            {
                case ConsumerIteratorState.Done:
                    return false;
                case ConsumerIteratorState.Ready:
                    return true;
                default:
                    return MaybeComputeNext();
            }
        }

        /// <summary>
        /// Resets the enumerator's state to NotReady.
        /// </summary>
        public void Reset()
        {
            state = ConsumerIteratorState.NotReady;
        }

        public void Dispose()
        {
        }

        private bool MaybeComputeNext()
        {
            state = ConsumerIteratorState.Failed;
            nextItem = this.MakeNext();
            if (state == ConsumerIteratorState.Done)
            {
                return false;
            }

            state = ConsumerIteratorState.Ready;
            return true;
        }

        private Message MakeNext()
        {
            if (current == null || !current.MoveNext())
            {
                FetchedDataChunk found;
                if (consumerTimeoutMs < 0)
                {
                    found = this.channel.Take();
                }
                else
                {
                    bool done = channel.TryTake(out found, consumerTimeoutMs);
                    if (!done)
                    {
                        Logger.Debug("Consumer iterator timing out...");
                        throw new ConsumerTimeoutException();
                    }
                }

                if (found.Equals(ZookeeperConsumerConnector.ShutdownCommand))
                {
                    Logger.Debug("Received the shutdown command");
                    channel.Add(found);
                    return this.AllDone();
                }

                currentTopicInfo = found.TopicInfo;
                if (currentTopicInfo.GetConsumeOffset() != found.FetchOffset)
                {
                    Logger.ErrorFormat(
                        CultureInfo.CurrentCulture,
                        "consumed offset: {0} doesn't match fetch offset: {1} for {2}; consumer may lose data",
                        currentTopicInfo.GetConsumeOffset(),
                        found.FetchOffset,
                        currentTopicInfo);
                    currentTopicInfo.ResetConsumeOffset(found.FetchOffset);
                }

                current = found.Messages.Messages.GetEnumerator();
                current.MoveNext();
            }

            return current.Current;
        }

        private Message AllDone()
        {
            this.state = ConsumerIteratorState.Done;
            return null;
        }
    }
}
