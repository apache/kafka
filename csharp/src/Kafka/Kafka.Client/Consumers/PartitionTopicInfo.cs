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

namespace Kafka.Client.Consumers
{
    using System.Collections.Concurrent;
    using System.Globalization;
    using System.Reflection;
    using System.Threading;
    using Kafka.Client.Cluster;
    using Kafka.Client.Messages;
    using log4net;

    /// <summary>
    /// Represents topic in brokers's partition.
    /// </summary>
    internal class PartitionTopicInfo
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly object consumedOffsetLock = new object();

        private readonly object fetchedOffsetLock = new object();

        private readonly BlockingCollection<FetchedDataChunk> chunkQueue;

        private long consumedOffset;

        private long fetchedOffset;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionTopicInfo"/> class.
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="brokerId">
        /// The broker ID.
        /// </param>
        /// <param name="partition">
        /// The broker's partition.
        /// </param>
        /// <param name="chunkQueue">
        /// The chunk queue.
        /// </param>
        /// <param name="consumedOffset">
        /// The consumed offset value.
        /// </param>
        /// <param name="fetchedOffset">
        /// The fetched offset value.
        /// </param>
        /// <param name="fetchSize">
        /// The fetch size.
        /// </param>
        public PartitionTopicInfo(
            string topic, 
            int brokerId, 
            Partition partition, 
            BlockingCollection<FetchedDataChunk> chunkQueue, 
            long consumedOffset, 
            long fetchedOffset, 
            int fetchSize)
        {
            this.Topic = topic;
            this.Partition = partition;
            this.chunkQueue = chunkQueue;
            this.BrokerId = brokerId;
            this.consumedOffset = consumedOffset;
            this.fetchedOffset = fetchedOffset;
            this.FetchSize = fetchSize;
            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture, "initial consumer offset of {0} is {1}", this, consumedOffset);
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture, "initial fetch offset of {0} is {1}", this, fetchedOffset);
            }
        }

        /// <summary>
        /// Gets broker ID.
        /// </summary>
        public int BrokerId { get; private set; }

        /// <summary>
        /// Gets the fetch size.
        /// </summary>
        public int FetchSize { get; private set; }

        /// <summary>
        /// Gets the partition.
        /// </summary>
        public Partition Partition { get; private set; }

        /// <summary>
        /// Gets the topic.
        /// </summary>
        public string Topic { get; private set; }

        /// <summary>
        /// Records the given number of bytes as having been consumed
        /// </summary>
        /// <param name="messageSize">
        /// The message size.
        /// </param>
        public void Consumed(int messageSize)
        {
            long newOffset;
            lock (this.consumedOffsetLock)
            {
                this.consumedOffset += messageSize;
                newOffset = this.consumedOffset;
            }

            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture, "updated consume offset of {0} to {1}", this, newOffset);
            }
        }

        public int Add(BufferedMessageSet messages, long fetchOffset)
        {
            int size = messages.SetSize;
            if (size > 0)
            {
                long newOffset = Interlocked.Add(ref this.fetchedOffset, size);
                Logger.Debug("Updated fetch offset of " + this + " to " + newOffset);
                this.chunkQueue.Add(new FetchedDataChunk(messages, this, fetchOffset));
            }

            return size;
        }

        public long GetConsumeOffset()
        {
            lock (this.consumedOffsetLock)
            {
                return this.consumedOffset;
            }
        }

        public long GetFetchOffset()
        {
            lock (this.fetchedOffsetLock)
            {
                return this.fetchedOffset;
            }
        }

        public void ResetConsumeOffset(long newConsumeOffset)
        {
            lock (this.consumedOffsetLock)
            {
                this.consumedOffset = newConsumeOffset;
            }

            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture, "reset consume offset of {0} to {1}", this, newConsumeOffset);
            }
        }

        public void ResetFetchOffset(long newFetchOffset)
        {
            lock (this.fetchedOffsetLock)
            {
                this.fetchedOffset = newFetchOffset;
            }

            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat(
                    CultureInfo.CurrentCulture, "reset fetch offset of {0} to {1}", this, newFetchOffset);
            }
        }

        public override string ToString()
        {
            return this.Topic + ":" + this.Partition;
        }
    }
}