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

namespace Kafka.Client.Requests
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class FetchRequest : AbstractRequest, IWritable
    {
        /// <summary>
        /// Maximum size.
        /// </summary>
        private static readonly int DefaultMaxSize = 1048576;
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultOffsetSize = 8;
        public const byte DefaultMaxSizeSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize + DefaultTopicSizeSize + DefaultPartitionSize + DefaultRequestIdSize + DefaultOffsetSize + DefaultMaxSizeSize;
        public const byte DefaultHeaderAsPartOfMultirequestSize = DefaultTopicSizeSize + DefaultPartitionSize + DefaultOffsetSize + DefaultMaxSizeSize;

        public static int GetRequestLength(string topic, string encoding = DefaultEncoding)
        {
            short topicLength = GetTopicLength(topic, encoding);
            return topicLength + DefaultHeaderSize;
        }

        public static int GetRequestAsPartOfMultirequestLength(string topic, string encoding = DefaultEncoding)
        {
            short topicLength = GetTopicLength(topic, encoding);
            return topicLength + DefaultHeaderAsPartOfMultirequestSize;
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        public FetchRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="offset">The offset in the topic/partition to retrieve from.</param>
        public FetchRequest(string topic, int partition, long offset)
            : this(topic, partition, offset, DefaultMaxSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="offset">The offset in the topic/partition to retrieve from.</param>
        /// <param name="maxSize">The maximum size.</param>
        public FetchRequest(string topic, int partition, long offset, int maxSize)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            MaxSize = maxSize;

            int length = GetRequestLength(topic, DefaultEncoding);
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        /// <summary>
        /// Gets or sets the offset to request.
        /// </summary>
        public long Offset { get; set; }

        /// <summary>
        /// Gets or sets the maximum size to pass in the request.
        /// </summary>
        public int MaxSize { get; set; }

        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.Fetch;
            }
        }

        /// <summary>
        /// Writes content into given stream
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
        public void WriteTo(MemoryStream output)
        {
            Guard.Assert<ArgumentNullException>(() => output != null);

            using (var writer = new KafkaBinaryWriter(output))
            {
                writer.Write(this.RequestBuffer.Capacity - DefaultRequestSizeSize);
                writer.Write(this.RequestTypeId);
                this.WriteTo(writer);
            }
        }

        /// <summary>
        /// Writes content into given writer
        /// </summary>
        /// <param name="writer">
        /// The writer.
        /// </param>
        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.Assert<ArgumentNullException>(() => writer != null);

            writer.WriteTopic(this.Topic, DefaultEncoding);
            writer.Write(this.Partition);
            writer.Write(this.Offset);
            writer.Write(this.MaxSize);
        }

        public override string ToString()
        {
            return String.Format(
                CultureInfo.CurrentCulture,
                "topic: {0}, part: {1}, offset: {2}, maxSize: {3}",
                this.Topic,
                this.Partition,
                this.Offset,
                this.MaxSize);
        }
    }
}
