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

namespace Kafka.Client.Requests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Constructs a request to send to Kafka to get the current offset for a given topic
    /// </summary>
    public class OffsetRequest : AbstractRequest, IWritable
    {
        /// <summary>
        /// The latest time constant.
        /// </summary>
        public static readonly long LatestTime = -1L;

        /// <summary>
        /// The earliest time constant.
        /// </summary>
        public static readonly long EarliestTime = -2L;

        public const string SmallestTime = "smallest";

        public const string LargestTime = "largest";

        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultTimeSize = 8;
        public const byte DefaultMaxOffsetsSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize + DefaultTopicSizeSize + DefaultPartitionSize + DefaultRequestIdSize + DefaultTimeSize + DefaultMaxOffsetsSize;

        public static int GetRequestLength(string topic, string encoding = DefaultEncoding)
        {
            short topicLength = GetTopicLength(topic, encoding);
            return topicLength + DefaultHeaderSize;
        }

        /// <summary>
        /// Initializes a new instance of the OffsetRequest class.
        /// </summary>
        public OffsetRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the OffsetRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="time">The time from which to request offsets.</param>
        /// <param name="maxOffsets">The maximum amount of offsets to return.</param>
        public OffsetRequest(string topic, int partition, long time, int maxOffsets)
        {
            Topic = topic;
            Partition = partition;
            Time = time;
            MaxOffsets = maxOffsets;

            int length = GetRequestLength(topic, DefaultEncoding);
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        /// <summary>
        /// Gets the time.
        /// </summary>
        public long Time { get; private set; }

        /// <summary>
        /// Gets the maximum number of offsets to return.
        /// </summary>
        public int MaxOffsets { get; private set; }

        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.Offsets;
            }
        }

        /// <summary>
        /// Writes content into given stream
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
        public void WriteTo(System.IO.MemoryStream output)
        {
            Guard.NotNull(output, "output");

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
            Guard.NotNull(writer, "writer");

            writer.WriteTopic(this.Topic, DefaultEncoding);
            writer.Write(this.Partition);
            writer.Write(this.Time);
            writer.Write(this.MaxOffsets);
        }
    }
}
