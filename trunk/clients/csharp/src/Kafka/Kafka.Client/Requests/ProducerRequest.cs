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
    using System.IO;
    using System.Text;
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class ProducerRequest : AbstractRequest, IWritable
    {
        public const int RandomPartition = -1;
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultSetSizeSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize + DefaultTopicSizeSize + DefaultPartitionSize + DefaultRequestIdSize + DefaultSetSizeSize;

        public static int GetRequestLength(string topic, int messegesSize, string encoding = DefaultEncoding)
        {
            short topicLength = GetTopicLength(topic, encoding);
            return topicLength + DefaultHeaderSize + messegesSize;
        }

        public ProducerRequest(string topic, int partition, BufferedMessageSet messages)
        {
            Guard.NotNull(messages, "messages");

            int length = GetRequestLength(topic, messages.SetSize);
            this.RequestBuffer = new BoundedBuffer(length);
            this.Topic = topic;
            this.Partition = partition;
            this.MessageSet = messages;
            this.WriteTo(this.RequestBuffer);
        }

        /// <summary>
        /// Initializes a new instance of the ProducerRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="messages">The list of messages to send.</param>
        public ProducerRequest(string topic, int partition, IEnumerable<Message> messages)
            : this(topic, partition, new BufferedMessageSet(messages))
        {
        }

        public BufferedMessageSet MessageSet { get; private set; }

        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.Produce;
            }
        }

        public int TotalSize
        {
            get
            {
                return (int)this.RequestBuffer.Length;
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
            writer.Write(this.MessageSet.SetSize);
            this.MessageSet.WriteTo(writer);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Request size: ");
            sb.Append(this.TotalSize);
            sb.Append(", RequestId: ");
            sb.Append(this.RequestTypeId);
            sb.Append("(");
            sb.Append((RequestTypes)this.RequestTypeId);
            sb.Append(")");
            sb.Append(", Topic: ");
            sb.Append(this.Topic);
            sb.Append(", Partition: ");
            sb.Append(this.Partition);
            sb.Append(", Set size: ");
            sb.Append(this.MessageSet.SetSize);
            sb.Append(", Set {");
            sb.Append(this.MessageSet.ToString());
            sb.Append("}");
            return sb.ToString();
        }
    }
}
