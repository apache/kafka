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

namespace Kafka.Client.Messages
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// A collection of messages stored as memory stream
    /// </summary>
    public class BufferedMessageSet : MessageSet
    {
        /// <summary>
        /// Gets the error code
        /// </summary>
        public int ErrorCode { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="BufferedMessageSet"/> class.
        /// </summary>
        /// <param name="messages">
        /// The list of messages.
        /// </param>
        public BufferedMessageSet(IEnumerable<Message> messages) : this(messages, ErrorMapping.NoError)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BufferedMessageSet"/> class.
        /// </summary>
        /// <param name="messages">
        /// The list of messages.
        /// </param>
        /// <param name="errorCode">
        /// The error code.
        /// </param>
        public BufferedMessageSet(IEnumerable<Message> messages, int errorCode)
        {
            int length = GetMessageSetSize(messages);
            this.Messages = messages;
            this.SetBuffer = new BoundedBuffer(length);
            this.WriteTo(this.SetBuffer);
            this.ErrorCode = errorCode;
        }

        /// <summary>
        /// Gets set internal buffer
        /// </summary>
        public MemoryStream SetBuffer { get; private set; }

        /// <summary>
        /// Gets the list of messages.
        /// </summary>
        public IEnumerable<Message> Messages { get; private set; }

        /// <summary>
        /// Gets the total set size.
        /// </summary>
        public override int SetSize
        {
            get 
            {
                return (int)this.SetBuffer.Length;
            }
        }

        /// <summary>
        /// Writes content into given stream
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
        public sealed override void WriteTo(MemoryStream output)
        {
            Guard.Assert<ArgumentNullException>(() => output != null);
            using (var writer = new KafkaBinaryWriter(output))
            {
                this.WriteTo(writer);
            }
        }

        /// <summary>
        /// Writes content into given writer
        /// </summary>
        /// <param name="writer">
        /// The writer.
        /// </param>
        public sealed override void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.Assert<ArgumentNullException>(() => writer != null);
            foreach (var message in this.Messages)
            {
                writer.Write(message.Size);
                message.WriteTo(writer);
            }
        }

        /// <summary>
        /// Gets string representation of set
        /// </summary>
        /// <returns>
        /// String representation of set
        /// </returns>
        public override string ToString()
        {
            using (var reader = new KafkaBinaryReader(this.SetBuffer))
            {
                return ParseFrom(reader, this.SetSize);
            }
        }

        /// <summary>
        /// Helper method to get string representation of set
        /// </summary>
        /// <param name="reader">
        /// The reader.
        /// </param>
        /// <param name="count">
        /// The count.
        /// </param>
        /// <returns>
        /// String representation of set
        /// </returns>
        internal static string ParseFrom(KafkaBinaryReader reader, int count)
        {
            Guard.Assert<ArgumentNullException>(() => reader != null);
            var sb = new StringBuilder();
            int i = 1;
            while (reader.BaseStream.Position != reader.BaseStream.Length)
            {
                sb.Append("Message ");
                sb.Append(i);
                sb.Append(" {Length: ");
                int msgSize = reader.ReadInt32();
                sb.Append(msgSize);
                sb.Append(", ");
                sb.Append(Message.ParseFrom(reader, msgSize));
                sb.AppendLine("} ");
                i++;
            }

            return sb.ToString();
        }

        internal static BufferedMessageSet ParseFrom(byte[] bytes)
        {
            var messages = new List<Message>();
            int processed = 0;
            int length = bytes.Length - 4;
            while (processed <= length)
            {
                int messageSize = BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(processed).Take(4).ToArray()), 0);
                messages.Add(Message.ParseFrom(bytes.Skip(processed).Take(messageSize + 4).ToArray()));
                processed += 4 + messageSize;
            }

            return new BufferedMessageSet(messages);
        }
    }
}
