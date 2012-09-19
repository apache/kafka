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
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// A set of messages. A message set has a fixed serialized form, though the container
    /// for the bytes could be either in-memory or on disk.
    /// </summary>
    /// <remarks>
    /// Format:
    /// 4 byte size containing an integer N
    /// N message bytes as described in the message class
    /// </remarks>
    public abstract class MessageSet : IWritable
    {
        protected const byte DefaultMessageLengthSize = 4;

        /// <summary>
        /// Gives the size of a size-delimited entry in a message set
        /// </summary>
        /// <param name="message">
        /// The message.
        /// </param>
        /// <returns>
        /// Size of message
        /// </returns>
        public static int GetEntrySize(Message message)
        {
            Guard.NotNull(message, "message");

            return message.Size + DefaultMessageLengthSize;
        }

        /// <summary>
        /// Gives the size of a list of messages
        /// </summary>
        /// <param name="messages">
        /// The messages.
        /// </param>
        /// <returns>
        /// Size of all messages
        /// </returns>
        public static int GetMessageSetSize(IEnumerable<Message> messages)
        {
            return messages == null ? 0 : messages.Sum(x => GetEntrySize(x));
        }

        /// <summary>
        /// Gets the total size of this message set in bytes
        /// </summary>
        public abstract int SetSize { get; }

        /// <summary>
        /// Writes content into given stream
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
        public abstract void WriteTo(MemoryStream output);

        /// <summary>
        /// Writes content into given writer
        /// </summary>
        /// <param name="writer">
        /// The writer.
        /// </param>
        public abstract void WriteTo(KafkaBinaryWriter writer);
    }
}
