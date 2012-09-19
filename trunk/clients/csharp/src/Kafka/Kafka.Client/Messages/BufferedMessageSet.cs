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

using Kafka.Client.Exceptions;

namespace Kafka.Client.Messages
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using Kafka.Client.Consumers;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using log4net;

    /// <summary>
    /// A collection of messages stored as memory stream
    /// </summary>
    public class BufferedMessageSet : MessageSet, IEnumerable<MessageAndOffset>, IEnumerator<MessageAndOffset>
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private MemoryStream topIter;
        private int topIterPosition;
        private long currValidBytes = 0;
        private IEnumerator<MessageAndOffset> innerIter = null;
        private long lastMessageSize = 0;
        private long deepValidByteCount = -1;
        private long shallowValidByteCount = -1;
        private ConsumerIteratorState state = ConsumerIteratorState.NotReady;
        private MessageAndOffset nextItem;

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
        public BufferedMessageSet(IEnumerable<Message> messages)
            : this(messages, ErrorMapping.NoError)
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
            this.ErrorCode = errorCode;
            this.topIterPosition = 0;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BufferedMessageSet"/> class with compression.
        /// </summary>
        /// <param name="compressionCodec">compression method</param>
        /// <param name="messages">messages to add</param>
        public BufferedMessageSet(CompressionCodecs compressionCodec, IEnumerable<Message> messages)
        {
            IEnumerable<Message> messagesToAdd;
            switch (compressionCodec)
            {
                case CompressionCodecs.NoCompressionCodec:
                    messagesToAdd = messages;
                    break;
                default:
                    var message = CompressionUtils.Compress(messages, compressionCodec);
                    messagesToAdd = new List<Message>() { message };
                    break;
            }

            int length = GetMessageSetSize(messagesToAdd);
            this.Messages = messagesToAdd;
            this.ErrorCode = ErrorMapping.NoError;
            this.topIterPosition = 0;
        }

        /// <summary>
        /// Gets the list of messages.
        /// </summary>
        public IEnumerable<Message> Messages { get; private set; }

        /// <summary>
        /// Gets the total set size.
        /// </summary>
        public override int SetSize
        {
            get { return GetMessageSetSize(this.Messages); }
        }

        public MessageAndOffset Current
        {
            get
            {
                if (!MoveNext())
                {
                    throw new NoSuchElementException();
                }

                state = ConsumerIteratorState.NotReady;
                if (nextItem != null)
                {
                    return nextItem;
                }

                throw new IllegalStateException("Expected item but none found.");
            }
        }

        object IEnumerator.Current
        {
            get { return this.Current; }
        }

        /// <summary>
        /// Writes content into given stream
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
        public sealed override void WriteTo(MemoryStream output)
        {
            Guard.NotNull(output, "output");
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
            Guard.NotNull(writer, "writer");
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
            var sb = new StringBuilder();
            int i = 1;
            foreach (var message in this.Messages)
            {
                sb.Append("Message ");
                sb.Append(i);
                sb.Append(" {Length: ");
                sb.Append(message.Size);
                sb.Append(", ");
                sb.Append(message.ToString());
                sb.AppendLine("} ");
                i++;
            }

            return sb.ToString();
        }

        internal static BufferedMessageSet ParseFrom(KafkaBinaryReader reader, int size)
        {
            if (size == 0)
            {
                return new BufferedMessageSet(Enumerable.Empty<Message>());
            }

            short errorCode = reader.ReadInt16();
            if (errorCode != KafkaException.NoError)
            {
                throw new KafkaException(errorCode);
            }

            int readed = 2;
            if (readed == size)
            {
                return new BufferedMessageSet(Enumerable.Empty<Message>());
            }

            var messages = new List<Message>();
            do
            {
                int msgSize = reader.ReadInt32();
                readed += 4;
                Message msg = Message.ParseFrom(reader, msgSize);
                readed += msgSize;
                messages.Add(msg);
            }
            while (readed < size);
            if (size != readed)
            {
                throw new KafkaException(KafkaException.InvalidRetchSizeCode);
            }

            return new BufferedMessageSet(messages);
        }

        internal static IList<BufferedMessageSet> ParseMultiFrom(KafkaBinaryReader reader, int size, int count)
        {
            var result = new List<BufferedMessageSet>();
            if (size == 0)
            {
                return result;
            }

            int readed = 0;
            short errorCode = reader.ReadInt16();
            readed += 2;
            if (errorCode != KafkaException.NoError)
            {
                throw new KafkaException(errorCode);
            }

            for (int i = 0; i < count; i++)
            {
                int partSize = reader.ReadInt32();
                readed += 4;
                var item = ParseFrom(reader, partSize);
                readed += partSize;
                result.Add(item);
            }

            if (size != readed)
            {
                throw new KafkaException(KafkaException.InvalidRetchSizeCode);
            }

            return result;
        }

        [Obsolete]
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

        public IEnumerator<MessageAndOffset> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private bool InnerDone()
        {
            return innerIter == null || !innerIter.MoveNext();
        }

        private MessageAndOffset MakeNextOuter()
        {
            if (topIterPosition >= this.Messages.Count())
            {
                return AllDone();
            }

            Message newMessage = this.Messages.ToList()[topIterPosition];
            topIterPosition++;
            switch (newMessage.CompressionCodec)
            {
                case CompressionCodecs.NoCompressionCodec:
                    if (Logger.IsDebugEnabled)
                    {
                        Logger.DebugFormat(
                            CultureInfo.CurrentCulture,
                            "Message is uncompressed. Valid byte count = {0}",
                            currValidBytes);
                    }

                    innerIter = null;
                    currValidBytes += 4 + newMessage.Size;
                    return new MessageAndOffset(newMessage, currValidBytes);
                default:
                    if (Logger.IsDebugEnabled)
                    {
                        Logger.DebugFormat(CultureInfo.CurrentCulture, "Message is compressed. Valid byte count = {0}", currValidBytes);
                    }

                    innerIter = CompressionUtils.Decompress(newMessage).GetEnumerator();
                    return MakeNext();
            }
        }

        private MessageAndOffset MakeNext()
        {
            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat(CultureInfo.CurrentCulture, "MakeNext() in deepIterator: innerDone = {0}", InnerDone());
            }

            switch (InnerDone())
            {
                case true:
                    return MakeNextOuter();
                default:
                    var messageAndOffset = innerIter.Current;
                    if (!innerIter.MoveNext())
                    {
                        currValidBytes += 4 + lastMessageSize;
                    }

                    return new MessageAndOffset(messageAndOffset.Message, currValidBytes);
            }
        }

        private MessageAndOffset AllDone()
        {
            state = ConsumerIteratorState.Done;
            return null;
        }

        public void Dispose()
        {
        }

        public bool MoveNext()
        {
            if (state == ConsumerIteratorState.Failed)
            {
                throw new IllegalStateException("Iterator is in failed state");
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

        private bool MaybeComputeNext()
        {
            state = ConsumerIteratorState.Failed;
            nextItem = MakeNext();
            if (state == ConsumerIteratorState.Done)
            {
                return false;
            }
            else
            {
                state = ConsumerIteratorState.Ready;
                return true;
            }
        }

        public void Reset()
        {
            this.topIterPosition = 0;
        }
    }
}
