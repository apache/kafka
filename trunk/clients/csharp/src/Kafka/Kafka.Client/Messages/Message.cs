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
    using System.IO;
    using System.Linq;
    using System.Text;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Message send to Kafaka server
    /// </summary>
    /// <remarks>
    /// Format:
    /// 1 byte "magic" identifier to allow format changes
    /// 4 byte CRC32 of the payload
    /// N - 5 byte payload
    /// </remarks>
    public class Message : IWritable
    {
        private const byte DefaultMagicValue = 1;
        private const byte DefaultMagicLength = 1;
        private const byte DefaultCrcLength = 4;
        private const int DefaultHeaderSize = DefaultMagicLength + DefaultCrcLength;
        private const byte CompressionCodeMask = 3;

        public CompressionCodecs CompressionCodec
        {
            get
            {
                switch (Magic)
                {
                    case 0:
                        return CompressionCodecs.NoCompressionCodec;
                    case 1:
                        return Messages.CompressionCodec.GetCompressionCodec(Attributes & CompressionCodeMask);
                    default:
                        throw new KafkaException(KafkaException.InvalidMessageCode);
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Message"/> class.
        /// </summary>
        /// <param name="payload">
        /// The payload.
        /// </param>
        /// <param name="checksum">
        /// The checksum.
        /// </param>
        /// <remarks>
        /// Initializes with default magic number
        /// </remarks>
        public Message(byte[] payload, byte[] checksum)
            : this(payload, checksum, CompressionCodecs.NoCompressionCodec)
        {
            Guard.NotNull(payload, "payload");
            Guard.NotNull(checksum, "checksum");
            Guard.Count(checksum, 4, "checksum");
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Message"/> class.
        /// </summary>
        /// <param name="payload">
        /// The payload.
        /// </param>
        /// <remarks>
        /// Initializes the magic number as default and the checksum as null. It will be automatically computed.
        /// </remarks>
        public Message(byte[] payload)
            : this(payload, CompressionCodecs.NoCompressionCodec)
        {
            Guard.NotNull(payload, "payload");
        }

        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <remarks>
        /// Initializes the checksum as null.  It will be automatically computed.
        /// </remarks>
        /// <param name="payload">The data for the payload.</param>
        /// <param name="magic">The magic identifier.</param>
        public Message(byte[] payload, CompressionCodecs compressionCodec)
            : this(payload, Crc32Hasher.Compute(payload), compressionCodec)
        {
            Guard.NotNull(payload, "payload");
        }

        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <param name="payload">The data for the payload.</param>
        /// <param name="magic">The magic identifier.</param>
        /// <param name="checksum">The checksum for the payload.</param>
        public Message(byte[] payload, byte[] checksum, CompressionCodecs compressionCodec)
        {
            Guard.NotNull(payload, "payload");
            Guard.NotNull(checksum, "checksum");
            Guard.Count(checksum, 4, "checksum");

            int length = DefaultHeaderSize + payload.Length;
            this.Payload = payload;
            this.Magic = DefaultMagicValue;
            
            if (compressionCodec != CompressionCodecs.NoCompressionCodec)
            {
                this.Attributes |=
                    (byte)(CompressionCodeMask & Messages.CompressionCodec.GetCompressionCodecValue(compressionCodec));
            }

            if (Magic == 1)
            {
                length++;
            }

            this.Checksum = checksum;
            this.Size = length;
        }

        /// <summary>
        /// Gets the payload.
        /// </summary>
        public byte[] Payload { get; private set; }

        /// <summary>
        /// Gets the magic bytes.
        /// </summary>
        public byte Magic { get; private set; }

        /// <summary>
        /// Gets the CRC32 checksum for the payload.
        /// </summary>
        public byte[] Checksum { get; private set; }

        /// <summary>
        /// Gets the Attributes for the message.
        /// </summary>
        public byte Attributes { get; private set; }

        /// <summary>
        /// Gets the total size of message.
        /// </summary>
        public int Size { get; private set; }

        /// <summary>
        /// Gets the payload size.
        /// </summary>
        public int PayloadSize
        {
            get
            {
                return this.Payload.Length;
            }
        }

        /// <summary>
        /// Writes message data into given message buffer
        /// </summary>
        /// <param name="output">
        /// The output.
        /// </param>
        public void WriteTo(MemoryStream output)
        {
            Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                this.WriteTo(writer);
            }
        }

        /// <summary>
        /// Writes message data using given writer
        /// </summary>
        /// <param name="writer">
        /// The writer.
        /// </param>
        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");
            writer.Write(this.Magic);
            writer.Write(this.Attributes);
            writer.Write(this.Checksum);
            writer.Write(this.Payload);
        }

        /// <summary>
        /// Try to show the payload as decoded to UTF-8.
        /// </summary>
        /// <returns>The decoded payload as string.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Magic: ");
            sb.Append(this.Magic);
            if (this.Magic == 1)
            {
                sb.Append(", Attributes: ");
                sb.Append(this.Attributes);
            }

            sb.Append(", Checksum: ");
            for (int i = 0; i < 4; i++)
            {
                sb.Append("[");
                sb.Append(this.Checksum[i]);
                sb.Append("]");
            }

            sb.Append(", topic: ");
            try
            {
                sb.Append(Encoding.UTF8.GetString(this.Payload));
            }
            catch (Exception)
            {
                sb.Append("n/a");
            }

            return sb.ToString();
        }

        [Obsolete("Use KafkaBinaryReader instead")]
        public static Message FromMessageBytes(byte[] data)
        {
            byte magic = data[0];
            byte[] checksum;
            byte[] payload;
            byte attributes;
            if (magic == (byte)1)
            {
                attributes = data[1];
                checksum = data.Skip(2).Take(4).ToArray();
                payload = data.Skip(6).ToArray();
                return new Message(payload, checksum, Messages.CompressionCodec.GetCompressionCodec(attributes & CompressionCodeMask));
            }
            else
            {
                checksum = data.Skip(1).Take(4).ToArray();
                payload = data.Skip(5).ToArray();
                return new Message(payload, checksum);
            }
        }

        internal static Message ParseFrom(KafkaBinaryReader reader, int size)
        {
            Message result;
            int readed = 0;
            byte magic = reader.ReadByte();
            readed++;
            byte[] checksum;
            byte[] payload;
            if (magic == 1)
            {
                byte attributes = reader.ReadByte();
                readed++;
                checksum = reader.ReadBytes(4);
                readed += 4;
                payload = reader.ReadBytes(size - (DefaultHeaderSize + 1));
                readed += size - (DefaultHeaderSize + 1);
                result = new Message(payload, checksum, Messages.CompressionCodec.GetCompressionCodec(attributes & CompressionCodeMask));
            }
            else
            {
                checksum = reader.ReadBytes(4);
                readed += 4;
                payload = reader.ReadBytes(size - DefaultHeaderSize);
                readed += size - DefaultHeaderSize;
                result = new Message(payload, checksum);
            }

            if (size != readed)
            {
                throw new KafkaException(KafkaException.InvalidRetchSizeCode);
            }

            return result;
        }

        /// <summary>
        /// Parses a message from a byte array given the format Kafka likes. 
        /// </summary>
        /// <param name="data">The data for a message.</param>
        /// <returns>The message.</returns>
        [Obsolete("Use KafkaBinaryReader instead")]
        public static Message ParseFrom(byte[] data)
        {
            int size = BitConverter.ToInt32(BitWorks.ReverseBytes(data.Take(4).ToArray()), 0);
            byte magic = data[4];
            byte[] checksum;
            byte[] payload;
            byte attributes;
            if (magic == 1)
            {
                attributes = data[5];
                checksum = data.Skip(6).Take(4).ToArray();
                payload = data.Skip(10).Take(size).ToArray();
                return new Message(payload, checksum, Messages.CompressionCodec.GetCompressionCodec(attributes & CompressionCodeMask));
            }
            else
            {
                checksum = data.Skip(5).Take(4).ToArray();
                payload = data.Skip(9).Take(size).ToArray();
                return new Message(payload, checksum);
            }
        }
    }
}
