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

namespace Kafka.Client.Messages
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Text;
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
        private const byte DefaultMagicValue = 0;
        private const byte DefaultMagicLength = 1;
        private const byte DefaultCrcLength = 4;
        private const int DefaultHeaderSize = DefaultMagicLength + DefaultCrcLength;

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
            : this(payload, DefaultMagicValue, checksum)
        {
            Guard.Assert<ArgumentNullException>(() => payload != null);
            Guard.Assert<ArgumentNullException>(() => checksum != null);
            Guard.Assert<ArgumentException>(() => checksum.Length == 4);
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
            : this(payload, DefaultMagicValue)
        {
            Guard.Assert<ArgumentNullException>(() => payload != null);
        }

        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <remarks>
        /// Initializes the checksum as null.  It will be automatically computed.
        /// </remarks>
        /// <param name="payload">The data for the payload.</param>
        /// <param name="magic">The magic identifier.</param>
        public Message(byte[] payload, byte magic)
            : this(payload, magic, Crc32Hasher.Compute(payload))
        {
            Guard.Assert<ArgumentNullException>(() => payload != null);
        }

        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <param name="payload">The data for the payload.</param>
        /// <param name="magic">The magic identifier.</param>
        /// <param name="checksum">The checksum for the payload.</param>
        public Message(byte[] payload, byte magic, byte[] checksum)
        {
            Guard.Assert<ArgumentNullException>(() => payload != null);
            Guard.Assert<ArgumentNullException>(() => checksum != null);

            int length = DefaultHeaderSize + payload.Length;
            this.Payload = payload;
            this.Magic = magic;
            this.Checksum = checksum;
            this.MessageBuffer = new BoundedBuffer(length);
            this.WriteTo(this.MessageBuffer);
        }

        /// <summary>
        /// Gets internal message buffer.
        /// </summary>
        public MemoryStream MessageBuffer { get; private set; }

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
        /// Gets the total size of message.
        /// </summary>
        public int Size
        {
            get
            {
                return (int)this.MessageBuffer.Length;
            }
        }

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
            Guard.Assert<ArgumentNullException>(() => output != null);

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
            Guard.Assert<ArgumentNullException>(() => writer != null);

            writer.Write(this.Magic);
            writer.Write(this.Checksum);
            writer.Write(this.Payload);
        }

        /// <summary>
        /// Try to show the payload as decoded to UTF-8.
        /// </summary>
        /// <returns>The decoded payload as string.</returns>
        public override string ToString()
        {
            using (var reader = new KafkaBinaryReader(this.MessageBuffer))
            {
                return ParseFrom(reader, this.Size);
            }
        }

        /// <summary>
        /// Creates string representation of message
        /// </summary>
        /// <param name="reader">
        /// The reader.
        /// </param>
        /// <param name="count">
        /// The count.
        /// </param>
        /// <returns>
        /// String representation of message
        /// </returns>
        public static string ParseFrom(KafkaBinaryReader reader, int count)
        {
            Guard.Assert<ArgumentNullException>(() => reader != null);
            var sb = new StringBuilder();
            int payloadSize = count - DefaultHeaderSize;
            sb.Append("Magic: ");
            sb.Append(reader.ReadByte());
            sb.Append(", Checksum: ");
            for (int i = 0; i < 4; i++)
            {
                sb.Append("[");
                sb.Append(reader.ReadByte());
                sb.Append("]");
            }

            sb.Append(", topic: ");
            var encodedPayload = reader.ReadBytes(payloadSize);
            try
            {
                sb.Append(Encoding.UTF8.GetString(encodedPayload));
            }
            catch (Exception)
            {
                sb.Append("n/a");
            }

            return sb.ToString();
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
            byte[] checksum = data.Skip(5).Take(4).ToArray();
            byte[] payload = data.Skip(9).Take(size).ToArray();

            return new Message(payload, magic, checksum);
        }
    }
}
