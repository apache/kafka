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

using System;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client
{
    /// <summary>
    /// Message for Kafka.
    /// </summary>
    /// <remarks>
    /// A message. The format of an N byte message is the following:
    /// <list type="bullet">
    ///     <item>
    ///         <description>1 byte "magic" identifier to allow format changes</description>
    ///     </item>
    ///     <item>
    ///         <description>4 byte CRC32 of the payload</description>
    ///     </item>
    ///     <item>
    ///         <description>N - 5 byte payload</description>
    ///     </item>
    /// </list>
    /// </remarks>
    public class Message
    {
        /// <summary>
        /// Magic identifier for Kafka.
        /// </summary>
        private static readonly byte DefaultMagicIdentifier = 0;

        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <remarks>
        /// Uses the <see cref="DefaultMagicIdentifier"/> as a default.
        /// </remarks>
        /// <param name="payload">The data for the payload.</param>
        public Message(byte[] payload) : this(payload, DefaultMagicIdentifier)
        {
        }

        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <remarks>
        /// Initializes the checksum as null.  It will be automatically computed.
        /// </remarks>
        /// <param name="payload">The data for the payload.</param>
        /// <param name="magic">The magic identifier.</param>
        public Message(byte[] payload, byte magic) : this(payload, magic, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <param name="payload">The data for the payload.</param>
        /// <param name="magic">The magic identifier.</param>
        /// <param name="checksum">The checksum for the payload.</param>
        public Message(byte[] payload, byte magic, byte[] checksum)
        {
            Payload = payload;
            Magic = magic;
            Checksum = checksum == null ? CalculateChecksum() : checksum;
        }
    
        /// <summary>
        /// Gets the magic bytes.
        /// </summary>
        public byte Magic { get; private set; }
        
        /// <summary>
        /// Gets the CRC32 checksum for the payload.
        /// </summary>
        public byte[] Checksum { get; private set; }

        /// <summary>
        /// Gets the payload.
        /// </summary>
        public byte[] Payload { get; private set; }

        /// <summary>
        /// Parses a message from a byte array given the format Kafka likes. 
        /// </summary>
        /// <param name="data">The data for a message.</param>
        /// <returns>The message.</returns>
        public static Message ParseFrom(byte[] data)
        {
            int size = BitConverter.ToInt32(BitWorks.ReverseBytes(data.Take(4).ToArray<byte>()), 0);
            byte magic = data[4];
            byte[] checksum = data.Skip(5).Take(4).ToArray<byte>();
            byte[] payload = data.Skip(9).Take(size).ToArray<byte>();

            return new Message(payload, magic, checksum);
        }

        /// <summary>
        /// Converts the message to bytes in the format Kafka likes.
        /// </summary>
        /// <returns>The byte array.</returns>
        public byte[] GetBytes()
        {
            byte[] encodedMessage = new byte[Payload.Length + 1 + Checksum.Length];
            encodedMessage[0] = Magic;
            Buffer.BlockCopy(Checksum, 0, encodedMessage, 1, Checksum.Length);
            Buffer.BlockCopy(Payload, 0, encodedMessage, 1 + Checksum.Length, Payload.Length);

            return encodedMessage;
        }

        /// <summary>
        /// Determines if the message is valid given the payload and its checksum.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public bool IsValid()
        {
            return Checksum.SequenceEqual(CalculateChecksum());
        }

        /// <summary>
        /// Try to show the payload as decoded to UTF-8.
        /// </summary>
        /// <returns>The decoded payload as string.</returns>
        public override string ToString()
        {
            return Encoding.UTF8.GetString(Payload);
        }

        /// <summary>
        /// Calculates the CRC32 checksum on the payload of the message.
        /// </summary>
        /// <returns>The checksum given the payload.</returns>
        private byte[] CalculateChecksum()
        { 
            Crc32 crc32 = new Crc32();
            return crc32.ComputeHash(Payload);
        }
    }
}
