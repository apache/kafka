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

namespace Kafka.Client.Serialization
{
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Net.Sockets;

    /// <summary>
    /// Reads data from underlying stream using big endian bytes order for primitive types
    /// and UTF-8 encoding for strings.
    /// </summary>
    public class KafkaBinaryReader : BinaryReader
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaBinaryReader"/> class
        /// using big endian bytes order for primive types and UTF-8 encoding for strings.
        /// </summary>
        /// <param name="input">
        /// The input stream.
        /// </param>
        public KafkaBinaryReader(Stream input)
            : base(input)
        { 
        }

        /// <summary>
        /// Resets position pointer.
        /// </summary>
        /// <param name="disposing">
        /// Not used
        /// </param>
        protected override void Dispose(bool disposing)
        {
            if (this.BaseStream.CanSeek)
            {
                this.BaseStream.Position = 0;
            }
        }

        /// <summary>
        /// Reads two-bytes signed integer from the current stream using big endian bytes order 
        /// and advances the stream position by two bytes
        /// </summary>
        /// <returns>
        /// The two-byte signed integer read from the current stream.
        /// </returns>
        public override short ReadInt16()
        {
            short value = base.ReadInt16();
            short currentOrdered = IPAddress.NetworkToHostOrder(value);
            return currentOrdered;
        }

        /// <summary>
        /// Reads four-bytes signed integer from the current stream using big endian bytes order 
        /// and advances the stream position by four bytes
        /// </summary>
        /// <returns>
        /// The four-byte signed integer read from the current stream.
        /// </returns>
        public override int ReadInt32()
        {
            int value = base.ReadInt32();
            int currentOrdered = IPAddress.NetworkToHostOrder(value);
            return currentOrdered;
        }

        /// <summary>
        /// Reads eight-bytes signed integer from the current stream using big endian bytes order 
        /// and advances the stream position by eight bytes
        /// </summary>
        /// <returns>
        /// The eight-byte signed integer read from the current stream.
        /// </returns>
        public override long ReadInt64()
        {
            long value = base.ReadInt64();
            long currentOrdered = IPAddress.NetworkToHostOrder(value);
            return currentOrdered;
        }

        /// <summary>
        /// Reads four-bytes signed integer from the current stream using big endian bytes order 
        /// and advances the stream position by four bytes
        /// </summary>
        /// <returns>
        /// The four-byte signed integer read from the current stream.
        /// </returns>
        public override int Read()
        {
            int value = base.Read();
            int currentOrdered = IPAddress.NetworkToHostOrder(value);
            return currentOrdered;
        }

        /// <summary>
        /// Reads fixed-length topic from underlying stream using given encoding.
        /// </summary>
        /// <param name="encoding">
        /// The encoding to use.
        /// </param>
        /// <returns>
        /// The read topic.
        /// </returns>
        public string ReadTopic(string encoding)
        {
            short length = this.ReadInt16();
            if (length == -1)
            {
                return null;
            }

            var bytes = this.ReadBytes(length);
            Encoding encoder = Encoding.GetEncoding(encoding);
            return encoder.GetString(bytes);
        }

        public bool DataAvailabe
        {
            get
            {
                if (this.BaseStream is NetworkStream)
                {
                    return ((NetworkStream)this.BaseStream).DataAvailable;
                }

                return this.BaseStream.Length != this.BaseStream.Position;
            }
        }
    }
}
