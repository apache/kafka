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

    /// <summary>
    /// Writes data into underlying stream using big endian bytes order for primitive types
    /// and UTF-8 encoding for strings.
    /// </summary>
    public class KafkaBinaryWriter : BinaryWriter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaBinaryWriter"/> class 
        /// using big endian bytes order for primive types and UTF-8 encoding for strings.
        /// </summary>
        protected KafkaBinaryWriter()
        {  
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaBinaryWriter"/> class 
        /// using big endian bytes order for primive types and UTF-8 encoding for strings.
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
        public KafkaBinaryWriter(Stream output)
            : base(output)
        {
        }
  
        /// <summary>
        /// Flushes data into stream and resets position pointer.
        /// </summary>
        /// <param name="disposing">
        /// Not used
        /// </param>
        protected override void Dispose(bool disposing)
        {
            this.Flush();
            this.OutStream.Position = 0;
        }

        /// <summary>
        /// Writes four-bytes signed integer to the current stream using big endian bytes order 
        /// and advances the stream position by four bytes
        /// </summary>
        /// <param name="value">
        /// The value to write.
        /// </param>
        public override void Write(int value)
        {
            int bigOrdered = IPAddress.HostToNetworkOrder(value);
            base.Write(bigOrdered);
        }

        /// <summary>
        /// Writes eight-bytes signed integer to the current stream using big endian bytes order 
        /// and advances the stream position by eight bytes
        /// </summary>
        /// <param name="value">
        /// The value to write.
        /// </param>
        public override void Write(long value)
        {
            long bigOrdered = IPAddress.HostToNetworkOrder(value);
            base.Write(bigOrdered);
        }

        /// <summary>
        /// Writes two-bytes signed integer to the current stream using big endian bytes order 
        /// and advances the stream position by two bytes
        /// </summary>
        /// <param name="value">
        /// The value to write.
        /// </param>
        public override void Write(short value)
        {
            short bigOrdered = IPAddress.HostToNetworkOrder(value);
            base.Write(bigOrdered);
        }

        /// <summary>
        /// Writes topic and his size into underlying stream using given encoding.
        /// </summary>
        /// <param name="topic">
        /// The topic to write.
        /// </param>
        /// <param name="encoding">
        /// The encoding to use.
        /// </param>
        public void WriteTopic(string topic, string encoding)
        {
            if (string.IsNullOrEmpty(topic))
            {
                short defaultTopic = -1;
                this.Write(defaultTopic);
            }
            else
            {
                var length = (short)topic.Length;
                this.Write(length);
                Encoding encoder = Encoding.GetEncoding(encoding);
                byte[] encodedTopic = encoder.GetBytes(topic);
                this.Write(encodedTopic);
            }
        }
    }
}
