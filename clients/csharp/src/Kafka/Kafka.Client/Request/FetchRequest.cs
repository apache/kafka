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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class FetchRequest : AbstractRequest
    {
        /// <summary>
        /// Maximum size.
        /// </summary>
        private static readonly int DefaultMaxSize = 1048576;

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        public FetchRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="offset">The offset in the topic/partition to retrieve from.</param>
        public FetchRequest(string topic, int partition, long offset)
            : this(topic, partition, offset, DefaultMaxSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the FetchRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="offset">The offset in the topic/partition to retrieve from.</param>
        /// <param name="maxSize">The maximum size.</param>
        public FetchRequest(string topic, int partition, long offset, int maxSize)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            MaxSize = maxSize;
        }

        /// <summary>
        /// Gets or sets the offset to request.
        /// </summary>
        public long Offset { get; set; }

        /// <summary>
        /// Gets or sets the maximum size to pass in the request.
        /// </summary>
        public int MaxSize { get; set; }

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public override bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(Topic);
        }

        /// <summary>
        /// Gets the bytes matching the expected Kafka structure. 
        /// </summary>
        /// <returns>The byte array of the request.</returns>
        public override byte[] GetBytes()
        {
            byte[] internalBytes = GetInternalBytes();

            List<byte> request = new List<byte>();

            // add the 2 for the RequestType.Fetch
            request.AddRange(BitWorks.GetBytesReversed(internalBytes.Length + 2));
            request.AddRange(BitWorks.GetBytesReversed((short)RequestType.Fetch));
            request.AddRange(internalBytes);

            return request.ToArray<byte>();
        }

        /// <summary>
        /// Gets the bytes representing the request which is used when generating a multi-request.
        /// </summary>
        /// <remarks>
        /// The <see cref="GetBytes"/> method is used for sending a single <see cref="RequestType.Fetch"/>.
        /// It prefixes this byte array with the request type and the number of messages. This method
        /// is used to supply the <see cref="MultiFetchRequest"/> with the contents for its message.
        /// </remarks>
        /// <returns>The bytes that represent this <see cref="FetchRequest"/>.</returns>
        internal byte[] GetInternalBytes()
        {
            // TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
            int requestSize = 2 + Topic.Length + 4 + 8 + 4;

            List<byte> request = new List<byte>();
            request.AddRange(BitWorks.GetBytesReversed((short)Topic.Length));
            request.AddRange(Encoding.ASCII.GetBytes(Topic));
            request.AddRange(BitWorks.GetBytesReversed(Partition));
            request.AddRange(BitWorks.GetBytesReversed(Offset));
            request.AddRange(BitWorks.GetBytesReversed(MaxSize));

            return request.ToArray<byte>();
        }
    }
}
