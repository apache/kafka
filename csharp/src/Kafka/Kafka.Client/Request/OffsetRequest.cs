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
    public class OffsetRequest : AbstractRequest
    {
        /// <summary>
        /// The latest time constant.
        /// </summary>
        public static readonly long LatestTime = -1L;

        /// <summary>
        /// The earliest time constant.
        /// </summary>
        public static readonly long EarliestTime = -2L;

        /// <summary>
        /// Initializes a new instance of the OffsetRequest class.
        /// </summary>
        public OffsetRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the OffsetRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="time">The time from which to request offsets.</param>
        /// <param name="maxOffsets">The maximum amount of offsets to return.</param>
        public OffsetRequest(string topic, int partition, long time, int maxOffsets)
        {
            Topic = topic;
            Partition = partition;
            Time = time;
            MaxOffsets = maxOffsets;
        }

        /// <summary>
        /// Gets the time.
        /// </summary>
        public long Time { get; private set; }

        /// <summary>
        /// Gets the maximum number of offsets to return.
        /// </summary>
        public int MaxOffsets { get; private set; }

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public override bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(Topic);
        }

        /// <summary>
        /// Converts the request to an array of bytes that is expected by Kafka.
        /// </summary>
        /// <returns>An array of bytes that represents the request.</returns>
        public override byte[] GetBytes()
        {
            byte[] requestBytes = BitWorks.GetBytesReversed(Convert.ToInt16((int)RequestType.Offsets));
            byte[] topicLengthBytes = BitWorks.GetBytesReversed(Convert.ToInt16(Topic.Length));
            byte[] topicBytes = Encoding.UTF8.GetBytes(Topic);
            byte[] partitionBytes = BitWorks.GetBytesReversed(Partition);
            byte[] timeBytes = BitWorks.GetBytesReversed(Time);
            byte[] maxOffsetsBytes = BitWorks.GetBytesReversed(MaxOffsets);

            List<byte> encodedMessageSet = new List<byte>();
            encodedMessageSet.AddRange(requestBytes);
            encodedMessageSet.AddRange(topicLengthBytes);
            encodedMessageSet.AddRange(topicBytes);
            encodedMessageSet.AddRange(partitionBytes);
            encodedMessageSet.AddRange(timeBytes);
            encodedMessageSet.AddRange(maxOffsetsBytes);
            encodedMessageSet.InsertRange(0, BitWorks.GetBytesReversed(encodedMessageSet.Count));

            return encodedMessageSet.ToArray();
        }
    }
}
