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

namespace Kafka.Client.Consumers
{
    using System;
    using Kafka.Client.Messages;

    internal class FetchedDataChunk : IEquatable<FetchedDataChunk>
    {
        public BufferedMessageSet Messages { get; set; }

        public PartitionTopicInfo TopicInfo { get; set; }

        public long FetchOffset { get; set; }

        public FetchedDataChunk(BufferedMessageSet messages, PartitionTopicInfo topicInfo, long fetchOffset)
        {
            this.Messages = messages;
            this.TopicInfo = topicInfo;
            this.FetchOffset = fetchOffset;
        }

        public override bool Equals(object obj)
        {
            FetchedDataChunk other = obj as FetchedDataChunk;
            if (other == null)
            {
                return false;
            }
            else
            {
                return this.Equals(other);
            }
        }

        public bool Equals(FetchedDataChunk other)
        {
            return this.Messages == other.Messages &&
                    this.TopicInfo == other.TopicInfo &&
                    this.FetchOffset == other.FetchOffset;
        }
    }
}
