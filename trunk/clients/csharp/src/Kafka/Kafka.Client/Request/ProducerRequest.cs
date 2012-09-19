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
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client.Request
{
    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class ProducerRequest : AbstractRequest
    {
        /// <summary>
        /// Initializes a new instance of the ProducerRequest class.
        /// </summary>
        public ProducerRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the ProducerRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="messages">The list of messages to send.</param>
        public ProducerRequest(string topic, int partition, IList<Message> messages)
        {
            Topic = topic;
            Partition = partition;
            Messages = messages;
        }

        /// <summary>
        /// Gets or sets the messages to publish.
        /// </summary>
        public IList<Message> Messages { get; set; }

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public override bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(Topic) && Messages != null && Messages.Count > 0;
        }

        /// <summary>
        /// Gets the bytes matching the expected Kafka structure. 
        /// </summary>
        /// <returns>The byte array of the request.</returns>
        public override byte[] GetBytes()
        {
            List<byte> encodedMessageSet = new List<byte>();
            encodedMessageSet.AddRange(GetInternalBytes());

            byte[] requestBytes = BitWorks.GetBytesReversed(Convert.ToInt16((int)RequestType.Produce));
            encodedMessageSet.InsertRange(0, requestBytes);
            encodedMessageSet.InsertRange(0, BitWorks.GetBytesReversed(encodedMessageSet.Count));

            return encodedMessageSet.ToArray();
        }

        /// <summary>
        /// Gets the bytes representing the request which is used when generating a multi-request.
        /// </summary>
        /// <remarks>
        /// The <see cref="GetBytes"/> method is used for sending a single <see cref="RequestType.Produce"/>.
        /// It prefixes this byte array with the request type and the number of messages. This method
        /// is used to supply the <see cref="MultiProducerRequest"/> with the contents for its message.
        /// </remarks>
        /// <returns>The bytes that represent this <see cref="ProducerRequest"/>.</returns>
        internal byte[] GetInternalBytes()
        {
            List<byte> messagePack = new List<byte>();
            foreach (Message message in Messages)
            {
                byte[] messageBytes = message.GetBytes();
                messagePack.AddRange(BitWorks.GetBytesReversed(messageBytes.Length));
                messagePack.AddRange(messageBytes);
            }

            byte[] topicLengthBytes = BitWorks.GetBytesReversed(Convert.ToInt16(Topic.Length));
            byte[] topicBytes = Encoding.UTF8.GetBytes(Topic);
            byte[] partitionBytes = BitWorks.GetBytesReversed(Partition);
            byte[] messagePackLengthBytes = BitWorks.GetBytesReversed(messagePack.Count);
            byte[] messagePackBytes = messagePack.ToArray();

            List<byte> encodedMessageSet = new List<byte>();
            encodedMessageSet.AddRange(topicLengthBytes);
            encodedMessageSet.AddRange(topicBytes);
            encodedMessageSet.AddRange(partitionBytes);
            encodedMessageSet.AddRange(messagePackLengthBytes);
            encodedMessageSet.AddRange(messagePackBytes);

            return encodedMessageSet.ToArray();
        }
    }
}
