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
using Kafka.Client.Request;
using Kafka.Client.Util;

namespace Kafka.Client
{
    /// <summary>
    /// Consumes messages from Kafka.
    /// </summary>
    public class Consumer
    {
        /// <summary>
        /// Maximum size.
        /// </summary>
        private static readonly int MaxSize = 1048576;

        /// <summary>
        /// Initializes a new instance of the Consumer class.
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        public Consumer(string server, int port)
        {
            Server = server;
            Port = port;
        }

        /// <summary>
        /// Gets the server to which the connection is to be established.
        /// </summary>
        public string Server { get; private set; }

        /// <summary>
        /// Gets the port to which the connection is to be established.
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// Consumes messages from Kafka.
        /// </summary>
        /// <param name="topic">The topic to consume from.</param>
        /// <param name="partition">The partition to consume from.</param>
        /// <param name="offset">The offset to start at.</param>
        /// <returns>A list of messages from Kafka.</returns>
        public List<Message> Consume(string topic, int partition, long offset)
        {
            return Consume(topic, partition, offset, MaxSize);
        }

        /// <summary>
        /// Consumes messages from Kafka.
        /// </summary>
        /// <param name="topic">The topic to consume from.</param>
        /// <param name="partition">The partition to consume from.</param>
        /// <param name="offset">The offset to start at.</param>
        /// <param name="maxSize">The maximum size.</param>
        /// <returns>A list of messages from Kafka.</returns>
        public List<Message> Consume(string topic, int partition, long offset, int maxSize)
        {
            return Consume(new FetchRequest(topic, partition, offset, maxSize));
        }

        /// <summary>
        /// Consumes messages from Kafka.
        /// </summary>
        /// <param name="request">The request to send to Kafka.</param>
        /// <returns>A list of messages from Kafka.</returns>
        public List<Message> Consume(FetchRequest request)
        {
            List<Message> messages = new List<Message>();
            using (KafkaConnection connection = new KafkaConnection(Server, Port))
            {
                connection.Write(request.GetBytes());
                int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(connection.Read(4)), 0);

                if (dataLength > 0) 
                {
                    byte[] data = connection.Read(dataLength);

                    int errorCode = BitConverter.ToInt16(BitWorks.ReverseBytes(data.Take(2).ToArray<byte>()), 0);
                    if (errorCode != KafkaException.NoError)
                    {
                        throw new KafkaException(errorCode);
                    }

                    // skip the error code and process the rest
                    byte[] unbufferedData = data.Skip(2).ToArray();

                    int processed = 0;
                    int length = unbufferedData.Length - 4;
                    int messageSize = 0;
                    while (processed <= length) 
                    {
                        messageSize = BitConverter.ToInt32(BitWorks.ReverseBytes(unbufferedData.Skip(processed).Take(4).ToArray<byte>()), 0);
                        messages.Add(Message.ParseFrom(unbufferedData.Skip(processed).Take(messageSize + 4).ToArray<byte>()));
                        processed += 4 + messageSize;
                    }
                }
            }

            return messages;
        }

        /// <summary>
        /// Executes a multi-fetch operation.
        /// </summary>
        /// <param name="request">The request to push to Kafka.</param>
        /// <returns>
        /// A list containing sets of messages. The message sets should match the request order.
        /// </returns>
        public List<List<Message>> Consume(MultiFetchRequest request)
        {
            int fetchRequests = request.ConsumerRequests.Count;

            List<List<Message>> messages = new List<List<Message>>();
            using (KafkaConnection connection = new KafkaConnection(Server, Port))
            {
                connection.Write(request.GetBytes());
                int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(connection.Read(4)), 0);

                if (dataLength > 0)
                {
                    byte[] data = connection.Read(dataLength);

                    int position = 0;

                    int errorCode = BitConverter.ToInt16(BitWorks.ReverseBytes(data.Take(2).ToArray<byte>()), 0);
                    if (errorCode != KafkaException.NoError)
                    {
                        throw new KafkaException(errorCode);
                    }

                    // skip the error code and process the rest
                    position = position + 2;

                    for (int ix = 0; ix < fetchRequests; ix++)
                    {
                        messages.Add(new List<Message>()); 

                        int messageSetSize = BitConverter.ToInt32(BitWorks.ReverseBytes(data.Skip(position).Take(4).ToArray<byte>()), 0);
                        position = position + 4;

                        errorCode = BitConverter.ToInt16(BitWorks.ReverseBytes(data.Skip(position).Take(2).ToArray<byte>()), 0);
                        if (errorCode != KafkaException.NoError)
                        {
                            throw new KafkaException(errorCode);
                        }

                        // skip the error code and process the rest
                        position = position + 2;

                        byte[] messageSetBytes = data.Skip(position).ToArray<byte>().Take(messageSetSize).ToArray<byte>();

                        int processed = 0;
                        int messageSize = 0;

                        // dropped 2 bytes at the end...padding???
                        while (processed < messageSetBytes.Length - 2)
                        {
                            messageSize = BitConverter.ToInt32(BitWorks.ReverseBytes(messageSetBytes.Skip(processed).Take(4).ToArray<byte>()), 0);
                            messages[ix].Add(Message.ParseFrom(messageSetBytes.Skip(processed).Take(messageSize + 4).ToArray<byte>()));
                            processed += 4 + messageSize;
                        }

                        position = position + processed;
                    }
                }
            }

            return messages;
        }

        /// <summary>
        /// Get a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="topic">The topic to check.</param>
        /// <param name="partition">The partition on the topic.</param>
        /// <param name="time">time in millisecs (if -1, just get from the latest available)</param>
        /// <param name="maxNumOffsets">That maximum number of offsets to return.</param>
        /// <returns>List of offsets, in descending order.</returns>
        public IList<long> GetOffsetsBefore(string topic, int partition, long time, int maxNumOffsets)
        {
            return GetOffsetsBefore(new OffsetRequest(topic, partition, time, maxNumOffsets));
        }

        /// <summary>
        /// Get a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="request">The offset request.</param>
        /// <returns>List of offsets, in descending order.</returns>
        public IList<long> GetOffsetsBefore(OffsetRequest request)
        {
            List<long> offsets = new List<long>();

            using (KafkaConnection connection = new KafkaConnection(Server, Port))
            {
                connection.Write(request.GetBytes());

                int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(connection.Read(4)), 0);
                
                if (dataLength > 0)
                {
                    byte[] data = connection.Read(dataLength);

                    int errorCode = BitConverter.ToInt16(BitWorks.ReverseBytes(data.Take(2).ToArray<byte>()), 0);
                    if (errorCode != KafkaException.NoError)
                    {
                        throw new KafkaException(errorCode);
                    }

                    // skip the error code and process the rest
                    byte[] unbufferedData = data.Skip(2).ToArray();

                    // first four bytes are the number of offsets
                    int numOfOffsets = BitConverter.ToInt32(BitWorks.ReverseBytes(unbufferedData.Take(4).ToArray<byte>()), 0);

                    int position = 0;
                    for (int ix = 0; ix < numOfOffsets; ix++)
                    {
                        position = (ix * 8) + 4;
                        offsets.Add(BitConverter.ToInt64(BitWorks.ReverseBytes(unbufferedData.Skip(position).Take(8).ToArray<byte>()), 0));
                    }
                }
            }

            return offsets;
        }
    }
}
