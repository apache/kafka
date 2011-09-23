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
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using Kafka.Client.Cfg;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using log4net;

    /// <summary>
    /// The low-level API of consumer of Kafka messages
    /// </summary>
    /// <remarks>
    /// Maintains a connection to a single broker and has a close correspondence 
    /// to the network requests sent to the server.
    /// Also, is completely stateless.
    /// </remarks>
    public class Consumer : IConsumer
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);  

        private readonly ConsumerConfig config;

        /// <summary>
        /// Gets the server to which the connection is to be established.
        /// </summary>
        public string Host { get; private set; }

        /// <summary>
        /// Gets the port to which the connection is to be established.
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="Consumer"/> class.
        /// </summary>
        /// <param name="config">
        /// The consumer configuration.
        /// </param>
        public Consumer(ConsumerConfig config)
        {
            Guard.Assert<ArgumentNullException>(() => config != null);

            this.config = config;
            this.Host = config.Host;
            this.Port = config.Port;
        }

        /// <summary>
        /// Fetch a set of messages from a topic.
        /// </summary>
        /// <param name="request">
        /// Specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
        /// </param>
        /// <returns>
        /// A set of fetched messages.
        /// </returns>
        /// <remarks>
        /// Offset is passed in on every request, allowing the user to maintain this metadata 
        /// however they choose.
        /// </remarks>
        public BufferedMessageSet Fetch(FetchRequest request)
        {
            BufferedMessageSet result = null;
            using (var conn = new KafkaConnection(this.Host, this.Port))
            {
                short tryCounter = 1;
                bool success = false;
                while (!success && tryCounter <= this.config.NumberOfTries)
                {
                    try
                    {
                        result = Fetch(conn, request);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        //// if maximum number of tries reached
                        if (tryCounter == this.config.NumberOfTries)
                        {
                            throw;
                        }

                        tryCounter++;
                        Logger.InfoFormat(CultureInfo.CurrentCulture, "Fetch reconnect due to {0}", ex);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Combine multiple fetch requests in one call.
        /// </summary>
        /// <param name="request">
        /// The list of fetch requests.
        /// </param>
        /// <returns>
        /// A list of sets of fetched messages.
        /// </returns>
        /// <remarks>
        /// Offset is passed in on every request, allowing the user to maintain this metadata 
        /// however they choose.
        /// </remarks>
        public IList<BufferedMessageSet> MultiFetch(MultiFetchRequest request)
        {
            var result = new List<BufferedMessageSet>();
            using (var conn = new KafkaConnection(this.Host, this.Port))
            {
                short tryCounter = 1;
                bool success = false;
                while (!success && tryCounter <= this.config.NumberOfTries)
                {
                    try
                    {
                        MultiFetch(conn, request, result);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        // if maximum number of tries reached
                        if (tryCounter == this.config.NumberOfTries)
                        {
                            throw;
                        }

                        tryCounter++;
                        Logger.InfoFormat(CultureInfo.CurrentCulture, "MultiFetch reconnect due to {0}", ex);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Gets a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="request">
        /// The offset request.
        /// </param>
        /// <returns>
        /// The list of offsets, in descending order.
        /// </returns>
        public IList<long> GetOffsetsBefore(OffsetRequest request)
        {
            var offsets = new List<long>();
            using (var conn = new KafkaConnection(this.Host, this.Port))
            {
                short tryCounter = 1;
                bool success = false;
                while (!success && tryCounter <= this.config.NumberOfTries)
                {
                    try
                    {
                        GetOffsetsBefore(conn, request, offsets);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        // if maximum number of tries reached
                        if (tryCounter == this.config.NumberOfTries)
                        {
                            throw;
                        }

                        tryCounter++;
                        Logger.InfoFormat(CultureInfo.CurrentCulture, "GetOffsetsBefore reconnect due to {0}", ex);
                    }
                }
            }

            return offsets;
        }

        private static BufferedMessageSet Fetch(KafkaConnection conn, FetchRequest request)
        {
            conn.Write(request);
            int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(conn.Read(4)), 0);
            if (dataLength > 0)
            {
                byte[] data = conn.Read(dataLength);

                int errorCode = BitConverter.ToInt16(BitWorks.ReverseBytes(data.Take(2).ToArray()), 0);
                if (errorCode != KafkaException.NoError)
                {
                    throw new KafkaException(errorCode);
                }

                // skip the error code
                byte[] unbufferedData = data.Skip(2).ToArray();
                return BufferedMessageSet.ParseFrom(unbufferedData);
            }

            return null;
        }

        private static void MultiFetch(KafkaConnection conn, MultiFetchRequest request, IList<BufferedMessageSet> result)
        {
            result.Clear();
            conn.Write(request);
            int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(conn.Read(4)), 0);
            if (dataLength <= 0)
            {
                return;
            }

            byte[] data = conn.Read(dataLength);

            int errorCode = BitConverter.ToInt16(BitWorks.ReverseBytes(data.Take(2).ToArray()), 0);
            if (errorCode != KafkaException.NoError)
            {
                throw new KafkaException(errorCode);
            }

            // skip the error code
            byte[] unbufferedData = data.Skip(2).ToArray();
            for (int i = 0; i < request.ConsumerRequests.Count; i++)
            {
                int partLength = BitConverter.ToInt32(BitWorks.ReverseBytes(unbufferedData.Take(4).ToArray()), 0);
                errorCode = BitConverter.ToInt16(BitWorks.ReverseBytes(unbufferedData.Skip(4).Take(2).ToArray()), 0);
                if (errorCode != KafkaException.NoError)
                {
                    throw new KafkaException(errorCode);
                }

                result.Add(BufferedMessageSet.ParseFrom(unbufferedData.Skip(6).Take(partLength - 2).ToArray()));
                unbufferedData = unbufferedData.Skip(partLength + 4).ToArray();
            }
        }

        private static void GetOffsetsBefore(KafkaConnection conn, OffsetRequest request, IList<long> offsets)
        {
            offsets.Clear(); // to make sure the list is clean after some previous attampts to get data
            conn.Write(request);
            int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(conn.Read(4)), 0);

            if (dataLength > 0)
            {
                byte[] data = conn.Read(dataLength);

                int errorCode = BitConverter.ToInt16(BitWorks.ReverseBytes(data.Take(2).ToArray()), 0);
                if (errorCode != KafkaException.NoError)
                {
                    throw new KafkaException(errorCode);
                }

                // skip the error code and process the rest
                byte[] unbufferedData = data.Skip(2).ToArray();

                // first four bytes are the number of offsets
                int numOfOffsets =
                    BitConverter.ToInt32(BitWorks.ReverseBytes(unbufferedData.Take(4).ToArray()), 0);

                for (int ix = 0; ix < numOfOffsets; ix++)
                {
                    int position = (ix * 8) + 4;
                    offsets.Add(
                        BitConverter.ToInt64(
                            BitWorks.ReverseBytes(unbufferedData.Skip(position).Take(8).ToArray()), 0));
                }
            }
        }
    }
}
