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

namespace Kafka.Client.Tests.Request
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using NUnit.Framework;

    /// <summary>
    /// Tests for the <see cref="FetchRequest"/> class.
    /// </summary>
    [TestFixture]
    public class FetchRequestTests
    {
        /// <summary>
        /// Tests to ensure that the request follows the expected structure.
        /// </summary>
        [Test]
        public void GetBytesValidStructure()
        {
            string topicName = "topic";
            FetchRequest request = new FetchRequest(topicName, 1, 10L, 100);

            // REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
            int requestSize = 2 + 2 + topicName.Length + 4 + 8 + 4;

            MemoryStream ms = new MemoryStream();
            request.WriteTo(ms);
            byte[] bytes = ms.ToArray();
            Assert.IsNotNull(bytes);

            // add 4 bytes for the length of the message at the beginning
            Assert.AreEqual(requestSize + 4, bytes.Length);

            // first 4 bytes = the message length
            Assert.AreEqual(25, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the request type
            Assert.AreEqual((short)RequestTypes.Fetch, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the topic length
            Assert.AreEqual((short)topicName.Length, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));

            // next few bytes = the topic
            Assert.AreEqual(topicName, Encoding.ASCII.GetString(bytes.Skip(8).Take(topicName.Length).ToArray<byte>()));

            // next 4 bytes = the partition
            Assert.AreEqual(1, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(8 + topicName.Length).Take(4).ToArray<byte>()), 0));

            // next 8 bytes = the offset
            Assert.AreEqual(10, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(12 + topicName.Length).Take(8).ToArray<byte>()), 0));

            // last 4 bytes = the max size
            Assert.AreEqual(100, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(20 + +topicName.Length).Take(4).ToArray<byte>()), 0));
        }
    }
}
