/*
 * Copyright 2011 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
    /// Tests the <see cref="OffsetRequest"/> class.
    /// </summary>
    [TestFixture]
    public class OffsetRequestTests
    {
        /// <summary>
        /// Validates the list of bytes meet Kafka expectations.
        /// </summary>
        [Test]
        public void GetBytesValid()
        {
            string topicName = "topic";
            OffsetRequest request = new OffsetRequest(topicName, 0, OffsetRequest.LatestTime, 10);

            // format = len(request) + requesttype + len(topic) + topic + partition + time + max
            // total byte count = 4 + (2 + 2 + 5 + 4 + 8 + 4)
            MemoryStream ms = new MemoryStream();
            request.WriteTo(ms);
            byte[] bytes = ms.ToArray();
            Assert.IsNotNull(bytes);
            Assert.AreEqual(29, bytes.Length);

            // first 4 bytes = the length of the request
            Assert.AreEqual(25, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the RequestType which in this case should be Produce
            Assert.AreEqual((short)RequestTypes.Offsets, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the length of the topic
            Assert.AreEqual((short)5, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));

            // next 5 bytes = the topic
            Assert.AreEqual(topicName, Encoding.ASCII.GetString(bytes.Skip(8).Take(5).ToArray<byte>()));

            // next 4 bytes = the partition
            Assert.AreEqual(0, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(13).Take(4).ToArray<byte>()), 0));

            // next 8 bytes = time
            Assert.AreEqual(OffsetRequest.LatestTime, BitConverter.ToInt64(BitWorks.ReverseBytes(bytes.Skip(17).Take(8).ToArray<byte>()), 0));

            // next 4 bytes = max offsets
            Assert.AreEqual(10, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(25).Take(4).ToArray<byte>()), 0));
        }
    }
}
