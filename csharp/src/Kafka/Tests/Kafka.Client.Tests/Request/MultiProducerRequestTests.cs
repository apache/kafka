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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using NUnit.Framework;

    /// <summary>
    /// Tests for the <see cref="MultiProducerRequest"/> class.
    /// </summary>
    [TestFixture]
    public class MultiProducerRequestTests
    {
        /// <summary>
        /// Test to ensure a valid format in the returned byte array as expected by Kafka.
        /// </summary>
        [Test]
        public void WriteToValidFormat()
        {
            List<ProducerRequest> requests = new List<ProducerRequest>
            { 
                new ProducerRequest("topic a", 0, new List<Message> { new Message(new byte[10]) }),
                new ProducerRequest("topic a", 0, new List<Message> { new Message(new byte[10]) }),
                new ProducerRequest("topic b", 0, new List<Message> { new Message(new byte[10]) }),
                new ProducerRequest("topic c", 0, new List<Message> { new Message(new byte[10]) })
            };

            MultiProducerRequest request = new MultiProducerRequest(requests);

            // format = len(request) + requesttype + requestcount + requestpackage
            // total byte count = 4 + (2 + 2 + 144)
            MemoryStream ms = new MemoryStream();
            request.WriteTo(ms);
            byte[] bytes = ms.ToArray();
            Assert.IsNotNull(bytes);
            Assert.AreEqual(152, bytes.Length);

            // first 4 bytes = the length of the request
            Assert.AreEqual(148, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the RequestType which in this case should be Produce
            Assert.AreEqual((short)RequestTypes.MultiProduce, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the number of messages
            Assert.AreEqual((short)4, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));
        }
    }
}
