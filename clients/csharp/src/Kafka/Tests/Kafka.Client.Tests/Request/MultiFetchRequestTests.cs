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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using NUnit.Framework;

    /// <summary>
    /// Tests for the <see cref="MultiFetchRequest"/> class.
    /// </summary>
    [TestFixture]
    public class MultiFetchRequestTests
    {
        /// <summary>
        /// Tests for an invalid multi-request constructor with no requests given.
        /// </summary>
        [Test]
        public void ThrowsExceptionWhenNullArgumentPassedToTheConstructor()
        {
            MultiFetchRequest multiRequest;
            Assert.Throws<ArgumentNullException>(() => multiRequest = new MultiFetchRequest(null));
        }

        /// <summary>
        /// Test to ensure a valid format in the returned byte array as expected by Kafka.
        /// </summary>
        [Test]
        public void GetBytesValidFormat()
        {
            List<FetchRequest> requests = new List<FetchRequest>
            { 
                new FetchRequest("topic a", 0, 0),
                new FetchRequest("topic a", 0, 0),
                new FetchRequest("topic b", 0, 0),
                new FetchRequest("topic c", 0, 0)
            };

            MultiFetchRequest request = new MultiFetchRequest(requests);

            // format = len(request) + requesttype + requestcount + requestpackage
            // total byte count = 4 + (2 + 2 + 100)
            MemoryStream ms = new MemoryStream();
            request.WriteTo(ms);
            byte[] bytes = ms.ToArray();
            Assert.IsNotNull(bytes);
            Assert.AreEqual(108, bytes.Length);

            // first 4 bytes = the length of the request
            Assert.AreEqual(104, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the RequestType which in this case should be Produce
            Assert.AreEqual((short)RequestTypes.MultiFetch, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the number of messages
            Assert.AreEqual((short)4, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));
        }
    }
}
