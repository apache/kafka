using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Request;
using Kafka.Client.Util;
using NUnit.Framework;

namespace Kafka.Client.Request.Tests
{
    /// <summary>
    /// Tests for the <see cref="MultiFetchRequest"/> class.
    /// </summary>
    [TestFixture]
    public class MultiFetchRequestTests
    {
        /// <summary>
        /// Tests a valid multi-consumer request.
        /// </summary>
        [Test]
        public void IsValidTrue()
        {
            List<FetchRequest> requests = new List<FetchRequest>
            { 
                new FetchRequest("topic a", 0, 0),
                new FetchRequest("topic a", 0, 0),
                new FetchRequest("topic b", 0, 0),
                new FetchRequest("topic c", 0, 0)
            };

            MultiFetchRequest multiRequest = new MultiFetchRequest(requests);
            Assert.IsTrue(multiRequest.IsValid());
        }

        /// <summary>
        /// Tests for an invalid multi-request with no requests provided.
        /// </summary>
        [Test]
        public void IsValidNoRequests()
        {
            MultiFetchRequest multiRequest = new MultiFetchRequest(new List<FetchRequest>());
            Assert.IsFalse(multiRequest.IsValid());
        }

        /// <summary>
        /// Tests for an invalid multi-request with no requests provided.
        /// </summary>
        [Test]
        public void IsValidNullRequests()
        {
            MultiFetchRequest multiRequest = new MultiFetchRequest(null);
            Assert.IsFalse(multiRequest.IsValid());
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
            byte[] bytes = request.GetBytes();
            Assert.IsNotNull(bytes);
            Assert.AreEqual(108, bytes.Length);

            // first 4 bytes = the length of the request
            Assert.AreEqual(104, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the RequestType which in this case should be Produce
            Assert.AreEqual((short)RequestType.MultiFetch, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the number of messages
            Assert.AreEqual((short)4, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));
        }
    }
}
