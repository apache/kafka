using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Request;
using Kafka.Client.Util;
using NUnit.Framework;

namespace Kafka.Client.Request.Tests
{
    /// <summary>
    /// Tests for the <see cref="MultiProducerRequest"/> class.
    /// </summary>
    [TestFixture]
    public class MultiProducerRequestTests
    {
        /// <summary>
        /// Tests a valid multi-producer request.
        /// </summary>
        [Test]
        public void IsValidTrue()
        {
            List<ProducerRequest> requests = new List<ProducerRequest>
            { 
                new ProducerRequest("topic a", 0, new List<Message> { new Message(new byte[10]) }),
                new ProducerRequest("topic a", 0, new List<Message> { new Message(new byte[10]) }),
                new ProducerRequest("topic b", 0, new List<Message> { new Message(new byte[10]) }),
                new ProducerRequest("topic c", 0, new List<Message> { new Message(new byte[10]) })
            };

            MultiProducerRequest multiRequest = new MultiProducerRequest(requests);
            Assert.IsTrue(multiRequest.IsValid());
        }

        /// <summary>
        /// Tests for an invalid multi-request with no requests provided.
        /// </summary>
        [Test]
        public void IsValidNoRequests()
        {
            MultiProducerRequest multiRequest = new MultiProducerRequest(new List<ProducerRequest>());
            Assert.IsFalse(multiRequest.IsValid());
        }

        /// <summary>
        /// Tests for an invalid multi-request with no requests provided.
        /// </summary>
        [Test]
        public void IsValidNullRequests()
        {
            MultiProducerRequest multiRequest = new MultiProducerRequest(null);
            Assert.IsFalse(multiRequest.IsValid());
        }

        /// <summary>
        /// Test to ensure a valid format in the returned byte array as expected by Kafka.
        /// </summary>
        [Test]
        public void GetBytesValidFormat()
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
            byte[] bytes = request.GetBytes();
            Assert.IsNotNull(bytes);
            Assert.AreEqual(152, bytes.Length);

            // first 4 bytes = the length of the request
            Assert.AreEqual(148, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the RequestType which in this case should be Produce
            Assert.AreEqual((short)RequestType.MultiProduce, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the number of messages
            Assert.AreEqual((short)4, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));
        }
    }
}
