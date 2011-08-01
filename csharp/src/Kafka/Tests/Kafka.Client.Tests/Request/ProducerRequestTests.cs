using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Request;
using Kafka.Client.Util;
using NUnit.Framework;

namespace Kafka.Client.Request.Tests
{
    /// <summary>
    /// Tests for the <see cref="ProducerRequest"/> class.
    /// </summary>
    [TestFixture]
    public class ProducerRequestTests
    {
        /// <summary>
        /// Tests a valid producer request.
        /// </summary>
        [Test]
        public void IsValidTrue()
        {
            ProducerRequest request = new ProducerRequest(
                "topic", 0, new List<Message> { new Message(new byte[10]) });
            Assert.IsTrue(request.IsValid());
        }

        /// <summary>
        /// Tests a invalid producer request with no topic.
        /// </summary>
        [Test]
        public void IsValidFalseNoTopic()
        {
            ProducerRequest request = new ProducerRequest(null, 0, null);
            Assert.IsFalse(request.IsValid());
        }

        /// <summary>
        /// Tests a invalid producer request with no messages to send.
        /// </summary>
        [Test]
        public void IsValidFalseNoMessages()
        {
            ProducerRequest request = new ProducerRequest("topic", 0, null);
            Assert.IsFalse(request.IsValid());
        }

        /// <summary>
        /// Test to ensure a valid format in the returned byte array as expected by Kafka.
        /// </summary>
        [Test]
        public void GetBytesValidFormat()
        {
            string topicName = "topic";
            ProducerRequest request = new ProducerRequest(
                topicName, 0, new List<Message> { new Message(new byte[10]) });

            // format = len(request) + requesttype + len(topic) + topic + partition + len(messagepack) + message
            // total byte count = 4 + (2 + 2 + 5 + 4 + 4 + 19)
            byte[] bytes = request.GetBytes();
            Assert.IsNotNull(bytes);
            Assert.AreEqual(40, bytes.Length);

            // first 4 bytes = the length of the request
            Assert.AreEqual(36, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the RequestType which in this case should be Produce
            Assert.AreEqual((short)RequestType.Produce, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the length of the topic
            Assert.AreEqual((short)5, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));

            // next 5 bytes = the topic
            Assert.AreEqual(topicName, Encoding.ASCII.GetString(bytes.Skip(8).Take(5).ToArray<byte>()));

            // next 4 bytes = the partition
            Assert.AreEqual(0, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(13).Take(4).ToArray<byte>()), 0));

            // next 4 bytes = the length of the individual messages in the pack
            Assert.AreEqual(19, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(17).Take(4).ToArray<byte>()), 0));

            // fianl bytes = the individual messages in the pack
            Assert.AreEqual(19, bytes.Skip(21).ToArray<byte>().Length);
        }
    }
}
