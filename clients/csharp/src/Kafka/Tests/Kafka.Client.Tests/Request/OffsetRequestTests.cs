using System;
using System.Linq;
using System.Text;
using Kafka.Client.Request;
using Kafka.Client.Util;
using NUnit.Framework;

namespace Kafka.Client.Request.Tests
{
    /// <summary>
    /// Tests the <see cref="OffsetRequest"/> class.
    /// </summary>
    [TestFixture]
    public class OffsetRequestTests
    {
        /// <summary>
        /// Tests a valid request.  
        /// </summary>
        [Test]
        public void IsValidTrue()
        {
            FetchRequest request = new FetchRequest("topic", 1, 10L, 100);
            Assert.IsTrue(request.IsValid());
        }

        /// <summary>
        /// Tests a invalid request with no topic.
        /// </summary>
        [Test]
        public void IsValidNoTopic()
        {
            FetchRequest request = new FetchRequest(" ", 1, 10L, 100);
            Assert.IsFalse(request.IsValid());
        }

        /// <summary>
        /// Tests a invalid request with no topic.
        /// </summary>
        [Test]
        public void IsValidNulltopic()
        {
            FetchRequest request = new FetchRequest(null, 1, 10L, 100);
            Assert.IsFalse(request.IsValid());
        }

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
            byte[] bytes = request.GetBytes();
            Assert.IsNotNull(bytes);
            Assert.AreEqual(29, bytes.Length);

            // first 4 bytes = the length of the request
            Assert.AreEqual(25, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the RequestType which in this case should be Produce
            Assert.AreEqual((short)RequestType.Offsets, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

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
