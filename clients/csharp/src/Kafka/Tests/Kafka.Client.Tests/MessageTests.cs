using System;
using System.Linq;
using System.Text;
using Kafka.Client.Util;
using NUnit.Framework;

namespace Kafka.Client.Tests
{
    /// <summary>
    /// Tests for the <see cref="Message"/> class.
    /// </summary>
    [TestFixture]
    public class MessageTests
    {
        /// <summary>
        /// Demonstrates a properly parsed message.
        /// </summary>
        [Test]
        public void ParseFromValid()
        {
            Crc32 crc32 = new Crc32();

            string payload = "kafka";
            byte magic = 0;
            byte[] payloadData = Encoding.UTF8.GetBytes(payload);
            byte[] payloadSize = BitConverter.GetBytes(payloadData.Length);
            byte[] checksum = crc32.ComputeHash(payloadData);
            byte[] messageData = new byte[payloadData.Length + 1 + payloadSize.Length + checksum.Length];

            Buffer.BlockCopy(payloadSize, 0, messageData, 0, payloadSize.Length);
            messageData[4] = magic;
            Buffer.BlockCopy(checksum, 0, messageData, payloadSize.Length + 1, checksum.Length);
            Buffer.BlockCopy(payloadData, 0, messageData, payloadSize.Length + 1 + checksum.Length, payloadData.Length);

            Message message = Message.ParseFrom(messageData);

            Assert.IsNotNull(message);
            Assert.AreEqual(magic, message.Magic);
            Assert.IsTrue(payloadData.SequenceEqual(message.Payload));
            Assert.IsTrue(checksum.SequenceEqual(message.Checksum));
        }

        /// <summary>
        /// Ensure that the bytes returned from the message are in valid kafka sequence.
        /// </summary>
        [Test]
        public void GetBytesValidSequence()
        {
            Message message = new Message(new byte[10], (byte)245);

            byte[] bytes = message.GetBytes();

            Assert.IsNotNull(bytes);

            // len(payload) + 1 + 4
            Assert.AreEqual(15, bytes.Length);

            // first 4 bytes = the magic number
            Assert.AreEqual((byte)245, bytes[0]);

            // next 4 bytes = the checksum
            Assert.IsTrue(message.Checksum.SequenceEqual(bytes.Skip(1).Take(4).ToArray<byte>()));

            // remaining bytes = the payload
            Assert.AreEqual(10, bytes.Skip(5).ToArray<byte>().Length);
        }
    }
}
