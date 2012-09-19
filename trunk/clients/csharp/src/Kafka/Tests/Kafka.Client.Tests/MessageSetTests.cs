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

namespace Kafka.Client.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Kafka.Client.Messages;
    using Kafka.Client.Utils;
    using NUnit.Framework;

    [TestFixture]
    public class MessageSetTests
    {
        private const int MessageLengthPartLength = 4;
        private const int MagicNumberPartLength = 1;
        private const int AttributesPartLength = 1;
        private const int ChecksumPartLength = 4;
        
        private const int MessageLengthPartOffset = 0;
        private const int MagicNumberPartOffset = 4;
        private const int AttributesPartOffset = 5;
        private const int ChecksumPartOffset = 6;
        private const int DataPartOffset = 10;

        [Test]
        public void BufferedMessageSetWriteToValidSequence()
        {
            byte[] messageBytes = new byte[] { 1, 2, 3, 4, 5 };
            Message msg1 = new Message(messageBytes);
            Message msg2 = new Message(messageBytes);
            MessageSet messageSet = new BufferedMessageSet(new List<Message>() { msg1, msg2 });
            MemoryStream ms = new MemoryStream();
            messageSet.WriteTo(ms);

            ////first message

            byte[] messageLength = new byte[MessageLengthPartLength];
            Array.Copy(ms.ToArray(), MessageLengthPartOffset, messageLength, 0, MessageLengthPartLength);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(messageLength);
            }

            Assert.AreEqual(MagicNumberPartLength + AttributesPartLength + ChecksumPartLength + messageBytes.Length, BitConverter.ToInt32(messageLength, 0));

            Assert.AreEqual(1, ms.ToArray()[MagicNumberPartOffset]);    // default magic number should be 1

            byte[] checksumPart = new byte[ChecksumPartLength];
            Array.Copy(ms.ToArray(), ChecksumPartOffset, checksumPart, 0, ChecksumPartLength);
            Assert.AreEqual(Crc32Hasher.Compute(messageBytes), checksumPart);

            byte[] dataPart = new byte[messageBytes.Length];
            Array.Copy(ms.ToArray(), DataPartOffset, dataPart, 0, messageBytes.Length);
            Assert.AreEqual(messageBytes, dataPart);

            ////second message
            int secondMessageOffset = MessageLengthPartLength + MagicNumberPartLength + AttributesPartLength + ChecksumPartLength +
                                      messageBytes.Length;

            messageLength = new byte[MessageLengthPartLength];
            Array.Copy(ms.ToArray(), secondMessageOffset + MessageLengthPartOffset, messageLength, 0, MessageLengthPartLength);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(messageLength);
            }

            Assert.AreEqual(MagicNumberPartLength + AttributesPartLength + ChecksumPartLength + messageBytes.Length, BitConverter.ToInt32(messageLength, 0));

            Assert.AreEqual(1, ms.ToArray()[secondMessageOffset + MagicNumberPartOffset]);    // default magic number should be 1

            checksumPart = new byte[ChecksumPartLength];
            Array.Copy(ms.ToArray(), secondMessageOffset + ChecksumPartOffset, checksumPart, 0, ChecksumPartLength);
            Assert.AreEqual(Crc32Hasher.Compute(messageBytes), checksumPart);

            dataPart = new byte[messageBytes.Length];
            Array.Copy(ms.ToArray(), secondMessageOffset + DataPartOffset, dataPart, 0, messageBytes.Length);
            Assert.AreEqual(messageBytes, dataPart);
        }

        [Test]
        public void SetSizeValid()
        {
            byte[] messageBytes = new byte[] { 1, 2, 3, 4, 5 };
            Message msg1 = new Message(messageBytes);
            Message msg2 = new Message(messageBytes);
            MessageSet messageSet = new BufferedMessageSet(new List<Message>() { msg1, msg2 });
            Assert.AreEqual(
                2 * (MessageLengthPartLength + MagicNumberPartLength + AttributesPartLength + ChecksumPartLength + messageBytes.Length),
                messageSet.SetSize);
        }
    }
}
