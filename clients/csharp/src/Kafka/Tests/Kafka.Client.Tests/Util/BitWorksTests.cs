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

namespace Kafka.Client.Tests.Util
{
    using System;
    using Kafka.Client.Utils;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="BitWorks"/> utility class.
    /// </summary>
    [TestFixture]
    public class BitWorksTests
    {
        /// <summary>
        /// Ensures bytes are returned reversed.
        /// </summary>
        [Test]
        public void GetBytesReversedShortValid()
        {
            short val = (short)100;
            byte[] normal = BitConverter.GetBytes(val);
            byte[] reversed = BitWorks.GetBytesReversed(val);

            TestReversedArray(normal, reversed);
        }

        /// <summary>
        /// Ensures bytes are returned reversed.
        /// </summary>
        [Test]
        public void GetBytesReversedIntValid()
        {
            int val = 100;
            byte[] normal = BitConverter.GetBytes(val);
            byte[] reversed = BitWorks.GetBytesReversed(val);

            TestReversedArray(normal, reversed);
        }

        /// <summary>
        /// Ensures bytes are returned reversed.
        /// </summary>
        [Test]
        public void GetBytesReversedLongValid()
        {
            long val = 100L;
            byte[] normal = BitConverter.GetBytes(val);
            byte[] reversed = BitWorks.GetBytesReversed(val);

            TestReversedArray(normal, reversed);
        }

        /// <summary>
        /// Null array will reverse to a null.
        /// </summary>
        [Test]
        public void ReverseBytesNullArray()
        {
            byte[] arr = null;
            Assert.IsNull(BitWorks.ReverseBytes(arr));
        }

        /// <summary>
        /// Zero length array will reverse to a zero length array.
        /// </summary>
        [Test]
        public void ReverseBytesZeroLengthArray()
        {
            byte[] arr = new byte[0];
            byte[] reversedArr = BitWorks.ReverseBytes(arr);
            Assert.IsNotNull(reversedArr);
            Assert.AreEqual(0, reversedArr.Length);
        }

        /// <summary>
        /// Array is reversed.
        /// </summary>
        [Test]
        public void ReverseBytesValid()
        {
            byte[] arr = BitConverter.GetBytes((short)1);
            byte[] original = new byte[2];
            arr.CopyTo(original, 0);
            byte[] reversedArr = BitWorks.ReverseBytes(arr);

            TestReversedArray(original, reversedArr);
        }

        /// <summary>
        /// Performs asserts for two arrays that should be exactly the same, but values
        /// in one are in reverse order of the other.
        /// </summary>
        /// <param name="normal">The "normal" array.</param>
        /// <param name="reversed">The array that is in reverse order to the "normal" one.</param>
        private static void TestReversedArray(byte[] normal, byte[] reversed)
        {
            Assert.IsNotNull(reversed);
            Assert.AreEqual(normal.Length, reversed.Length);
            for (int ix = 0; ix < normal.Length; ix++)
            {
                Assert.AreEqual(normal[ix], reversed[reversed.Length - 1 - ix]);
            }
        }
    }
}
