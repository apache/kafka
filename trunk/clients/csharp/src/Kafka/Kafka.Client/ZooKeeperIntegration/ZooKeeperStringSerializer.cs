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

namespace Kafka.Client.ZooKeeperIntegration
{
    using System;
    using System.Linq;
    using System.Text;
    using Kafka.Client.Utils;

    /// <summary>
    /// Zookeeper is able to store data in form of byte arrays. This interfacte is a bridge between those byte-array format
    /// and higher level objects.
    /// </summary>
    internal class ZooKeeperStringSerializer : IZooKeeperSerializer
    {
        public static readonly ZooKeeperStringSerializer Serializer = new ZooKeeperStringSerializer();

        /// <summary>
        /// Prevents a default instance of the <see cref="ZooKeeperStringSerializer"/> class from being created.
        /// </summary>
        private ZooKeeperStringSerializer()
        {
        }

        /// <summary>
        /// Serializes data using UTF-8 encoding
        /// </summary>
        /// <param name="obj">
        /// The data to serialize
        /// </param>
        /// <returns>
        /// Serialized data
        /// </returns>
        public byte[] Serialize(object obj)
        {
            Guard.NotNull(obj, "obj");
            return Encoding.UTF8.GetBytes(obj.ToString());
        }

        /// <summary>
        /// Deserializes data using UTF-8 encoding
        /// </summary>
        /// <param name="bytes">
        /// The serialized data
        /// </param>
        /// <returns>
        /// The deserialized data
        /// </returns>
        public object Deserialize(byte[] bytes)
        {
            Guard.NotNull(bytes, "bytes");
            Guard.Greater(bytes.Count(), 0, "bytes");

            return bytes == null ? null : Encoding.UTF8.GetString(bytes);
        }
    }
}
