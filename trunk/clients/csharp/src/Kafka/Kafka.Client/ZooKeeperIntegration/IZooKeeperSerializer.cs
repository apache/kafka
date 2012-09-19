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
    /// <summary>
    /// Zookeeper is able to store data in form of byte arrays. This interfacte is a bridge between those byte-array format
    /// and higher level objects.
    /// </summary>
    internal interface IZooKeeperSerializer
    {
        /// <summary>
        /// Serializes data
        /// </summary>
        /// <param name="obj">
        /// The data to serialize
        /// </param>
        /// <returns>
        /// Serialized data
        /// </returns>
        byte[] Serialize(object obj);

        /// <summary>
        /// Deserializes data
        /// </summary>
        /// <param name="bytes">
        /// The serialized data
        /// </param>
        /// <returns>
        /// The deserialized data
        /// </returns>
        object Deserialize(byte[] bytes);
    }
}
