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

namespace Kafka.Client.Cluster
{
    /// <summary>
    /// Represents Kafka broker
    /// </summary>
    internal class Broker
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Broker"/> class.
        /// </summary>
        /// <param name="id">
        /// The broker id.
        /// </param>
        /// <param name="creatorId">
        /// The broker creator id.
        /// </param>
        /// <param name="host">
        /// The broker host.
        /// </param>
        /// <param name="port">
        /// The broker port.
        /// </param>
        public Broker(int id, string creatorId, string host, int port)
        {
            this.Id = id;
            this.CreatorId = creatorId;
            this.Host = host;
            this.Port = port;
        }

        /// <summary>
        /// Gets the broker Id.
        /// </summary>
        public int Id { get; private set; }

        /// <summary>
        /// Gets the broker creatorId.
        /// </summary>
        public string CreatorId { get; private set; }

        /// <summary>
        /// Gets the broker host.
        /// </summary>
        public string Host { get; private set; }

        /// <summary>
        /// Gets the broker port.
        /// </summary>
        public int Port { get; private set; }
    }
}
