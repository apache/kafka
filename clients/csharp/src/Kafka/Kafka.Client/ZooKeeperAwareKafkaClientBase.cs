/*
 * Copyright 2011 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

namespace Kafka.Client
{
    using Kafka.Client.Cfg;

    /// <summary>
    /// A base class for all Kafka clients that support ZooKeeper based automatic broker discovery
    /// </summary>
    public abstract class ZooKeeperAwareKafkaClientBase : KafkaClientBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperAwareKafkaClientBase"/> class.
        /// </summary>
        /// <param name="config">The config.</param>
        protected ZooKeeperAwareKafkaClientBase(ZKConfig config)
        {
            this.IsZooKeeperEnabled = config != null && !string.IsNullOrEmpty(config.ZkConnect);
        }

        /// <summary>
        /// Gets a value indicating whether ZooKeeper based automatic broker discovery is enabled.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is zoo keeper enabled; otherwise, <c>false</c>.
        /// </value>
        protected bool IsZooKeeperEnabled { get; private set; }
    }
}
