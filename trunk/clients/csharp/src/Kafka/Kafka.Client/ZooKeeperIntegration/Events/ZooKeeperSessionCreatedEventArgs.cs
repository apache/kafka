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

namespace Kafka.Client.ZooKeeperIntegration.Events
{
    /// <summary>
    /// Contains ZooKeeper session created event data
    /// </summary>
    internal class ZooKeeperSessionCreatedEventArgs : ZooKeeperEventArgs
    {
        public static new readonly ZooKeeperSessionCreatedEventArgs Empty = new ZooKeeperSessionCreatedEventArgs();

        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperSessionCreatedEventArgs"/> class.
        /// </summary>
        protected ZooKeeperSessionCreatedEventArgs()
            : base("New session created")
        {
        }

        /// <summary>
        /// Gets the event type.
        /// </summary>
        public override ZooKeeperEventTypes Type
        {
            get
            {
                return ZooKeeperEventTypes.SessionCreated;
            }
        }
    }
}
