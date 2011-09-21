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

namespace Kafka.Client.ZooKeeperIntegration.Events
{
    using ZooKeeperNet;

    /// <summary>
    /// Contains ZooKeeper session state changed event data
    /// </summary>
    internal class ZooKeeperStateChangedEventArgs : ZooKeeperEventArgs
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperStateChangedEventArgs"/> class.
        /// </summary>
        /// <param name="state">
        /// The current ZooKeeper state.
        /// </param>
        public ZooKeeperStateChangedEventArgs(KeeperState state)
            : base("State changed to " + state)
        {
            this.State = state;
        }

        /// <summary>
        /// Gets current ZooKeeper state
        /// </summary>
        public KeeperState State { get; private set; }

        /// <summary>
        /// Gets the event type.
        /// </summary>
        public override ZooKeeperEventTypes Type
        {
            get
            {
                return ZooKeeperEventTypes.StateChanged;
            }
        }
    }
}
