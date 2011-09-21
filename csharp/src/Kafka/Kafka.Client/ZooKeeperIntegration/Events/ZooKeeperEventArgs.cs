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
    using System;

    /// <summary>
    /// Base class for classes containing ZooKeeper event data
    /// </summary>
    internal abstract class ZooKeeperEventArgs : EventArgs
    {
        private readonly string description;

        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperEventArgs"/> class.
        /// </summary>
        /// <param name="description">
        /// The event description.
        /// </param>
        protected ZooKeeperEventArgs(string description)
        {
            this.description = description;
        }

        /// <summary>
        /// Gets string representation of event data
        /// </summary>
        /// <returns>
        /// String representation of event data
        /// </returns>
        public override string ToString()
        {
            return "ZooKeeperEvent[" + this.description + "]";
        }

        /// <summary>
        /// Gets the event type.
        /// </summary>
        public abstract ZooKeeperEventTypes Type { get; }
    }
}
