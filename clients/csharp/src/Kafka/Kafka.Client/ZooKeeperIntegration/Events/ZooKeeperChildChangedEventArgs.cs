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
    using System.Collections.Generic;

    /// <summary>
    /// Contains znode children changed event data
    /// </summary>
    internal class ZooKeeperChildChangedEventArgs : ZooKeeperEventArgs
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperChildChangedEventArgs"/> class.
        /// </summary>
        /// <param name="path">
        /// The path.
        /// </param>
        public ZooKeeperChildChangedEventArgs(string path)
            : base("Children of " + path + " changed")
        {
            this.Path = path;
        }

        /// <summary>
        /// Gets the znode path
        /// </summary>
        public string Path { get; private set; }

        /// <summary>
        /// Gets or sets the current znode children
        /// </summary>
        public IList<string> Children { get; set; }

        /// <summary>
        /// Gets the current event type
        /// </summary>
        public override ZooKeeperEventTypes Type
        {
            get
            {
                return ZooKeeperEventTypes.ChildChanged;
            }
        }
    }
}
