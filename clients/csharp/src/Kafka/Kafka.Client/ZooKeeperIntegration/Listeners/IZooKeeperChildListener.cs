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

namespace Kafka.Client.ZooKeeperIntegration.Listeners
{
    using Kafka.Client.ZooKeeperIntegration.Events;

    /// <summary>
    /// Listener that can be registered for listening on ZooKeeper znode changes for a given path
    /// </summary>
    internal interface IZooKeeperChildListener
    {
        /// <summary>
        /// Called when the children of the given path changed
        /// </summary>
        /// <param name="args">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperChildChangedEventArgs"/> instance containing the event data
        /// as parent path and children (null if parent was deleted).
        /// </param>
        /// <remarks> 
        /// http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches
        /// </remarks>
        void HandleChildChange(ZooKeeperChildChangedEventArgs args);

        /// <summary>
        /// Resets the state of listener.
        /// </summary>
        void ResetState();
    }
}
