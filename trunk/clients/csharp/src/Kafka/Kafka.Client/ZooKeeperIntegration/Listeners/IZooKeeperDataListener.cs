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
    /// Listener that can be registered for listening on ZooKeeper znode data changes for a given path
    /// </summary>
    internal interface IZooKeeperDataListener
    {
        /// <summary>
        /// Called when the data of the given path changed
        /// </summary>
        /// <param name="args">The <see cref="ZooKeeperDataChangedEventArgs"/> instance containing the event data
        /// as path and data.
        /// </param>
        /// <remarks> 
        /// http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches
        /// </remarks>
        void HandleDataChange(ZooKeeperDataChangedEventArgs args);

        /// <summary>
        /// Called when the data of the given path was deleted
        /// </summary>
        /// <param name="args">The <see cref="ZooKeeperDataChangedEventArgs"/> instance containing the event data
        /// as path.
        /// </param>
        /// <remarks> 
        /// http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches
        /// </remarks>
        void HandleDataDelete(ZooKeeperDataChangedEventArgs args);
    }
}
