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
    /// Handles the session expiration event in ZooKeeper
    /// </summary>
    internal interface IZooKeeperStateListener
    {
        /// <summary>
        /// Called when the ZooKeeper connection state has changed.
        /// </summary>
        /// <param name="args">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperStateChangedEventArgs"/> instance containing the event data.</param>
        void HandleStateChanged(ZooKeeperStateChangedEventArgs args);

        /// <summary>
        /// Called after the ZooKeeper session has expired and a new session has been created.
        /// </summary>
        /// <param name="args">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperSessionCreatedEventArgs"/> instance containing the event data.</param>
        /// <remarks>
        /// You would have to re-create any ephemeral nodes here.
        /// </remarks>
        void HandleSessionCreated(ZooKeeperSessionCreatedEventArgs args);
    }
}
