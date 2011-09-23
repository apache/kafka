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
    using System;
    using System.Collections.Generic;
    using Org.Apache.Zookeeper.Data;
    using ZooKeeperNet;

    /// <summary>
    /// Abstracts connection with ZooKeeper server
    /// </summary>
    internal interface IZooKeeperConnection : IDisposable
    {
        /// <summary>
        /// Gets the ZooKeeper client state
        /// </summary>
        ZooKeeper.States ClientState { get; }

        /// <summary>
        /// Gets the list of ZooKeeper servers.
        /// </summary>
        string Servers { get; }

        /// <summary>
        /// Gets the ZooKeeper session timeout
        /// </summary>
        int SessionTimeout { get; }

        /// <summary>
        /// Gets ZooKeeper client.
        /// </summary>
        ZooKeeper Client { get; }

        /// <summary>
        /// Connects to ZooKeeper server
        /// </summary>
        /// <param name="watcher">
        /// The watcher to be installed in ZooKeeper.
        /// </param>
        void Connect(IWatcher watcher);

        /// <summary>
        /// Creates znode using given create mode for given path and writes given data to it
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="data">
        /// The data to write.
        /// </param>
        /// <param name="mode">
        /// The create mode.
        /// </param>
        /// <returns>
        /// The created znode's path
        /// </returns>
        string Create(string path, byte[] data, CreateMode mode);

        /// <summary>
        /// Deletes znode for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        void Delete(string path);

        /// <summary>
        /// Checks whether znode for a given path exists.
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="watch">
        /// Indicates whether should reinstall watcher in ZooKeeper.
        /// </param>
        /// <returns>
        /// Result of check
        /// </returns>
        bool Exists(string path, bool watch);

        /// <summary>
        /// Gets all children for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="watch">
        /// Indicates whether should reinstall watcher in ZooKeeper.
        /// </param>
        /// <returns>
        /// Children
        /// </returns>
        IList<string> GetChildren(string path, bool watch);

        /// <summary>
        /// Fetches data from a given path in ZooKeeper
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="stats">
        /// The statistics.
        /// </param>
        /// <param name="watch">
        /// Indicates whether should reinstall watcher in ZooKeeper.
        /// </param>
        /// <returns>
        /// Data
        /// </returns>
        byte[] ReadData(string path, Stat stats, bool watch);

        /// <summary>
        /// Writes data for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="data">
        /// The data to write.
        /// </param>
        void WriteData(string path, byte[] data);

        /// <summary>
        /// Writes data for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="data">
        /// The data to write.
        /// </param>
        /// <param name="version">
        /// Expected version of data
        /// </param>
        void WriteData(string path, byte[] data, int version);

        /// <summary>
        /// Gets time when connetion was created
        /// </summary>
        /// <param name="path">
        /// The path.
        /// </param>
        /// <returns>
        /// Connection creation time
        /// </returns>
        long GetCreateTime(string path);
    }
}
