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
    using System.IO;
    using System.Reflection;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Utils;
    using log4net;
    using Org.Apache.Zookeeper.Data;
    using ZooKeeperNet;

    /// <summary>
    /// Abstracts connection with ZooKeeper server
    /// </summary>
    internal class ZooKeeperConnection : IZooKeeperConnection
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public const int DefaultSessionTimeout = 30000;

        private readonly object syncLock = new object();

        private readonly object shuttingDownLock = new object();

        private volatile bool disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperConnection"/> class.
        /// </summary>
        /// <param name="servers">
        /// The list of ZooKeeper servers.
        /// </param>
        public ZooKeeperConnection(string servers)
            : this(servers, DefaultSessionTimeout)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperConnection"/> class.
        /// </summary>
        /// <param name="servers">
        /// The list of ZooKeeper servers.
        /// </param>
        /// <param name="sessionTimeout">
        /// The session timeout.
        /// </param>
        public ZooKeeperConnection(string servers, int sessionTimeout)
        {
            this.Servers = servers;
            this.SessionTimeout = sessionTimeout;
        }

        /// <summary>
        /// Gets the list of ZooKeeper servers.
        /// </summary>
        public string Servers { get; private set; }

        /// <summary>
        /// Gets the ZooKeeper session timeout
        /// </summary>
        public int SessionTimeout { get; private set; }

        /// <summary>
        /// Gets ZooKeeper client.
        /// </summary>
        public ZooKeeper Client { get; private set; }

        /// <summary>
        /// Gets the ZooKeeper client state
        /// </summary>
        public ZooKeeper.States ClientState
        {
            get
            {
                return this.Client == null ? null : this.Client.State;
            }
        }

        /// <summary>
        /// Connects to ZooKeeper server
        /// </summary>
        /// <param name="watcher">
        /// The watcher to be installed in ZooKeeper.
        /// </param>
        public void Connect(IWatcher watcher)
        {
            this.EnsuresNotDisposed();
            lock (this.syncLock)
            {
                if (this.Client != null)
                {
                    throw new InvalidOperationException("ZooKeeper client has already been started");
                }

                try
                {
                    Logger.Debug("Starting ZK client");
                    this.Client = new ZooKeeper(this.Servers, new TimeSpan(0, 0, 0, 0, this.SessionTimeout), watcher);
                }
                catch (IOException exc)
                {
                    throw new ZooKeeperException("Unable to connect to " + this.Servers, exc);
                }
            }
        }

        /// <summary>
        /// Deletes znode for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        public void Delete(string path)
        {
            Guard.NotNullNorEmpty(path, "path");

            this.EnsuresNotDisposed();
            this.Client.Delete(path, -1);
        }

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
        public bool Exists(string path, bool watch)
        {
            Guard.NotNullNorEmpty(path, "path");

            this.EnsuresNotDisposed();
            return this.Client.Exists(path, true) != null;
        }

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
        public string Create(string path, byte[] data, CreateMode mode)
        {
            Guard.NotNullNorEmpty(path, "path");

            this.EnsuresNotDisposed();
            return this.Client.Create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
        }

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
        public IList<string> GetChildren(string path, bool watch)
        {
            Guard.NotNullNorEmpty(path, "path");

            this.EnsuresNotDisposed();
            return this.Client.GetChildren(path, watch);
        }

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
        public byte[] ReadData(string path, Stat stats, bool watch)
        {
            Guard.NotNullNorEmpty(path, "path");

            this.EnsuresNotDisposed();
            return this.Client.GetData(path, watch, stats);
        }

        /// <summary>
        /// Writes data for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="data">
        /// The data to write.
        /// </param>
        public void WriteData(string path, byte[] data)
        {
            Guard.NotNullNorEmpty(path, "path");

            this.EnsuresNotDisposed();
            this.WriteData(path, data, -1);
        }

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
        public void WriteData(string path, byte[] data, int version)
        {
            Guard.NotNullNorEmpty(path, "path");

            this.EnsuresNotDisposed();
            this.Client.SetData(path, data, version);
        }

        /// <summary>
        /// Gets time when connetion was created
        /// </summary>
        /// <param name="path">
        /// The path.
        /// </param>
        /// <returns>
        /// Connection creation time
        /// </returns>
        public long GetCreateTime(string path)
        {
            Guard.NotNullNorEmpty(path, "path");

            this.EnsuresNotDisposed();
            Stat stats = this.Client.Exists(path, false);
            return stats != null ? stats.Ctime : -1;
        }

        /// <summary>
        /// Closes underlying ZooKeeper client
        /// </summary>
        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            lock (this.shuttingDownLock)
            {
                if (this.disposed)
                {
                    return;
                }

                this.disposed = true;
            }
            
            try
            {
                if (this.Client != null)
                {
                    Logger.Debug("Closing ZooKeeper client connected to " + this.Servers);
                    this.Client.Dispose();
                    this.Client = null;
                    Logger.Debug("ZooKeeper client connection closed");
                }
            }
            catch (Exception exc)
            {
                Logger.Warn("Ignoring unexpected errors on closing", exc);
            }
        }

        /// <summary>
        /// Ensures object wasn't disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }
    }
}
