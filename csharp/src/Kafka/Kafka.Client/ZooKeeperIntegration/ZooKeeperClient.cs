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

namespace Kafka.Client.ZooKeeperIntegration
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Utils;
    using log4net;
    using Org.Apache.Zookeeper.Data;
    using ZooKeeperNet;

    /// <summary>
    /// Abstracts the interaction with zookeeper and allows permanent (not just one time) watches on nodes in ZooKeeper 
    /// </summary>
    internal partial class ZooKeeperClient : IZooKeeperClient
    {
        private const int DefaultConnectionTimeout = int.MaxValue;
        public const string DefaultConsumersPath = "/consumers";
        public const string DefaultBrokerIdsPath = "/brokers/ids";
        public const string DefaultBrokerTopicsPath = "/brokers/topics";
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private IZooKeeperConnection connection;
        private bool shutdownTriggered;
        private KeeperState currentState;
        private readonly IZooKeeperSerializer serializer;
        private readonly object stateChangedLock = new object();
        private readonly object znodeChangedLock = new object();
        private readonly object somethingChanged = new object();
        private readonly object shuttingDownLock = new object();
        private volatile bool disposed;
        private readonly int connectionTimeout;

        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperClient"/> class.
        /// </summary>
        /// <param name="connection">
        /// The connection to ZooKeeper.
        /// </param>
        /// <param name="serializer">
        /// The given serializer.
        /// </param>
        /// <param name="connectionTimeout">
        /// The connection timeout (in miliseconds). Default is infinitive.
        /// </param>
        /// <remarks>
        /// Default serializer is string UTF-8 serializer
        /// </remarks>
        public ZooKeeperClient(
            IZooKeeperConnection connection, 
            IZooKeeperSerializer serializer, 
            int connectionTimeout = DefaultConnectionTimeout)
        {
            this.serializer = serializer;
            this.connection = connection;
            this.connectionTimeout = connectionTimeout;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperClient"/> class.
        /// </summary>
        /// <param name="servers">
        /// The list of ZooKeeper servers.
        /// </param>
        /// <param name="sessionTimeout">
        /// The session timeout (in miliseconds).
        /// </param>
        /// <param name="serializer">
        /// The given serializer.
        /// </param>
        /// <remarks>
        /// Default serializer is string UTF-8 serializer.
        /// It is recommended to use quite large sessions timeouts for ZooKeeper.
        /// </remarks>
        public ZooKeeperClient(string servers, int sessionTimeout, IZooKeeperSerializer serializer)
            : this(new ZooKeeperConnection(servers, sessionTimeout), serializer)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ZooKeeperClient"/> class.
        /// </summary>
        /// <param name="servers">
        /// The list of ZooKeeper servers.
        /// </param>
        /// <param name="sessionTimeout">
        /// The session timeout (in miliseconds).
        /// </param>
        /// <param name="serializer">
        /// The given serializer.
        /// </param>
        /// <param name="connectionTimeout">
        /// The connection timeout (in miliseconds).
        /// </param>
        /// <remarks>
        /// Default serializer is string UTF-8 serializer.
        /// It is recommended to use quite large sessions timeouts for ZooKeeper.
        /// </remarks>
        public ZooKeeperClient(
            string servers, 
            int sessionTimeout, 
            IZooKeeperSerializer serializer,
            int connectionTimeout)
            : this(new ZooKeeperConnection(servers, sessionTimeout), serializer, connectionTimeout)
        {
        }

        /// <summary>
        /// Connects to ZooKeeper server within given time period and installs watcher in ZooKeeper
        /// </summary>
        /// <remarks>
        /// Also, starts background thread for event handling
        /// </remarks>
        public void Connect()
        {
            this.EnsuresNotDisposed();
            bool started = false;
            try
            {
                this.shutdownTriggered = false;
                this.eventWorker = new Thread(this.RunEventWorker) { IsBackground = true };
                this.eventWorker.Name = "ZooKeeperkWatcher-EventThread-" + this.eventWorker.ManagedThreadId + "-" + this.connection.Servers;
                this.eventWorker.Start();
                this.connection.Connect(this);
                Logger.Debug("Awaiting connection to Zookeeper server");
                if (!this.WaitUntilConnected(this.connectionTimeout))
                {
                    throw new ZooKeeperException(
                        "Unable to connect to zookeeper server within timeout: " + this.connection.SessionTimeout);
                }

                started = true;
                Logger.Debug("Connection to Zookeeper server established");
            }
            catch (ThreadInterruptedException)
            {
                throw new InvalidOperationException(
                    "Not connected with zookeeper server yet. Current state is " + this.connection.ClientState);
            }
            finally
            {
                if (!started)
                {
                    this.Disconnect();
                }
            }
        }

        /// <summary>
        /// Closes current connection to ZooKeeper
        /// </summary>
        /// <remarks>
        /// Also, stops background thread
        /// </remarks>
        public void Disconnect()
        {
            Logger.Debug("Closing ZooKeeperClient...");
            this.shutdownTriggered = true;
            this.eventWorker.Interrupt();
            this.eventWorker.Join(2000);
            this.connection.Dispose();
            this.connection = null;
        }

        /// <summary>
        /// Re-connect to ZooKeeper server when session expired
        /// </summary>
        /// <param name="servers">
        /// The servers.
        /// </param>
        /// <param name="connectionTimeout">
        /// The connection timeout.
        /// </param>
        public void Reconnect(string servers, int connectionTimeout)
        {
            this.EnsuresNotDisposed();
            Logger.Debug("Reconnecting");
            this.connection.Dispose();
            this.connection = new ZooKeeperConnection(servers, connectionTimeout);
            this.connection.Connect(this);
            Logger.Debug("Reconnected");
        }

        /// <summary>
        /// Waits untill ZooKeeper connection is established
        /// </summary>
        /// <param name="connectionTimeout">
        /// The connection timeout.
        /// </param>
        /// <returns>
        /// Status
        /// </returns>
        public bool WaitUntilConnected(int connectionTimeout)
        {
            Guard.Assert<ArgumentOutOfRangeException>(() => connectionTimeout > 0);
            this.EnsuresNotDisposed();
            if (this.eventWorker != null && this.eventWorker == Thread.CurrentThread)
            {
                throw new InvalidOperationException("Must not be done in the ZooKeeper event thread.");
            }

            Logger.Debug("Waiting for keeper state: " + KeeperState.SyncConnected);
            bool stillWaiting = true;
            lock (this.stateChangedLock)
            {
                while (this.currentState != KeeperState.SyncConnected)
                {
                    if (!stillWaiting)
                    {
                        return false;
                    }

                    stillWaiting = Monitor.Wait(this.stateChangedLock, connectionTimeout);
                }

                Logger.Debug("State is " + this.currentState);
            }

            return true;
        }

        /// <summary>
        /// Retries given delegate until connections is established
        /// </summary>
        /// <param name="callback">
        /// The delegate to invoke.
        /// </param>
        /// <typeparam name="T">
        /// Type of data returned by delegate 
        /// </typeparam>
        /// <returns>
        /// data returned by delegate
        /// </returns>
        public T RetryUntilConnected<T>(Func<T> callback)
        {
            Guard.Assert<ArgumentNullException>(() => callback != null);
            this.EnsuresNotDisposed();
            if (this.zooKeeperEventWorker != null && this.zooKeeperEventWorker == Thread.CurrentThread)
            {
                throw new InvalidOperationException("Must not be done in the zookeeper event thread");
            }

            while (true)
            {
                try
                {
                    return callback();
                }
                catch (KeeperException.ConnectionLossException)
                {
                    Thread.Yield();
                    this.WaitUntilConnected(this.connection.SessionTimeout);
                }
                catch (KeeperException.SessionExpiredException)
                {
                    Thread.Yield();
                    this.WaitUntilConnected(this.connection.SessionTimeout);
                }
            }
        }

        /// <summary>
        /// Checks whether znode for a given path exists
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Result of check
        /// </returns>
        /// <remarks>
        /// Will reinstall watcher in ZooKeeper if any listener for given path exists 
        /// </remarks>
        public bool Exists(string path)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            bool hasListeners = this.HasListeners(path);
            return this.Exists(path, hasListeners);
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
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            return this.RetryUntilConnected(
                () => this.connection.Exists(path, watch));
        }

        /// <summary>
        /// Gets all children for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Children
        /// </returns>
        /// <remarks>
        /// Will reinstall watcher in ZooKeeper if any listener for given path exists 
        /// </remarks>
        public IList<string> GetChildren(string path)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            bool hasListeners = this.HasListeners(path);
            return this.GetChildren(path, hasListeners);
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
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            return this.RetryUntilConnected(
                () => this.connection.GetChildren(path, watch));
        }

        /// <summary>
        /// Counts number of children for a given path.
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Number of children 
        /// </returns>
        /// <remarks>
        /// Will reinstall watcher in ZooKeeper if any listener for given path exists.
        /// Returns 0 if path does not exist
        /// </remarks>
        public int CountChildren(string path)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            try
            {
                return this.GetChildren(path).Count;
            }
            catch (KeeperException.NoNodeException)
            {
                return 0;
            }
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
        /// <typeparam name="T">
        /// Expected type of data
        /// </typeparam>
        /// <returns>
        /// Data
        /// </returns>
        /// <remarks>
        /// Uses given serializer to deserialize data
        /// Use null for stats
        /// </remarks>
        public T ReadData<T>(string path, Stat stats, bool watch)
            where T : class 
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            byte[] bytes = this.RetryUntilConnected(
                () => this.connection.ReadData(path, stats, watch));
            return this.serializer.Deserialize(bytes) as T;
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
        /// <typeparam name="T">
        /// Expected type of data
        /// </typeparam>
        /// <returns>
        /// Data
        /// </returns>
        /// <remarks>
        /// Uses given serializer to deserialize data.
        /// Will reinstall watcher in ZooKeeper if any listener for given path exists.
        /// Use null for stats
        /// </remarks>
        public T ReadData<T>(string path, Stat stats) where T : class
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            bool hasListeners = this.HasListeners(path);
            return this.ReadData<T>(path, null, hasListeners);
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
        public void WriteData(string path, object data)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

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
        /// <param name="expectedVersion">
        /// Expected version of data
        /// </param>
        /// <remarks>
        /// Use -1 for expected version
        /// </remarks>
        public void WriteData(string path, object data, int expectedVersion)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            byte[] bytes = this.serializer.Serialize(data);
            this.RetryUntilConnected(
                () =>
                    {
                        this.connection.WriteData(path, bytes, expectedVersion);
                        return null as object;
                    });
        }

        /// <summary>
        /// Deletes znode for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Status
        /// </returns>
        public bool Delete(string path)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            return this.RetryUntilConnected(
                () =>
                    {
                        try
                        {
                            this.connection.Delete(path);
                            return true;
                        }
                        catch (KeeperException.NoNodeException)
                        {
                            return false;
                        }
                    });
        }

        /// <summary>
        /// Deletes znode and his children for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Status
        /// </returns>
        public bool DeleteRecursive(string path)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            IList<string> children;
            try
            {
                children = this.GetChildren(path, false);
            }
            catch (KeeperException.NoNodeException)
            {
                return true;
            }

            foreach (var child in children)
            {
                if (!this.DeleteRecursive(path + "/" + child))
                {
                    return false;
                }
            }

            return this.Delete(path);
        }

        /// <summary>
        /// Creates persistent znode and all intermediate znodes (if do not exist) for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        public void MakeSurePersistentPathExists(string path)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            if (!this.Exists(path))
            {
                this.CreatePersistent(path, true);
            }
        }

        /// <summary>
        /// Fetches children for a given path
        /// </summary>
        /// <param name="path">
        /// The path.
        /// </param>
        /// <returns>
        /// Children or null, if znode does not exist
        /// </returns>
        public IList<string> GetChildrenParentMayNotExist(string path)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            try
            {
                return this.GetChildren(path);
            }
            catch (KeeperException.NoNodeException)
            {
                return null;
            }
        }

        /// <summary>
        /// Fetches data from a given path in ZooKeeper
        /// </summary>
        /// <typeparam name="T">
        /// Expected type of data
        /// </typeparam>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Data or null, if znode does not exist
        /// </returns>
        public T ReadData<T>(string path)
            where T : class
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            return this.ReadData<T>(path, false);
        }

        /// <summary>
        /// Closes connection to ZooKeeper
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
                this.Disconnect();
            }
            catch (ThreadInterruptedException)
            {
            }
            catch (Exception exc)
            {
                Logger.Debug("Ignoring unexpected errors on closing ZooKeeperClient", exc);
            }

            Logger.Debug("Closing ZooKeeperClient... done");
        }

        /// <summary>
        /// Creates a persistent znode for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="createParents">
        /// Indicates, whether should create missing intermediate znodes
        /// </param>
        /// <remarks>
        /// Persistent znodes won't disappear after session close
        /// </remarks>
        public void CreatePersistent(string path, bool createParents)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.EnsuresNotDisposed();
            try
            {
                this.Create(path, null, CreateMode.Persistent);
            }
            catch (KeeperException.NodeExistsException)
            {
                if (!createParents)
                {
                    throw;
                }
            }
            catch (KeeperException.NoNodeException)
            {
                if (!createParents)
                {
                    throw;
                }

                string parentDir = path.Substring(0, path.LastIndexOf('/'));
                this.CreatePersistent(parentDir, true);
                this.CreatePersistent(path, true);
            }
        }

        /// <summary>
        /// Creates a persistent znode for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <remarks>
        /// Persistent znodes won't disappear after session close
        /// Doesn't re-create missing intermediate znodes
        /// </remarks>
        public void CreatePersistent(string path)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.CreatePersistent(path, false);
        }

        /// <summary>
        /// Creates a persistent znode for a given path and writes data into it
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="data">
        /// The data to write.
        /// </param>
        /// <remarks>
        /// Persistent znodes won't disappear after session close
        /// Doesn't re-create missing intermediate znodes
        /// </remarks>
        public void CreatePersistent(string path, object data)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.Create(path, data, CreateMode.Persistent);
        }

        /// <summary>
        /// Creates a sequential, persistent znode for a given path and writes data into it
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="data">
        /// The data to write.
        /// </param>
        /// <remarks>
        /// Persistent znodes won't disappear after session close
        /// Doesn't re-create missing intermediate znodes
        /// </remarks>
        /// <returns>
        /// The created znode's path
        /// </returns>
        public string CreatePersistentSequential(string path, object data)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            return this.Create(path, data, CreateMode.PersistentSequential);
        }

        /// <summary>
        /// Helper method to create znode
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
        private string Create(string path, object data, CreateMode mode)
        {
            if (path == null)
            {
                throw new ArgumentNullException("Path must not be null");
            }

            byte[] bytes = data == null ? null : this.serializer.Serialize(data);
            return this.RetryUntilConnected(() => 
                this.connection.Create(path, bytes, mode));
        }

        /// <summary>
        /// Creates a ephemeral znode for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <remarks>
        /// Ephemeral znodes will disappear after session close
        /// </remarks>
        public void CreateEphemeral(string path)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.Create(path, null, CreateMode.Ephemeral);
        }

        /// <summary>
        /// Creates a ephemeral znode for a given path and writes data into it
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="data">
        /// The data to write.
        /// </param>
        /// <remarks>
        /// Ephemeral znodes will disappear after session close
        /// </remarks>
        public void CreateEphemeral(string path, object data)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            this.Create(path, data, CreateMode.Ephemeral);
        }

        /// <summary>
        /// Creates a ephemeral, sequential znode for a given path and writes data into it
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="data">
        /// The data to write.
        /// </param>
        /// <remarks>
        /// Ephemeral znodes will disappear after session close
        /// </remarks>
        /// <returns>
        /// Created znode's path
        /// </returns>
        public string CreateEphemeralSequential(string path, object data)
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            return this.Create(path, data, CreateMode.EphemeralSequential);
        }

        /// <summary>
        /// Fetches data for given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="returnNullIfPathNotExists">
        /// Indicates, whether should return null or throw exception when 
        /// znode doesn't exist
        /// </param>
        /// <typeparam name="T">
        /// Expected type of data
        /// </typeparam>
        /// <returns>
        /// Data
        /// </returns>
        public T ReadData<T>(string path, bool returnNullIfPathNotExists)
            where T : class 
        {
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(path));

            try
            {
                return this.ReadData<T>(path, null);
            }
            catch (KeeperException.NoNodeException)
            {
                if (!returnNullIfPathNotExists)
                {
                    throw;
                }

                return null;
            }
        }

        /// <summary>
        /// Ensures that object wasn't disposed
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
