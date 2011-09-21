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
    using Kafka.Client.ZooKeeperIntegration.Listeners;
    using Org.Apache.Zookeeper.Data;
    using ZooKeeperNet;

    /// <summary>
    /// Abstracts the interaction with zookeeper
    /// </summary>
    internal interface IZooKeeperClient : IWatcher, IDisposable
    {
        /// <summary>
        /// Gets time (in miliseconds) of event thread idleness
        /// </summary>
        /// <remarks>
        /// Used for testing purpose
        /// </remarks>
        int IdleTime { get; }

        /// <summary>
        /// Connects to ZooKeeper server within given time period and installs watcher in ZooKeeper
        /// </summary>
        void Connect();

        /// <summary>
        /// Closes current connection to ZooKeeper
        /// </summary>
        void Disconnect();

        /// <summary>
        /// Re-connect to ZooKeeper server when session expired
        /// </summary>
        /// <param name="servers">
        /// The servers.
        /// </param>
        /// <param name="connectionTimeout">
        /// The connection timeout.
        /// </param>
        void Reconnect(string servers, int connectionTimeout);

        /// <summary>
        /// Waits untill ZooKeeper connection is established
        /// </summary>
        /// <param name="connectionTimeout">
        /// The connection timeout.
        /// </param>
        /// <returns>
        /// Status
        /// </returns>
        bool WaitUntilConnected(int connectionTimeout);

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
        T RetryUntilConnected<T>(Func<T> callback);

        /// <summary>
        /// Subscribes listeners on ZooKeeper state changes events
        /// </summary>
        /// <param name="listener">
        /// The listener.
        /// </param>
        void Subscribe(IZooKeeperStateListener listener);

        /// <summary>
        /// Un-subscribes listeners on ZooKeeper state changes events
        /// </summary>
        /// <param name="listener">
        /// The listener.
        /// </param>
        void Unsubscribe(IZooKeeperStateListener listener);

        /// <summary>
        /// Subscribes listeners on ZooKeeper child changes under given path
        /// </summary>
        /// <param name="path">
        /// The parent path.
        /// </param>
        /// <param name="listener">
        /// The listener.
        /// </param>
        void Subscribe(string path, IZooKeeperChildListener listener);

        /// <summary>
        /// Un-subscribes listeners on ZooKeeper child changes under given path
        /// </summary>
        /// <param name="path">
        /// The parent path.
        /// </param>
        /// <param name="listener">
        /// The listener.
        /// </param>
        void Unsubscribe(string path, IZooKeeperChildListener listener);

        /// <summary>
        /// Subscribes listeners on ZooKeeper data changes under given path
        /// </summary>
        /// <param name="path">
        /// The parent path.
        /// </param>
        /// <param name="listener">
        /// The listener.
        /// </param>
        void Subscribe(string path, IZooKeeperDataListener listener);

        /// <summary>
        /// Un-subscribes listeners on ZooKeeper data changes under given path
        /// </summary>
        /// <param name="path">
        /// The parent path.
        /// </param>
        /// <param name="listener">
        /// The listener.
        /// </param>
        void Unsubscribe(string path, IZooKeeperDataListener listener);

        /// <summary>
        /// Un-subscribes all listeners
        /// </summary>
        void UnsubscribeAll();

        /// <summary>
        /// Installs a child watch for the given path. 
        /// </summary>
        /// <param name="path">
        /// The parent path.
        /// </param>
        /// <returns>
        /// the current children of the path or null if the znode with the given path doesn't exist
        /// </returns>
        IList<string> WatchForChilds(string path);

        /// <summary>
        /// Installs a data watch for the given path. 
        /// </summary>
        /// <param name="path">
        /// The parent path.
        /// </param>
        void WatchForData(string path);

        /// <summary>
        /// Checks whether znode for a given path exists
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Result of check
        /// </returns>
        bool Exists(string path);

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
        /// <returns>
        /// Children
        /// </returns>
        IList<string> GetChildren(string path);

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
        /// Counts number of children for a given path.
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Number of children 
        /// </returns>
        int CountChildren(string path);

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
        T ReadData<T>(string path, Stat stats, bool watch)
            where T : class;

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
        T ReadData<T>(string path, Stat stats)
            where T : class;

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
        T ReadData<T>(string path)
            where T : class;

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
        T ReadData<T>(string path, bool returnNullIfPathNotExists)
            where T : class;

        /// <summary>
        /// Writes data for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="data">
        /// The data to write.
        /// </param>
        void WriteData(string path, object data);

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
        void WriteData(string path, object data, int expectedVersion);

        /// <summary>
        /// Deletes znode for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Status
        /// </returns>
        bool Delete(string path);

        /// <summary>
        /// Deletes znode and his children for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <returns>
        /// Status
        /// </returns>
        bool DeleteRecursive(string path);

        /// <summary>
        /// Creates persistent znode and all intermediate znodes (if do not exist) for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        void MakeSurePersistentPathExists(string path);

        /// <summary>
        /// Fetches children for a given path
        /// </summary>
        /// <param name="path">
        /// The path.
        /// </param>
        /// <returns>
        /// Children or null, if znode does not exist
        /// </returns>
        IList<string> GetChildrenParentMayNotExist(string path);

        /// <summary>
        /// Creates a persistent znode for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <param name="createParents">
        /// Indicates whether should create all intermediate znodes
        /// </param>
        /// <remarks>
        /// Persistent znodes won't disappear after session close
        /// Doesn't re-create missing intermediate znodes
        /// </remarks>
        void CreatePersistent(string path, bool createParents);

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
        void CreatePersistent(string path);

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
        /// </remarks>
        void CreatePersistent(string path, object data);

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
        /// Persistent znodes won't dissapear after session close
        /// </remarks>
        /// <returns>
        /// The created znode's path
        /// </returns>
        string CreatePersistentSequential(string path, object data);

        /// <summary>
        /// Creates a ephemeral znode for a given path
        /// </summary>
        /// <param name="path">
        /// The given path.
        /// </param>
        /// <remarks>
        /// Ephemeral znodes will disappear after session close
        /// </remarks>
        void CreateEphemeral(string path);

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
        void CreateEphemeral(string path, object data);

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
        string CreateEphemeralSequential(string path, object data);
    }
}
