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

namespace Kafka.Client.ZooKeeperIntegration.Listeners
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using Kafka.Client.Cluster;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration.Events;
    using log4net;

    /// <summary>
    /// Listens to new broker registrations under a particular topic, in zookeeper and
    /// keeps the related data structures updated
    /// </summary>
    internal class BrokerTopicsListener : IZooKeeperChildListener
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly IDictionary<int, Broker> actualBrokerIdMap;
        private readonly Action<int, string, int> callback;
        private readonly IDictionary<string, SortedSet<Partition>> actualBrokerTopicsPartitionsMap;
        private IDictionary<int, Broker> oldBrokerIdMap;
        private IDictionary<string, SortedSet<Partition>> oldBrokerTopicsPartitionsMap;
        private readonly IZooKeeperClient zkclient;
        private readonly object syncLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="BrokerTopicsListener"/> class.
        /// </summary>
        /// <param name="zkclient">The wrapper on ZooKeeper client.</param>
        /// <param name="actualBrokerTopicsPartitionsMap">The actual broker topics partitions map.</param>
        /// <param name="actualBrokerIdMap">The actual broker id map.</param>
        /// <param name="callback">The callback invoked after new broker is added.</param>
        public BrokerTopicsListener(
            IZooKeeperClient zkclient,
            IDictionary<string, SortedSet<Partition>> actualBrokerTopicsPartitionsMap, 
            IDictionary<int, Broker> actualBrokerIdMap, 
            Action<int, string, int> callback)
        {
            this.zkclient = zkclient;
            this.actualBrokerTopicsPartitionsMap = actualBrokerTopicsPartitionsMap;
            this.actualBrokerIdMap = actualBrokerIdMap;
            this.callback = callback;
            this.oldBrokerIdMap = new Dictionary<int, Broker>(this.actualBrokerIdMap);
            this.oldBrokerTopicsPartitionsMap = new Dictionary<string, SortedSet<Partition>>(this.actualBrokerTopicsPartitionsMap);
            Logger.Debug("Creating broker topics listener to watch the following paths - \n"
                + "/broker/topics, /broker/topics/topic, /broker/ids");
            Logger.Debug("Initialized this broker topics listener with initial mapping of broker id to "
                + "partition id per topic with " + this.oldBrokerTopicsPartitionsMap.ToMultiString(
                    x => x.Key + " --> " + x.Value.ToMultiString(y => y.ToString(), ","), "; "));
        }

        /// <summary>
        /// Called when the children of the given path changed
        /// </summary>
        /// <param name="e">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperChildChangedEventArgs"/> instance containing the event data
        /// as parent path and children (null if parent was deleted).
        /// </param>
        public void HandleChildChange(ZooKeeperChildChangedEventArgs e)
        {
            Guard.Assert<ArgumentNullException>(() => e != null);
            Guard.Assert<ArgumentException>(() => !string.IsNullOrEmpty(e.Path));
            Guard.Assert<ArgumentNullException>(() => e.Children != null);

            lock (this.syncLock)
            {
                try
                {
                    string path = e.Path;
                    IList<string> childs = e.Children;
                    Logger.Debug("Watcher fired for path: " + path);
                    switch (path)
                    {
                        case ZooKeeperClient.DefaultBrokerTopicsPath:
                            List<string> oldTopics = this.oldBrokerTopicsPartitionsMap.Keys.ToList();
                            List<string> newTopics = childs.Except(oldTopics).ToList();
                            Logger.Debug("List of topics was changed at " + e.Path);
                            Logger.Debug("Current topics -> " + e.Children.ToMultiString(","));
                            Logger.Debug("Old list of topics -> " + oldTopics.ToMultiString(","));
                            Logger.Debug("List of newly registered topics -> " + newTopics.ToMultiString(","));
                            foreach (var newTopic in newTopics)
                            {
                                string brokerTopicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + newTopic;
                                IList<string> brokerList = this.zkclient.GetChildrenParentMayNotExist(brokerTopicPath);
                                this.ProcessNewBrokerInExistingTopic(newTopic, brokerList);
                                this.zkclient.Subscribe(ZooKeeperClient.DefaultBrokerTopicsPath + "/" + newTopic, this);
                            }

                            break;
                        case ZooKeeperClient.DefaultBrokerIdsPath:
                            Logger.Debug("List of brokers changed in the Kafka cluster " + e.Path);
                            Logger.Debug("Currently registered list of brokers -> " + e.Children.ToMultiString(","));
                            this.ProcessBrokerChange(path, childs);
                            break;
                        default:
                            string[] parts = path.Split('/');
                            string topic = parts.Last();
                            if (parts.Length == 4 && parts[2] == "topics" && childs != null)
                            {
                                Logger.Debug("List of brokers changed at " + path);
                                Logger.Debug(
                                    "Currently registered list of brokers for topic " + topic + " -> " +
                                    childs.ToMultiString(","));
                                this.ProcessNewBrokerInExistingTopic(topic, childs);
                            }

                            break;
                    }

                    this.oldBrokerTopicsPartitionsMap = this.actualBrokerTopicsPartitionsMap;
                    this.oldBrokerIdMap = this.actualBrokerIdMap;
                }
                catch (Exception exc)
                {
                    Logger.Debug("Error while handling " + e, exc);
                }
            }
        }

        /// <summary>
        /// Resets the state of listener.
        /// </summary>
        public void ResetState()
        {
            Logger.Debug("Before reseting broker topic partitions state -> " 
                + this.oldBrokerTopicsPartitionsMap.ToMultiString(
                x => x.Key + " --> " + x.Value.ToMultiString(y => y.ToString(), ","), "; "));
            this.oldBrokerTopicsPartitionsMap = actualBrokerTopicsPartitionsMap;
            Logger.Debug("After reseting broker topic partitions state -> "
                + this.oldBrokerTopicsPartitionsMap.ToMultiString(
                x => x.Key + " --> " + x.Value.ToMultiString(y => y.ToString(), ","), "; "));
            Logger.Debug("Before reseting broker id map state -> "
                + this.oldBrokerIdMap.ToMultiString(", "));
            this.oldBrokerIdMap = this.actualBrokerIdMap;
            Logger.Debug("After reseting broker id map state -> "
                + this.oldBrokerIdMap.ToMultiString(", "));
        }

        /// <summary>
        /// Generate the updated mapping of (brokerId, numPartitions) for the new list of brokers
        /// registered under some topic.
        /// </summary>
        /// <param name="topic">The path of the topic under which the brokers have changed..</param>
        /// <param name="childs">The list of changed brokers.</param>
        private void ProcessNewBrokerInExistingTopic(string topic, IEnumerable<string> childs)
        {
            if (this.actualBrokerTopicsPartitionsMap.ContainsKey(topic))
            {
                Logger.Debug("Old list of brokers -> " + this.oldBrokerTopicsPartitionsMap[topic].ToMultiString(x => x.BrokerId.ToString(), ","));
            }

            var updatedBrokers = new SortedSet<int>(childs.Select(x => int.Parse(x, CultureInfo.InvariantCulture)));
            string brokerTopicPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + topic;
            var sortedBrokerPartitions = new SortedDictionary<int, int>();
            foreach (var bid in updatedBrokers)
            {
                var num = this.zkclient.ReadData<string>(brokerTopicPath + "/" + bid);
                sortedBrokerPartitions.Add(bid, int.Parse(num, CultureInfo.InvariantCulture));
            }

            var updatedBrokerParts = new SortedSet<Partition>();
            foreach (var bp in sortedBrokerPartitions)
            {
                for (int i = 0; i < bp.Value; i++)
                {
                    var bidPid = new Partition(bp.Key, i);
                    updatedBrokerParts.Add(bidPid);
                }
            }

            Logger.Debug("Currently registered list of brokers for topic " + topic + " -> " + childs.ToMultiString(", "));
            SortedSet<Partition> mergedBrokerParts = updatedBrokerParts;
            if (this.actualBrokerTopicsPartitionsMap.ContainsKey(topic))
            {
                SortedSet<Partition> oldBrokerParts = this.actualBrokerTopicsPartitionsMap[topic];
                Logger.Debug(
                    "Unregistered list of brokers for topic " + topic + " -> " + oldBrokerParts.ToMultiString(", "));
                foreach (var oldBrokerPart in oldBrokerParts)
                {
                    mergedBrokerParts.Add(oldBrokerPart);
                }
            }
            else
            {
                this.actualBrokerTopicsPartitionsMap.Add(topic, null);
            }

            this.actualBrokerTopicsPartitionsMap[topic] = new SortedSet<Partition>(mergedBrokerParts.Where(x => this.actualBrokerIdMap.ContainsKey(x.BrokerId)));
        }

        /// <summary>
        /// Processes change in the broker lists.
        /// </summary>
        /// <param name="path">The parent path of brokers list.</param>
        /// <param name="childs">The current brokers.</param>
        private void ProcessBrokerChange(string path, IEnumerable<string> childs)
        {
            if (path != ZooKeeperClient.DefaultBrokerIdsPath)
            {
                return;
            }

            List<int> updatedBrokers = childs.Select(x => int.Parse(x, CultureInfo.InvariantCulture)).ToList();
            List<int> oldBrokers = this.oldBrokerIdMap.Select(x => x.Key).ToList();
            List<int> newBrokers = updatedBrokers.Except(oldBrokers).ToList();
            Logger.Debug("List of newly registered brokers -> " + newBrokers.ToMultiString(","));
            foreach (int bid in newBrokers)
            {
                string brokerInfo = this.zkclient.ReadData<string>(ZooKeeperClient.DefaultBrokerIdsPath + "/" + bid);
                string[] brokerHost = brokerInfo.Split(':');
                var port = int.Parse(brokerHost[2], CultureInfo.InvariantCulture); 
                this.actualBrokerIdMap.Add(bid, new Broker(bid, brokerHost[1], brokerHost[1], port));
                if (this.callback != null)
                {
                    Logger.Debug("Invoking the callback for broker: " + bid);
                    this.callback(bid, brokerHost[1], port);
                }
            }

            List<int> deadBrokers = oldBrokers.Except(updatedBrokers).ToList();
            Logger.Debug("Deleting broker ids for dead brokers -> " + deadBrokers.ToMultiString(","));
            foreach (int bid in deadBrokers)
            {
                Logger.Debug("Deleting dead broker: " + bid);
                this.actualBrokerIdMap.Remove(bid);
                foreach (var topicMap in this.actualBrokerTopicsPartitionsMap)
                {
                    int affected = topicMap.Value.RemoveWhere(x => x.BrokerId == bid);
                    if (affected > 0)
                    {
                        Logger.Debug("Removing dead broker " + bid + " for topic: " + topicMap.Key);
                        Logger.Debug("Actual list of mapped brokers is -> " + topicMap.Value.ToMultiString(x => x.ToString(), ","));
                    }
                }
            }
        }
    }
}
