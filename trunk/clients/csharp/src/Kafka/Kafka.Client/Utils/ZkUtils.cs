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

namespace Kafka.Client.Utils
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using Kafka.Client.Cluster;
    using Kafka.Client.ZooKeeperIntegration;
    using log4net;
    using ZooKeeperNet;

    internal class ZkUtils
    {
        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        internal static void UpdatePersistentPath(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                zkClient.WriteData(path, data);
            }
            catch (KeeperException.NoNodeException)
            {
                CreateParentPath(zkClient, path);

                try
                {
                    zkClient.CreatePersistent(path, data);
                }
                catch (KeeperException.NodeExistsException)
                {
                    zkClient.WriteData(path, data);
                }
            }
        }

        internal static void CreateParentPath(IZooKeeperClient zkClient, string path)
        {
            string parentDir = path.Substring(0, path.LastIndexOf('/'));
            if (parentDir.Length != 0)
            {
                zkClient.CreatePersistent(parentDir, true);
            }
        }

        internal static void DeletePath(IZooKeeperClient zkClient, string path)
        {
            try
            {
                zkClient.Delete(path);
            }
            catch (KeeperException.NoNodeException)
            {
                Logger.InfoFormat(CultureInfo.CurrentCulture, "{0} deleted during connection loss; this is ok", path);
            }
        }

        internal static IDictionary<string, IList<string>> GetPartitionsForTopics(IZooKeeperClient zkClient, IEnumerable<string> topics)
        {
            var result = new Dictionary<string, IList<string>>();
            foreach (string topic in topics)
            {
                var partList = new List<string>();
                var brokers =
                    zkClient.GetChildrenParentMayNotExist(ZooKeeperClient.DefaultBrokerTopicsPath + "/" + topic);
                foreach (var broker in brokers)
                {
                    var numberOfParts =
                        int.Parse(
                            zkClient.ReadData<string>(ZooKeeperClient.DefaultBrokerTopicsPath + "/" + topic + "/" +
                                                      broker),
                                                      CultureInfo.CurrentCulture);
                    for (int i = 0; i < numberOfParts; i++)
                    {
                        partList.Add(broker + "-" + i);
                    }
                }

                partList.Sort();
                result.Add(topic, partList);
            }

            return result;
        }

        internal static void CreateEphemeralPathExpectConflict(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                CreateEphemeralPath(zkClient, path, data);
            }
            catch (KeeperException.NodeExistsException)
            {
                string storedData;
                try
                {
                    storedData = zkClient.ReadData<string>(path);
                }
                catch (KeeperException.NoNodeException)
                {
                    // the node disappeared; treat as if node existed and let caller handles this
                    throw;
                }

                if (storedData == null || storedData != data)
                {
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "conflict in {0} data: {1} stored data: {2}", path, data, storedData);
                    throw;
                }
                else
                {
                    // otherwise, the creation succeeded, return normally
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "{0} exits with value {1} during connection loss; this is ok", path, data);
                }
            }
        }

        internal static void CreateEphemeralPath(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                zkClient.CreateEphemeral(path, data);
            }
            catch (KeeperException.NoNodeException)
            {
                ZkUtils.CreateParentPath(zkClient, path);
                zkClient.CreateEphemeral(path, data);
            }
        }
    }
}
