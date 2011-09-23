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

namespace Kafka.Client.Cfg
{
    using System.Configuration;
    using System.Text;

    /// <summary>
    /// Implementation of the custom configuration section for the kafka client
    /// </summary>
    public class KafkaClientConfiguration : ConfigurationSection
    {
        private static KafkaClientConfiguration config = ConfigurationManager.GetSection("kafkaClientConfiguration") as KafkaClientConfiguration;
        private bool enabled = true;

        public static KafkaClientConfiguration GetConfiguration()
        {
            config.enabled = !string.IsNullOrEmpty(config.ZooKeeperServers.AddressList);
            return config;
        }

        [ConfigurationProperty("kafkaServer")]
        public KafkaServer KafkaServer
        {
            get { return (KafkaServer)this["kafkaServer"]; }
            set { this["kafkaServer"] = value; }
        }

        [ConfigurationProperty("consumer")]
        public Consumer Consumer
        {
            get { return (Consumer)this["consumer"]; }
            set { this["consumer"] = value; }
        }

        [ConfigurationProperty("brokerPartitionInfos")]
        public BrokerPartitionInfoCollection BrokerPartitionInfos
        {
            get
            {
                return (BrokerPartitionInfoCollection)this["brokerPartitionInfos"] ??
                       new BrokerPartitionInfoCollection();
            }
        }

        [ConfigurationProperty("zooKeeperServers")]
        public ZooKeeperServers ZooKeeperServers
        {
            get { return (ZooKeeperServers)this["zooKeeperServers"]; }
            set { this["zooKeeperServers"] = value; }
        }

        public string GetBrokerPartitionInfosAsString()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < BrokerPartitionInfos.Count; i++)
            {
                sb.Append(BrokerPartitionInfos[i].GetBrokerPartitionInfoAsString());
                if ((i + 1) < BrokerPartitionInfos.Count)
                {
                    sb.Append(",");
                }
            }

            return sb.ToString();
        }

        internal void SupressZooKeeper()
        {
            this.enabled = false;
        }

        public bool IsZooKeeperEnabled
        {
            get { return this.enabled; }
        }
    }
}
