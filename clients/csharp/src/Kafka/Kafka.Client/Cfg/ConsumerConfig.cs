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

namespace Kafka.Client.Cfg
{
    /// <summary>
    /// Configuration used by the consumer
    /// </summary>
    public class ConsumerConfig : ZKConfig
    {
        public const short DefaultNumberOfTries = 2;

        public short NumberOfTries { get; set; }

        public string Host { get; set; }

        public int Port { get; set; }

        public string GroupId { get; set; }

        public int Timeout { get; set; }

        public string AutoOffsetReset { get; set; }

        public bool AutoCommit { get; set; }

        public int AutoCommitIntervalMs { get; set; }

        public int FetchSize { get; set; }

        public int BackOffIncrementMs { get; set; }

        public ConsumerConfig()
        {
            this.NumberOfTries = DefaultNumberOfTries;
        }

        public ConsumerConfig(KafkaClientConfiguration kafkaClientConfiguration) : this()
        {
            this.Host = kafkaClientConfiguration.KafkaServer.Address;
            this.Port = kafkaClientConfiguration.KafkaServer.Port;
            this.NumberOfTries = kafkaClientConfiguration.Consumer.NumberOfTries;
            this.GroupId = kafkaClientConfiguration.Consumer.GroupId;
            this.Timeout = kafkaClientConfiguration.Consumer.Timeout;
            this.AutoOffsetReset = kafkaClientConfiguration.Consumer.AutoOffsetReset;
            this.AutoCommit = kafkaClientConfiguration.Consumer.AutoCommit;
            this.AutoCommitIntervalMs = kafkaClientConfiguration.Consumer.AutoCommitIntervalMs;
            this.FetchSize = kafkaClientConfiguration.Consumer.FetchSize;
            this.BackOffIncrementMs = kafkaClientConfiguration.Consumer.BackOffIncrementMs;
            if (kafkaClientConfiguration.IsZooKeeperEnabled)
            {
                this.ZkConnect = kafkaClientConfiguration.ZooKeeperServers.AddressList;
                this.ZkSessionTimeoutMs = kafkaClientConfiguration.ZooKeeperServers.SessionTimeout;
                this.ZkConnectionTimeoutMs = kafkaClientConfiguration.ZooKeeperServers.ConnectionTimeout;
            }
        }
    }
}
