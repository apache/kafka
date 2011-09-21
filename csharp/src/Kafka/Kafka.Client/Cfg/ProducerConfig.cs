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
    using System;
    using System.Collections.Generic;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Utils;

    /// <summary>
    /// High-level API configuration for the producer
    /// </summary>
    public class ProducerConfig : ZKConfig, ISyncProducerConfigShared, IAsyncProducerConfigShared
    {
        public const ProducerTypes DefaultProducerType = ProducerTypes.Sync;
        public static readonly string DefaultPartitioner = typeof(DefaultPartitioner<>).FullName;

        public ProducerConfig()
        {
            this.ProducerType = DefaultProducerType;
            this.BufferSize = SyncProducerConfig.DefaultBufferSize;
            this.ConnectTimeout = SyncProducerConfig.DefaultConnectTimeout;
            this.SocketTimeout = SyncProducerConfig.DefaultSocketTimeout;
            this.ReconnectInterval = SyncProducerConfig.DefaultReconnectInterval;
            this.MaxMessageSize = SyncProducerConfig.DefaultMaxMessageSize;
            this.QueueTime = AsyncProducerConfig.DefaultQueueTime;
            this.QueueSize = AsyncProducerConfig.DefaultQueueSize;
            this.BatchSize = AsyncProducerConfig.DefaultBatchSize;
            this.SerializerClass = AsyncProducerConfig.DefaultSerializerClass; 
        }

        public ProducerConfig(KafkaClientConfiguration kafkaClientConfiguration) 
            : this()
        {
            Guard.Assert<ArgumentNullException>(() => kafkaClientConfiguration != null);
            if (kafkaClientConfiguration.IsZooKeeperEnabled)
            {
                this.ZkConnect = kafkaClientConfiguration.ZooKeeperServers.AddressList;
                this.ZkSessionTimeoutMs = kafkaClientConfiguration.ZooKeeperServers.SessionTimeout;
                this.ZkConnectionTimeoutMs = kafkaClientConfiguration.ZooKeeperServers.ConnectionTimeout;
            }

            this.BrokerPartitionInfo = kafkaClientConfiguration.GetBrokerPartitionInfosAsString();
        }

        public string BrokerPartitionInfo { get; set; }

        public string PartitionerClass { get; set; }

        public ProducerTypes ProducerType { get; set; }

        public int BufferSize { get; set; }

        public int ConnectTimeout { get; set; }

        public int SocketTimeout { get; set; }

        public int ReconnectInterval { get; set; }

        public int MaxMessageSize { get; set; }

        public int QueueTime { get; set; }

        public int QueueSize { get; set; }

        public int BatchSize { get; set; }

        public string SerializerClass { get; set; }

        public string CallbackHandler { get; set; }

        public string EventHandler { get; set; }

        public IDictionary<string, string> CallbackHandlerProps { get; set; }

        public IDictionary<string, string> EventHandlerProps { get; set; }
    }
}
