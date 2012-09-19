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
    using System;
    using System.Collections.Generic;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Configuration used by the asynchronous producer
    /// </summary>
    public class AsyncProducerConfig : SyncProducerConfig, IAsyncProducerConfigShared
    {
        public const int DefaultQueueTime = 5000;

        public const int DefaultQueueSize = 10000;

        public const int DefaultBatchSize = 200;

        public static readonly string DefaultSerializerClass = typeof(DefaultEncoder).FullName; 

        public AsyncProducerConfig()
        {
            this.QueueTime = DefaultQueueTime;
            this.QueueSize = DefaultQueueSize;
            this.BatchSize = DefaultBatchSize;
            this.SerializerClass = DefaultSerializerClass;  
        }

        public AsyncProducerConfig(KafkaClientConfiguration kafkaClientConfiguration) 
            : this()
        {
            Guard.Assert<ArgumentNullException>(() => kafkaClientConfiguration != null);
            Guard.Assert<ArgumentNullException>(() => kafkaClientConfiguration.KafkaServer != null);
            Guard.Assert<ArgumentNullException>(() => kafkaClientConfiguration.KafkaServer.Address != null);
            Guard.Assert<ArgumentOutOfRangeException>(() => kafkaClientConfiguration.KafkaServer.Port > 0);

            this.Host = kafkaClientConfiguration.KafkaServer.Address;
            this.Port = kafkaClientConfiguration.KafkaServer.Port;
        }

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
