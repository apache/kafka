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
    using Kafka.Client.Utils;

    public class SyncProducerConfig : ISyncProducerConfigShared
    {
        public const int DefaultBufferSize = 102400;

        public const int DefaultConnectTimeout = 5000;

        public const int DefaultSocketTimeout = 30000;

        public const int DefaultReconnectInterval = 30000;

        public const int DefaultMaxMessageSize = 1000000;

        public SyncProducerConfig()
        {
            this.BufferSize = DefaultBufferSize;
            this.ConnectTimeout = DefaultConnectTimeout;
            this.SocketTimeout = DefaultSocketTimeout;
            this.ReconnectInterval = DefaultReconnectInterval;
            this.MaxMessageSize = DefaultMaxMessageSize;
        }

        public SyncProducerConfig(KafkaClientConfiguration kafkaClientConfiguration) : this()
        {
            Guard.Assert<ArgumentNullException>(() => kafkaClientConfiguration != null);

            this.Host = kafkaClientConfiguration.KafkaServer.Address;
            this.Port = kafkaClientConfiguration.KafkaServer.Port;
        }

        public int BufferSize { get; set; }

        public int ConnectTimeout { get; set; }

        public int SocketTimeout { get; set; }

        public int ReconnectInterval { get; set; }

        public int MaxMessageSize { get; set; }

        public string Host { get; set; }

        public int Port { get; set; }
    }
}
