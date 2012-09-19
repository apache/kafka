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

namespace Kafka.Client.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Requests;

    public static class TestHelper
    {
        public static long GetCurrentKafkaOffset(string topic, ConsumerConfiguration clientConfig)
        {
            return GetCurrentKafkaOffset(topic, clientConfig.Broker.Host, clientConfig.Broker.Port);
        }

        public static long GetCurrentKafkaOffset(string topic, string address, int port)
        {
            return GetCurrentKafkaOffset(topic, address, port, 0);
        }

        public static long GetCurrentKafkaOffset(string topic, string address, int port, int partition)
        {
            var request = new OffsetRequest(topic, partition, DateTime.Now.AddDays(-5).Ticks, 10);
            var consumerConfig = new ConsumerConfiguration(address, port);
            IConsumer consumer = new Consumer(consumerConfig, address, port);
            IList<long> list = consumer.GetOffsetsBefore(request);
            return list.Sum();
        }
    }
}
