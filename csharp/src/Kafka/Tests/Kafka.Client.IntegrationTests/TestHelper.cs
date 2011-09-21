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

namespace Kafka.Client.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Requests;

    public static class TestHelper
    {
        public static long GetCurrentKafkaOffset(string topic, KafkaClientConfiguration clientConfig)
        {
            return GetCurrentKafkaOffset(topic, clientConfig.KafkaServer.Address, clientConfig.KafkaServer.Port);
        }

        public static long GetCurrentKafkaOffset(string topic, string address, int port)
        {
            OffsetRequest request = new OffsetRequest(topic, 0, DateTime.Now.AddDays(-5).Ticks, 10);
            ConsumerConfig consumerConfig = new ConsumerConfig();
            consumerConfig.Host = address;
            consumerConfig.Port = port;
            IConsumer consumer = new Consumers.Consumer(consumerConfig);
            IList<long> list = consumer.GetOffsetsBefore(request);
            if (list.Count > 0)
            {
                return list[0];
            }
            else
            {
                return 0;
            }
        }
    }
}
