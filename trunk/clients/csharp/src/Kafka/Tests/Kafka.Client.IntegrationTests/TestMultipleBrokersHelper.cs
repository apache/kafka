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
    using System.Collections.Generic;
    using Kafka.Client.Cfg;

    public class TestMultipleBrokersHelper
    {
        private readonly Dictionary<int, Dictionary<int, long>> offsets = new Dictionary<int, Dictionary<int, long>>();

        private readonly string topic;

        public TestMultipleBrokersHelper(string topic)
        {
            this.topic = topic;
        }

        public SyncProducerConfiguration BrokerThatHasChanged { get; private set; }

        public int PartitionThatHasChanged { get; private set; }

        public long OffsetFromBeforeTheChange
        {
            get
            {
                return this.BrokerThatHasChanged != null ? this.offsets[this.BrokerThatHasChanged.BrokerId][this.PartitionThatHasChanged] : 0;
            }
        }

        public void GetCurrentOffsets(IEnumerable<SyncProducerConfiguration> brokers)
        {
            foreach (var broker in brokers)
            {
                offsets.Add(broker.BrokerId, new Dictionary<int, long>());
                offsets[broker.BrokerId].Add(0, TestHelper.GetCurrentKafkaOffset(topic, broker.Host, broker.Port, 0));
                offsets[broker.BrokerId].Add(1, TestHelper.GetCurrentKafkaOffset(topic, broker.Host, broker.Port, 1));
            }
        }

        public bool CheckIfAnyBrokerHasChanged(IEnumerable<SyncProducerConfiguration> brokers)
        {
            foreach (var broker in brokers)
            {
                if (TestHelper.GetCurrentKafkaOffset(topic, broker.Host, broker.Port, 0) != offsets[broker.BrokerId][0])
                {
                    this.BrokerThatHasChanged = broker;
                    this.PartitionThatHasChanged = 0;
                    return true;
                }

                if (TestHelper.GetCurrentKafkaOffset(topic, broker.Host, broker.Port, 1) != offsets[broker.BrokerId][1])
                {
                    this.BrokerThatHasChanged = broker;
                    this.PartitionThatHasChanged = 1;
                    return true;
                }
            }

            return false;
        }
    }
}
