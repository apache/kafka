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
        private BrokerPartitionInfoCollection configBrokers =
            KafkaClientConfiguration.GetConfiguration().BrokerPartitionInfos;

        private Dictionary<int, long> offsets = new Dictionary<int, long>();

        private BrokerPartitionInfo changedBroker;

        private string topic;

        public TestMultipleBrokersHelper(string topic)
        {
            this.topic = topic;
        }

        public BrokerPartitionInfo BrokerThatHasChanged
        {
            get { return changedBroker; }
        }

        public long OffsetFromBeforeTheChange
        {
            get
            {
                if (changedBroker != null)
                {
                    return offsets[changedBroker.Id];
                }
                else
                {
                    return 0;
                }
            }
        }

        public void GetCurrentOffsets()
        {
            foreach (BrokerPartitionInfo broker in configBrokers)
            {
                offsets.Add(broker.Id, TestHelper.GetCurrentKafkaOffset(topic, broker.Address, broker.Port));
            }
        }

        public bool CheckIfAnyBrokerHasChanged()
        {
            foreach (BrokerPartitionInfo broker in configBrokers)
            {
                if (TestHelper.GetCurrentKafkaOffset(topic, broker.Address, broker.Port) != offsets[broker.Id])
                {
                    changedBroker = broker;
                    return true;
                }
            }

            return false;
        }
    }
}
