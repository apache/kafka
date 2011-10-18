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
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.ZooKeeperIntegration;
    using NUnit.Framework;

    public abstract class IntegrationFixtureBase
    {
        protected string CurrentTestTopic { get; set; }

        protected ProducerConfiguration ConfigBasedSyncProdConfig
        {
            get
            {
                return ProducerConfiguration.Configure(ProducerConfiguration.DefaultSectionName);
            }
        }

        protected SyncProducerConfiguration SyncProducerConfig1
        {
            get
            {
                var prodConfig = this.ConfigBasedSyncProdConfig;
                return new SyncProducerConfiguration(
                    prodConfig,
                    prodConfig.Brokers[0].BrokerId,
                    prodConfig.Brokers[0].Host,
                    prodConfig.Brokers[0].Port);
            }
        }

        protected SyncProducerConfiguration SyncProducerConfig2
        {
            get
            {
                var prodConfig = this.ConfigBasedSyncProdConfig;
                return new SyncProducerConfiguration(
                    prodConfig,
                    prodConfig.Brokers[1].BrokerId,
                    prodConfig.Brokers[1].Host,
                    prodConfig.Brokers[1].Port);
            }
        }

        protected SyncProducerConfiguration SyncProducerConfig3
        {
            get
            {
                var prodConfig = this.ConfigBasedSyncProdConfig;
                return new SyncProducerConfiguration(
                    prodConfig,
                    prodConfig.Brokers[2].BrokerId,
                    prodConfig.Brokers[2].Host,
                    prodConfig.Brokers[2].Port);
            }
        }

        protected ProducerConfiguration ZooKeeperBasedSyncProdConfig
        {
            get
            {
                return ProducerConfiguration.Configure(ProducerConfiguration.DefaultSectionName + 2);
            }
        }

        protected AsyncProducerConfiguration AsyncProducerConfig1
        {
            get
            {
                var asyncUberConfig = ProducerConfiguration.Configure(ProducerConfiguration.DefaultSectionName + 3);
                return new AsyncProducerConfiguration(
                    asyncUberConfig,
                    asyncUberConfig.Brokers[0].BrokerId,
                    asyncUberConfig.Brokers[0].Host,
                    asyncUberConfig.Brokers[0].Port);
            }
        }

        protected ConsumerConfiguration ConsumerConfig1
        {
            get
            {
                return ConsumerConfiguration.Configure(ConsumerConfiguration.DefaultSection + 1);
            }
        }

        protected ConsumerConfiguration ConsumerConfig2
        {
            get
            {
                return ConsumerConfiguration.Configure(ConsumerConfiguration.DefaultSection + 2);
            }
        }

        protected ConsumerConfiguration ConsumerConfig3
        {
            get
            {
                return ConsumerConfiguration.Configure(ConsumerConfiguration.DefaultSection + 3);
            }
        }

        protected ConsumerConfiguration ZooKeeperBasedConsumerConfig
        {
            get
            {
                return ConsumerConfiguration.Configure(ConsumerConfiguration.DefaultSection + 4);
            }
        }

        [SetUp]
        public void SetupCurrentTestTopic()
        {
            CurrentTestTopic = TestContext.CurrentContext.Test.Name + "_" + Guid.NewGuid();
        }

        internal static void WaitUntillIdle(IZooKeeperClient client, int timeout)
        {
            Thread.Sleep(timeout);
            int rest = client.IdleTime.HasValue ? timeout - client.IdleTime.Value : timeout;
            while (rest > 0)
            {
                Thread.Sleep(rest);
                rest = client.IdleTime.HasValue ? timeout - client.IdleTime.Value : timeout;
            }
        }
    }
}
