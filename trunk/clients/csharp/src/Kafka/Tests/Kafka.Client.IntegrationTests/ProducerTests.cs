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
    using System.Linq;
    using System.Text;
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using NUnit.Framework;

    [TestFixture]
    public class ProducerTests : IntegrationFixtureBase
    {
        /// <summary>
        /// Maximum amount of time to wait trying to get a specific test message from Kafka server (in miliseconds)
        /// </summary>
        private readonly int maxTestWaitTimeInMiliseconds = 5000;

        [Test]
        public void ProducerSends1Message()
        {
            var prodConfig = this.ConfigBasedSyncProdConfig;

            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            var originalMessage = new Message(Encoding.UTF8.GetBytes("TestData"));

            var multipleBrokersHelper = new TestMultipleBrokersHelper(CurrentTestTopic);
            multipleBrokersHelper.GetCurrentOffsets(
                new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 });
            using (var producer = new Producer(prodConfig))
            {
                var producerData = new ProducerData<string, Message>(
                    CurrentTestTopic, new List<Message> { originalMessage });
                producer.Send(producerData);
                Thread.Sleep(waitSingle);
            }

            while (
                !multipleBrokersHelper.CheckIfAnyBrokerHasChanged(
                    new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 }))
            {
                totalWaitTimeInMiliseconds += waitSingle;
                Thread.Sleep(waitSingle);
                if (totalWaitTimeInMiliseconds > this.maxTestWaitTimeInMiliseconds)
                {
                    Assert.Fail("None of the brokers changed their offset after sending a message");
                }
            }

            totalWaitTimeInMiliseconds = 0;

            var consumerConfig = new ConsumerConfiguration(
                multipleBrokersHelper.BrokerThatHasChanged.Host, multipleBrokersHelper.BrokerThatHasChanged.Port);
            IConsumer consumer = new Consumer(consumerConfig);
            var request1 = new FetchRequest(CurrentTestTopic, multipleBrokersHelper.PartitionThatHasChanged, multipleBrokersHelper.OffsetFromBeforeTheChange);
            BufferedMessageSet response;
            while (true)
            {
                Thread.Sleep(waitSingle);
                response = consumer.Fetch(request1);
                if (response != null && response.Messages.Count() > 0)
                {
                    break;
                }

                totalWaitTimeInMiliseconds += waitSingle;
                if (totalWaitTimeInMiliseconds >= this.maxTestWaitTimeInMiliseconds)
                {
                    break;
                }
            }

            Assert.NotNull(response);
            Assert.AreEqual(1, response.Messages.Count());
            Assert.AreEqual(originalMessage.ToString(), response.Messages.First().ToString());
        }

        [Test]
        public void ProducerSends3Messages()
        {
            var prodConfig = this.ConfigBasedSyncProdConfig;

            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            var originalMessage1 = new Message(Encoding.UTF8.GetBytes("TestData1"));
            var originalMessage2 = new Message(Encoding.UTF8.GetBytes("TestData2"));
            var originalMessage3 = new Message(Encoding.UTF8.GetBytes("TestData3"));
            var originalMessageList = new List<Message> { originalMessage1, originalMessage2, originalMessage3 };

            var multipleBrokersHelper = new TestMultipleBrokersHelper(CurrentTestTopic);
            multipleBrokersHelper.GetCurrentOffsets(
                new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 });
            using (var producer = new Producer(prodConfig))
            {
                var producerData = new ProducerData<string, Message>(CurrentTestTopic, originalMessageList);
                producer.Send(producerData);
            }

            Thread.Sleep(waitSingle);
            while (
                !multipleBrokersHelper.CheckIfAnyBrokerHasChanged(
                    new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 }))
            {
                totalWaitTimeInMiliseconds += waitSingle;
                Thread.Sleep(waitSingle);
                if (totalWaitTimeInMiliseconds > this.maxTestWaitTimeInMiliseconds)
                {
                    Assert.Fail("None of the brokers changed their offset after sending a message");
                }
            }

            totalWaitTimeInMiliseconds = 0;

            var consumerConfig = new ConsumerConfiguration(
                multipleBrokersHelper.BrokerThatHasChanged.Host, multipleBrokersHelper.BrokerThatHasChanged.Port);
            IConsumer consumer = new Consumer(consumerConfig);
            var request = new FetchRequest(CurrentTestTopic, multipleBrokersHelper.PartitionThatHasChanged, multipleBrokersHelper.OffsetFromBeforeTheChange);

            BufferedMessageSet response;
            while (true)
            {
                Thread.Sleep(waitSingle);
                response = consumer.Fetch(request);
                if (response != null && response.Messages.Count() > 2)
                {
                    break;
                }

                totalWaitTimeInMiliseconds += waitSingle;
                if (totalWaitTimeInMiliseconds >= this.maxTestWaitTimeInMiliseconds)
                {
                    break;
                }
            }

            Assert.NotNull(response);
            Assert.AreEqual(3, response.Messages.Count());
            Assert.AreEqual(originalMessage1.ToString(), response.Messages.First().ToString());
            Assert.AreEqual(originalMessage2.ToString(), response.Messages.Skip(1).First().ToString());
            Assert.AreEqual(originalMessage3.ToString(), response.Messages.Skip(2).First().ToString());
        }

        [Test]
        public void ProducerSends1MessageUsingNotDefaultEncoder()
        {
            var prodConfig = this.ConfigBasedSyncProdConfig;

            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            string originalMessage = "TestData";

            var multipleBrokersHelper = new TestMultipleBrokersHelper(CurrentTestTopic);
            multipleBrokersHelper.GetCurrentOffsets(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 });
            using (var producer = new Producer<string, string>(prodConfig, null, new StringEncoder(), null))
            {
                var producerData = new ProducerData<string, string>(
                    CurrentTestTopic, new List<string> { originalMessage });

                producer.Send(producerData);
            }

            Thread.Sleep(waitSingle);

            while (!multipleBrokersHelper.CheckIfAnyBrokerHasChanged(new[] { this.SyncProducerConfig1, this.SyncProducerConfig2, this.SyncProducerConfig3 }))
            {
                totalWaitTimeInMiliseconds += waitSingle;
                Thread.Sleep(waitSingle);
                if (totalWaitTimeInMiliseconds > this.maxTestWaitTimeInMiliseconds)
                {
                    Assert.Fail("None of the brokers changed their offset after sending a message");
                }
            }

            totalWaitTimeInMiliseconds = 0;

            var consumerConfig = new ConsumerConfiguration(
                multipleBrokersHelper.BrokerThatHasChanged.Host,
                    multipleBrokersHelper.BrokerThatHasChanged.Port);
            IConsumer consumer = new Consumer(consumerConfig);
            var request = new FetchRequest(CurrentTestTopic, multipleBrokersHelper.PartitionThatHasChanged, multipleBrokersHelper.OffsetFromBeforeTheChange);

            BufferedMessageSet response;
            while (true)
            {
                Thread.Sleep(waitSingle);
                response = consumer.Fetch(request);
                if (response != null && response.Messages.Count() > 0)
                {
                    break;
                }

                totalWaitTimeInMiliseconds += waitSingle;
                if (totalWaitTimeInMiliseconds >= this.maxTestWaitTimeInMiliseconds)
                {
                    break;
                }
            }

            Assert.NotNull(response);
            Assert.AreEqual(1, response.Messages.Count());
            Assert.AreEqual(originalMessage, Encoding.UTF8.GetString(response.Messages.First().Payload));
        }
    }
}
