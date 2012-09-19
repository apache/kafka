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
    using System.Text;
    using System.Threading;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers.Async;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using NUnit.Framework;

    /// <summary>
    /// Contains tests that go all the way to Kafka and back.
    /// </summary>
    [TestFixture]
    public class KafkaIntegrationTest : IntegrationFixtureBase
    {
        /// <summary>
        /// Maximum amount of time to wait trying to get a specific test message from Kafka server (in miliseconds)
        /// </summary>
        private static readonly int MaxTestWaitTimeInMiliseconds = 5000;

        /// <summary>
        /// Sends a pair of message to Kafka.
        /// </summary>
        [Test]
        public void ProducerSendsMessage()
        {
            var prodConfig = this.SyncProducerConfig1;

            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);

            using (var producer = new SyncProducer(prodConfig))
            {
                var producerRequest = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { msg1, msg2 });
                producer.Send(producerRequest);
            }
        }

        /// <summary>
        /// Sends a message with long topic to Kafka.
        /// </summary>
        [Test]
        public void ProducerSendsMessageWithLongTopic()
        {
            var prodConfig = this.SyncProducerConfig1;

            var msg = new Message(Encoding.UTF8.GetBytes("test message"));
            string topic = "ThisIsAVeryLongTopicThisIsAVeryLongTopicThisIsAVeryLongTopicThisIsAVeryLongTopicThisIsAVeryLongTopicThisIsAVeryLongTopic";
            using (var producer = new SyncProducer(prodConfig))
            {
                var producerRequest = new ProducerRequest(topic, 0, new List<Message> { msg });
                producer.Send(producerRequest);
            }
        }

        /// <summary>
        /// Asynchronously sends many random messages to Kafka
        /// </summary>
        [Test]
        public void AsyncProducerSendsManyLongRandomMessages()
        {
            var prodConfig = this.AsyncProducerConfig1;
            List<Message> messages = GenerateRandomTextMessages(50);
            using (var producer = new AsyncProducer(prodConfig))
            {
                producer.Send(CurrentTestTopic, 0, messages);
            }
        }

        /// <summary>
        /// Asynchronously sends few short fixed messages to Kafka
        /// </summary>
        [Test]
        public void AsyncProducerSendsFewShortFixedMessages()
        {
            var prodConfig = this.AsyncProducerConfig1;

            var messages = new List<Message>
                                         {
                                             new Message(Encoding.UTF8.GetBytes("Async Test Message 1")),
                                             new Message(Encoding.UTF8.GetBytes("Async Test Message 2")),
                                             new Message(Encoding.UTF8.GetBytes("Async Test Message 3")),
                                             new Message(Encoding.UTF8.GetBytes("Async Test Message 4"))
                                         };

            using (var producer = new AsyncProducer(prodConfig))
            {
                producer.Send(CurrentTestTopic, 0, messages);
            }
        }

        /// <summary>
        /// Asynchronously sends few short fixed messages to Kafka in separate send actions
        /// </summary>
        [Test]
        public void AsyncProducerSendsFewShortFixedMessagesInSeparateSendActions()
        {
            var prodConfig = this.AsyncProducerConfig1;

            using (var producer = new AsyncProducer(prodConfig))
            {
                var req1 = new ProducerRequest(
                    CurrentTestTopic,
                    0,
                    new List<Message> { new Message(Encoding.UTF8.GetBytes("Async Test Message 1")) });
                producer.Send(req1);

                var req2 = new ProducerRequest(
                    CurrentTestTopic,
                    0,
                    new List<Message> { new Message(Encoding.UTF8.GetBytes("Async Test Message 2")) });
                producer.Send(req2);

                var req3 = new ProducerRequest(
                    CurrentTestTopic,
                    0,
                    new List<Message> { new Message(Encoding.UTF8.GetBytes("Async Test Message 3")) });
                producer.Send(req3);
            }
        }

        [Test]
        public void AsyncProducerSendsMessageWithCallbackClass()
        {
            var prodConfig = this.AsyncProducerConfig1;

            var messages = new List<Message>
                                         {
                                             new Message(Encoding.UTF8.GetBytes("Async Test Message 1")),
                                         };
            var myHandler = new TestCallbackHandler();
            using (var producer = new AsyncProducer(prodConfig, myHandler))
            {
                producer.Send(CurrentTestTopic, 0, messages);
            }

            Thread.Sleep(1000);
            Assert.IsTrue(myHandler.WasRun);
        }

        [Test]
        public void AsyncProducerSendsMessageWithCallback()
        {
            var prodConfig = this.AsyncProducerConfig1;

            var messages = new List<Message>
                                         {
                                             new Message(Encoding.UTF8.GetBytes("Async Test Message 1")),
                                         };
            var myHandler = new TestCallbackHandler();
            using (var producer = new AsyncProducer(prodConfig))
            {
                producer.Send(CurrentTestTopic, 0, messages, myHandler.Handle);
            }

            Thread.Sleep(1000);
            Assert.IsTrue(myHandler.WasRun);
        }

        private class TestCallbackHandler : ICallbackHandler
        {
            public bool WasRun { get; private set; }

            public void Handle(RequestContext<ProducerRequest> context)
            {
                WasRun = true;
            }
        }

        /// <summary>
        /// Send a multi-produce request to Kafka.
        /// </summary>
        [Test]
        public void ProducerSendMultiRequest()
        {
            var prodConfig = this.SyncProducerConfig1;

            var requests = new List<ProducerRequest>
            { 
                new ProducerRequest(CurrentTestTopic, 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("1: " + DateTime.UtcNow)) }),
                new ProducerRequest(CurrentTestTopic, 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("2: " + DateTime.UtcNow)) }),
                new ProducerRequest(CurrentTestTopic, 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("3: " + DateTime.UtcNow)) }),
                new ProducerRequest(CurrentTestTopic, 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("4: " + DateTime.UtcNow)) })
            };

            using (var producer = new SyncProducer(prodConfig))
            {
                producer.MultiSend(requests);
            }
        }

        /// <summary>
        /// Generates messages for Kafka then gets them back.
        /// </summary>
        [Test]
        public void ConsumerFetchMessage()
        {
            var consumerConfig = this.ConsumerConfig1;
            ProducerSendsMessage();
            Thread.Sleep(1000);
            IConsumer consumer = new Consumer(consumerConfig);
            var request = new FetchRequest(CurrentTestTopic, 0, 0);
            BufferedMessageSet response = consumer.Fetch(request);
            Assert.NotNull(response);
            int count = 0;
            foreach (var message in response)
            {
                count++;
                Console.WriteLine(message.Message);
            }

            Assert.AreEqual(2, count);
        }

        /// <summary>
        /// Generates multiple messages for Kafka then gets them back.
        /// </summary>
        [Test]
        public void ConsumerMultiFetchGetsMessage()
        {
            var config = this.ConsumerConfig1;

            ProducerSendMultiRequest();
            Thread.Sleep(2000);
            IConsumer cons = new Consumer(config);
            var request = new MultiFetchRequest(new List<FetchRequest>
            {
                new FetchRequest(CurrentTestTopic, 0, 0),
                new FetchRequest(CurrentTestTopic, 0, 0),
                new FetchRequest(CurrentTestTopic, 0, 0)
            });

            IList<BufferedMessageSet> response = cons.MultiFetch(request);
            Assert.AreEqual(3, response.Count);
            for (int ix = 0; ix < response.Count; ix++)
            {
                IEnumerable<Message> messageSet = response[ix].Messages;
                Assert.AreEqual(4, messageSet.Count());
                Console.WriteLine(string.Format("Request #{0}-->", ix));
                foreach (Message msg in messageSet)
                {
                    Console.WriteLine(msg.ToString());
                }
            }
        }

        /// <summary>
        /// Gets offsets from Kafka.
        /// </summary>
        [Test]
        public void ConsumerGetsOffsets()
        {
            var consumerConfig = this.ConsumerConfig1;

            var request = new OffsetRequest(CurrentTestTopic, 0, DateTime.Now.AddHours(-24).Ticks, 10);
            IConsumer consumer = new Consumer(consumerConfig);
            IList<long> list = consumer.GetOffsetsBefore(request);

            foreach (long l in list)
            {
                Console.Out.WriteLine(l);
            }
        }

        /// <summary>
        /// Synchronous Producer sends a single simple message and then a consumer consumes it
        /// </summary>
        [Test]
        public void ProducerSendsAndConsumerReceivesSingleSimpleMessage()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ConsumerConfig1;

            var sourceMessage = new Message(Encoding.UTF8.GetBytes("test message"));
            long currentOffset = TestHelper.GetCurrentKafkaOffset(CurrentTestTopic, consumerConfig);
            using (var producer = new SyncProducer(prodConfig))
            {
                var producerRequest = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { sourceMessage });
                producer.Send(producerRequest);
            }

            IConsumer consumer = new Consumer(consumerConfig);
            var request = new FetchRequest(CurrentTestTopic, 0, currentOffset);
            BufferedMessageSet response;
            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            while (true)
            {
                Thread.Sleep(waitSingle);
                response = consumer.Fetch(request);
                if (response != null && response.Messages.Count() > 0)
                {
                    break;
                }

                totalWaitTimeInMiliseconds += waitSingle;
                if (totalWaitTimeInMiliseconds >= MaxTestWaitTimeInMiliseconds)
                {
                    break;
                }
            }

            Assert.NotNull(response);
            Assert.AreEqual(1, response.Messages.Count());
            Message resultMessage = response.Messages.First();
            Assert.AreEqual(sourceMessage.ToString(), resultMessage.ToString());
        }

        /// <summary>
        /// Asynchronous Producer sends a single simple message and then a consumer consumes it
        /// </summary>
        [Test]
        public void AsyncProducerSendsAndConsumerReceivesSingleSimpleMessage()
        {
            var prodConfig = this.AsyncProducerConfig1;
            var consumerConfig = this.ConsumerConfig1;

            var sourceMessage = new Message(Encoding.UTF8.GetBytes("test message"));
            using (var producer = new AsyncProducer(prodConfig))
            {
                var producerRequest = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { sourceMessage });
                producer.Send(producerRequest);
            }

            long currentOffset = TestHelper.GetCurrentKafkaOffset(CurrentTestTopic, consumerConfig);
            IConsumer consumer = new Consumer(consumerConfig);
            var request = new FetchRequest(CurrentTestTopic, 0, currentOffset);

            BufferedMessageSet response;
            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            while (true)
            {
                Thread.Sleep(waitSingle);
                response = consumer.Fetch(request);
                if (response != null && response.Messages.Count() > 0)
                {
                    break;
                }

                totalWaitTimeInMiliseconds += waitSingle;
                if (totalWaitTimeInMiliseconds >= MaxTestWaitTimeInMiliseconds)
                {
                    break;
                }
            }

            Assert.NotNull(response);
            Assert.AreEqual(1, response.Messages.Count());
            Message resultMessage = response.Messages.First();
            Assert.AreEqual(sourceMessage.ToString(), resultMessage.ToString());
        }

        /// <summary>
        /// Synchronous producer sends a multi request and a consumer receives it from to Kafka.
        /// </summary>
        [Test]
        public void ProducerSendsAndConsumerReceivesMultiRequest()
        {
            var prodConfig = this.SyncProducerConfig1;
            var consumerConfig = this.ConsumerConfig1;

            string testTopic1 = CurrentTestTopic + "1";
            string testTopic2 = CurrentTestTopic + "2";
            string testTopic3 = CurrentTestTopic + "3";

            var sourceMessage1 = new Message(Encoding.UTF8.GetBytes("1: TestMessage"));
            var sourceMessage2 = new Message(Encoding.UTF8.GetBytes("2: TestMessage"));
            var sourceMessage3 = new Message(Encoding.UTF8.GetBytes("3: TestMessage"));
            var sourceMessage4 = new Message(Encoding.UTF8.GetBytes("4: TestMessage"));

            var requests = new List<ProducerRequest>
            { 
                new ProducerRequest(testTopic1, 0, new List<Message> { sourceMessage1 }),
                new ProducerRequest(testTopic1, 0, new List<Message> { sourceMessage2 }),
                new ProducerRequest(testTopic2, 0, new List<Message> { sourceMessage3 }),
                new ProducerRequest(testTopic3, 0, new List<Message> { sourceMessage4 })
            };

            long currentOffset1 = TestHelper.GetCurrentKafkaOffset(testTopic1, consumerConfig);
            long currentOffset2 = TestHelper.GetCurrentKafkaOffset(testTopic2, consumerConfig);
            long currentOffset3 = TestHelper.GetCurrentKafkaOffset(testTopic3, consumerConfig);

            using (var producer = new SyncProducer(prodConfig))
            {
                producer.MultiSend(requests);
            }

            IConsumer consumer = new Consumer(consumerConfig);
            var request = new MultiFetchRequest(new List<FetchRequest>
            {
                new FetchRequest(testTopic1, 0, currentOffset1),
                new FetchRequest(testTopic2, 0, currentOffset2),
                new FetchRequest(testTopic3, 0, currentOffset3)
            });
            IList<BufferedMessageSet> messageSets;
            int totalWaitTimeInMiliseconds = 0;
            int waitSingle = 100;
            while (true)
            {
                Thread.Sleep(waitSingle);
                messageSets = consumer.MultiFetch(request);
                if (messageSets.Count > 2 && messageSets[0].Messages.Count() > 0 && messageSets[1].Messages.Count() > 0 && messageSets[2].Messages.Count() > 0)
                {
                    break;
                }

                totalWaitTimeInMiliseconds += waitSingle;
                if (totalWaitTimeInMiliseconds >= MaxTestWaitTimeInMiliseconds)
                {
                    break;
                }
            }

            Assert.AreEqual(3, messageSets.Count);
            Assert.AreEqual(2, messageSets[0].Messages.Count());
            Assert.AreEqual(1, messageSets[1].Messages.Count());
            Assert.AreEqual(1, messageSets[2].Messages.Count());
            Assert.AreEqual(sourceMessage1.ToString(), messageSets[0].Messages.First().ToString());
            Assert.AreEqual(sourceMessage2.ToString(), messageSets[0].Messages.Skip(1).First().ToString());
            Assert.AreEqual(sourceMessage3.ToString(), messageSets[1].Messages.First().ToString());
            Assert.AreEqual(sourceMessage4.ToString(), messageSets[2].Messages.First().ToString());
        }

        /// <summary>
        /// Gererates a randome list of text messages.
        /// </summary>
        /// <param name="numberOfMessages">The number of messages to generate.</param>
        /// <returns>A list of random text messages.</returns>
        private static List<Message> GenerateRandomTextMessages(int numberOfMessages)
        {
            var messages = new List<Message>();
            for (int ix = 0; ix < numberOfMessages; ix++)
            {
                ////messages.Add(new Message(GenerateRandomBytes(10000)));
                messages.Add(new Message(Encoding.UTF8.GetBytes(GenerateRandomMessage(10000))));
            }

            return messages;
        }

        /// <summary>
        /// Generate a random message text.
        /// </summary>
        /// <param name="length">Length of the message string.</param>
        /// <returns>Random message string.</returns>
        private static string GenerateRandomMessage(int length)
        {
            var builder = new StringBuilder();
            var random = new Random();
            for (int i = 0; i < length; i++)
            {
                char ch = Convert.ToChar(Convert.ToInt32(
                    Math.Floor((26 * random.NextDouble()) + 65)));
                builder.Append(ch);
            }

            return builder.ToString();
        }
    }
}
