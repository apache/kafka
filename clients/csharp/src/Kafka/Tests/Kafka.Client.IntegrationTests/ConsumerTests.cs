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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using NUnit.Framework;

    [TestFixture]
    public class ConsumerTests : IntegrationFixtureBase
    {
        /// <summary>
        /// Kafka Client configuration
        /// </summary>
        private static KafkaClientConfiguration clientConfig;

        [TestFixtureSetUp]
        public void SetUp()
        {
            clientConfig = KafkaClientConfiguration.GetConfiguration();
        }

        [Test]
        public void ConsumerConnectorIsCreatedConnectsDisconnectsAndShutsDown()
        {
            var config = new ConsumerConfig(clientConfig);
            using (new ZookeeperConsumerConnector(config, true))
            {
            }
        }

        [Test]
        public void SimpleSyncProducerSends2MessagesAndConsumerConnectorGetsThemBack()
        {
            // first producing
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);

            var producerConfig = new SyncProducerConfig(clientConfig);
            var producer = new SyncProducer(producerConfig);
            var producerRequest = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { msg1, msg2 });
            producer.Send(producerRequest);

            // now consuming
            var config = new ConsumerConfig(clientConfig) { AutoCommit = false };
            var resultMessages = new List<Message>();
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
                var sets = messages[CurrentTestTopic];
                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            resultMessages.Add(message);
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(2, resultMessages.Count);
            Assert.AreEqual(msg1.ToString(), resultMessages[0].ToString());
            Assert.AreEqual(msg2.ToString(), resultMessages[1].ToString());
        }

        [Test]
        public void OneMessageIsSentAndReceivedThenExceptionsWhenNoMessageThenAnotherMessageIsSentAndReceived()
        {
            // first producing
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            var producerConfig = new SyncProducerConfig(clientConfig);
            var producer = new SyncProducer(producerConfig);
            var producerRequest = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { msg1 });
            producer.Send(producerRequest);

            // now consuming
            var config = new ConsumerConfig(clientConfig) { AutoCommit = false, Timeout = 5000 };
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);
                var sets = messages[CurrentTestTopic];
                KafkaMessageStream myStream = sets[0];
                var enumerator = myStream.GetEnumerator();

                Assert.IsTrue(enumerator.MoveNext());
                Assert.AreEqual(msg1.ToString(), enumerator.Current.ToString());

                Assert.Throws<ConsumerTimeoutException>(() => enumerator.MoveNext());

                Assert.Throws<Exception>(() => enumerator.MoveNext()); // iterator is in failed state

                enumerator.Reset();

                // producing again
                string payload2 = "kafka 2.";
                byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
                var msg2 = new Message(payloadData2);

                var producerRequest2 = new ProducerRequest(CurrentTestTopic, 0, new List<Message> { msg2 });
                producer.Send(producerRequest2);

                Thread.Sleep(3000);

                Assert.IsTrue(enumerator.MoveNext());
                Assert.AreEqual(msg2.ToString(), enumerator.Current.ToString());
            }
        }

        [Test]
        public void ConsumerConnectorConsumesTwoDifferentTopics()
        {
            string topic1 = CurrentTestTopic + "1";
            string topic2 = CurrentTestTopic + "2";

            // first producing
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);

            var producerConfig = new SyncProducerConfig(clientConfig);
            var producer = new SyncProducer(producerConfig);
            var producerRequest1 = new ProducerRequest(topic1, 0, new List<Message> { msg1 });
            producer.Send(producerRequest1);
            var producerRequest2 = new ProducerRequest(topic2, 0, new List<Message> { msg2 });
            producer.Send(producerRequest2);

            // now consuming
            var config = new ConsumerConfig(clientConfig) { AutoCommit = false };
            var resultMessages1 = new List<Message>();
            var resultMessages2 = new List<Message>();
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var topicCount = new Dictionary<string, int> { { topic1, 1 }, { topic2, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);

                Assert.IsTrue(messages.ContainsKey(topic1));
                Assert.IsTrue(messages.ContainsKey(topic2));

                var sets1 = messages[topic1];
                try
                {
                    foreach (var set in sets1)
                    {
                        foreach (var message in set)
                        {
                            resultMessages1.Add(message);
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }

                var sets2 = messages[topic2];
                try
                {
                    foreach (var set in sets2)
                    {
                        foreach (var message in set)
                        {
                            resultMessages2.Add(message);
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(1, resultMessages1.Count);
            Assert.AreEqual(msg1.ToString(), resultMessages1[0].ToString());

            Assert.AreEqual(1, resultMessages2.Count);
            Assert.AreEqual(msg2.ToString(), resultMessages2[0].ToString());
        }

        [Test]
        public void ConsumerConnectorReceivesAShutdownSignal()
        {
            // now consuming
            var config = new ConsumerConfig(clientConfig) { AutoCommit = false };
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);

                // putting the shutdown command into the queue
                FieldInfo fi = typeof(ZookeeperConsumerConnector).GetField(
                    "queues", BindingFlags.NonPublic | BindingFlags.Instance);
                var value =
                    (IDictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>>)
                    fi.GetValue(consumerConnector);
                foreach (var topicConsumerQueueMap in value)
                {
                    topicConsumerQueueMap.Value.Add(ZookeeperConsumerConnector.ShutdownCommand);
                }

                var sets = messages[CurrentTestTopic];
                var resultMessages = new List<Message>();

                foreach (var set in sets)
                {
                    foreach (var message in set)
                    {
                        resultMessages.Add(message);
                    }
                }

                Assert.AreEqual(0, resultMessages.Count);
            }
        }

        [Test]
        public void ProducersSendMessagesToDifferentPartitionsAndConsumerConnectorGetsThemBack()
        {
            // first producing
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);

            var producerConfig = new SyncProducerConfig(clientConfig);
            var producer = new SyncProducer(producerConfig);
            var producerRequest1 = new ProducerRequest(CurrentTestTopic, 0, new List<Message>() { msg1 });
            producer.Send(producerRequest1);
            var producerRequest2 = new ProducerRequest(CurrentTestTopic, 1, new List<Message>() { msg2 });
            producer.Send(producerRequest2);

            // now consuming
            var config = new ConsumerConfig(clientConfig) { AutoCommit = false };
            var resultMessages = new List<Message>();
            using (IConsumerConnector consumerConnector = new ZookeeperConsumerConnector(config, true))
            {
                var topicCount = new Dictionary<string, int> { { CurrentTestTopic, 1 } };
                var messages = consumerConnector.CreateMessageStreams(topicCount);

                var sets = messages[CurrentTestTopic];
                
                try
                {
                    foreach (var set in sets)
                    {
                        foreach (var message in set)
                        {
                            resultMessages.Add(message);
                        }
                    }
                }
                catch (ConsumerTimeoutException)
                {
                    // do nothing, this is expected
                }
            }

            Assert.AreEqual(2, resultMessages.Count);
            Assert.AreEqual(msg1.ToString(), resultMessages[0].ToString());
            Assert.AreEqual(msg2.ToString(), resultMessages[1].ToString());
        }
    }
}
