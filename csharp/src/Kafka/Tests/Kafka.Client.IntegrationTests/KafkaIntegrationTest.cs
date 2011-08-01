using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Kafka.Client.Request;
using NUnit.Framework;

namespace Kafka.Client.Tests
{
    /// <summary>
    /// Contains tests that go all the way to Kafka and back.
    /// </summary>
    [TestFixture]
    [Ignore("Requires a Kafka server running to execute")]
    public class KafkaIntegrationTest
    {
        /// <summary>
        /// Kafka server to test against.
        /// </summary>
        private static readonly string KafkaServer = "192.168.50.203";

        /// <summary>
        /// Port of the Kafka server to test against.
        /// </summary>
        private static readonly int KafkaPort = 9092;

        /// <summary>
        /// Sends a pair of message to Kafka.
        /// </summary>
        [Test]
        public void ProducerSendsMessage()
        {
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            Message msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            Message msg2 = new Message(payloadData2);

            Producer producer = new Producer(KafkaServer, KafkaPort);
            producer.Send("test", 0, new List<Message> { msg1, msg2 });
        }

        /// <summary>
        /// Asynchronously sends a pair of message to Kafka.
        /// </summary>
        [Test]
        public void ProducerSendsMessageAsynchronously()
        {
            bool waiting = true;

            List<Message> messages = GenerateRandomMessages(50);

            Producer producer = new Producer(KafkaServer, KafkaPort);
            producer.SendAsync(
                "test",
                0,
                messages,
                (requestContext) => { waiting = false; });

            while (waiting)
            {
                Console.WriteLine("Keep going...");
                Thread.Sleep(10);
            }
        }

        /// <summary>
        /// Send a multi-produce request to Kafka.
        /// </summary>
        [Test]
        public void ProducerSendMultiRequest()
        {
            List<ProducerRequest> requests = new List<ProducerRequest>
            { 
                new ProducerRequest("test", 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("1: " + DateTime.UtcNow)) }),
                new ProducerRequest("test", 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("2: " + DateTime.UtcNow)) }),
                new ProducerRequest("testa", 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("3: " + DateTime.UtcNow)) }),
                new ProducerRequest("testa", 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("4: " + DateTime.UtcNow)) })
            };

            MultiProducerRequest request = new MultiProducerRequest(requests);
            Producer producer = new Producer(KafkaServer, KafkaPort);
            producer.Send(request);
        }

        /// <summary>
        /// Generates messages for Kafka then gets them back.
        /// </summary>
        [Test]
        public void ConsumerFetchMessage()
        {
            ProducerSendsMessage();

            Consumer consumer = new Consumer(KafkaServer, KafkaPort);
            List<Message> messages = consumer.Consume("test", 0, 0);

            foreach (Message msg in messages)
            {
                Console.WriteLine(msg);
            }
        }

        /// <summary>
        /// Generates multiple messages for Kafka then gets them back.
        /// </summary>
        [Test]
        public void ConsumerMultiFetchGetsMessage()
        {
            ProducerSendMultiRequest();

            Consumer consumer = new Consumer(KafkaServer, KafkaPort);
            MultiFetchRequest request = new MultiFetchRequest(new List<FetchRequest>
            {
                new FetchRequest("test", 0, 0),
                new FetchRequest("test", 0, 0),
                new FetchRequest("testa", 0, 0)
            });

            List<List<Message>> messages = consumer.Consume(request);

            for (int ix = 0; ix < messages.Count; ix++)
            {
                List<Message> messageSet = messages[ix];
                Console.WriteLine(string.Format("Request #{0}-->", ix));
                foreach (Message msg in messageSet)
                {
                    Console.WriteLine(msg);
                }
            }
        }

        /// <summary>
        /// Gets offsets from Kafka.
        /// </summary>
        [Test]
        public void ConsumerGetsOffsets()
        {
            OffsetRequest request = new OffsetRequest("test", 0, DateTime.Now.AddHours(-24).Ticks, 10);

            Consumer consumer = new Consumer(KafkaServer, KafkaPort);
            IList<long> list = consumer.GetOffsetsBefore(request);

            foreach (long l in list)
            {
                Console.Out.WriteLine(l);
            }
        }

        /// <summary>
        /// Gererates a randome list of messages.
        /// </summary>
        /// <param name="numberOfMessages">The number of messages to generate.</param>
        /// <returns>A list of random messages.</returns>
        private static List<Message> GenerateRandomMessages(int numberOfMessages)
        {
            List<Message> messages = new List<Message>();
            for (int ix = 0; ix < numberOfMessages; ix++)
            {
                messages.Add(new Message(GenerateRandomBytes(10000)));
            }

            return messages;
        }

        /// <summary>
        /// Generate a random set of bytes.
        /// </summary>
        /// <param name="length">Length of the byte array.</param>
        /// <returns>Random byte array.</returns>
        private static byte[] GenerateRandomBytes(int length)
        {
            byte[] randBytes = new byte[length];
            Random randNum = new Random();
            randNum.NextBytes(randBytes);

            return randBytes;
        }
    }
}
