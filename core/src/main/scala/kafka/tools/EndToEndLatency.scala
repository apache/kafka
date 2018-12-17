/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.{Collections, Arrays, Properties}

import joptsimple.OptionParser
import kafka.utils.{CommandLineUtils, Exit}
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.{admin, CommonClientConfigs}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._
import scala.util.Random


/**
 * This class records the average end to end latency for a single message to travel through Kafka
 */

object EndToEndLatency {
  private val timeout: Long = 60000
  private val defaultReplicationFactor: Short = 1
  private val defaultNumPartitions: Int = 1

  def main(args: Array[String]) {
    // Parse input arguments.
    val parser = new OptionParser(false)
    val bootstrapServersOpt = parser.accepts("broker-list", "REQUIRED: The server(s) to connect to.")
      .withRequiredArg
      .describedAs("host")
      .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to produce to/consume from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val numMessagesOpt = parser.accepts("messages", "REQUIRED: The number of messages to send/consume.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
    val acksOpt = parser.accepts("acks", "REQUIRED: The number of acknowledgments the producer requires the leader to " +
      "have received before considering a request complete.")
      .withRequiredArg
      .describedAs("acks")
      .ofType(classOf[String])
    val recordSizeOpt = parser.accepts("record-size", "REQUIRED: Message size in bytes.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
    val propsFileOpt = parser.accepts("properties-file", "Producer/Consumer config properties file.")
      .withRequiredArg
      .describedAs("properties file")
      .ofType(classOf[String])
    val helpOpt = parser.accepts("help", "Print usage information.")

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "This tool is used to record the average end to end latency for a single message to travel through Kafka.")

    val options = parser.parse(args: _*)

    if(options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      Exit.exit(0)
    }

    CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServersOpt, topicOpt, numMessagesOpt, acksOpt, recordSizeOpt)

    val brokerList = options.valueOf(bootstrapServersOpt)
    val topic = options.valueOf(topicOpt)
    val numMessages = options.valueOf(numMessagesOpt).intValue()
    val producerAcks = options.valueOf(acksOpt)
    val messageLen = options.valueOf(recordSizeOpt).intValue()
    val propsFile = if (options.has(propsFileOpt)) Some(options.valueOf(propsFileOpt)).filter(_.nonEmpty) else None

    if (!List("1", "all").contains(producerAcks))
      throw new IllegalArgumentException("Latency testing requires synchronous acknowledgement. Please use 1 or all")

    def loadPropsWithBootstrapServers: Properties = {
      val props = propsFile.map(Utils.loadProps).getOrElse(new Properties())
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
      props
    }

    val consumerProps = loadPropsWithBootstrapServers
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis())
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0") //ensure we have no temporal batching
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)

    val producerProps = loadPropsWithBootstrapServers
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0") //ensure writes are synchronous
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    producerProps.put(ProducerConfig.ACKS_CONFIG, producerAcks.toString)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def finalise() {
      consumer.commitSync()
      producer.close()
      consumer.close()
    }

    // create topic if it does not exist
    if (!consumer.listTopics().containsKey(topic)) {
      try {
        createTopic(topic, loadPropsWithBootstrapServers)
      } catch {
        case t: Throwable =>
          finalise()
          throw new RuntimeException(s"Failed to create topic $topic", t)
      }
    }

    val topicPartitions = consumer.partitionsFor(topic).asScala
      .map(p => new TopicPartition(p.topic(), p.partition())).asJava
    consumer.assign(topicPartitions)
    consumer.seekToEnd(topicPartitions)
    consumer.assignment().asScala.foreach(consumer.position)

    var totalTime = 0.0
    val latencies = new Array[Long](numMessages)
    val random = new Random(0)

    for (i <- 0 until numMessages) {
      val message = randomBytesOfLen(random, messageLen)
      val begin = System.nanoTime

      //Send message (of random bytes) synchronously then immediately poll for it
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
      val recordIter = consumer.poll(Duration.ofMillis(timeout)).iterator

      val elapsed = System.nanoTime - begin

      //Check we got results
      if (!recordIter.hasNext) {
        finalise()
        throw new RuntimeException(s"poll() timed out before finding a result (timeout:[$timeout])")
      }

      //Check result matches the original record
      val sent = new String(message, StandardCharsets.UTF_8)
      val read = new String(recordIter.next().value(), StandardCharsets.UTF_8)
      if (!read.equals(sent)) {
        finalise()
        throw new RuntimeException(s"The message read [$read] did not match the message sent [$sent]")
      }

      //Check we only got the one message
      if (recordIter.hasNext) {
        val count = 1 + recordIter.asScala.size
        throw new RuntimeException(s"Only one result was expected during this test. We found [$count]")
      }

      //Report progress
      if (i % 1000 == 0)
        println(i + "\t" + elapsed / 1000.0 / 1000.0)
      totalTime += elapsed
      latencies(i) = elapsed / 1000 / 1000
    }

    //Results
    println("Avg latency: %.4f ms\n".format(totalTime / numMessages / 1000.0 / 1000.0))
    Arrays.sort(latencies)
    val p50 = latencies((latencies.length * 0.5).toInt)
    val p99 = latencies((latencies.length * 0.99).toInt)
    val p999 = latencies((latencies.length * 0.999).toInt)
    println("Percentiles: 50th = %d, 99th = %d, 99.9th = %d".format(p50, p99, p999))

    finalise()
  }

  def randomBytesOfLen(random: Random, len: Int): Array[Byte] = {
    Array.fill(len)((random.nextInt(26) + 65).toByte)
  }

  def createTopic(topic: String, props: Properties): Unit = {
    println("Topic \"%s\" does not exist. Will create topic with %d partition(s) and replication factor = %d"
              .format(topic, defaultNumPartitions, defaultReplicationFactor))

    val adminClient = admin.AdminClient.create(props)
    val newTopic = new NewTopic(topic, defaultNumPartitions, defaultReplicationFactor)
    try adminClient.createTopics(Collections.singleton(newTopic)).all().get()
    finally Utils.closeQuietly(adminClient, "AdminClient")
  }
}
