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
import java.util.{Arrays, Collections, Properties}

import kafka.utils.Exit
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._
import scala.util.Random


/**
 * This class records the average end to end latency for a single message to travel through Kafka
 *
 * broker_list = location of the bootstrap broker for both the producer and the consumer
 * num_messages = # messages to send
 * producer_acks = See ProducerConfig.ACKS_DOC
 * message_size_bytes = size of each message in bytes
 *
 * e.g. [localhost:9092 test 10000 1 20]
 */

object EndToEndLatency {
  private val timeout: Long = 60000

  def main(args: Array[String]) {
    if (args.length != 5 && args.length != 6) {
      System.err.println("USAGE: java " + getClass.getName + " broker_list topic num_messages producer_acks message_size_bytes [optional] properties_file")
      Exit.exit(1)
    }

    val brokerList = args(0)
    val topic = args(1)
    val numMessages = args(2).toInt
    val producerAcks = args(3)
    val messageLen = args(4).toInt
    val propsFile = if (args.length > 5) Some(args(5)).filter(_.nonEmpty) else None

    if (!List("1", "all").contains(producerAcks))
      throw new IllegalArgumentException("Latency testing requires synchronous acknowledgement. Please use 1 or all")

    def loadProps: Properties = propsFile.map(Utils.loadProps).getOrElse(new Properties())

    val consumerProps = loadProps
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis())
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0") //ensure we have no temporal batching

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
    consumer.subscribe(Collections.singletonList(topic))

    val producerProps = loadProps
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
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

    //Ensure we are at latest offset. seekToEnd evaluates lazily, that is to say actually performs the seek only when
    //a poll() or position() request is issued. Hence we need to poll after we seek to ensure we see our first write.
    consumer.seekToEnd(Collections.emptyList())
    consumer.poll(0)

    var totalTime = 0.0
    val latencies = new Array[Long](numMessages)
    val random = new Random(0)

    for (i <- 0 until numMessages) {
      val message = randomBytesOfLen(random, messageLen)
      val begin = System.nanoTime

      //Send message (of random bytes) synchronously then immediately poll for it
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
      val recordIter = consumer.poll(timeout).iterator

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
}
