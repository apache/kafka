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

import java.util.{Arrays, Properties}

import org.apache.kafka.clients.consumer.{CommitType, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


/**
 * This class records the average end to end latency for a single message to travel through Kafka
 *
 * broker_list = location of the bootstrap broker for both the producer and the consumer
 * num_messages = # messages to send
 * producer_acks = See ProducerConfig.ACKS_DOC
 * message_size_bytes = size of each message in bytes
 * use_busy_wait = if False the client will sleep for 1ms (ConsumerConfig.RETRY_BACKOFF_MS_CONFIG) between requests. This limits the lower granularity. Setting to true provides finer grained results (useful when running locally).
 *
 *  e.g. [localhost:9092 test 10000 1 20 true]
 */

object EndToEndLatency {
  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println("USAGE: java " + getClass.getName + " broker_list topic num_messages producer_acks message_size_bytes use_busy_wait")
      System.exit(1)
    }

    val brokerList = args(0)
    val topic = args(1)
    val numMessages = args(2).toInt
    val producerAcks = args(3).toInt
    val messageLen = if (args.length > 4) args(4).toInt else 20
    val busyWait = args(5).toBoolean

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, topic)
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, "1")
    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0")

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
    consumer.subscribe(topic)

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0")
    producerProps.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    producerProps.put(ProducerConfig.ACKS_CONFIG, producerAcks.toString)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def finalise() {
      consumer.commit(CommitType.SYNC)
      producer.close()
      consumer.close()
    }

    //Ensure we are at latest offset
    var recordIter = consumer.poll(0).iterator
    Thread.sleep(2000)
    consumer.seekToEnd()

    var totalTime = 0.0
    val latencies = new Array[Long](numMessages)

    for (i <- 0 until numMessages) {
      val message = arrayOfLen(messageLen)
      val begin = System.nanoTime

      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, message))

      if (busyWait)
        while (!recordIter.hasNext)
          recordIter = consumer.poll(0).iterator
      else
        recordIter = consumer.poll(Long.MaxValue).iterator

      val elapsed = System.nanoTime - begin

      //Check result matches original record
      val sent = new String(message)
      val read = new String(recordIter.next().value())
      if (!read.equals(sent)) {
        finalise()
        throw new RuntimeException(s"The message read [$read] did not match the message sent [$sent]")
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

  def arrayOfLen(len: Int): Array[Byte] = {
    Array.fill(len)((scala.util.Random.nextInt(26) + 65).toByte)
  }
}