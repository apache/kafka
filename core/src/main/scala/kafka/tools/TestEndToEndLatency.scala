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

package kafka.tools

import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord, KafkaProducer}

import kafka.consumer._

import java.util.Properties
import java.util.Arrays

object TestEndToEndLatency {
  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println("USAGE: java " + getClass().getName + " broker_list zookeeper_connect topic num_messages consumer_fetch_max_wait producer_acks")
      System.exit(1)
    }

    val brokerList = args(0)
    val zkConnect = args(1)
    val topic = args(2)
    val numMessages = args(3).toInt
    val consumerFetchMaxWait = args(4).toInt
    val producerAcks = args(5).toInt

    val consumerProps = new Properties()
    consumerProps.put("group.id", topic)
    consumerProps.put("auto.commit.enable", "false")
    consumerProps.put("auto.offset.reset", "largest")
    consumerProps.put("zookeeper.connect", zkConnect)
    consumerProps.put("fetch.wait.max.ms", consumerFetchMaxWait.toString)
    consumerProps.put("socket.timeout.ms", 1201000.toString)

    val config = new ConsumerConfig(consumerProps)
    val connector = Consumer.create(config)
    val stream = connector.createMessageStreams(Map(topic -> 1)).get(topic).head.head
    val iter = stream.iterator

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0")
    producerProps.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    producerProps.put(ProducerConfig.ACKS_CONFIG, producerAcks.toString)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    val producer = new KafkaProducer[Array[Byte],Array[Byte]](producerProps)

    // make sure the consumer fetcher has started before sending data since otherwise
    // the consumption from the tail will skip the first message and hence be blocked
    Thread.sleep(5000)

    val message = "hello there beautiful".getBytes
    var totalTime = 0.0
    val latencies = new Array[Long](numMessages)
    for (i <- 0 until numMessages) {
      val begin = System.nanoTime
      producer.send(new ProducerRecord[Array[Byte],Array[Byte]](topic, message))
      val received = iter.next
      val elapsed = System.nanoTime - begin
      // poor man's progress bar
      if (i % 1000 == 0)
        println(i + "\t" + elapsed / 1000.0 / 1000.0)
      totalTime += elapsed
      latencies(i) = (elapsed / 1000 / 1000)
    }
    println("Avg latency: %.4f ms\n".format(totalTime / numMessages / 1000.0 / 1000.0))
    Arrays.sort(latencies)
    val p50 = latencies((latencies.length * 0.5).toInt)
    val p99 = latencies((latencies.length * 0.99).toInt) 
    val p999 = latencies((latencies.length * 0.999).toInt)
    println("Percentiles: 50th = %d, 99th = %d, 99.9th = %d".format(p50, p99, p999))
    producer.close()
    connector.commitOffsets(true)
    connector.shutdown()
    System.exit(0)
  }
}