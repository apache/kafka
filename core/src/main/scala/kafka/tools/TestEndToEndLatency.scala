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

import java.util.Properties
import java.util.Arrays
import kafka.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord, KafkaProducer}

object TestEndToEndLatency {
  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("USAGE: java " + getClass().getName + " broker_list zookeeper_connect topic num_messages")
      System.exit(1)
    }

    val brokerList = args(0)
    val zkConnect = args(1)
    val topic = args(2)
    val numMessages = args(3).toInt

    val consumerProps = new Properties()
    consumerProps.put("group.id", topic)
    consumerProps.put("auto.commit.enable", "false")
    consumerProps.put("auto.offset.reset", "largest")
    consumerProps.put("zookeeper.connect", zkConnect)
    consumerProps.put("fetch.wait.max.ms", "1")
    consumerProps.put("socket.timeout.ms", 1201000.toString)

    val config = new ConsumerConfig(consumerProps)
    val connector = Consumer.create(config)
    var stream = connector.createMessageStreams(Map(topic -> 1)).get(topic).head.head
    val iter = stream.iterator

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0")
    producerProps.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    val producer = new KafkaProducer(producerProps)

    val message = "hello there beautiful".getBytes
    var totalTime = 0.0
    val latencies = new Array[Long](numMessages)
    for (i <- 0 until numMessages) {
      var begin = System.nanoTime
      producer.send(new ProducerRecord(topic, message))
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