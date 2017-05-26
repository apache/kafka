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

import java.util.{Arrays, Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._
import scala.util.Random
import org.{ HdrHistogram => hdr }
import java.io._




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
  private val timeout: Long = 5000
  val histogram = new hdr.AtomicHistogram(10000000000L, 3)

  def main(args: Array[String]) {
    if (args.length != 7 && args.length != 8) {
      System.err.println("USAGE: java " + getClass.getName + " broker_list topic num_messages producer_acks message_size_bytes [optional] properties_file")
      System.exit(1)
    }

    val brokerList = args(0)
    val topic = args(1)
    val numMessages = args(2).toInt
    val producerAcks = args(3)
    val messageLen = args(4).toInt
    val maxTime = args(5).toInt
    val threads = args(6).toInt
    val warmupTime = args(7).toInt
    val propsFile = if (args.length > 8) Some(args(8)).filter(_.nonEmpty) else None

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


    val producerProps = loadProps
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0") //ensure writes are synchronous
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    producerProps.put(ProducerConfig.ACKS_CONFIG, producerAcks.toString)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    def finalise() {
      val out_file = new java.io.FileOutputStream("latency_histogram.log")
      val out_stream = new java.io.PrintStream(out_file)
      histogram.outputPercentileDistribution(out_stream, 1000.0)
    }

    val producer_threads = (1 to threads).map{e => new Thread(new ThrottledKafkaProducer(producerProps, numMessages, topic, maxTime, messageLen)) }
    producer_threads.map{e => e.start() }

    val consumer_threads = (1 to threads).map{e => new Thread(new ThrottledKafkaConsumer(consumerProps, histogram, timeout, numMessages, topic, warmupTime))}
    consumer_threads.map{e => e.start() }

    producer_threads.map{e => e.join() }
    println("Produced all the messages")

    consumer_threads.map{e => e.join() }
    println("Consumed all the messages")


    //Results
    finalise()
  }

  // TODO: Throttling yet to be done
  class ThrottledKafkaProducer(producerProps: Properties, numMessages: Int, topic: String, maxTime: Int, messageLen: Int) extends Runnable {

    def run() {
          // This will block until a connection comes in.
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

      val startTimeMS = System.currentTimeMillis
      var timeDiff = true
      var i = 0
      val spaceBytes = Array.fill(messageLen)(' '.toByte)
      while(timeDiff){

        val begin = System.nanoTime
        val message = begin.toString.getBytes() ++ spaceBytes

        val currentTimeMS = System.currentTimeMillis

        if((currentTimeMS - startTimeMS)< (maxTime*60*1000)){
          producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, message)).get()
          i += 1
          //Report progress
          if (i % 10000 == 0)
            println(i + "\t messages produced")
        } else {
          timeDiff = false
        }
        //Send message (of random bytes) synchronously then immediately poll for it

        
      }
      producer.close()
    }
  }

  // TODO: Throttling yet to be done
  class ThrottledKafkaConsumer(consumerProps: Properties, histogram: hdr.Histogram, timeout: Long, numMessages: Int, topic: String, warmupTime: Int) extends Runnable {

    def run() {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
      // This will block until a connection comes in.
      consumer.subscribe(Collections.singletonList(topic))
      consumer.seekToEnd(Collections.emptyList())
      consumer.poll(0)

      var done = true
      var counter = 0
      val max_count_value = 15
      val startTimeMS = System.currentTimeMillis
      while(done) {
        val recordIter = consumer.poll(timeout).iterator
      //Check we got results
        if (recordIter.hasNext) {
          val read = new String(recordIter.next().value())
          val elapsed = System.nanoTime - read.trim.toLong
          val currentTimeMS = System.currentTimeMillis
          if((currentTimeMS - startTimeMS)>warmupTime*60*1000){
            histogram.recordValue(elapsed)
          }
          // println(elapsed)
        } else {
          counter += 1
          if(counter > max_count_value){
            done = false
          }
        }

      }
      consumer.commitSync()
      consumer.close()


      println("After break")

    }
  }



  def randomBytesOfLen(random: Random, len: Int): Array[Byte] = {
    Array.fill(len)((random.nextInt(26) + 65).toByte)
  }
}
