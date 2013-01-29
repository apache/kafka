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

package kafka

import joptsimple.OptionParser
import java.util.Properties
import java.util.Random
import java.io._
import scala.io.Source
import scala.io.BufferedSource
import kafka.producer._
import kafka.consumer._
import kafka.serializer._
import kafka.utils._

/**
 * This is a torture test that runs against an existing broker. Here is how it works:
 * 
 * It produces a series of specially formatted messages to one or more partitions. Each message it produces
 * it logs out to a text file. The messages have a limited set of keys, so there is duplication in the key space.
 * 
 * The broker will clean its log as the test runs.
 * 
 * When the specified number of messages have been produced we create a consumer and consume all the messages in the topic
 * and write that out to another text file.
 * 
 * Using a stable unix sort we sort both the producer log of what was sent and the consumer log of what was retrieved by the message key. 
 * Then we compare the final message in both logs for each key. If this final message is not the same for all keys we
 * print an error and exit with exit code 1, otherwise we print the size reduction and exit with exit code 0.
 */
object TestLogCleaning {

  def main(args: Array[String]) {
    val parser = new OptionParser
    val numMessagesOpt = parser.accepts("messages", "The number of messages to send or consume.")
                               .withRequiredArg
                               .describedAs("count")
                               .ofType(classOf[java.lang.Long])
                               .defaultsTo(Long.MaxValue)
    val numDupsOpt = parser.accepts("duplicates", "The number of duplicates for each key.")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(5)
    val brokerOpt = parser.accepts("broker", "Url to connect to.")
                          .withRequiredArg
                          .describedAs("url")
                          .ofType(classOf[String])
    val topicsOpt = parser.accepts("topics", "The number of topics to test.")
                          .withRequiredArg
                          .describedAs("count")
                          .ofType(classOf[java.lang.Integer])
                          .defaultsTo(1)
    val zkConnectOpt = parser.accepts("zk", "Zk url.")
                             .withRequiredArg
                             .describedAs("url")
                             .ofType(classOf[String])
    val sleepSecsOpt = parser.accepts("sleep", "Time to sleep between production and consumption.")
                             .withRequiredArg
                             .describedAs("ms")
                             .ofType(classOf[java.lang.Integer])
                             .defaultsTo(0)
    val cleanupOpt = parser.accepts("cleanup", "Delete temp files when done.")
    
    val options = parser.parse(args:_*)
    
    if(!options.has(brokerOpt) || !options.has(zkConnectOpt) || !options.has(numMessagesOpt)) {
      parser.printHelpOn(System.err)
      System.exit(1)
    }
    
    // parse options
    val messages = options.valueOf(numMessagesOpt).longValue
    val dups = options.valueOf(numDupsOpt).intValue
    val brokerUrl = options.valueOf(brokerOpt)
    val topicCount = options.valueOf(topicsOpt).intValue
    val zkUrl = options.valueOf(zkConnectOpt)
    val sleepSecs = options.valueOf(sleepSecsOpt).intValue
    val cleanup = options.has(cleanupOpt)
    
    val testId = new Random().nextInt(Int.MaxValue)
    val topics = (0 until topicCount).map("log-cleaner-test-" + testId + "-" + _).toArray
    
    println("Producing %d messages...".format(messages))
    val producedDataFile = produceMessages(brokerUrl, topics, messages, dups, cleanup)
    println("Sleeping for %d seconds...".format(sleepSecs))
    Thread.sleep(sleepSecs * 1000)
    println("Consuming messages...")
    val consumedDataFile = consumeMessages(zkUrl, topics, cleanup)
    
    val producedLines = lineCount(producedDataFile)
    val consumedLines = lineCount(consumedDataFile)
    val reduction = 1.0 - consumedLines.toDouble/producedLines.toDouble
    println("%d rows of data produced, %d rows of data consumed (%.1f%% reduction).".format(producedLines, consumedLines, 100 * reduction))
    
    println("Validating output files...")
    validateOutput(externalSort(producedDataFile), externalSort(consumedDataFile))
    println("All done.")
  }
  
  def lineCount(file: File): Int = io.Source.fromFile(file).getLines.size
  
  def validateOutput(produced: BufferedReader, consumed: BufferedReader) {
    while(true) {
      val prod = readFinalValue(produced)
      val cons = readFinalValue(consumed)
      if(prod == null && cons == null) {
        return
      } else if(prod != cons) {
        System.err.println("Validation failed prod = %s, cons = %s!".format(prod, cons))
        System.exit(1)
      }
    }
  }
  
  def readFinalValue(reader: BufferedReader): (String, Int, Int) = {
    def readTuple() = {
      val line = reader.readLine
      if(line == null)
        null
      else
        line.split("\t")
    }
    var prev = readTuple()
    if(prev == null)
      return null
    while(true) {
      reader.mark(1024)
      val curr = readTuple()
      if(curr == null || curr(0) != prev(0) || curr(1) != prev(1)) {
        reader.reset()
        return (prev(0), prev(1).toInt, prev(2).toInt)
      } else {
        prev = curr
      }
    }
    return null
  }
  
  def externalSort(file: File): BufferedReader = {
    val builder = new ProcessBuilder("sort", "--key=1,2", "--stable", "--buffer-size=20%", file.getAbsolutePath)
    val process = builder.start()
    new BufferedReader(new InputStreamReader(process.getInputStream()))
  }
  
  def produceMessages(brokerUrl: String, 
                      topics: Array[String], 
                      messages: Long, 
                      dups: Int, 
                      cleanup: Boolean): File = {
    val producerProps = new Properties
    producerProps.setProperty("producer.type", "async")
    producerProps.setProperty("broker.list", brokerUrl)
    producerProps.setProperty("serializer.class", classOf[StringEncoder].getName)
    producerProps.setProperty("key.serializer.class", classOf[StringEncoder].getName)
    producerProps.setProperty("queue.enqueue.timeout.ms", "-1")
    producerProps.setProperty("batch.size", 1000.toString)
    val producer = new Producer[String, String](new ProducerConfig(producerProps))
    val rand = new Random(1)
    val keyCount = (messages / dups).toInt
    val producedFile = File.createTempFile("kafka-log-cleaner-produced-", ".txt")
    if(cleanup)
      producedFile.deleteOnExit()
    val producedWriter = new BufferedWriter(new FileWriter(producedFile), 1024*1024)
    for(i <- 0L until (messages * topics.length)) {
      val topic = topics((i % topics.length).toInt)
      val key = rand.nextInt(keyCount)
      producer.send(KeyedMessage(topic = topic, key = key.toString, message = i.toString))
      producedWriter.write("%s\t%s\t%s\n".format(topic, key, i))
    }
    producedWriter.close()
    producer.close()
    producedFile
  }
  
  def consumeMessages(zkUrl: String, topics: Array[String], cleanup: Boolean): File = {
    val consumerProps = new Properties
    consumerProps.setProperty("group.id", "log-cleaner-test-" + new Random().nextInt(Int.MaxValue))
    consumerProps.setProperty("zk.connect", zkUrl)
    consumerProps.setProperty("consumer.timeout.ms", (5*1000).toString)
    val connector = new ZookeeperConsumerConnector(new ConsumerConfig(consumerProps))
    val streams = connector.createMessageStreams(topics.map(topic => (topic, 1)).toMap, new StringDecoder, new StringDecoder)
    val consumedFile = File.createTempFile("kafka-log-cleaner-consumed-", ".txt")
    if(cleanup)
      consumedFile.deleteOnExit()
    val consumedWriter = new BufferedWriter(new FileWriter(consumedFile))
    for(topic <- topics) {
      val stream = streams(topic).head
      try {
        for(item <- stream)
          consumedWriter.write("%s\t%s\t%s\n".format(topic, item.key, item.message))
      } catch {
        case e: ConsumerTimeoutException => 
      }
    }
    consumedWriter.close()
    connector.shutdown()
    consumedFile
  }
  
}