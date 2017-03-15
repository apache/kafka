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

import joptsimple.OptionParser
import java.util.Properties
import java.util.Random
import java.io._

import kafka.consumer._
import kafka.serializer._
import kafka.utils._
import kafka.log.Log
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.record.FileRecords

import scala.collection.JavaConverters._

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
    val messageCompressionOpt = parser.accepts("compression-type", "message compression type")
                               .withOptionalArg()
                               .describedAs("compressionType")
                               .ofType(classOf[java.lang.String])
                               .defaultsTo("none")
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
    val percentDeletesOpt = parser.accepts("percent-deletes", "The percentage of updates that are deletes.")
    		                      .withRequiredArg
    		                      .describedAs("percent")
    		                      .ofType(classOf[java.lang.Integer])
    		                      .defaultsTo(0)
    val zkConnectOpt = parser.accepts("zk", "Zk url.")
                             .withRequiredArg
                             .describedAs("url")
                             .ofType(classOf[String])
    val sleepSecsOpt = parser.accepts("sleep", "Time to sleep between production and consumption.")
                             .withRequiredArg
                             .describedAs("ms")
                             .ofType(classOf[java.lang.Integer])
                             .defaultsTo(0)
    val dumpOpt = parser.accepts("dump", "Dump the message contents of a topic partition that contains test data from this test to standard out.")
                        .withRequiredArg
                        .describedAs("directory")
                        .ofType(classOf[String])

    val options = parser.parse(args:_*)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "An integration test for log cleaning.")

    if(options.has(dumpOpt)) {
      dumpLog(new File(options.valueOf(dumpOpt)))
      Exit.exit(0)
    }

    CommandLineUtils.checkRequiredArgs(parser, options, brokerOpt, zkConnectOpt, numMessagesOpt)

    // parse options
    val messages = options.valueOf(numMessagesOpt).longValue
    val compressionType = options.valueOf(messageCompressionOpt)
    val percentDeletes = options.valueOf(percentDeletesOpt).intValue
    val dups = options.valueOf(numDupsOpt).intValue
    val brokerUrl = options.valueOf(brokerOpt)
    val topicCount = options.valueOf(topicsOpt).intValue
    val zkUrl = options.valueOf(zkConnectOpt)
    val sleepSecs = options.valueOf(sleepSecsOpt).intValue

    val testId = new Random().nextInt(Int.MaxValue)
    val topics = (0 until topicCount).map("log-cleaner-test-" + testId + "-" + _).toArray

    println("Producing %d messages...".format(messages))
    val producedDataFile = produceMessages(brokerUrl, topics, messages, compressionType, dups, percentDeletes)
    println("Sleeping for %d seconds...".format(sleepSecs))
    Thread.sleep(sleepSecs * 1000)
    println("Consuming messages...")
    val consumedDataFile = consumeMessages(zkUrl, topics)

    val producedLines = lineCount(producedDataFile)
    val consumedLines = lineCount(consumedDataFile)
    val reduction = 1.0 - consumedLines.toDouble/producedLines.toDouble
    println("%d rows of data produced, %d rows of data consumed (%.1f%% reduction).".format(producedLines, consumedLines, 100 * reduction))
    
    println("De-duplicating and validating output files...")
    validateOutput(producedDataFile, consumedDataFile)
    producedDataFile.delete()
    consumedDataFile.delete()
  }
  
  def dumpLog(dir: File) {
    require(dir.exists, "Non-existent directory: " + dir.getAbsolutePath)
    for (file <- dir.list.sorted; if file.endsWith(Log.LogFileSuffix)) {
      val fileRecords = FileRecords.open(new File(dir, file))
      for (entry <- fileRecords.shallowEntries.asScala) {
        val key = TestUtils.readString(entry.record.key)
        val content = 
          if(entry.record.hasNullValue)
            null
          else
            TestUtils.readString(entry.record.value)
        println("offset = %s, key = %s, content = %s".format(entry.offset, key, content))
      }
    }
  }
  
  def lineCount(file: File): Int = io.Source.fromFile(file).getLines.size
  
  def validateOutput(producedDataFile: File, consumedDataFile: File) {
    val producedReader = externalSort(producedDataFile)
    val consumedReader = externalSort(consumedDataFile)
    val produced = valuesIterator(producedReader)
    val consumed = valuesIterator(consumedReader)
    val producedDedupedFile = new File(producedDataFile.getAbsolutePath + ".deduped")
    val producedDeduped = new BufferedWriter(new FileWriter(producedDedupedFile), 1024*1024)
    val consumedDedupedFile = new File(consumedDataFile.getAbsolutePath + ".deduped")
    val consumedDeduped = new BufferedWriter(new FileWriter(consumedDedupedFile), 1024*1024)
    var total = 0
    var mismatched = 0
    while(produced.hasNext && consumed.hasNext) {
      val p = produced.next()
      producedDeduped.write(p.toString)
      producedDeduped.newLine()
      val c = consumed.next()
      consumedDeduped.write(c.toString)
      consumedDeduped.newLine()
      if(p != c)
        mismatched += 1
      total += 1
    }
    producedDeduped.close()
    consumedDeduped.close()
    println("Validated " + total + " values, " + mismatched + " mismatches.")
    require(!produced.hasNext, "Additional values produced not found in consumer log.")
    require(!consumed.hasNext, "Additional values consumed not found in producer log.")
    require(mismatched == 0, "Non-zero number of row mismatches.")
    // if all the checks worked out we can delete the deduped files
    producedDedupedFile.delete()
    consumedDedupedFile.delete()
  }
  
  def valuesIterator(reader: BufferedReader) = {
    new IteratorTemplate[TestRecord] {
      def makeNext(): TestRecord = {
        var next = readNext(reader)
        while(next != null && next.delete)
          next = readNext(reader)
        if(next == null)
          allDone()
        else
          next
      }
    }
  }
  
  def readNext(reader: BufferedReader): TestRecord = {
    var line = reader.readLine()
    if(line == null)
      return null
    var curr = new TestRecord(line)
    while(true) {
      line = peekLine(reader)
      if(line == null)
        return curr
      val next = new TestRecord(line)
      if(next == null || next.topicAndKey != curr.topicAndKey)
        return curr
      curr = next
      reader.readLine()
    }
    null
  }
  
  def peekLine(reader: BufferedReader) = {
    reader.mark(4096)
    val line = reader.readLine
    reader.reset()
    line
  }
  
  def externalSort(file: File): BufferedReader = {
    val builder = new ProcessBuilder("sort", "--key=1,2", "--stable", "--buffer-size=20%", "--temporary-directory=" + System.getProperty("java.io.tmpdir"), file.getAbsolutePath)
    val process = builder.start()
    new Thread() {
      override def run() {
        val exitCode = process.waitFor()
        if(exitCode != 0) {
          System.err.println("Process exited abnormally.")
          while(process.getErrorStream.available > 0) {
            System.err.write(process.getErrorStream().read())
          }
        }
      }
    }.start()
    new BufferedReader(new InputStreamReader(process.getInputStream()), 10*1024*1024)
  }

  def produceMessages(brokerUrl: String,
                      topics: Array[String],
                      messages: Long,
                      compressionType: String,
                      dups: Int,
                      percentDeletes: Int): File = {
    val producerProps = new Properties
    producerProps.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType)
    val producer = new KafkaProducer[Array[Byte],Array[Byte]](producerProps)
    val rand = new Random(1)
    val keyCount = (messages / dups).toInt
    val producedFile = File.createTempFile("kafka-log-cleaner-produced-", ".txt")
    println("Logging produce requests to " + producedFile.getAbsolutePath)
    val producedWriter = new BufferedWriter(new FileWriter(producedFile), 1024*1024)
    for(i <- 0L until (messages * topics.length)) {
      val topic = topics((i % topics.length).toInt)
      val key = rand.nextInt(keyCount)
      val delete = i % 100 < percentDeletes
      val msg = 
        if(delete)
          new ProducerRecord[Array[Byte],Array[Byte]](topic, key.toString.getBytes(), null)
        else
          new ProducerRecord[Array[Byte],Array[Byte]](topic, key.toString.getBytes(), i.toString.getBytes())
      producer.send(msg)
      producedWriter.write(TestRecord(topic, key, i, delete).toString)
      producedWriter.newLine()
    }
    producedWriter.close()
    producer.close()
    producedFile
  }
  
  def makeConsumer(zkUrl: String, topics: Array[String]): ZookeeperConsumerConnector = {
    val consumerProps = new Properties
    consumerProps.setProperty("group.id", "log-cleaner-test-" + new Random().nextInt(Int.MaxValue))
    consumerProps.setProperty("zookeeper.connect", zkUrl)
    consumerProps.setProperty("consumer.timeout.ms", (20*1000).toString)
    consumerProps.setProperty("auto.offset.reset", "smallest")
    new ZookeeperConsumerConnector(new ConsumerConfig(consumerProps))
  }
  
  def consumeMessages(zkUrl: String, topics: Array[String]): File = {
    val connector = makeConsumer(zkUrl, topics)
    val streams = connector.createMessageStreams(topics.map(topic => (topic, 1)).toMap, new StringDecoder, new StringDecoder)
    val consumedFile = File.createTempFile("kafka-log-cleaner-consumed-", ".txt")
    println("Logging consumed messages to " + consumedFile.getAbsolutePath)
    val consumedWriter = new BufferedWriter(new FileWriter(consumedFile))
    for(topic <- topics) {
      val stream = streams(topic).head
      try {
        for(item <- stream) {
          val delete = item.message == null
          val value = if(delete) -1L else item.message.toLong
          consumedWriter.write(TestRecord(topic, item.key.toInt, value, delete).toString)
          consumedWriter.newLine()
        }
      } catch {
        case _: ConsumerTimeoutException =>
      }
    }
    consumedWriter.close()
    connector.shutdown()
    consumedFile
  }
  
}

case class TestRecord(topic: String, key: Int, value: Long, delete: Boolean) {
  def this(pieces: Array[String]) = this(pieces(0), pieces(1).toInt, pieces(2).toLong, pieces(3) == "d")
  def this(line: String) = this(line.split("\t"))
  override def toString = topic + "\t" +  key + "\t" + value + "\t" + (if(delete) "d" else "u")
  def topicAndKey = topic + key
}
