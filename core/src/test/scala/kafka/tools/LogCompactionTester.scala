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

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.{Properties, Random}

import joptsimple.OptionParser
import kafka.utils._
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringDeserializer}
import org.apache.kafka.common.utils.{AbstractIterator, Utils}
import org.apache.kafka.server.util.CommandLineUtils

import scala.jdk.CollectionConverters._

/**
 * This is a torture test that runs against an existing broker
 *
 * Here is how it works:
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
object LogCompactionTester {

  //maximum line size while reading produced/consumed record text file
  private val ReadAheadLimit = 4906

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val numMessagesOpt = parser.accepts("messages", "The number of messages to send or consume.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(Long.MaxValue)
    val messageCompressionOpt = parser.accepts("compression-type", "message compression type")
      .withOptionalArg
      .describedAs("compressionType")
      .ofType(classOf[java.lang.String])
      .defaultsTo("none")
    val numDupsOpt = parser.accepts("duplicates", "The number of duplicates for each key.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(5)
    val brokerOpt = parser.accepts("bootstrap-server", "The server(s) to connect to.")
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
    val sleepSecsOpt = parser.accepts("sleep", "Time in milliseconds to sleep between production and consumption.")
      .withRequiredArg
      .describedAs("ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(0)

    val options = parser.parse(args: _*)

    if (args.isEmpty)
      CommandLineUtils.printUsageAndExit(parser, "A tool to test log compaction. Valid options are: ")

    CommandLineUtils.checkRequiredArgs(parser, options, brokerOpt, numMessagesOpt)

    // parse options
    val messages = options.valueOf(numMessagesOpt).longValue
    val compressionType = options.valueOf(messageCompressionOpt)
    val percentDeletes = options.valueOf(percentDeletesOpt).intValue
    val dups = options.valueOf(numDupsOpt).intValue
    val brokerUrl = options.valueOf(brokerOpt)
    val topicCount = options.valueOf(topicsOpt).intValue
    val sleepSecs = options.valueOf(sleepSecsOpt).intValue

    val testId = new Random().nextLong
    val topics = (0 until topicCount).map("log-cleaner-test-" + testId + "-" + _).toArray
    createTopics(brokerUrl, topics.toSeq)

    println(s"Producing $messages messages..to topics ${topics.mkString(",")}")
    val producedDataFilePath = produceMessages(brokerUrl, topics, messages, compressionType, dups, percentDeletes)
    println(s"Sleeping for $sleepSecs seconds...")
    Thread.sleep(sleepSecs * 1000)
    println("Consuming messages...")
    val consumedDataFilePath = consumeMessages(brokerUrl, topics)

    val producedLines = lineCount(producedDataFilePath)
    val consumedLines = lineCount(consumedDataFilePath)
    val reduction = 100 * (1.0 - consumedLines.toDouble / producedLines.toDouble)
    println(f"$producedLines%d rows of data produced, $consumedLines%d rows of data consumed ($reduction%.1f%% reduction).")

    println("De-duplicating and validating output files...")
    validateOutput(producedDataFilePath.toFile, consumedDataFilePath.toFile)
    Utils.delete(producedDataFilePath.toFile)
    Utils.delete(consumedDataFilePath.toFile)
    //if you change this line, we need to update test_log_compaction_tool.py system test
    println("Data verification is completed")
  }

  def createTopics(brokerUrl: String, topics: Seq[String]): Unit = {
    val adminConfig = new Properties
    adminConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
    val adminClient = Admin.create(adminConfig)

    try {
      val topicConfigs = Map(TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT)
      val newTopics = topics.map(name => new NewTopic(name, 1, 1.toShort).configs(topicConfigs.asJava)).asJava
      adminClient.createTopics(newTopics).all.get

      var pendingTopics: Seq[String] = Seq()
      TestUtils.waitUntilTrue(() => {
        val allTopics = adminClient.listTopics.names.get.asScala.toSeq
        pendingTopics = topics.filter(topicName => !allTopics.contains(topicName))
        pendingTopics.isEmpty
      }, s"timed out waiting for topics : $pendingTopics")

    } finally adminClient.close()
  }

  def lineCount(filPath: Path): Int = Files.readAllLines(filPath).size

  def validateOutput(producedDataFile: File, consumedDataFile: File): Unit = {
    val producedReader = externalSort(producedDataFile)
    val consumedReader = externalSort(consumedDataFile)
    val produced = valuesIterator(producedReader)
    val consumed = valuesIterator(consumedReader)

    val producedDedupedFile = new File(producedDataFile.getAbsolutePath + ".deduped")
    val producedDeduped : BufferedWriter = Files.newBufferedWriter(producedDedupedFile.toPath, UTF_8)

    val consumedDedupedFile = new File(consumedDataFile.getAbsolutePath + ".deduped")
    val consumedDeduped : BufferedWriter = Files.newBufferedWriter(consumedDedupedFile.toPath, UTF_8)
    var total = 0
    var mismatched = 0
    while (produced.hasNext && consumed.hasNext) {
      val p = produced.next()
      producedDeduped.write(p.toString)
      producedDeduped.newLine()
      val c = consumed.next()
      consumedDeduped.write(c.toString)
      consumedDeduped.newLine()
      if (p != c)
        mismatched += 1
      total += 1
    }
    producedDeduped.close()
    consumedDeduped.close()
    println(s"Validated $total values, $mismatched mismatches.")
    require(!produced.hasNext, "Additional values produced not found in consumer log.")
    require(!consumed.hasNext, "Additional values consumed not found in producer log.")
    require(mismatched == 0, "Non-zero number of row mismatches.")
    // if all the checks worked out we can delete the deduped files
    Utils.delete(producedDedupedFile)
    Utils.delete(consumedDedupedFile)
  }

  def require(requirement: Boolean, message: => Any): Unit = {
    if (!requirement) {
      System.err.println(s"Data validation failed : $message")
      Exit.exit(1)
    }
  }

  def valuesIterator(reader: BufferedReader): Iterator[TestRecord] = {
    new AbstractIterator[TestRecord] {
      def makeNext(): TestRecord = {
        var next = readNext(reader)
        while (next != null && next.delete)
          next = readNext(reader)
        if (next == null)
          allDone()
        else
          next
      }
    }.asScala
  }

  def readNext(reader: BufferedReader): TestRecord = {
    var line = reader.readLine()
    if (line == null)
      return null
    var curr = TestRecord.parse(line)
    while (true) {
      line = peekLine(reader)
      if (line == null)
        return curr
      val next = TestRecord.parse(line)
      if (next == null || next.topicAndKey != curr.topicAndKey)
        return curr
      curr = next
      reader.readLine()
    }
    null
  }

  def peekLine(reader: BufferedReader) = {
    reader.mark(ReadAheadLimit)
    val line = reader.readLine
    reader.reset()
    line
  }

  def externalSort(file: File): BufferedReader = {
    val builder = new ProcessBuilder("sort", "--key=1,2", "--stable", "--buffer-size=20%", "--temporary-directory=" + Files.createTempDirectory("log_compaction_test"), file.getAbsolutePath)
    val process = builder.start
    new Thread() {
      override def run(): Unit = {
        val exitCode = process.waitFor()
        if (exitCode != 0) {
          System.err.println("Process exited abnormally.")
          while (process.getErrorStream.available > 0) {
            System.err.write(process.getErrorStream.read())
          }
        }
      }
    }.start()
    new BufferedReader(new InputStreamReader(process.getInputStream, UTF_8), 10 * 1024 * 1024)
  }

  def produceMessages(brokerUrl: String,
                      topics: Array[String],
                      messages: Long,
                      compressionType: String,
                      dups: Int,
                      percentDeletes: Int): Path = {
    val producerProps = new Properties
    producerProps.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType)
    val producer = new KafkaProducer(producerProps, new ByteArraySerializer, new ByteArraySerializer)
    try {
      val rand = new Random(1)
      val keyCount = (messages / dups).toInt
      val producedFilePath = Files.createTempFile("kafka-log-cleaner-produced-", ".txt")
      println(s"Logging produce requests to $producedFilePath")
      val producedWriter: BufferedWriter = Files.newBufferedWriter(producedFilePath, UTF_8)
      for (i <- 0L until (messages * topics.length)) {
        val topic = topics((i % topics.length).toInt)
        val key = rand.nextInt(keyCount)
        val delete = (i % 100) < percentDeletes
        val msg =
          if (delete)
            new ProducerRecord[Array[Byte], Array[Byte]](topic, key.toString.getBytes(UTF_8), null)
          else
            new ProducerRecord(topic, key.toString.getBytes(UTF_8), i.toString.getBytes(UTF_8))
        producer.send(msg)
        producedWriter.write(TestRecord(topic, key, i, delete).toString)
        producedWriter.newLine()
      }
      producedWriter.close()
      producedFilePath
    } finally {
      producer.close()
    }
  }

  def createConsumer(brokerUrl: String): Consumer[String, String] = {
    val consumerProps = new Properties
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "log-cleaner-test-" + new Random().nextInt(Int.MaxValue))
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    new KafkaConsumer(consumerProps, new StringDeserializer, new StringDeserializer)
  }

  def consumeMessages(brokerUrl: String, topics: Array[String]): Path = {
    val consumer = createConsumer(brokerUrl)
    consumer.subscribe(topics.toSeq.asJava)
    val consumedFilePath = Files.createTempFile("kafka-log-cleaner-consumed-", ".txt")
    println(s"Logging consumed messages to $consumedFilePath")
    val consumedWriter: BufferedWriter = Files.newBufferedWriter(consumedFilePath, UTF_8)

    try {
      var done = false
      while (!done) {
        val consumerRecords = consumer.poll(Duration.ofSeconds(20))
        if (!consumerRecords.isEmpty) {
          for (record <- consumerRecords.asScala) {
            val delete = record.value == null
            val value = if (delete) -1L else record.value.toLong
            consumedWriter.write(TestRecord(record.topic, record.key.toInt, value, delete).toString)
            consumedWriter.newLine()
          }
        } else {
          done = true
        }
      }
      consumedFilePath
    } finally {
      consumedWriter.close()
      consumer.close()
    }
  }

  def readString(buffer: ByteBuffer): String = {
    Utils.utf8(buffer)
  }

}

case class TestRecord(topic: String, key: Int, value: Long, delete: Boolean) {
  override def toString = topic + "\t" + key + "\t" + value + "\t" + (if (delete) "d" else "u")
  def topicAndKey = topic + key
}

object TestRecord {
  def parse(line: String): TestRecord = {
    val components = line.split("\t")
    new TestRecord(components(0), components(1).toInt, components(2).toLong, components(3) == "d")
  }
}
