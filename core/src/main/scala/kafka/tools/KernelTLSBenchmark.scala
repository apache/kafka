package kafka.tools

import com.typesafe.scalalogging.LazyLogging
import joptsimple.OptionException
import kafka.utils.{CommandDefaultOptions, CommandLineUtils}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, SslConfigs}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils

import java.time.Duration
import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.{Properties, UUID}
import scala.collection.JavaConverters._

object KernelTLSBenchmark extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = new KernelTLSBenchmarkConfig(args)
    println("Warming up page cache...")

    val adminProps = filterProps(config.props, AdminClientConfig.configNames)
    val adminClient = AdminClient.create(adminProps)
    val (partitionCount, leaderIds) = getPartitions(config.topic, adminClient)

    config.partitions.foreach(partitions => {
      if (partitions > partitionCount) {
        throw new IllegalArgumentException(
          s"Number of partitions of topic ${config.topic} found to " +
            s"be ${partitionCount}, which is less than $partitions")
      }
    })

    val partitionsToConsume: Int = config.partitions match {
      case Some(p) => p
      case None => partitionCount
    }

    runConsume(print = false, 1, partitionsToConsume, config)
    val withDisabled = multipleRuns(
      print = true, kernelOffloadEnabled = false, adminClient, partitionsToConsume, leaderIds, config)
    val withEnabled = multipleRuns(
      print = true, kernelOffloadEnabled = true, adminClient, partitionsToConsume, leaderIds, config)
    val gainPercentage = 100.0 * (withEnabled - withDisabled) / withDisabled
    println("Throughput gain percentage = %.2f%%".format(gainPercentage))
  }

  private def filterProps(in: Properties, allowedKeys: util.Set[String]): Properties = {
    val out = new Properties()
    val map = in.asScala
      .filter(entry => allowedKeys.contains(entry._1))
      .asJava
      out.putAll(map)
    out
  }

  private def getPartitions(topicName: String, adminClient: AdminClient): (Int, Set[Int]) = {
    val result = adminClient.describeTopics(Seq(topicName).asJava).all().get()
    val partitionCount = result.get(topicName).partitions().size()
    val leaderIds = result.get(topicName).partitions().asScala
      .map(tpInfo => tpInfo.leader().id()).toSet
    (partitionCount, leaderIds)
  }

  private def setKernelTlsConfig(
    kernelOffloadEnabled: Boolean, adminClient: AdminClient,
    brokerIds: Iterable[Int], sslListenerName: String): Unit = {
    val configKey = s"listener.name.${sslListenerName.toLowerCase}.${SslConfigs.SSL_KERNEL_OFFLOAD_ENABLE_CONFIG}"
    val configValue = if (kernelOffloadEnabled) "true" else "false"
    val configEntry = new ConfigEntry(configKey, configValue)

    val configMap = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]

    brokerIds.foreach(brokerId => {
      val configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString)
      configMap.put(configResource, Seq(new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET)).asJava)
    })

    val result = adminClient.incrementalAlterConfigs(configMap)
    result.all().get()
  }

  private def multipleRuns(
    print: Boolean, kernelOffloadEnabled: Boolean, adminClient: AdminClient,
    partitionsToConsume: Int, brokerIds: Iterable[Int], config: KernelTLSBenchmarkConfig): Double = {
    setKernelTlsConfig(kernelOffloadEnabled, adminClient, brokerIds, config.sslListenerName)
    Thread.sleep(10 * 1000)
    val enableStr = if (kernelOffloadEnabled) "enabled" else "disabled"
    if (print) {
      println(s"Consuming with KTLS $enableStr")
    }
    var totalBytesRead: Long = 0
    var totalElapsedMillis: Long = 0
    for (runIndex <- 1 to config.numRuns) {
      val (runBytesRead: Long, runElapsedMillis: Long) = runConsume(print, runIndex, partitionsToConsume, config)
      totalBytesRead += runBytesRead
      totalElapsedMillis += runElapsedMillis
    }
    val totalMB = totalBytesRead * 1.0 / (1024 * 1024)
    val totalSec = totalElapsedMillis / 1000.0
    val totalMBPerSec = totalMB / totalSec
    if (print) {
      println("Total throughput with KTLS %s = %.2f MB/s, time elapsed = %d ms"
        .format(enableStr, totalMBPerSec, totalElapsedMillis))
    }
    totalMBPerSec
  }

  private def runConsume(print: Boolean, runIndex: Int, partitionsToConsume: Int, config: KernelTLSBenchmarkConfig): (Long, Long) = {
    val groupId = UUID.randomUUID.toString
    val props = filterProps(config.props, ConsumerConfig.configNames)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val totalRecordsRead = new AtomicLong(0)
    val totalBytesRead = new AtomicLong(0)

    var startMs, endMs = 0L

    val countDownLatch = new CountDownLatch(partitionsToConsume)

    if (print) {
      printf(s"[Run $runIndex] Fetching records...")
    }
    startMs = System.currentTimeMillis
    for (partition <- 0 to partitionsToConsume - 1) {
      val runnable = new ConsumeRunnable(
        config.topic, partition, props, config, countDownLatch, totalRecordsRead, totalBytesRead)
      val thread = new Thread(runnable, "consumer-" + partition.toString)
      thread.start()
    }

    countDownLatch.await()
    endMs = System.currentTimeMillis

    val elapsedMillis = endMs - startMs
    val elapsedSecs = elapsedMillis / 1000.0

    val totalMBRead = (totalBytesRead.get * 1.0) / (1024 * 1024)
    val mbRate: Double = totalMBRead / elapsedSecs
    val messageRate = totalRecordsRead.get / elapsedSecs

    if (print) {
      println(" Throughput = %.2f MB/s".format(mbRate))
    }
    return (totalBytesRead.get, elapsedMillis)
  }

  class ConsumeRunnable(
    topic: String, partition: Int, props: Properties, config: KernelTLSBenchmarkConfig, countDownLatch: CountDownLatch,
    totalRecordsRead: AtomicLong, totalBytesRead: AtomicLong) extends Runnable {
    override def run(): Unit = {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
      consumer.assign(Seq(new TopicPartition(topic, partition)).asJava)

      // Now start the benchmark
      var currentTimeMillis = System.currentTimeMillis
      var lastConsumedTime = currentTimeMillis

      var tot: Long = 0
      while (totalRecordsRead.get < config.numRecords && currentTimeMillis - lastConsumedTime <= config.timeoutMs) {
        val records = consumer.poll(Duration.ofMillis(100)).asScala
        currentTimeMillis = System.currentTimeMillis
        if (records.nonEmpty)
          lastConsumedTime = currentTimeMillis
        var bytesRead = 0L
        var recordsRead = 0L
        for (record <- records) {
          recordsRead += 1
          if (record.key != null)
            bytesRead += record.key.length
          if (record.value != null)
            bytesRead += record.value.length
        }
        totalRecordsRead.addAndGet(recordsRead)
        totalBytesRead.addAndGet(bytesRead)
        tot += recordsRead
      }

      if (totalRecordsRead.get() < config.numRecords) {
        println(s"WARNING: Exiting before consuming the expected number of records: timeout (${config.timeoutMs} ms) exceeded. ")
      }
      consumer.close()
      countDownLatch.countDown()
    }
  }

  class KernelTLSBenchmarkConfig(args: Array[String]) extends CommandDefaultOptions(args) {
    val consumerConfigOpt = parser.accepts("consumer-config", "Consumer config properties file.")
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val numRecordsOpt = parser.accepts("records", "REQUIRED: The number of records to consume")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Long])
    val partitionsOpt = parser.accepts("partitions", "REQUIRED: The number of partitions from which to consume")
      .withRequiredArg
      .describedAs("partitions")
      .ofType(classOf[java.lang.Integer])
    val numRunsOpt = parser.accepts("runs", "Number of runs to perform during the benchmark.")
      .withRequiredArg
      .describedAs("runs")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    val sslListenerNameOpt = parser.accepts("ssl-listener-name",
      "The name of the SSL listener as configured in Kafka broker config.")
      .withRequiredArg
      .describedAs("ssl listener name")
      .ofType(classOf[String])
      .defaultsTo("SSL")

    try
      options = parser.parse(args: _*)
    catch {
      case e: OptionException =>
        CommandLineUtils.printUsageAndDie(parser, e.getMessage)
    }

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps in performance test for the full zookeeper consumer")

    CommandLineUtils.checkRequiredArgs(parser, options,
      consumerConfigOpt, topicOpt, numRecordsOpt, numRunsOpt)

    val props: Properties = Utils.loadProps(options.valueOf(consumerConfigOpt))

    import org.apache.kafka.clients.consumer.ConsumerConfig

    // props.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupIdOpt))
    // props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, options.valueOf(socketBufferSizeOpt).toString)
    // props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, options.valueOf(fetchSizeOpt).toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    val topic = options.valueOf(topicOpt)
    val numRecords = options.valueOf(numRecordsOpt).longValue
    val numRuns = options.valueOf(numRunsOpt).intValue
    val sslListenerName = options.valueOf(sslListenerNameOpt)
    val timeoutMs = 10 * 1000
    val partitions : Option[Int] = if (options.has(partitionsOpt)) Some(options.valueOf(partitionsOpt).intValue()) else None
  }
}
