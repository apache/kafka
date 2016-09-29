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

package other.kafka

import java.awt.image.BufferedImage
import java.io.{File, FileOutputStream, PrintWriter}
import javax.imageio.ImageIO

import kafka.admin.ReassignPartitionsCommand
import kafka.common.TopicAndPartition
import kafka.server.{KafkaConfig, KafkaServer, QuotaType}
import kafka.utils.TestUtils._
import kafka.utils.ZkUtils._
import kafka.utils.{CoreUtils, Logging, TestUtils, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.ProducerRecord
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartFactory, ChartFrame, JFreeChart}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq, mutable}

object ReplicationQuotasPerformance {
  new File("Experiments").mkdir()
  private val dir = "Experiments/Run" + System.currentTimeMillis().toString.substring(8)
  new File(dir).mkdir()
  private val log = new File(dir, "Log.txt")
  private var showGraphsOnExperimentCompletion = false

  def main(args: Array[String]): Unit = {
    if (args.length > 0 && args(0) == "graph") showGraphsOnExperimentCompletion = true

    val configs = Seq(
      new Config("Experiment1", brokers = 5, partitions = 5, throttle = 1000 * 1000, msgCount = 100, msgSize = 100 * 1000),
      new Config("Experiment2", brokers = 5, partitions = 50, throttle = 10 * 1000 * 1000, msgCount = 10 * 100, msgSize = 100 * 1000),
      new Config("Experiment3", brokers = 50, partitions = 50, throttle = 2 * 1000 * 1000, msgCount = 10 * 100, msgSize = 100 * 1000),
      new Config("Experiment4", brokers = 25, partitions = 100, throttle = 4 * 1000 * 1000, msgCount = 1 * 1000, msgSize = 100 * 1000),
      new Config("Experiment5", brokers = 5, partitions = 50, throttle = 50 * 1000 * 1000, msgCount = 1 * 1000, msgSize = 100 * 1000)
    )
    configs.foreach(run(_))
    if(!showGraphsOnExperimentCompletion)
      System.exit(0)
  }

  def run(config: Config) {
    val experiment = new Experiment()
    try {
      experiment.setUp
      experiment.run(config)
    }
    catch {case e: Exception => e.printStackTrace()}
    finally {
      experiment.tearDown
    }
  }

  case class Config(name: String, brokers: Int, partitions: Int, throttle: Long, msgCount: Int, msgSize: Int) {
    val targetBytesPerBrokerMB: Long = msgCount.toLong * msgSize.toLong * partitions.toLong / brokers.toLong / 1000000
    appendToJournal

    def appendToJournal(): Unit = {
      val stream = new FileOutputStream(
        log,
        true)
      val message = s"\n\n$name " +
        s"\n\t- BrokerCount: $brokers" +
        s"\n\t- PartitionCount: $partitions" +
        f"\n\t- Throttle: $throttle%,.0f MB/s" +
        f"\n\t- MsgCount: $msgCount%,.0f " +
        f"\n\t- MsgSize: $msgSize%,.0f " +
        s"\n\t- TargetBytesPerBrokerMB: $targetBytesPerBrokerMB\n\n"

      new PrintWriter(stream) {
        append(message)
        close
      }
      println(message)
    }
  }

  class Experiment extends ZooKeeperTestHarness with Logging {
    val topicName = "my-topic"
    var experimentName = "unset"
    val partitionId = 0
    var servers: Seq[KafkaServer] = null
    val leaderRates = mutable.Map[Int, Array[Double]]()
    val followerRates = mutable.Map[Int, Array[Double]]()

    def startBrokers(brokerIds: Seq[Int]) {
      println("Starting Brokers")
      servers = brokerIds.map(i => createBrokerConfig(i, zkConnect))
        .map(c => createServer(KafkaConfig.fromProps(c)))
    }

    override def tearDown() {
      servers.par.foreach(_.shutdown())
      servers.par.foreach(server => CoreUtils.delete(server.config.logDirs))
      super.tearDown()
    }

    def run(config: Config) {
      experimentName = config.name
      val brokers = (100 to 100 + config.brokers)
      var count = 0
      val shift = Math.round(config.brokers / 2)

      def nextReplicaRoundRobin(): Int = {
        count = count + 1
        100 + (count + shift) % config.brokers
      }
      val replicas = (0 to config.partitions).map(partition => partition -> Seq(nextReplicaRoundRobin())).toMap

      startBrokers(brokers)
      createTopic(zkUtils, topicName, replicas, servers)

      println("Writing Data")
      val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers), retries = 5, acks = 0)
      (0 to config.msgCount).foreach { x =>
        (0 to config.partitions).foreach { partition =>
          producer.send(new ProducerRecord(topicName, partition, null, new Array[Byte](config.msgSize)))
        }
      }

      println("Starting Reassignment")
      val newAssignment = ReassignPartitionsCommand.generateAssignment(zkUtils, brokers, json(topicName), true)._1
      ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment), config.throttle)

      //Await completion
      waitForReassignmentToComplete()

      renderChart(leaderRates, "Leader")
      renderChart(followerRates, "Follower")
      logOutput(config, replicas, newAssignment)
    }

    def logOutput(config: Config, replicas: Map[Int, Seq[Int]], newAssignment: Map[TopicAndPartition, Seq[Int]]): Unit = {
      val actual = zkUtils.getPartitionAssignmentForTopics(Seq(topicName))(topicName)
      val existing = zkUtils.getReplicaAssignmentForTopics(newAssignment.map(_._1.topic).toSeq)
      val moves = new ReassignPartitionsCommand(zkUtils, newAssignment).replicaMoves(existing, newAssignment)
      val allMoves = moves.get(topicName)
      val physicalMoves = allMoves.get.split(",").size / 2

      //Long stats
      println("The replicas are " + replicas.toSeq.sortBy(_._1).map("\n" + _))
      println("This is the current replica assignment:\n" + actual.toSeq)
      println("proposed assignment is: \n" + newAssignment)
      println("moves are: " + allMoves)
      println("This is the assigment we eneded up with" + actual)

      //Test Stats
      println(s"numBrokers: ${config.brokers}")
      println(s"numPartitions: ${config.partitions}")
      println(s"throttle: ${config.throttle}")
      println(s"numMessages: ${config.msgCount}")
      println(s"msgSize: ${config.msgSize}")
      println(s"We will write ${config.targetBytesPerBrokerMB / 1000000}MB of data per broker")
      println(s"Worst case duration is ${config.targetBytesPerBrokerMB / config.throttle}")
      println(s"Move count is : $physicalMoves")
    }

    private def waitForOffsetsToMatch(offset: Int, partitionId: Int, broker: KafkaServer, topic: String): Boolean = {
      waitUntilTrue(() => {
        offset == broker.getLogManager.getLog(TopicAndPartition(topic, partitionId))
          .map(_.logEndOffset).getOrElse(0)
      }, s"Offsets did not match for partition $partitionId on broker ${broker.config.brokerId}", 60000)
    }

    def waitForReassignmentToComplete() {
      waitUntilTrue(() => {
        printRateMetrics()
        val success = !zkUtils.pathExists(ReassignPartitionsPath)
        success
      }, s"Znode ${ZkUtils.ReassignPartitionsPath} wasn't deleted", Int.MaxValue, pause = 1000L)
    }

    def renderChart(data: mutable.Map[Int, Array[Double]], name: String): Unit = {
      val dataset = new XYSeriesCollection

      data.foreach { case (broker, values) =>
        println("Found values for broker " + broker + " = " + values.map(_.toString).toSeq)
        val series = new XYSeries("Broker:" + broker)
        var x = 0
        values.foreach { value =>
          series.add(x, value)
          x = x + 1
        }
        dataset.addSeries(series)
      }

      val chart: JFreeChart = ChartFactory.createXYLineChart(
        experimentName + " - " + name + " Throttling Performance",
        "Time (s)",
        "Throttle Throughput (B/s)",
        dataset
        , PlotOrientation.VERTICAL, false, true, false
      )

      saveToFile(chart.createBufferedImage(1000, 700), experimentName + "-" + name)

      if(showGraphsOnExperimentCompletion) {
        val frame = new ChartFrame(
          experimentName,
          chart
        )
        frame.pack()
        frame.setVisible(true)
      }

      println(s"Chart generated for $name")
    }

    def saveToFile(img: BufferedImage, name: String) {
      val out = new File(dir, name + ".png")
      ImageIO.write(img, "png", out)
    }

    def record(rates: mutable.Map[Int, Array[Double]], brokerId: Int, currentRate: Double) = {
      var leaderRatesBroker: Array[Double] = rates.getOrElse(brokerId, Array[Double]())
      leaderRatesBroker = leaderRatesBroker ++ Array(currentRate)
      rates.put(brokerId, leaderRatesBroker)
    }

    def printRateMetrics() {
      //    println("Printing rates on servers "+ servers)
      for (broker <- servers) {
        val leaderRate: Double = measuredRate(broker, QuotaType.LeaderReplication)

        if (broker.config.brokerId == 100)
          warn("waiting... Leader rate on 101 is " + leaderRate)

        record(leaderRates, broker.config.brokerId, leaderRate)
        if (leaderRate > 0)
          trace("Leader Rate on " + broker.config.brokerId + " is " + leaderRate)

        val followerRate: Double = measuredRate(broker, QuotaType.FollowerReplication)
        record(followerRates, broker.config.brokerId, followerRate)
        if (followerRate > 0)
          trace("Follower Rate on " + broker.config.brokerId + " is " + followerRate)
      }
    }

    private def measuredRate(broker: KafkaServer, repType: QuotaType): Double = {
      val metricName = broker.metrics.metricName("byte-rate", repType.toString)
      if (broker.metrics.metrics.asScala.contains(metricName))
        broker.metrics.metrics.asScala(metricName).value
      else -1
    }

    def json(topic: String*): String = {
      val topicStr = topic.map {
        t => "{\"topic\": \"" + t + "\"}"
      }.mkString(",")
      s"""{"topics": [$topicStr],"version":1}"""
    }
  }
}

