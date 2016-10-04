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

/**
  * Test rig for measuring throttling performance. Configure the parameters for a set of experiments, then execute them
  * and view the html output file, with charts, that are produced. You can also render the charts to the screen if
  * you wish.
  *
  * Currently you'll need about 40GB of disk space to run these experiments (data written x2). Tune the msgSize
  * & #partitions to adjust this.
  */
object ReplicationQuotasTestRig {
  new File("Experiments").mkdir()
  private val dir = "Experiments/Run" + System.currentTimeMillis().toString.substring(8)
  new File(dir).mkdir()


  def main(args: Array[String]): Unit = {
    val displayChartsOnScreen = if (args.length > 0 && args(0) == "show-gui") true else false
    val journal = new Journal()

    val experiments = Seq(
      new ExperimentDef("Experiment1", brokers = 5, partitions = 20, throttle = 1000 * 1000, msgsPerPartition = 100, msgSize = 100 * 1000),            //200MB total data written
      new ExperimentDef("Experiment2", brokers = 5, partitions = 50, throttle = 10 * 1000 * 1000, msgsPerPartition = 10 * 100, msgSize = 100 * 1000),  //5GB total data written
      new ExperimentDef("Experiment3", brokers = 50, partitions = 50, throttle = 2 * 1000 * 1000, msgsPerPartition = 10 * 100, msgSize = 100 * 1000),  //5GB total data written
      new ExperimentDef("Experiment4", brokers = 25, partitions = 100, throttle = 4 * 1000 * 1000, msgsPerPartition = 1 * 1000, msgSize = 100 * 1000), //10GB total data written
      new ExperimentDef("Experiment5", brokers = 5, partitions = 50, throttle = 50 * 1000 * 1000, msgsPerPartition = 4 * 1000, msgSize = 100 * 1000)   //20GB total data written
    )
    experiments.foreach(run(_, journal, displayChartsOnScreen))

    if (!displayChartsOnScreen)
      System.exit(0)
  }

  def run(config: ExperimentDef, journal: Journal, displayChartsOnScreen: Boolean) {
    val experiment = new Experiment()
    try {
      experiment.setUp
      experiment.run(config, journal, displayChartsOnScreen)
      journal.footer()
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      experiment.tearDown
    }
  }

  case class ExperimentDef(name: String, brokers: Int, partitions: Int, throttle: Long, msgsPerPartition: Int, msgSize: Int) {
    val targetBytesPerBrokerMB: Long = msgsPerPartition.toLong * msgSize.toLong * partitions.toLong / brokers.toLong / 1000000
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

    def run(config: ExperimentDef, journal: Journal, displayChartsOnScreen: Boolean) {
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
      (0 to config.msgsPerPartition).foreach { x =>
        (0 to config.partitions).foreach { partition =>
          producer.send(new ProducerRecord(topicName, partition, null, new Array[Byte](config.msgSize)))
        }
      }

      println("Starting Reassignment")
      val newAssignment = ReassignPartitionsCommand.generateAssignment(zkUtils, brokers, json(topicName), true)._1
      ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment), config.throttle)

      //Await completion
      waitForReassignmentToComplete()

      journal.appendToJournal(config)
      renderChart(leaderRates, "Leader", journal, displayChartsOnScreen)
      renderChart(followerRates, "Follower", journal, displayChartsOnScreen)
      logOutput(config, replicas, newAssignment)

      println("Output can be found here: " + journal.path())
    }

    def logOutput(config: ExperimentDef, replicas: Map[Int, Seq[Int]], newAssignment: Map[TopicAndPartition, Seq[Int]]): Unit = {
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
      println(s"numMessages: ${config.msgsPerPartition}")
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
        !zkUtils.pathExists(ReassignPartitionsPath)
      }, s"Znode ${ZkUtils.ReassignPartitionsPath} wasn't deleted", 60 * 60 * 1000, pause = 1000L)
    }

    def renderChart(data: mutable.Map[Int, Array[Double]], name: String, journal: Journal, displayChartsOnScreen: Boolean): Unit = {
      val dataset = addDataToChart(data)
      val chart = createChart(name, dataset)

      writeToFile(name, journal, chart)
      maybeDisplayOnScreen(displayChartsOnScreen, chart)
      println(s"Chart generated for $name")
    }

    def maybeDisplayOnScreen(displayChartsOnScreen: Boolean, chart: JFreeChart): Unit = {
      if (displayChartsOnScreen) {
        val frame = new ChartFrame(experimentName, chart)
        frame.pack()
        frame.setVisible(true)
      }
    }

    def writeToFile(name: String, journal: Journal, chart: JFreeChart): Unit = {
      val file = new File(dir, experimentName + "-" + name + ".png")
      ImageIO.write(chart.createBufferedImage(1000, 700), "png", file)
      journal.appendChart(file.getAbsolutePath, name.eq("Leader"))
    }

    def createChart(name: String, dataset: XYSeriesCollection): JFreeChart = {
      val chart: JFreeChart = ChartFactory.createXYLineChart(
        experimentName + " - " + name + " Throttling Performance",
        "Time (s)",
        "Throttle Throughput (B/s)",
        dataset
        , PlotOrientation.VERTICAL, false, true, false
      )
      chart
    }

    def addDataToChart(data: mutable.Map[Int, Array[Double]]): XYSeriesCollection = {
      val dataset = new XYSeriesCollection
      data.foreach { case (broker, values) =>
        val series = new XYSeries("Broker:" + broker)
        var x = 0
        values.foreach { value =>
          series.add(x, value)
          x += 1
        }
        dataset.addSeries(series)
      }
      dataset
    }

    def record(rates: mutable.Map[Int, Array[Double]], brokerId: Int, currentRate: Double) = {
      var leaderRatesBroker: Array[Double] = rates.getOrElse(brokerId, Array[Double]())
      leaderRatesBroker = leaderRatesBroker ++ Array(currentRate)
      rates.put(brokerId, leaderRatesBroker)
    }

    def printRateMetrics() {
      for (broker <- servers) {
        val leaderRate: Double = measuredRate(broker, QuotaType.LeaderReplication)
        if (broker.config.brokerId == 100)
          info("waiting... Leader rate on 101 is " + leaderRate)
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


  class Journal {
    private val log = new File(dir, "Log.html")
    header()

    def appendToJournal(config: ExperimentDef): Unit = {
      val message = s"\n\n<h3>${config.name}</h3>" +
        s"<p>- BrokerCount: ${config.brokers}" +
        s"<p>- PartitionCount: ${config.partitions}" +
        f"<p>- Throttle: ${config.throttle}%,.0f MB/s" +
        f"<p>- MsgCount: ${config.msgsPerPartition}%,.0f " +
        f"<p>- MsgSize: ${config.msgSize}%,.0f" +
        s"<p>- TargetBytesPerBrokerMB: ${config.targetBytesPerBrokerMB}<p>"
      append(message)
    }

    def appendChart(path: String, first: Boolean): Unit = {
      val message = new StringBuilder
      if (first)
        message.append("<p><p>")
      message.append("<img src=\"" + path + "\" alt=\"Chart\" style=\"width:600px;height:400px;align=\"middle\"\">")
      if (!first)
        message.append("<p><p>")
      append(message.toString())
    }

    def header(): Unit = {
      append("<html><head><h1>Replication Quotas Test Rig</h1></head><body>")
    }

    def footer(): Unit = {
      append("</body></html>")
    }

    def append(message: String): Unit = {
      val stream = new FileOutputStream(log, true)
      new PrintWriter(stream) {
        append(message)
        close
      }
    }

    def path(): String = {
      log.getAbsolutePath
    }
  }
}

