
package kafka.tools


import joptsimple._
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ZkUtils, ZKStringSerializer}
import org.apache.log4j.Logger
import kafka.consumer.SimpleConsumer
import collection.mutable.Map
object ConsumerOffsetChecker {
  private val logger = Logger.getLogger(getClass)

  private val consumerMap: Map[String, Option[SimpleConsumer]] = Map()

  private val BidPidPattern = """(\d+)-(\d+)""".r

  private val BrokerIpPattern = """.*:(\d+\.\d+\.\d+\.\d+):(\d+$)""".r
  // e.g., 127.0.0.1-1315436360737:127.0.0.1:9092

  private def getConsumer(zkClient: ZkClient, bid: String): Option[SimpleConsumer] = {
    val brokerInfo = ZkUtils.readDataMaybeNull(zkClient, "/brokers/ids/%s".format(bid))
    val consumer = brokerInfo match {
      case BrokerIpPattern(ip, port) =>
        Some(new SimpleConsumer(ip, port.toInt, 10000, 100000))
      case _ =>
        logger.error("Could not parse broker info %s".format(brokerInfo))
        None
    }
    consumer
  }

  private def processPartition(zkClient: ZkClient,
                               group: String, topic: String, bidPid: String) {
    val offset = ZkUtils.readData(zkClient, "/consumers/%s/offsets/%s/%s".
            format(group, topic, bidPid)).toLong
    val owner = ZkUtils.readDataMaybeNull(zkClient, "/consumers/%s/owners/%s/%s".
            format(group, topic, bidPid))
    println("%s,%s,%s (Group,Topic,BrokerId-PartitionId)".format(group, topic, bidPid))
    println("%20s%s".format("Owner = ", owner))
    println("%20s%d".format("Consumer offset = ", offset))
    println("%20s%,d (%,.2fG)".format("= ", offset, offset / math.pow(1024, 3)))

    bidPid match {
      case BidPidPattern(bid, pid) =>
        val consumerOpt = consumerMap.getOrElseUpdate(
          bid, getConsumer(zkClient, bid))
        consumerOpt match {
          case Some(consumer) =>
            val logSize =
              consumer.getOffsetsBefore(topic, pid.toInt, -1, 1).last.toLong
            println("%20s%d".format("Log size = ", logSize))
            println("%20s%,d (%,.2fG)".format("= ", logSize, logSize / math.pow(1024, 3)))

            val lag = logSize - offset
            println("%20s%d".format("Consumer lag = ", lag))
            println("%20s%,d (%,.2fG)".format("= ", lag, lag / math.pow(1024, 3)))
            println()
          case None => // ignore
        }
      case _ =>
        logger.error("Could not parse broker/partition pair %s".format(bidPid))
    }
  }

  private def processTopic(zkClient: ZkClient, group: String, topic: String) {
    val bidsPids = ZkUtils.getChildrenParentMayNotExist(
      zkClient, "/consumers/%s/offsets/%s".format(group, topic)).toList
    bidsPids.foreach {
      bidPid => processPartition(zkClient, group, topic, bidPid)
    }
  }

  private def printBrokerInfo() {
    println("BROKER INFO")
    for ((bid, consumerOpt) <- consumerMap)
      consumerOpt match {
        case Some(consumer) =>
          println("%s -> %s:%d".format(bid, consumer.host, consumer.port))
        case None => // ignore
      }
  }

  def main(args: Array[String]) {
    val parser = new OptionParser()

    val zkConnectOpt = parser.accepts("zkconnect", "ZooKeeper connect string.").
            withRequiredArg().defaultsTo("localhost:2181").ofType(classOf[String]);
    val topicsOpt = parser.accepts("topic",
            "Comma-separated list of consumer topics (all topics if absent).").
            withRequiredArg().ofType(classOf[String])
    val groupOpt = parser.accepts("group", "Consumer group.").
            withRequiredArg().ofType(classOf[String])
    parser.accepts("help", "Print this message.")

    val options = parser.parse(args : _*)

    if (options.has("help")) {
       parser.printHelpOn(System.out)
       System.exit(0)
    }

    for (opt <- List(groupOpt))
      if (!options.has(opt)) {
        System.err.println("Missing required argument: %s".format(opt))
        parser.printHelpOn(System.err)
        System.exit(1)
      }

    val zkConnect = options.valueOf(zkConnectOpt)
    val group = options.valueOf(groupOpt)
    val topics = if (options.has(topicsOpt)) Some(options.valueOf(topicsOpt))
      else None


    var zkClient: ZkClient = null
    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)

      val topicList = topics match {
        case Some(x) => x.split(",").view.toList
        case None => ZkUtils.getChildren(
          zkClient, "/consumers/%s/offsets".format(group)).toList
      }

      logger.debug("zkConnect = %s; topics = %s; group = %s".format(
        zkConnect, topicList.toString(), group))

      topicList.foreach {
        topic => processTopic(zkClient, group, topic)
      }

      printBrokerInfo()
    }
    finally {
      for (consumerOpt <- consumerMap.values) {
        consumerOpt match {
          case Some(consumer) => consumer.close()
          case None => // ignore
        }
      }
      if (zkClient != null)
        zkClient.close()
    }
  }
}

