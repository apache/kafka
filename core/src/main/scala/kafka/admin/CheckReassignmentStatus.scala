
package kafka.admin

import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import kafka.utils._
import scala.collection.Map

object CheckReassignmentStatus extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val jsonFileOpt = parser.accepts("path to json file", "REQUIRED: The JSON file with the list of partitions and the " +
      "new replicas they should be reassigned to")
      .withRequiredArg
      .describedAs("partition reassignment json file path")
      .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
      "form host:port. Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

    for(arg <- List(jsonFileOpt, zkConnectOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val jsonFile = options.valueOf(jsonFileOpt)
    val zkConnect = options.valueOf(zkConnectOpt)
    val jsonString = Utils.readFileIntoString(jsonFile)
    val zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)

    try {
      // read the json file into a string
      val partitionsToBeReassigned = SyncJSON.parseFull(jsonString) match {
        case Some(reassignedPartitions) =>
          val partitions = reassignedPartitions.asInstanceOf[Array[Map[String, String]]]
          partitions.map { m =>
            val topic = m.asInstanceOf[Map[String, String]].get("topic").get
            val partition = m.asInstanceOf[Map[String, String]].get("partition").get.toInt
            val replicasList = m.asInstanceOf[Map[String, String]].get("replicas").get
            val newReplicas = replicasList.split(",").map(_.toInt)
            ((topic, partition), newReplicas.toSeq)
          }.toMap
        case None => Map.empty[(String, Int), Seq[Int]]
      }

      val reassignedPartitionsStatus = checkIfReassignmentSucceeded(zkClient, partitionsToBeReassigned)
      reassignedPartitionsStatus.foreach { partition =>
        partition._2 match {
          case ReassignmentCompleted =>
            println("Partition [%s,%d] reassignment to %s completed successfully".format(partition._1, partition._2,
            partitionsToBeReassigned((partition._1._1, partition._1._2))))
          case ReassignmentFailed =>
            println("Partition [%s,%d] reassignment to %s failed".format(partition._1, partition._2,
            partitionsToBeReassigned((partition._1._1, partition._1._2))))
          case ReassignmentInProgress =>
            println("Partition [%s,%d] reassignment to %s in progress".format(partition._1, partition._2,
            partitionsToBeReassigned((partition._1._1, partition._1._2))))
        }
      }
    }
  }

  def checkIfReassignmentSucceeded(zkClient: ZkClient, partitionsToBeReassigned: Map[(String, Int), Seq[Int]])
  :Map[(String, Int), ReassignmentStatus] = {
    val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas)
    // for all partitions whose replica reassignment is complete, check the status
    partitionsToBeReassigned.map { topicAndPartition =>
      (topicAndPartition._1, checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition._1._1, topicAndPartition._1._2,
        topicAndPartition._2, partitionsToBeReassigned, partitionsBeingReassigned))
    }
  }

  def checkIfPartitionReassignmentSucceeded(zkClient: ZkClient, topic: String, partition: Int,
                                            reassignedReplicas: Seq[Int],
                                            partitionsToBeReassigned: Map[(String, Int), Seq[Int]],
                                            partitionsBeingReassigned: Map[(String, Int), Seq[Int]]): ReassignmentStatus = {
    val newReplicas = partitionsToBeReassigned((topic, partition))
    partitionsBeingReassigned.get((topic, partition)) match {
      case Some(partition) => ReassignmentInProgress
      case None =>
        // check if AR == RAR
        val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topic, partition)
        if(assignedReplicas == newReplicas)
          ReassignmentCompleted
        else
          ReassignmentFailed
    }
  }
}