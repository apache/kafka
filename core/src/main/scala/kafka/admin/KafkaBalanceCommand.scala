package kafka.admin

import joptsimple.OptionParser
import kafka.common. TopicAndPartition
import kafka.utils._
import scala.collection.{Seq, mutable}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Utils
/**
  * a command make the number of replicas and leaders on every broker is balance
  */
object kafkaBalanceCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new kafkaBalanceCommandOptions(args)
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "cluster replica or leader balance")

    val actions = Seq(opts.showClusterState, opts.kafkaRebalance, opts.leaderRebalance).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --show-cluster-state, --replica-rebalance or --leader-balance")

    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.zkConnectOpt)

    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect,
      30000,
      30000,
      JaasUtils.isZkSecurityEnabled())

    var exitCode = 0
    try {
      if(opts.options.has(opts.showClusterState))
        getKafkaBalanceStat(zkUtils)
      else if(opts.options.has(opts.leaderRebalance))
        leaderBalance(zkUtils, opts)
      else if(opts.options.has(opts.kafkaRebalance))
        executeKafkaBalance(zkUtils, opts)
    } catch {
      case e: Throwable =>
        println("Error while executing topic command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      zkUtils.close()
      Exit.exit(exitCode)
    }

  }



  def getTopAndPartitionInfo(zkUtils: ZkUtils): mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]] = {

    val topicSet = zkUtils.getAllPartitions()
    val tpmap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]] = mutable.Map.empty

    zkUtils.getReplicaAssignmentForTopics(topicSet.map(_.topic).toSeq).foreach(
      tpInfo => {
        val tmp: mutable.ListBuffer[Int] = mutable.ListBuffer.empty
        tpInfo._2.foreach(tmp.append(_))
        tpmap.getOrElseUpdate(tpInfo._1, tmp)
      }
    )
    tpmap
  }

  def getKafkaBalanceStat(zkUtils: ZkUtils): Unit = {
    val TPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]= getTopAndPartitionInfo(zkUtils)
    val BrokerMap: mutable.Map[Int, mutable.Set[TopicAndPartition]] = mutable.Map.empty
    val aliveBrokerList=zkUtils.getSortedBrokerList()
    aliveBrokerList.foreach(bid=>{
      BrokerMap.put(bid, mutable.Set.empty[TopicAndPartition])
    })

    TPMap.map{
      tp => {
        tp._2.map{
          bid=>{
            if (BrokerMap.contains(bid)) {
              BrokerMap(bid) += tp._1
            } else {
              // dead broker
            }
          }
        }
      }
    }

    val LeaderMap: mutable.Map[Int, Int] = mutable.Map.empty
    aliveBrokerList.foreach (bid=>{
      LeaderMap.put(bid, 0)
    })

    TPMap.map{
      tp => {
        if (LeaderMap.contains(tp._2.head)) {
          LeaderMap(tp._2.head) += 1
        } else {
          // dead broker
        }
      }
    }

    BrokerMap.foreach{
      brockinfo => {
        System.out.println(" broker["+brockinfo._1+"] has " + brockinfo._2.size + " TopicAndPartitions and " +
          LeaderMap(brockinfo._1) + " leaders.")
      }
    }
  }


  def executeKafkaBalance(zkUtils: ZkUtils, opts: kafkaBalanceCommandOptions): Unit = {
    // 0. get old TPMap
    val orgTPMap = getTopAndPartitionInfo(zkUtils)
    // 1. relplica balance
    val TPMap=getTopAndPartitionInfo(zkUtils)
    executeReplicaBalance(zkUtils, opts, TPMap)

    // 2. leader balance
    executeLeaderBalance(zkUtils, opts, TPMap)

    // 3. assign partition
    executeReassginPartition(zkUtils, TPMap)

    // 4. get balance state
    getBalanceState(zkUtils , orgTPMap, TPMap)
  }

  def getBalanceState(zkUtils: ZkUtils, orgTPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]],
                     newTPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]): Unit = {

    val startTime = System.currentTimeMillis()

    val needMoveTopicPartition = orgTPMap.filterNot(tpinfo => tpinfo._2.equals(newTPMap(tpinfo._1)))

    System.out.println("Kafka Balance has ["+orgTPMap.size+"] topics, need Move ["+needMoveTopicPartition.size+"] topics, now kafka balance start...")
    needMoveTopicPartition.foreach{
      tpinfo => {
        System.out.println(tpinfo._1+" : ["+tpinfo._2.mkString(",")+"] => ["+newTPMap(tpinfo._1).mkString(",")+"]")
      }
    }

    val curFinishTP = mutable.Set.empty[TopicAndPartition]
    var balanceFinish = false
    while(!balanceFinish) {
      var c = 0
      needMoveTopicPartition.map {
        tpinfo => {
          val curAR = zkUtils.getReplicasForPartition(tpinfo._1.topic, tpinfo._1.partition)
          if (curAR.equals(newTPMap(tpinfo._1))) {
            c += 1
            if (!curFinishTP.contains(tpinfo._1)) {
              System.out.println("Moved " + tpinfo._1 + " [" + orgTPMap(tpinfo._1).mkString(",") + "] => [" + curAR.mkString(",") + "]")
            }
            curFinishTP.add(tpinfo._1)
          }
        }
      }
      var processPer=100.toDouble
      if (needMoveTopicPartition.size!=0){
        processPer = c.toDouble/needMoveTopicPartition.size * 100
      }

      (needMoveTopicPartition.keySet -- curFinishTP).map{
        tpInfo => {
          val oldAR = needMoveTopicPartition(tpInfo)
          val newAR = newTPMap(tpInfo)
          System.out.println("Curent Process "+processPer.formatted("%.2f")+"%, Moving "+ tpInfo+" : ["+oldAR.mkString(",")+"] => ["+newAR.mkString(",")+"] ...")
        }
      }

      Thread.sleep(100)

      if (curFinishTP.size.equals(needMoveTopicPartition.size)){
        balanceFinish = true
        val useTime = System.currentTimeMillis() - startTime
        System.out.println("Curent Process "+ processPer + "% , finish "+ needMoveTopicPartition.size + "  balances, total cost "+ useTime + " ms.")

      }
    }

    getKafkaBalanceStat(zkUtils)
  }

  def executeReassginPartition(zkUtils: ZkUtils, TPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]): Unit ={
    val partitionsToBeReassigned = TPMap.map {
      tpInfo=> {
        (tpInfo._1, tpInfo._2.toSeq)
      }
    }.toMap

    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, partitionsToBeReassigned)
    if(reassignPartitionsCommand.reassignPartitions())
      println("Successfully started reassignment of partitions ")
    else
      println("Failed to reassign partitions ")
  }


  def executeReplicaBalance(zkUtils: ZkUtils, opts: kafkaBalanceCommandOptions,
                            TPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]): Unit = {
    // 1. get replica num for per broker
    // 2. sort brokers by replica num desc
    // 3  need get/move : a = (per broker - avg)
    // 4. to move...


    val BrokerMap: mutable.Map[Int, mutable.Set[TopicAndPartition]] = mutable.Map.empty
    val aliveBrokerList=zkUtils.getSortedBrokerList()
    aliveBrokerList.foreach(bid=>{
      BrokerMap.put(bid, mutable.Set.empty[TopicAndPartition])
    })

    TPMap.map{
      tp => {
        tp._2.map{
          bid=>{
            if (BrokerMap.contains(bid)) {
              BrokerMap(bid) += tp._1
            } else {
              // dead broker
            }
          }
        }
      }
    }
    val replicasAvgForBrokers = BrokerMap.map(_._2.size).sum/BrokerMap.size
    //default balance percentage is 10%
    val Percentage={
      if(opts.options.has(opts.showClusterState))
        opts.options.valueOf(opts.rbNumPercentageOpt).toInt
      else 10
    }
    val balanceThreshold =  Percentage * replicasAvgForBrokers/100
    // init List by Desc
    val replicasList = BrokerMap.map(btp => balanceItem (btp._1, btp._2.size - replicasAvgForBrokers)).toList.sortWith(_.rn2mv > _.rn2mv)
    for(i <- 0 until replicasList.size){
      for(j <- (0 until replicasList.size).toList.reverse){
        if ((replicasList(i).rn2mv <= 0 || replicasList(j).rn2mv >= 0)
          || finishBalanceForBroker(replicasList(i).rn2mv, replicasList(j).rn2mv, balanceThreshold)) {
        } else {
          for (tp <- BrokerMap(replicasList(i).brokerid)) {
            if (TPMap(tp).contains(replicasList(j).brokerid) ||
              (replicasList(i).rn2mv <= 0 || replicasList(j).rn2mv >= 0)
              || finishBalanceForBroker(replicasList(i).rn2mv, replicasList(j).rn2mv, balanceThreshold)) {

            } else {
              TPMap(tp) = TPMap(tp).filterNot(_.equals(replicasList(i).brokerid))
              TPMap(tp).append(replicasList(j).brokerid)

              BrokerMap(replicasList(i).brokerid).remove(tp)
              BrokerMap(replicasList(j).brokerid).add(tp)

              replicasList(i).rn2mv -= 1
              replicasList(j).rn2mv += 1
            }
          }

        }
      }
    }
  }

  def finishBalanceForBroker(fromBroker: Int, ToBroker: Int, balanceThreshold: Int): Boolean = {
    if (Math.abs(fromBroker) <= balanceThreshold && Math.abs(ToBroker) <= balanceThreshold) true
    else false
  }

  /**
    * @param brokerid the id of broker
    * @param rn2mv    the num of replicas need to move to make balance
    */
  case class balanceItem(var brokerid: Int, var rn2mv: Int)  {
  }

  def leaderBalance(zkUtils: ZkUtils, opts: kafkaBalanceCommandOptions): Unit ={
    // 0. get old TPMap
    val orgTPMap = getTopAndPartitionInfo(zkUtils)
    val TPMap = getTopAndPartitionInfo(zkUtils)
    // 1. leader balance
    executeLeaderBalance(zkUtils, opts, TPMap)
    // 2. assign partition
    executeReassginPartition(zkUtils, TPMap)
    // 3.reassgin partition state
    getBalanceState(zkUtils,orgTPMap, TPMap)
    // 4.do preferred replica leader election command
    preferredReplicaLeaderElection(zkUtils,TPMap)
  }

  def executeLeaderBalance(zkUtils: ZkUtils, opts: kafkaBalanceCommandOptions,
                           TPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]
                          ): Unit = {
    val BrokerLeaderMap: mutable.Map[Int, mutable.Set[TopicAndPartition]] = mutable.Map.empty

    val aliveBrokerList=zkUtils.getSortedBrokerList()
    aliveBrokerList.foreach(bid=>{
      BrokerLeaderMap.put(bid, mutable.Set.empty[TopicAndPartition])
    })
    TPMap.map{
      tp => {BrokerLeaderMap(tp._2.head) += tp._1}
    }

    val leadersAvgForBrokers = BrokerLeaderMap.map(_._2.size).sum/BrokerLeaderMap.size
    val leadersList = BrokerLeaderMap.map(btp => balanceItem (btp._1, btp._2.size - leadersAvgForBrokers)).toList.sortWith(_.rn2mv > _.rn2mv)

    for(i <- 0 until leadersList.size){
      for(j <- (0 until leadersList.size).toList.reverse){
        if ((leadersList(i).rn2mv <= 0 || leadersList(j).rn2mv >= 0)
          || finishBalanceForBroker(leadersList(i).rn2mv, leadersList(j).rn2mv, 0)) {
          // continue
        } else {
          for (tp <- BrokerLeaderMap(leadersList(i).brokerid)) {
            if (!TPMap(tp).contains(leadersList(j).brokerid) ||
              (leadersList(i).rn2mv <= 0 || leadersList(j).rn2mv >= 0)
              || finishBalanceForBroker(leadersList(i).rn2mv, leadersList(j).rn2mv, 0)) {
              // continue
            } else {
              TPMap(tp) -= leadersList(j).brokerid
              TPMap(tp).insert(0, leadersList(j).brokerid)

              BrokerLeaderMap(leadersList(i).brokerid).remove(tp)
              BrokerLeaderMap(leadersList(j).brokerid).add(tp)

              leadersList(i).rn2mv -= 1
              leadersList(j).rn2mv += 1
            }
          }
        }
      }
    }
  }

  def preferredReplicaLeaderElection(zkUtils: ZkUtils,TPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]): Unit ={
    val tp=TPMap.keySet
    val preferredLeaderElection=new PreferredReplicaLeaderElectionCommand(zkUtils,tp)
    preferredLeaderElection.moveLeaderToPreferredReplica()
  }


  class kafkaBalanceCommandOptions(args: Array[String]) {
    val parser = new OptionParser


    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
      "form host:port. Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])

    val showClusterState = parser.accepts("show-cluster-state", "show how many replicas and leaders on each broker")
      .withRequiredArg
      .describedAs("command")
      .ofType(classOf[String])

    val  kafkaRebalance= parser.accepts("replica-rebalance", "which make the replica num and leader num on each broker balanced")
      .withRequiredArg
      .describedAs("command")
      .ofType(classOf[String])

    val rbNumPercentageOpt = parser.accepts("replica-balance-num-percentage", "replica-balance-num-percentage......")
      .withRequiredArg
      .describedAs("integer")
      .ofType(classOf[String])

    val leaderRebalance= parser.accepts("leader-balance", "which make the leader num on each broker balanced")
      .withRequiredArg
      .describedAs("command")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)
  }

}

