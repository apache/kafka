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

package kafka.admin

import joptsimple.OptionParser
import kafka.common.TopicAndPartition
import kafka.utils._
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Utils
import scala.collection.{Seq, mutable}

/**
  * a command make the number of replicas and leaders on every broker is balance
  */
object ClusterBalanceCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new kafkaBalanceCommandOptions(args)
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "cluster replica or leader balance")

    val actions = Seq(opts.showClusterState, opts.replicaRebalance, opts.leaderRebalance,opts.clusterRebalance).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --show-cluster-state, --replica-balance , --leader-balance or --cluster-balance")

    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.zkConnectOpt)

    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect,
      30000,
      30000,
      JaasUtils.isZkSecurityEnabled())

    var exitCode = 0
    try {
      if(opts.options.has(opts.showClusterState))
        getClusterBalanceStat(zkUtils)
      else if(opts.options.has(opts.leaderRebalance))
        leaderBalance(zkUtils, opts)
      else if(opts.options.has(opts.replicaRebalance))
        ReplicaBalance(zkUtils, opts)
      else if(opts.options.has(opts.clusterRebalance))
        ClusterBalance(zkUtils, opts)
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

  def getClusterBalanceStat(zkUtils: ZkUtils): Unit = {
    val TPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]= getTopAndPartitionInfo(zkUtils)
    val BrokerMap: mutable.Map[Int, mutable.Set[TopicAndPartition]] = mutable.Map.empty
    val aliveBrokerList=zkUtils.getSortedBrokerList()
    aliveBrokerList.foreach(bid=>{
      BrokerMap.put(bid, mutable.Set.empty[TopicAndPartition])
    })

    TPMap.foreach{
      tp => {
        tp._2.foreach{
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

    TPMap.foreach{
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
        System.out.println(" broker[" + brockinfo._1 + "] has " + brockinfo._2.size + " TopicAndPartitions and " +
          LeaderMap(brockinfo._1) + " leaders.")
      }
    }
  }

  def ClusterBalance(zkUtils: ZkUtils, opts: kafkaBalanceCommandOptions): Unit = {
    // 0. get old TPMap
    val orgTPMap = getTopAndPartitionInfo(zkUtils)

    // 1. replica balance
    val TPMap=getTopAndPartitionInfo(zkUtils)
    executeReplicaBalance(zkUtils, opts, TPMap)

    // 2. leader balance
    executeLeaderBalance(zkUtils, opts,TPMap)

    // 3. assign partition
    executeReassginPartition(zkUtils, TPMap,orgTPMap)

    //4. get balance state
    getBalanceState(zkUtils ,TPMap,orgTPMap)

    //5.do preferred replica leader election command
    preferredReplicaLeaderElection(zkUtils,TPMap,orgTPMap)
  }

  def ReplicaBalance(zkUtils: ZkUtils, opts: kafkaBalanceCommandOptions): Unit = {
    // 0. get old TPMap
    val orgTPMap = getTopAndPartitionInfo(zkUtils)

    // 1. replica balance
    val TPMap=getTopAndPartitionInfo(zkUtils)
    executeReplicaBalance(zkUtils, opts, TPMap)

    // 2. assign partition
    executeReassginPartition(zkUtils, TPMap,orgTPMap)

    //3. get balance state
    getBalanceState(zkUtils ,TPMap,orgTPMap)

  }

  def getBalanceState(zkUtils: ZkUtils, newTPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]],
                      orgTPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]): Unit = {

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
      needMoveTopicPartition.foreach {
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
      if (needMoveTopicPartition.nonEmpty){
        processPer = c.toDouble/needMoveTopicPartition.size * 100
      }

      (needMoveTopicPartition.keySet -- curFinishTP).foreach{
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

    getClusterBalanceStat(zkUtils)
  }


  /**
    * use ReassignPartitionsCommand to make balance
    * with the new distribution of replicas we got
    */
  def executeReassginPartition(zkUtils: ZkUtils, TPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]],orgTPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]): Unit ={
    val partitionsToBeReassigned = TPMap.filterNot(tpInfo=>orgTPMap(tpInfo._1).equals(tpInfo._2)).map {
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


  /**
    * get the balanced assignment of replicas on every brokers
    */
  def executeReplicaBalance(zkUtils: ZkUtils, opts: kafkaBalanceCommandOptions,
                            TPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]): Unit = {
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
            }
          }
        }
      }
    }
    val replicasAvgForBrokers = BrokerMap.map(_._2.size).sum/BrokerMap.size
    //default balance percentage is 10%
    val Percentage={
      if(opts.options.has(opts.showClusterState))
        opts.options.valueOf(opts.rbPercentageOpt).toInt
      else 10
    }
    val balanceThreshold =  Percentage * replicasAvgForBrokers/100
    // init List by Desc
    val (fromBrokers,targetBrokers) = BrokerMap.map(btp => balanceItem (btp._1, btp._2.size - replicasAvgForBrokers)).toList.partition(_.rn2mv > 0)

    fromBrokers.foreach(
      fromBroker=>targetBrokers.filterNot(_.rn2mv >= 0).foreach(
        targetBroker=>{
          if (isFinishMove(fromBroker.rn2mv, targetBroker.rn2mv, balanceThreshold))
            BrokerMap(fromBroker.brokerid).filterNot(tp=>TPMap(tp).contains(targetBroker.brokerid))
              .foreach(
                tp=> if (isFinishMove(fromBroker.rn2mv, targetBroker.rn2mv, balanceThreshold))
                {
                  TPMap(tp) = TPMap(tp).filterNot(_.equals(fromBroker.brokerid))
                  TPMap(tp).append(targetBroker.brokerid)
                  BrokerMap(fromBroker.brokerid).remove(tp)
                  BrokerMap(targetBroker.brokerid).add(tp)
                  fromBroker.rn2mv -= 1
                  targetBroker.rn2mv += 1
                }
              )
        }
      )
    )
  }

  def isFinishMove(fromBroker: Int, TargetBroker: Int, balanceThreshold: Int): Boolean = {
    if ((Math.abs(fromBroker) > balanceThreshold || Math.abs(TargetBroker) > balanceThreshold)
      && (fromBroker > 0) && (TargetBroker < 0) )
      true
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
    executeReassginPartition(zkUtils, TPMap,orgTPMap)
    // 3.reassgin partition state
    getBalanceState(zkUtils,TPMap,orgTPMap)
    // 4.do preferred replica leader election command
    preferredReplicaLeaderElection(zkUtils,TPMap,orgTPMap)
  }

  /**
    * make the leader numbers on every broker balanced
    * during doing it we should make sure the target broker
    * should in AR of the TP we want to move.
    */
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
    val (fromBrokers,targetBrokers) = BrokerLeaderMap.map(btp => balanceItem (btp._1, btp._2.size - leadersAvgForBrokers)).toList.partition(_.rn2mv > 0)

    fromBrokers.foreach(
      fromBroker=>targetBrokers.filterNot(_.rn2mv >= 0).foreach(
        targetBroker=>{
          if (isFinishMove(fromBroker.rn2mv,targetBroker.rn2mv,0))
            BrokerLeaderMap(fromBroker.brokerid).filter(tp=>TPMap(tp).contains(targetBroker.brokerid))
              .foreach(
                tp=> if (isFinishMove(fromBroker.rn2mv,targetBroker.rn2mv,0))
                {
                  TPMap(tp) -= targetBroker.brokerid
                  TPMap(tp).insert(0, targetBroker.brokerid)
                  BrokerLeaderMap(fromBroker.brokerid).remove(tp)
                  BrokerLeaderMap(targetBroker.brokerid).add(tp)
                  fromBroker.rn2mv -= 1
                  targetBroker.rn2mv += 1
                }
              )
        }
      )
    )

  }

  /**
    * do PreferredReplicaLeaderElectionCommand with TP which we want leader balanced
    */
  def preferredReplicaLeaderElection(zkUtils: ZkUtils,TPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]],orgTPMap: mutable.Map[TopicAndPartition, mutable.ListBuffer[Int]]): Unit ={
    val tp=TPMap.filterNot(TP=>orgTPMap(TP._1).equals(TP._2))keySet
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


    val  replicaRebalance= parser.accepts("replica-balance", "which make the amounts of replicas on each broker balanced")


    val rbPercentageOpt = parser.accepts("replica-balance-threshold-percentage", "replica balance threshold percentage,default is 10 (means 10%)")
      .withRequiredArg
      .describedAs("integer")
      .ofType(classOf[String])

    val leaderRebalance= parser.accepts("leader-balance", "which make the amounts of leaders on each broker balanced")

    val clusterRebalance= parser.accepts("cluster-balance", "which execute replica balance and leader balance ")

    val options = parser.parse(args : _*)
  }

}




