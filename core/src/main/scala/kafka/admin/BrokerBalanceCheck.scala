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
package kafka.admin

import org.apache.kafka.common.TopicPartition

import scala.collection.Map

class BrokerBalanceCheck(val brokerMetadatas: Seq[BrokerMetadata],
                         val assignment: Map[TopicPartition, Seq[Int]]) {

  val brokerCount = brokerMetadatas.size

  // map of broker id -> rack id
  val brokerToRackMap = brokerMetadatas.collect {
    case BrokerMetadata(id, Some(rack)) => id -> rack
  }.toMap

  // sorted broker ids
  val brokers = brokerToRackMap.keys.toList.sortWith(_ < _)

  // map of rack id -> array of broker ids
  val rackToBrokersMap = brokerToRackMap groupBy{_._2} map {
    case (key, value) => (key, value.unzip._1.toArray)
  }

  var minBrokersPerRack = Int.MaxValue
  var maxBrokersPerRack = Int.MinValue
  rackToBrokersMap.values.foreach { s =>
    if (s.length < minBrokersPerRack)
      minBrokersPerRack = s.length
    else if (s.length > maxBrokersPerRack)
      maxBrokersPerRack = s.length
  }

  var minBrokerReplicaCount = Int.MaxValue
  var maxBrokerReplicaCount = Int.MinValue
  var replicaCountToBrokersMap: Map[Int, Array[Int]] = Map()
  var brokerToReplicaCountMap: Map[Int, Int] = Map()
  var rackToReplicaCountMap: Map[String, Int] = Map()

  // there is a better broker balance across racks only if one broker can be moved to another rack to provide a better balance
  if (maxBrokersPerRack > minBrokersPerRack + 1) {
    // map of broker id -> number of topic partition replicas in the broker
    brokerToReplicaCountMap = assignment.values.toSet.flatten.map {
      v => (v, assignment.keys.filter(assignment(_).contains(v)).size)
    }.toMap

    if (brokerToReplicaCountMap.size < brokerCount) {
      brokers.toSet.diff(brokerToReplicaCountMap.keySet).foreach { b =>
        brokerToReplicaCountMap = brokerToReplicaCountMap + (b -> 0)}
    }

    // map of rack id -> number of topic partition replicas in the rack
    rackToReplicaCountMap = rackToBrokersMap map {
      case (rack, brokers) => (rack, brokerToReplicaCountMap.filterKeys(brokers.contains).foldLeft(0)(_ + _._2))
    }

    // map of replica count -> brokers with that many replicas
    replicaCountToBrokersMap = brokerToReplicaCountMap groupBy{_._2} map {
      case (key, value) => (key, value.unzip._1.toArray)
    }

    // find minimum and maximum replica count in brokers
    var replicaCountMsg = ""
    brokers.foreach { b =>
      val count = brokerToReplicaCountMap.get(b).get
      replicaCountMsg = replicaCountMsg + "\t" + b + " -> " + count + "\n"
      if (count < minBrokerReplicaCount)
        minBrokerReplicaCount = count
      else if (count > maxBrokerReplicaCount)
        maxBrokerReplicaCount = count
    }
  }

  def report(): Option[String] = {
    if (maxBrokerReplicaCount <= minBrokerReplicaCount + 1)
      return None

    val msgBuilder = StringBuilder.newBuilder
    msgBuilder.append("\n\nWarning: In the proposed assignment the most loaded broker (")
    msgBuilder.append(replicaCountToBrokersMap.get(maxBrokerReplicaCount).get.toList.sortWith(_ < _).mkString(", "))
    msgBuilder.append(") has ")

    msgBuilder.append(minBrokerReplicaCount match {
      case 0 => "many more replicas than "
      case _ => f"${maxBrokerReplicaCount / (1.0 * minBrokerReplicaCount)}%.1f" + "x as many replicas as "
    })

    msgBuilder.append("the least loaded broker (")
    msgBuilder.append(replicaCountToBrokersMap.get(minBrokerReplicaCount).get.toList.sortWith(_ < _).mkString(", "))
    msgBuilder.append("). This is likely due to an uneven distribution of brokers across racks. You are advised to alter ")
    msgBuilder.append("the rack configuration so there are approximately the same number of brokers per rack.\n\nStats ")
    msgBuilder.append("for generated assignment:\n\n- Number of brokers per rack:\n")

    rackToBrokersMap.toSeq.sortBy(_._1).foreach(t => msgBuilder.append("\t" + t._1 + " -> " + t._2.length + " (" + t._2.sortWith(_ < _).mkString(", ") + ")" + "\n"))
    msgBuilder.append("\n- Number of replicas per broker:\n")

    brokers.foreach { b =>
      msgBuilder.append("\t" + b + " -> " + brokerToReplicaCountMap.get(b).get + "\n")
    }

    msgBuilder.append("\n- Number of replicas per rack:\n")
    rackToReplicaCountMap.toSeq.sortBy(_._1).foreach(t => msgBuilder.append("\t" + t._1 + " -> " + t._2 + "\n"))
    Some(msgBuilder.toString())
  }
}
