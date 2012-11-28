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

package kafka.zk

import junit.framework.Assert._
import java.util.Collections
import kafka.consumer.{ConsumerConfig, ZookeeperConsumerConnector}
import java.lang.Thread
import org.scalatest.junit.JUnit3Suite
import kafka.utils.{TestUtils, ZkUtils, ZKGroupTopicDirs, TestZKUtils}

class ZKLoadBalanceTest extends JUnit3Suite with ZooKeeperTestHarness {
  val zkConnect = TestZKUtils.zookeeperConnect
  var dirs : ZKGroupTopicDirs = null
  val topic = "topic1"
  val group = "group1"
  val firstConsumer = "consumer1"
  val secondConsumer = "consumer2"

  override def setUp() {
    super.setUp()

    dirs = new ZKGroupTopicDirs(group, topic)
  }

  def testLoadBalance() {
    // create the first partition
    ZkUtils.setupPartition(zkClient, 400, "broker1", 1111, "topic1", 1)
    // add the first consumer
    val consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, firstConsumer))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, false)
    zkConsumerConnector1.createMessageStreams(Map(topic -> 1))

    {
      // check Partition Owner Registry
      val actual_1 = getZKChildrenValues(dirs.consumerOwnerDir)
      val expected_1 = List( ("400-0", "group1_consumer1-0") )
      checkSetEqual(actual_1, expected_1)
    }

    // add a second consumer
    val consumerConfig2 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, secondConsumer))
    val ZKConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, false)
    ZKConsumerConnector2.createMessageStreams(Map(topic -> 1))
    // wait a bit to make sure rebalancing logic is triggered
    Thread.sleep(200)

    {
      // check Partition Owner Registry
      val actual_2 = getZKChildrenValues(dirs.consumerOwnerDir)
      val expected_2 = List( ("400-0", "group1_consumer1-0") )
      checkSetEqual(actual_2, expected_2)
    }

    {
      // add a few more partitions
      val brokers = List(
        (200, "broker2", 1111, "topic1", 2),
        (300, "broker3", 1111, "topic1", 2) )

      for ((brokerID, host, port, topic, nParts) <- brokers)
        ZkUtils.setupPartition(zkClient, brokerID, host, port, topic, nParts)


      // wait a bit to make sure rebalancing logic is triggered
      Thread.sleep(1000)
      // check Partition Owner Registry
      val actual_3 = getZKChildrenValues(dirs.consumerOwnerDir)
      val expected_3 = List( ("200-0", "group1_consumer1-0"),
                             ("200-1", "group1_consumer1-0"),
                             ("300-0", "group1_consumer1-0"),
                             ("300-1", "group1_consumer2-0"),
                             ("400-0", "group1_consumer2-0") )
      checkSetEqual(actual_3, expected_3)
    }

    {
      // now delete a partition
      ZkUtils.deletePartition(zkClient, 400, "topic1")

      // wait a bit to make sure rebalancing logic is triggered
      Thread.sleep(500)
      // check Partition Owner Registry
      val actual_4 = getZKChildrenValues(dirs.consumerOwnerDir)
      val expected_4 = List( ("200-0", "group1_consumer1-0"),
                             ("200-1", "group1_consumer1-0"),
                             ("300-0", "group1_consumer2-0"),
                             ("300-1", "group1_consumer2-0") )
      checkSetEqual(actual_4, expected_4)
    }

    zkConsumerConnector1.shutdown
    ZKConsumerConnector2.shutdown
  }

  private def getZKChildrenValues(path : String) : Seq[Tuple2[String,String]] = {
    import scala.collection.JavaConversions
    val children = zkClient.getChildren(path)
    Collections.sort(children)
    val childrenAsSeq : Seq[java.lang.String] = JavaConversions.asBuffer(children)
    childrenAsSeq.map(partition =>
      (partition, zkClient.readData(path + "/" + partition).asInstanceOf[String]))
  }

  private def checkSetEqual(actual : Seq[Tuple2[String,String]], expected : Seq[Tuple2[String,String]]) {
    assertEquals(expected.length, actual.length)
    for (i <- 0 until expected.length) {
      assertEquals(expected(i)._1, actual(i)._1)
      assertEquals(expected(i)._2, actual(i)._2)
    }
  }
}
