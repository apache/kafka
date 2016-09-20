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

import org.junit.Assert._
import org.junit.Test
import org.apache.kafka.common.TopicPartition

class BrokerBalanceCheckTest {
  val t1 = "topic1"
  val t1p0 = new TopicPartition(t1, 0)
  val t1p1 = new TopicPartition(t1, 1)
  val t1p2 = new TopicPartition(t1, 2)
  val t1p3 = new TopicPartition(t1, 3)


  /**
 	* Config: three brokers on rack1 and one broker on rack 2
 	* Content: one topic with 4 partitions and 2 replicas
 	* Assignment: there are 8 replicas total, therefore each rack can get 4 replicas: 4 replicas for
 	* brokers 0, 1, 2, and 4 replicas for broker 3
 	* Balance: there is a more balanced config
 	*/
  @Test
  def testRackAwareBrokerBalanceWithUnbalancedConfig() {
    val brokerMetadatas = Seq(
        new BrokerMetadata(0, Some("rack1")),
        new BrokerMetadata(1, Some("rack1")),
        new BrokerMetadata(2, Some("rack1")),
        new BrokerMetadata(3, Some("rack2"))
    )
    val assignment = Map(
        t1p0 -> Seq(0, 3),
        t1p1 -> Seq(1, 3),
        t1p2 -> Seq(2, 3),
        t1p3 -> Seq(0, 3)
    )
    val brokerBalanceCheck = new BrokerBalanceCheck(brokerMetadatas, assignment)
    assertTrue(brokerBalanceCheck.report().nonEmpty)
  }

  /**
 	* Config: two brokers on rack1 and two brokers on rack 2
 	* Content: one topic with 4 partitions and 2 replicas
 	* Assignment: there are 8 replicas total, therefore each rack gets 4 replicas: 4 replicas for
 	* brokers 0, 1, and 4 replicas for brokers 2, 3
 	* Balance: the config is already balanced
 	*/
  @Test
  def testRackAwareBrokerBalanceWithBalancedConfig() {
    val brokerMetadatas = Seq(
        new BrokerMetadata(0, Some("rack1")),
        new BrokerMetadata(1, Some("rack1")),
        new BrokerMetadata(2, Some("rack2")),
        new BrokerMetadata(3, Some("rack2"))
    )
    val assignment = Map(
        t1p0 -> Seq(0, 2),
        t1p1 -> Seq(1, 3),
        t1p2 -> Seq(0, 2),
        t1p3 -> Seq(1, 3)
    )
    val brokerBalanceCheck = new BrokerBalanceCheck(brokerMetadatas, assignment)
    assertTrue(brokerBalanceCheck.report().isEmpty)
  }

  /**
 	* Config: two brokers on rack1 and one broker on rack 2
 	* Content: one topic with 4 partitions and 2 replicas
 	* Assignment: there are 8 replicas total, therefore each rack can get 4 replicas: 4 replicas for
 	* brokers 0, 1, and 4 replicas for broker 2
 	* Balance: there is no config that is more balanced
 	*/
  @Test
  def testRackAwareBrokerBalanceWithAlmostBalancedConfig() {
    val brokerMetadatas = Seq(
        new BrokerMetadata(0, Some("rack1")),
        new BrokerMetadata(1, Some("rack1")),
        new BrokerMetadata(2, Some("rack2"))
    )
    val assignment = Map(
        t1p0 -> Seq(0, 2),
        t1p1 -> Seq(1, 2),
        t1p2 -> Seq(0, 2),
        t1p3 -> Seq(1, 2)
    )
    val brokerBalanceCheck = new BrokerBalanceCheck(brokerMetadatas, assignment)
    assertTrue(brokerBalanceCheck.report().isEmpty)
  }
}
