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

package kafka.coordinator

import kafka.common.Topic
import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.utils.{MockTime, TestUtils, ZkUtils}
import org.easymock.EasyMock
import org.junit.Test
import org.junit.Assert._

import scala.collection.{Map, Seq, mutable}

/**
  * Created by kimwo on 05-12-2016.
  */
class GroupCoordinatorTest {

  @Test
  def testOffsetsRetentionMsIntegerOverflow() {
    var props = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    props.setProperty(KafkaConfig.OffsetsRetentionMinutesProp, Integer.MAX_VALUE.toString)
    val config = KafkaConfig.fromProps(props)
    val replicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])
    var time = new MockTime

    val ret = mutable.Map[String, Map[Int, Seq[Int]]]()
    ret += (Topic.GroupMetadataTopicName -> Map(0 -> Seq(1), 1 -> Seq(1)))
    val zkUtils = EasyMock.createNiceMock(classOf[ZkUtils])
    EasyMock.expect(zkUtils.getPartitionAssignmentForTopics(Seq(Topic.GroupMetadataTopicName))).andReturn(ret)
    EasyMock.replay(zkUtils)

    val t = GroupCoordinator(config, zkUtils, replicaManager, time)

    assertEquals(t.offsetConfig.offsetsRetentionMs, Integer.MAX_VALUE * 60L * 1000L)
  }

}
