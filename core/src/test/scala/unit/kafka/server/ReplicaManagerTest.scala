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

package kafka.server

import org.scalatest.junit.JUnit3Suite
import org.junit.Test
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import java.util.concurrent.atomic.AtomicBoolean
import java.io.File
import org.easymock.EasyMock
import org.I0Itec.zkclient.ZkClient
import kafka.cluster.Replica
import kafka.log.{LogManager, LogConfig, Log}

class ReplicaManagerTest extends JUnit3Suite {
  @Test
  def testHighwaterMarkDirectoryMapping() {
    val props = TestUtils.createBrokerConfig(1)
    val dir = "/tmp/kafka-logs/"
    new File(dir).mkdir()
    props.setProperty("log.dirs", dir)
    val config = new KafkaConfig(props)
    val zkClient = EasyMock.createMock(classOf[ZkClient])
    val mockLogMgr = EasyMock.createMock(classOf[LogManager])
    val time: MockTime = new MockTime()
    val rm = new ReplicaManager(config, time, zkClient, new MockScheduler(time), mockLogMgr, new AtomicBoolean(false))
    val partition = rm.getOrCreatePartition("test-topic", 1, 1)
    partition.addReplicaIfNotExists(new Replica(1, partition, time, 0L, Option(new Log(new File("/tmp/kafka-logs/test-topic-1"), new LogConfig(), 0L, null))))
    rm.checkpointHighWatermarks()
  }
}
